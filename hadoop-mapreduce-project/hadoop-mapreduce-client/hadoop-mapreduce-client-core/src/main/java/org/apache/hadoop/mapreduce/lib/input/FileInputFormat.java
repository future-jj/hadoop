/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.lib.input;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.LocatedFileStatusFetcher;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.FileUtil.maybeIgnoreMissingDirectory;

/**
 * FileInputFormat 是Hadoop中所有基于文件的输入格式（TextInputFormat
 * SequenceFileInputFormat）的抽象基类
 * 专注于处理文件系统（HDFS，本地文件系统）的输入数据
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class FileInputFormat<K, V> extends InputFormat<K, V> {

  // 用于作业配置，通过JobConf 或 Configuration设置。输入文件路径
  public static final String INPUT_DIR = "mapreduce.input.fileinputformat.inputdir";
  // 分片的最大字节数
  public static final String SPLIT_MAXSIZE = "mapreduce.input.fileinputformat.split.maxsize";
  // 分片的最小字节数
  public static final String SPLIT_MINSIZE = "mapreduce.input.fileinputformat.split.minsize";
  // 自定义路径过滤器类（过滤隐藏文件）
  public static final String PATHFILTER_CLASS = "mapreduce.input.pathFilter.class";
  // 输入文件总数（统计用）
  public static final String NUM_INPUT_FILES = "mapreduce.input.fileinputformat.numinputfiles";
  // 是否递归读取子目录
  public static final String INPUT_DIR_RECURSIVE = "mapreduce.input.fileinputformat.input.dir.recursive";
  public static final String INPUT_DIR_NONRECURSIVE_IGNORE_SUBDIRS = "mapreduce.input.fileinputformat.input.dir.nonrecursive.ignore.subdirs";
  // 并发列出文件状态的线程数
  public static final String LIST_STATUS_NUM_THREADS = "mapreduce.input.fileinputformat.list-status.num-threads";
  // 控制并发列出输入目录文件的线程数，提升大目录的扫描效率
  public static final int DEFAULT_LIST_STATUS_NUM_THREADS = 1;

  private static final Logger LOG = LoggerFactory.getLogger(FileInputFormat.class);

  // 在分片时允许 10% 的大小冗余，避免生成过多小分片。
  private static final double SPLIT_SLOP = 1.1; // 10% slop

  @Deprecated
  public enum Counter {
    BYTES_READ
  }

  // 默认过滤以_或.开头的隐藏文件
  private static final PathFilter hiddenFileFilter = p -> {
    String name = p.getName();
    return !name.startsWith("_") && !name.startsWith(".");
  };

  /**
   * 组合多个路径过滤器，实现逻辑“与”操作，仅当路径通过所有指定的 PathFilter 时才接受
   * 允许组合多个过滤器（如默认的hiddenFileFilter + 用户自定义过滤器），无需要修改核心逻辑
   */
  private static class MultiPathFilter implements PathFilter {
    private List<PathFilter> filters;

    public MultiPathFilter(List<PathFilter> filters) {
      this.filters = filters;
    }

    public boolean accept(Path path) {
      for (PathFilter filter : filters) {
        if (!filter.accept(path)) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * 设置是否递归处理输入目录的子目录
   * 应用场景：适用于扁平目录结构，避免扫描深层嵌套目录（提升性能）
   * 
   * @param job
   * @param inputDirRecursive
   */
  public static void setInputDirRecursive(Job job, boolean inputDirRecursive) {
    job.getConfiguration().setBoolean(INPUT_DIR_RECURSIVE, inputDirRecursive);
  }

  /**
   * 检查作业是否启用了递归扫描输入目录。
   * 
   * @param job the job to look at.
   * @return should the files to be read recursively?
   */
  public static boolean getInputDirRecursive(JobContext job) {
    return job.getConfiguration().getBoolean(INPUT_DIR_RECURSIVE, false);
  }

  /**
   * 定义输入格式允许的最小分片大小（子类可覆盖）。
   * 
   * @return the number of bytes of the minimal split for this format
   */
  protected long getFormatMinSplitSize() {
    return 1;
  }

  /**
   * 判断文件是否可分片（影响 getSplits 的分片逻辑）。
   * 子类覆盖场景：​不可分片文件：如 GZIP 压缩文件（需顺序读取），返回 false。 ​自定义格式：若格式不支持随机读取，需禁止分片
   * 不可分片的文件将作为一个完整分片处理，由单个 Mapper 处理。
   * 
   * @param context  the job context
   * @param filename the file name to check
   * @return is this file splitable?
   */
  protected boolean isSplitable(JobContext context, Path filename) {
    return true;
  }

  /**
   * 设置自定义路径过滤器，过滤输入文件。
   * 
   * @param job    the job to modify
   * @param filter the PathFilter class use for filtering the input paths.
   */
  public static void setInputPathFilter(Job job, Class<? extends PathFilter> filter) {
    job.getConfiguration().setClass(PATHFILTER_CLASS, filter, PathFilter.class);
  }

  /**
   * 设置分片最小字节数
   * 避免生成过多小分片，减少任务调度开销。例如，处理大量小文件时，合并小文件到同一分片。
   * Set the minimum input split size
   * 
   * @param job  the job to modify
   * @param size the minimum size
   */
  public static void setMinInputSplitSize(Job job, long size) {
    job.getConfiguration().setLong(SPLIT_MINSIZE, size);
  }

  /**
   * Get the minimum split size
   * 
   * @param job the job
   * @return the minimum number of bytes that can be in a split
   */
  public static long getMinSplitSize(JobContext job) {
    return job.getConfiguration().getLong(SPLIT_MINSIZE, 1L);
  }

  /**
   * 设置分片的最大字节数
   * 防止分片过大导致任务执行时间过长或数据本地性下降。
   * 
   * @param job  the job to modify
   * @param size the maximum split size
   */
  public static void setMaxInputSplitSize(Job job, long size) {
    job.getConfiguration().setLong(SPLIT_MAXSIZE, size);
  }

  /**
   * Get the maximum split size.
   * 
   * @param context the job to look at.
   * @return the maximum number of bytes a split can include
   */
  public static long getMaxSplitSize(JobContext context) {
    return context.getConfiguration().getLong(SPLIT_MAXSIZE, Long.MAX_VALUE);
  }

  /**
   * 获取用户自定义的 PathFilter 实例，通过反射从配置项 mapreduce.input.pathFilter.class 加载。
   * 与默认的 hiddenFileFilter（过滤隐藏文件）组合成 MultiPathFilter，仅接受通过所有过滤器的文件。
   * 
   * @return the PathFilter instance set for the job, NULL if none has been set.
   */
  public static PathFilter getInputPathFilter(JobContext context) {
    Configuration conf = context.getConfiguration();
    Class<?> filterClass = conf.getClass(PATHFILTER_CLASS, null, PathFilter.class);
    return (filterClass != null) ? (PathFilter) ReflectionUtils.newInstance(filterClass, conf) : null;
  }

  /**
   * listStatus 负责扫描作业配置的输入路径，
   * 生成符合条件的文件列表（FileStatus），
   * 同时处理安全令牌、递归遍历、文件过滤和多线程优化。
   * 它是 FileInputFormat 分片（getSplits）的前置步骤。
   * 
   * @param job the job to list input paths for and attach tokens to.
   * @return array of FileStatus objects
   * @throws IOException if zero items.
   */
  protected List<FileStatus> listStatus(JobContext job) throws IOException {
    // 获取 mapreduce.input.fileinputformat.inputdir 配置的输入路径数组。
    Path[] dirs = getInputPaths(job);
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }
    // 在启用安全认证（如 Kerberos）的 Hadoop 集群中，访问 HDFS 需要 Delegation Token。
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs, job.getConfiguration());
    // 递归扫描控制
    boolean recursive = getInputDirRecursive(job);
    // 多过滤器组合
    List<PathFilter> filters = new ArrayList<>();
    filters.add(hiddenFileFilter);
    PathFilter jobFilter = getInputPathFilter(job);
    if (jobFilter != null) {
      filters.add(jobFilter);
    }
    PathFilter inputFilter = new MultiPathFilter(filters);
    List<FileStatus> result = null;
    // 多线程优化
    int numThreads = job.getConfiguration().getInt(LIST_STATUS_NUM_THREADS, DEFAULT_LIST_STATUS_NUM_THREADS);
    StopWatch sw = new StopWatch().start();
    // 线程数是1的时候进行串行执行
    if (numThreads == 1) {
      result = singleThreadedListStatus(job, dirs, inputFilter, recursive);
    } else {
      Iterable<FileStatus> locatedFiles = null;
      try {
        LocatedFileStatusFetcher locatedFileStatusFetcher = new LocatedFileStatusFetcher(
            job.getConfiguration(), dirs, recursive, inputFilter, true);
        // 通过 LocatedFileStatusFetcher 并发列出文件状态，提升大目录扫描效率。
        locatedFiles = locatedFileStatusFetcher.getFileStatuses();
      } catch (InterruptedException e) {
        throw (IOException) new InterruptedIOException("Interrupted while getting file statuses").initCause(e);
      }
      result = Lists.newArrayList(locatedFiles);
    }
    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Time taken to get FileStatuses: , {}", sw.now(TimeUnit.MILLISECONDS));
    }
    LOG.info("Total input files to process : , {}", result.size());
    return result;
  }

  // 方法用于单线程扫描输入路径并生成符合条件的文件列表
  private List<FileStatus> singleThreadedListStatus(JobContext job, Path[] dirs,
      PathFilter inputFilter, boolean recursive) throws IOException {
    // 符合条件的文件状态
    List<FileStatus> result = new ArrayList<>();
    // 扫描过程中发现的错误
    List<IOException> errors = new ArrayList<>();
    for (int i = 0; i < dirs.length; ++i) {
      Path p = dirs[i];
      // 根据路径的协议（如 hdfs:// 或 file://）获取对应的文件系统实例
      FileSystem fs = p.getFileSystem(job.getConfiguration());
      // 解析路径中的通配符（如 * 或 ?），并应用 inputFilter 过滤文件，返回匹配的 FileStatus 数组。
      FileStatus[] matches = fs.globStatus(p, inputFilter);
      // 路径不存在
      if (matches == null) {
        errors.add(new IOException("Input path does not exist: " + p));
        // 路径存在但无匹配文件
      } else if (matches.length == 0) {
        errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
      } else {
        // 路径存在且有匹配项
        for (FileStatus globStat : matches) {
          // 处理目录，需要进一步的遍历其中的内容
          if (globStat.isDirectory()) {
            RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(globStat.getPath());
            // 使用 RemoteIterator 逐条获取目录下的文件状态，避免一次性加载大目录导致内存压力
            while (iter.hasNext()) {
              LocatedFileStatus stat = iter.next();
              if (inputFilter.accept(stat.getPath())) {
                if (recursive && stat.isDirectory()) {
                  addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
                } else {
                  result.add(shrinkStatus(stat));
                }
              }
            }
          } else {
            result.add(globStat);
          }
        }
      }
    }

    if (!errors.isEmpty()) {
      throw new InvalidInputException(errors);
    }
    return result;
  }

  /**
   * Add files in the input path recursively into the results.
   * 是用于递归扫描目录并将符合条件的文件添加到结构列表的核心方法
   * 
   * @param result  存储最终符合条件的文件状态
   * @param fs  //  文件系统的实例，（如HDFS， S3）  
   * @param path // 当前扫描的路径
   * @param inputFilter //  文件路径过滤器
   * @throws IOException
   */
  protected void addInputPathRecursively(List<FileStatus> result, FileSystem fs, Path path, PathFilter inputFilter)
      throws IOException {
    try {
      //  获取目录下文件列表
      RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(path);
      while (iter.hasNext()) {
        LocatedFileStatus stat = iter.next();
        if (inputFilter.accept(stat.getPath())) {
          if (stat.isDirectory()) {
            //  如果当前项时目录，递归调用自身继续扫描
            addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
          } else {
            result.add(shrinkStatus(stat));
          }
        }
      }
    } catch (FileNotFoundException e) {
      // unless the store is capabile of list inconsistencies, rethrow.
      // because this is recursive, the caller method may also end up catching
      // and rethrowing, which is slighly inefficient but harmless.
      maybeIgnoreMissingDirectory(fs, path, e);
    }
  }

  /**
   * shrinkStatus 方法旨在优化内存使用，通过精简文件状态中的块位置信息，减少作业提交
   * 时的内存开销
   * @see BlockLocation
   * @see org.apache.hadoop.fs.HdfsBlockLocation
   * @param origStat 原始文件状态，可能包含HdfsBlockLocation
   * @return 精简后的FileStatus, 保留必要的元数据
   */
  public static FileStatus shrinkStatus(FileStatus origStat) {
    if (origStat.isDirectory() || origStat.getLen() == 0 ||
        !(origStat instanceof LocatedFileStatus)) {
      return origStat;
    } else {
      BlockLocation[] blockLocations = ((LocatedFileStatus) origStat).getBlockLocations();
      BlockLocation[] locs = new BlockLocation[blockLocations.length];
      int i = 0;
      for (BlockLocation location : blockLocations) {
        locs[i++] = new BlockLocation(location);
      }
      LocatedFileStatus newStat = new LocatedFileStatus(origStat, locs);
      return newStat;
    }
  }

  /**
   * A factory that makes the split for this class. It can be overridden
   * by sub-classes to make sub-types
   */
  protected FileSplit makeSplit(Path file, long start, long length,
      String[] hosts) {
    return new FileSplit(file, start, length, hosts);
  }

  /**
   * A factory that makes the split for this class. It can be overridden
   * by sub-classes to make sub-types
   */
  protected FileSplit makeSplit(Path file, long start, long length,
      String[] hosts, String[] inMemoryHosts) {
    return new FileSplit(file, start, length, hosts, inMemoryHosts);
  }

  /**
   * 是核心方法，负责将输入文件划分为多个逻辑分片（InputSplit），供MapReduce任务的Mapper处理
   * 
   * @param job the job context
   * @throws IOException
   */
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    StopWatch sw = new StopWatch().start();
    //  获取分片最小尺寸
    long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
    //  分片最大尺寸
    long maxSize = getMaxSplitSize(job);
    // 存储生成的分片
    List<InputSplit> splits = new ArrayList<>();
    //  获取所有的输入文件的状态
    List<FileStatus> files = listStatus(job);
    //  是否忽略子目录，（当不递归且配置忽略时）
    boolean ignoreDirs = !getInputDirRecursive(job)
        && job.getConfiguration().getBoolean(INPUT_DIR_NONRECURSIVE_IGNORE_SUBDIRS, false);
    for (FileStatus file : files) {
      if (ignoreDirs && file.isDirectory()) {  // 跳过目录
        continue;
      }
      Path path = file.getPath();
      long length = file.getLen();
      if (length != 0) {
        //  获取块的位置信息
        BlockLocation[] blkLocations;
        if (file instanceof LocatedFileStatus) {
          //  直接获取
          blkLocations = ((LocatedFileStatus) file).getBlockLocations();
        } else {
          //  通过文件系统查询
          FileSystem fs = path.getFileSystem(job.getConfiguration());
          blkLocations = fs.getFileBlockLocations(file, 0, length);
        }
        //  判断是否可分割
        if (isSplitable(job, path)) {
          //  HDFS块大小
          long blockSize = file.getBlockSize();
          long splitSize = computeSplitSize(blockSize, minSize, maxSize);
          long bytesRemaining = length;
          //  切割主分片
          while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
            int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
            splits.add(makeSplit(path, length - bytesRemaining, splitSize,
                blkLocations[blkIndex].getHosts(),
                blkLocations[blkIndex].getCachedHosts()));
            bytesRemaining -= splitSize;
          }
          //  处理剩余字节
          if (bytesRemaining != 0) {
            int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
            splits.add(makeSplit(path, length - bytesRemaining, bytesRemaining,
                blkLocations[blkIndex].getHosts(),
                blkLocations[blkIndex].getCachedHosts()));
          }
        } else { 
          //  不可分割逻辑
          if (LOG.isDebugEnabled()) {
            // Log only if the file is big enough to be splitted
            if (length > Math.min(file.getBlockSize(), minSize)) {
              LOG.debug("File is not splittable so no parallelization "
                  + "is possible: , {}" , file.getPath());
            }
          }
          splits.add(makeSplit(path, 0, length, blkLocations[0].getHosts(),
              blkLocations[0].getCachedHosts()));
        }
      } else {
        // Create empty hosts array for zero length files
        splits.add(makeSplit(path, 0, length, new String[0]));
      }
    }
    // Save the number of input files for metrics/loadgen
    job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Total # of splits generated by getSplits: {}"
      +", TimeTaken: , {}" , splits.size(), sw.now(TimeUnit.MILLISECONDS));
    }
    return splits;
  }

  /**
   * 计算文件分片大小，优先级为: minSize < splitSize < maxSize
   * @param blockSize HDFS文件块大小（默认128MB）
   * @param minSize 分片最小值
   * @param maxSize 分片最大值
   * @return
   */
  protected long computeSplitSize(long blockSize, long minSize, long maxSize) {
    return Math.max(minSize, Math.min(maxSize, blockSize));
  }

  /**
   * 根据文件偏移量缺点对应的HDFS块索引
   * 在生成FileSplit时，缺点分片对应的物理块位置，优化数据本地化
   * @param blkLocations    HDFS 文件块元数据数组，包含每个块的起始位置、长度和存储节点
   * @param offset  需要定位的文件偏移量（字节级别）
   * @return
   */
  protected int getBlockIndex(BlockLocation[] blkLocations, long offset) {
    for (int i = 0; i < blkLocations.length; i++) {
      //  blkLocations[i].getOffset() 块的起始位置
      //  blkLocations[i].getLength() 块的长度
      if ((blkLocations[i].getOffset() <= offset) 
          && (offset < blkLocations[i].getOffset() + blkLocations[i].getLength())) {
        return i;
      }
    }
    BlockLocation last = blkLocations[blkLocations.length - 1];
    long fileLength = last.getOffset() + last.getLength() - 1;
    throw new IllegalArgumentException("Offset " + offset +
        " is outside of file (0.." + fileLength + ")");
  }

  /**
   * Sets the given comma separated paths as the list of inputs
   * for the map-reduce job.
   * 
   * @param job                 the job
   * @param commaSeparatedPaths Comma separated paths to be set as
   *                            the list of inputs for the map-reduce job.
   */
  public static void setInputPaths(Job job, String commaSeparatedPaths) throws IOException {
    setInputPaths(job, StringUtils.stringToPath(getPathStrings(commaSeparatedPaths)));
  }

  /**
   * Add the given comma separated paths to the list of inputs for
   * the map-reduce job.
   * 
   * @param job                 The job to modify
   * @param commaSeparatedPaths Comma separated paths to be added to
   *                            the list of inputs for the map-reduce job.
   */
  public static void addInputPaths(Job job, String commaSeparatedPaths) throws IOException {
    for (String str : getPathStrings(commaSeparatedPaths)) {
      addInputPath(job, new Path(str));
    }
  }

  /**
   * 这段代码是Hadoop中用于设置MapReduce作业输入路径的核心方法，
   * 其功能是将多个Path对象转换为规范化且转义的字符串，并保存到作业配置中
   * 将一组 Path 对象转换为标准化的文件系统路径，并进行转义处理，最终以逗号分隔的形式存储到作业配置中。
   * 确保输入路径的跨节点一致性和特殊字符安全性，避免路径解析错误。
   * @param job        要配置的MapReduce
   * @param inputPaths 可变参数，表示输入目录/文件的 Path对象数组
   */
  public static void setInputPaths(Job job, Path... inputPaths) throws IOException {
    //  通过 Job 对象获取其关联的 Configuration，用于存储输入路径信息。
    Configuration conf = job.getConfiguration();
    // getFileSystem(conf) 根据配置获取path 对应的文件系统
    //  makeQualified(inputPaths[0]) 将路径转换为完全限定路径，确保路径包含文件系统信息  
    // /data/input → hdfs://namenode:8020/data/input。
    Path path = inputPaths[0].getFileSystem(conf).makeQualified(inputPaths[0]);
    //  转义路径中的特殊字符（如逗号,  转义为 \）避免后续解析错误
    StringBuilder str = new StringBuilder(StringUtils.escapeString(path.toString()));
    //  遍历剩余路径并拼接
    for (int i = 1; i < inputPaths.length; i++) {
      str.append(StringUtils.COMMA_STR);
      path = inputPaths[i].getFileSystem(conf).makeQualified(inputPaths[i]);
      str.append(StringUtils.escapeString(path.toString()));
    }
    conf.set(INPUT_DIR, str.toString());
  }

  /**
   * addInputPath 方法用于向 MapReduce 作业的输入路径列表中添加单个路径，确保路径格式标准化且安全
   * 将单个路径添加到作业的输入配置中，支持多次调用以增量构建输入源列表。
   * @param job  目标作业对象，用于访问和修改配置。
   * @param path 待添加的输入路径。
   */
  public static void addInputPath(Job job, Path path) throws IOException {
    Configuration conf = job.getConfiguration();
    path = path.getFileSystem(conf).makeQualified(path);
    String dirStr = StringUtils.escapeString(path.toString());
    String dirs = conf.get(INPUT_DIR);
    conf.set(INPUT_DIR, dirs == null ? dirStr : dirs + "," + dirStr);
  }

  /**
   * 这段代码时Hadoop 中用于解析包含通配符的逗号分割路径字符串的方法
   * 其核心的功能是正确的分割含通配符的路径，避免将通配符内的逗号误判为路径分隔符
   * @param commaSeparatedPaths  逗号分隔的路径字符串（""/logs/{2023,2024}/data,/backup"）
   * @return  输出：分割后的路径数组  ["/logs/{2023,2024}/data", "/backup"]
   */
  private static String[] getPathStrings(String commaSeparatedPaths) {
    int length = commaSeparatedPaths.length();

    //  跟踪当前大括号{} 的嵌套层数如(如 {{a,b},c}中， 遇到{时递增，}时递减)
    int curlyOpen = 0;
    //  当前路径的起始字符位置
    int pathStart = 0;
    //  标记是否处于通配符模式（即是否在{}内）
    boolean globPattern = false;
    //  存储最终分割后的路径
    List<String> pathStrings = new ArrayList<>();

    for (int i = 0; i < length; i++) {
      char ch = commaSeparatedPaths.charAt(i);
      switch (ch) {
        case '{': {
          //  递增 curlyOpen， 标记进入通配符模式
          curlyOpen++;
          if (!globPattern) {
            globPattern = true;
          }
          break;
        }
        case '}': {
          //  递减 curlyOpen，若归零退出通配符模式
          curlyOpen--;
          if (curlyOpen == 0 && globPattern) {
            globPattern = false;
          }
          break;
        }
        case ',': {
          if (!globPattern) {
            pathStrings.add(commaSeparatedPaths.substring(pathStart, i));
            pathStart = i + 1;
          }
          break;
        }
        default:
          continue; // nothing special to do for this character
      }
    }
    pathStrings.add(commaSeparatedPaths.substring(pathStart, length));

    return pathStrings.toArray(new String[0]);
  }

  /**
   * Get the list of input {@link Path}s for the map-reduce job.
   * 
   * @param context The job
   * @return the list of input {@link Path}s for the map-reduce job.
   */
  public static Path[] getInputPaths(JobContext context) {
    String dirs = context.getConfiguration().get(INPUT_DIR, "");
    String[] list = StringUtils.split(dirs);
    Path[] result = new Path[list.length];
    for (int i = 0; i < list.length; i++) {
      result[i] = new Path(StringUtils.unEscapeString(list[i]));
    }
    return result;
  }

}
