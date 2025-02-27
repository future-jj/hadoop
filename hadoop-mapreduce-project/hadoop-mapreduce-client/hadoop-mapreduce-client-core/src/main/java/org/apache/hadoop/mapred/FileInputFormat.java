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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * FileInputFormat 是所有基于文件（如文本文件、SequenceFile 等）的 InputFormat 的基类。
 * 所有处理文件输入的 InputFormat 实现类
 * （如 TextInputFormat、SequenceFileInputFormat）都应继承 FileInputFormat。
 * FileInputFormat 提供了 getSplits 方法的默认实现，用于将输入文件划分为逻辑分片（InputSplit）。
 * 子类可以通过覆盖 isSplitable 方法，控制某些文件是否允许分割
 * ​场景：
 * ​不可分割文件：如 GZIP 压缩文件，必须整体处理。
 * ​业务需求：确保单个文件由单个 Mapper 处理（例如全局有序文件）
 * 如果子类需要处理不可分割文件，必须覆盖 isSplitable 方法，否则会错误地分割文件。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class FileInputFormat<K, V> implements InputFormat<K, V> {

  public static final Logger LOG =
      LoggerFactory.getLogger(FileInputFormat.class);
  
  @Deprecated
  public enum Counter {
    BYTES_READ
  }

  //  记录输入文件的总数，供作业计数器（Job Counter）统计输入文件数量。
  public static final String NUM_INPUT_FILES =
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.NUM_INPUT_FILES;

  //  控制是否递归遍历输入目录的子目录。
  public static final String INPUT_DIR_RECURSIVE = 
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR_RECURSIVE;

    // 当 INPUT_DIR_RECURSIVE=false（不递归遍历）时，是否忽略输入目录中的子目录。
  public static final String INPUT_DIR_NONRECURSIVE_IGNORE_SUBDIRS =
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR_NONRECURSIVE_IGNORE_SUBDIRS;

  // 在切割文件为分片时，避免生成过小的分片。当剩余文件大小 bytesRemaining 满足以下条件时，停止切割：
  //  (bytesRemaining / splitSize) > SPLIT_SLOP 即，若剩余部分超过分片大小的 ​1.1 倍，则继续切割；否则将剩余部分作为一个完整分片。
  private static final double SPLIT_SLOP = 1.1;   // 10% slop

  private long minSplitSize = 1;

  /**
   * 隐藏文件过滤器 hiddenFileFilter
   * 过滤掉以 _ 或 . 开头的文件或目录（如临时文件.temp 或系统文件 _SUCCESS）
   * 在 listStatus() 方法中， 该过滤器与其他用户自定义过滤器结合，排除不需要处理的文件
   */
  private static final PathFilter hiddenFileFilter = new PathFilter(){
      public boolean accept(Path p){
        String name = p.getName(); 
        return !name.startsWith("_") && !name.startsWith("."); 
      }
    }; 

  protected void setMinSplitSize(long minSplitSize) {
    this.minSplitSize = minSplitSize;
  }

  /**
   * 组合多个PathFilter 仅当所有过滤器都接受路径时，路径才能被接受
   * 设计意图：将内置过滤器（排除隐藏文件）与用户自定义过滤器相互结合使用
   */
  private static class MultiPathFilter implements PathFilter {
    
    private List<PathFilter> filters;

    public MultiPathFilter(List<PathFilter> filters) {
      this.filters = filters;
    }

    //  遍历所有的过滤器，若任一过滤器拒绝路径，立即返回false
    //  所有过滤器均接受时返回false
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
   * 判断文件是否可分割，默认返回true（可分割）
   * 设计意图：允许子类覆盖此方法，处理不可分割的文件（比如压缩文件）
   */
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return true;
  }
  
  /**
   * 为给定的InputSplit 创建一个 RecordReader 实例，用于将分片数据解析为键值对（<K, V>）
   * 子类必须实现此方法，根据文件格式（文本, SequenceFile） 返回对应的RecordReader
   * @param split 输入分片InputSplit，包含数据的位置和大小
   * @param job 作业配置（JobConf）包括输入格式，压缩编码等信息
   * @param reporter 进度报告（Reporter）用于更新任务进度
   */
  public abstract RecordReader<K, V> getRecordReader(InputSplit split,
                                               JobConf job,
                                               Reporter reporter)
    throws IOException;

  /**
   * 设置一个用户自定义的 PathFilter 类，用于过滤输入路径（排除临时文件）
   * @param conf 作业配置
   * @param filter 用户自定义的 PathFilter
   */
  public static void setInputPathFilter(JobConf conf,
                                        Class<? extends PathFilter> filter) {
    conf.setClass(org.apache.hadoop.mapreduce.lib.input.
      FileInputFormat.PATHFILTER_CLASS, filter, PathFilter.class);
  }

  /**
   * 从作业配置哪里获取用户设置的PathFilter 的实例
   * 从流程配置中读取 FileInputFormat.PATHFILTER_CLASS 指定的类
   *  通过反射实例化此类
   */
  public static PathFilter getInputPathFilter(JobConf conf) {
    Class<? extends PathFilter> filterClass = conf.getClass(
	  org.apache.hadoop.mapreduce.lib.input.FileInputFormat.PATHFILTER_CLASS,
	  null, PathFilter.class);
    return (filterClass != null) ?
        ReflectionUtils.newInstance(filterClass, conf) : null;
  }

  /**
   * 递归遍历指定路径下的所有的目录和文件，将符合文件的文件添加到结构列表中
   * @param result 用户存储符合条件的文件状态
   * @param fs 文件系统实例
   * @param path 当前要遍历的路径
   * @param inputFilter 路径过滤器，决定是否包含某一个路径
   * @throws IOException 
   */
  protected void addInputPathRecursively(List<FileStatus> result,
      FileSystem fs, Path path, PathFilter inputFilter) 
      throws IOException {
    //  获取路径下的所有文件和子目录的状态信息（包含块位置信息）
    RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(path);
    //  遍历目录项
    while (iter.hasNext()) {
      LocatedFileStatus stat = iter.next();
      //  逐个处理目录中的每一个条目
      if (inputFilter.accept(stat.getPath())) {
        if (stat.isDirectory()) {
          addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
        } else {
          //  shrinkStatus的作用：将LocatedFileStatus（含块位置）转换为轻量级的FileStatus
          result.add(org.apache.hadoop.mapreduce.lib.input.
              FileInputFormat.shrinkStatus(stat));
        }
      }
    }
  }
  
  /**
   * 列出所有输入路径下的文件，应用过滤器，并返回文件状态数组。
   */
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    //  获取输入路径，从作业配置中读取输入路径的列表
    Path[] dirs = getInputPaths(job);
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }
    // 获取访问HDFS文件所需要的安全令牌
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs, job);
    
    //  是否递归遍历子目录，默认递归
    boolean recursive = job.getBoolean(INPUT_DIR_RECURSIVE, false);
    List<PathFilter> filters = new ArrayList<PathFilter>();

    filters.add(hiddenFileFilter);
    PathFilter jobFilter = getInputPathFilter(job);
    if (jobFilter != null) {
      filters.add(jobFilter);
    }
    PathFilter inputFilter = new MultiPathFilter(filters);
    FileStatus[] result;
    int numThreads = job
        .getInt(
            org.apache.hadoop.mapreduce.lib.input.FileInputFormat.LIST_STATUS_NUM_THREADS,
            org.apache.hadoop.mapreduce.lib.input.FileInputFormat.DEFAULT_LIST_STATUS_NUM_THREADS);
    
    StopWatch sw = new StopWatch().start();
    if (numThreads == 1) {
      List<FileStatus> locatedFiles = singleThreadedListStatus(job, dirs, inputFilter, recursive); 
      result = locatedFiles.toArray(new FileStatus[locatedFiles.size()]);
    } else {
      Iterable<FileStatus> locatedFiles = null;
      try {
        
        LocatedFileStatusFetcher locatedFileStatusFetcher = new LocatedFileStatusFetcher(
            job, dirs, recursive, inputFilter, false);
        locatedFiles = locatedFileStatusFetcher.getFileStatuses();
      } catch (InterruptedException e) {
        throw  (IOException)
            new InterruptedIOException("Interrupted while getting file statuses")
                .initCause(e);
      }
      result = Iterables.toArray(locatedFiles, FileStatus.class);
    }

    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Time taken to get FileStatuses: "
          + sw.now(TimeUnit.MILLISECONDS));
    }
    LOG.info("Total input files to process : " + result.length);
    return result;
  }
  
  private List<FileStatus> singleThreadedListStatus(JobConf job, Path[] dirs,
      PathFilter inputFilter, boolean recursive) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
    List<IOException> errors = new ArrayList<IOException>();
    for (Path p: dirs) {
      FileSystem fs = p.getFileSystem(job); 
      FileStatus[] matches = fs.globStatus(p, inputFilter);
      if (matches == null) {
        errors.add(new IOException("Input path does not exist: " + p));
      } else if (matches.length == 0) {
        errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
      } else {
        for (FileStatus globStat: matches) {
          if (globStat.isDirectory()) {
            RemoteIterator<LocatedFileStatus> iter =
                fs.listLocatedStatus(globStat.getPath());
            while (iter.hasNext()) {
              LocatedFileStatus stat = iter.next();
              if (inputFilter.accept(stat.getPath())) {
                if (recursive && stat.isDirectory()) {
                  addInputPathRecursively(result, fs, stat.getPath(),
                      inputFilter);
                } else {
                  result.add(org.apache.hadoop.mapreduce.lib.input.
                      FileInputFormat.shrinkStatus(stat));
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

  /** Splits files returned by {@link #listStatus(JobConf)} when
   * they're too big.*/ 
  public InputSplit[] getSplits(JobConf job, int numSplits)
    throws IOException {
    StopWatch sw = new StopWatch().start();
    FileStatus[] stats = listStatus(job);

    // Save the number of input files for metrics/loadgen
    job.setLong(NUM_INPUT_FILES, stats.length);
    long totalSize = 0;                           // compute total size
    boolean ignoreDirs = !job.getBoolean(INPUT_DIR_RECURSIVE, false)
      && job.getBoolean(INPUT_DIR_NONRECURSIVE_IGNORE_SUBDIRS, false);

    List<FileStatus> files = new ArrayList<>(stats.length);
    for (FileStatus file: stats) {                // check we have valid files
      if (file.isDirectory()) {
        if (!ignoreDirs) {
          throw new IOException("Not a file: "+ file.getPath());
        }
      } else {
        files.add(file);
        totalSize += file.getLen();
      }
    }

    long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
    long minSize = Math.max(job.getLong(org.apache.hadoop.mapreduce.lib.input.
      FileInputFormat.SPLIT_MINSIZE, 1), minSplitSize);

    // generate splits
    ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
    NetworkTopology clusterMap = new NetworkTopology();
    for (FileStatus file: files) {
      Path path = file.getPath();
      long length = file.getLen();
      if (length != 0) {
        FileSystem fs = path.getFileSystem(job);
        BlockLocation[] blkLocations;
        if (file instanceof LocatedFileStatus) {
          blkLocations = ((LocatedFileStatus) file).getBlockLocations();
        } else {
          blkLocations = fs.getFileBlockLocations(file, 0, length);
        }
        if (isSplitable(fs, path)) {
          long blockSize = file.getBlockSize();
          long splitSize = computeSplitSize(goalSize, minSize, blockSize);

          long bytesRemaining = length;
          while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
            String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,
                length-bytesRemaining, splitSize, clusterMap);
            splits.add(makeSplit(path, length-bytesRemaining, splitSize,
                splitHosts[0], splitHosts[1]));
            bytesRemaining -= splitSize;
          }

          if (bytesRemaining != 0) {
            String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations, length
                - bytesRemaining, bytesRemaining, clusterMap);
            splits.add(makeSplit(path, length - bytesRemaining, bytesRemaining,
                splitHosts[0], splitHosts[1]));
          }
        } else {
          if (LOG.isDebugEnabled()) {
            // Log only if the file is big enough to be splitted
            if (length > Math.min(file.getBlockSize(), minSize)) {
              LOG.debug("File is not splittable so no parallelization "
                  + "is possible: " + file.getPath());
            }
          }
          String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,0,length,clusterMap);
          splits.add(makeSplit(path, 0, length, splitHosts[0], splitHosts[1]));
        }
      } else { 
        //Create empty hosts array for zero length files
        splits.add(makeSplit(path, 0, length, new String[0]));
      }
    }
    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Total # of splits generated by getSplits: " + splits.size()
          + ", TimeTaken: " + sw.now(TimeUnit.MILLISECONDS));
    }
    return splits.toArray(new FileSplit[splits.size()]);
  }

  protected long computeSplitSize(long goalSize, long minSize,
                                       long blockSize) {
    return Math.max(minSize, Math.min(goalSize, blockSize));
  }

  protected int getBlockIndex(BlockLocation[] blkLocations, 
                              long offset) {
    for (int i = 0 ; i < blkLocations.length; i++) {
      // is the offset inside this block?
      if ((blkLocations[i].getOffset() <= offset) &&
          (offset < blkLocations[i].getOffset() + blkLocations[i].getLength())){
        return i;
      }
    }
    BlockLocation last = blkLocations[blkLocations.length -1];
    long fileLength = last.getOffset() + last.getLength() -1;
    throw new IllegalArgumentException("Offset " + offset + 
                                       " is outside of file (0.." +
                                       fileLength + ")");
  }

  /**
   * Sets the given comma separated paths as the list of inputs 
   * for the map-reduce job.
   * 
   * @param conf Configuration of the job
   * @param commaSeparatedPaths Comma separated paths to be set as 
   *        the list of inputs for the map-reduce job.
   */
  public static void setInputPaths(JobConf conf, String commaSeparatedPaths) {
    setInputPaths(conf, StringUtils.stringToPath(
                        getPathStrings(commaSeparatedPaths)));
  }

  /**
   * Add the given comma separated paths to the list of inputs for
   *  the map-reduce job.
   * 
   * @param conf The configuration of the job 
   * @param commaSeparatedPaths Comma separated paths to be added to
   *        the list of inputs for the map-reduce job.
   */
  public static void addInputPaths(JobConf conf, String commaSeparatedPaths) {
    for (String str : getPathStrings(commaSeparatedPaths)) {
      addInputPath(conf, new Path(str));
    }
  }

  /**
   * Set the array of {@link Path}s as the list of inputs
   * for the map-reduce job.
   * 
   * @param conf Configuration of the job. 
   * @param inputPaths the {@link Path}s of the input directories/files 
   * for the map-reduce job.
   */ 
  public static void setInputPaths(JobConf conf, Path... inputPaths) {
    Path path = new Path(conf.getWorkingDirectory(), inputPaths[0]);
    StringBuilder str = new StringBuilder(StringUtils.escapeString(path.toString()));
    for(int i = 1; i < inputPaths.length;i++) {
      str.append(StringUtils.COMMA_STR);
      path = new Path(conf.getWorkingDirectory(), inputPaths[i]);
      str.append(StringUtils.escapeString(path.toString()));
    }
    conf.set(org.apache.hadoop.mapreduce.lib.input.
      FileInputFormat.INPUT_DIR, str.toString());
  }

  /**
   * Add a {@link Path} to the list of inputs for the map-reduce job.
   * 
   * @param conf The configuration of the job 
   * @param path {@link Path} to be added to the list of inputs for 
   *            the map-reduce job.
   */
  public static void addInputPath(JobConf conf, Path path ) {
    path = new Path(conf.getWorkingDirectory(), path);
    String dirStr = StringUtils.escapeString(path.toString());
    String dirs = conf.get(org.apache.hadoop.mapreduce.lib.input.
      FileInputFormat.INPUT_DIR);
    conf.set(org.apache.hadoop.mapreduce.lib.input.
      FileInputFormat.INPUT_DIR, dirs == null ? dirStr :
      dirs + StringUtils.COMMA_STR + dirStr);
  }
         
  // This method escapes commas in the glob pattern of the given paths.
  private static String[] getPathStrings(String commaSeparatedPaths) {
    int length = commaSeparatedPaths.length();
    int curlyOpen = 0;
    int pathStart = 0;
    boolean globPattern = false;
    List<String> pathStrings = new ArrayList<String>();
    
    for (int i=0; i<length; i++) {
      char ch = commaSeparatedPaths.charAt(i);
      switch(ch) {
        case '{' : {
          curlyOpen++;
          if (!globPattern) {
            globPattern = true;
          }
          break;
        }
        case '}' : {
          curlyOpen--;
          if (curlyOpen == 0 && globPattern) {
            globPattern = false;
          }
          break;
        }
        case ',' : {
          if (!globPattern) {
            pathStrings.add(commaSeparatedPaths.substring(pathStart, i));
            pathStart = i + 1 ;
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
   * @param conf The configuration of the job 
   * @return the list of input {@link Path}s for the map-reduce job.
   */
  public static Path[] getInputPaths(JobConf conf) {
    String dirs = conf.get(org.apache.hadoop.mapreduce.lib.input.
      FileInputFormat.INPUT_DIR, "");
    String [] list = StringUtils.split(dirs);
    Path[] result = new Path[list.length];
    for (int i = 0; i < list.length; i++) {
      result[i] = new Path(StringUtils.unEscapeString(list[i]));
    }
    return result;
  }
  

  private void sortInDescendingOrder(List<NodeInfo> mylist) {
    Collections.sort(mylist, new Comparator<NodeInfo> () {
      public int compare(NodeInfo obj1, NodeInfo obj2) {

        if (obj1 == null || obj2 == null)
          return -1;

        if (obj1.getValue() == obj2.getValue()) {
          return 0;
        }
        else {
          return ((obj1.getValue() < obj2.getValue()) ? 1 : -1);
        }
      }
    }
    );
  }

  /** 
   * This function identifies and returns the hosts that contribute 
   * most for a given split. For calculating the contribution, rack
   * locality is treated on par with host locality, so hosts from racks
   * that contribute the most are preferred over hosts on racks that 
   * contribute less
   * @param blkLocations The list of block locations
   * @param offset 
   * @param splitSize 
   * @return an array of hosts that contribute most to this split
   * @throws IOException
   */
  protected String[] getSplitHosts(BlockLocation[] blkLocations, 
      long offset, long splitSize, NetworkTopology clusterMap) throws IOException {
    return getSplitHostsAndCachedHosts(blkLocations, offset, splitSize,
        clusterMap)[0];
  }
  
  /** 
   * This function identifies and returns the hosts that contribute 
   * most for a given split. For calculating the contribution, rack
   * locality is treated on par with host locality, so hosts from racks
   * that contribute the most are preferred over hosts on racks that 
   * contribute less
   * @param blkLocations The list of block locations
   * @param offset 
   * @param splitSize 
   * @return two arrays - one of hosts that contribute most to this split, and
   *    one of hosts that contribute most to this split that have the data
   *    cached on them
   * @throws IOException
   */
  private String[][] getSplitHostsAndCachedHosts(BlockLocation[] blkLocations, 
      long offset, long splitSize, NetworkTopology clusterMap)
  throws IOException {

    int startIndex = getBlockIndex(blkLocations, offset);

    long bytesInThisBlock = blkLocations[startIndex].getOffset() + 
                          blkLocations[startIndex].getLength() - offset;

    //If this is the only block, just return
    if (bytesInThisBlock >= splitSize) {
      return new String[][] { blkLocations[startIndex].getHosts(),
          blkLocations[startIndex].getCachedHosts() };
    }

    long bytesInFirstBlock = bytesInThisBlock;
    int index = startIndex + 1;
    splitSize -= bytesInThisBlock;

    while (splitSize > 0) {
      bytesInThisBlock =
        Math.min(splitSize, blkLocations[index++].getLength());
      splitSize -= bytesInThisBlock;
    }

    long bytesInLastBlock = bytesInThisBlock;
    int endIndex = index - 1;
    
    Map <Node,NodeInfo> hostsMap = new IdentityHashMap<Node,NodeInfo>();
    Map <Node,NodeInfo> racksMap = new IdentityHashMap<Node,NodeInfo>();
    String [] allTopos = new String[0];

    // Build the hierarchy and aggregate the contribution of 
    // bytes at each level. See TestGetSplitHosts.java 

    for (index = startIndex; index <= endIndex; index++) {

      // Establish the bytes in this block
      if (index == startIndex) {
        bytesInThisBlock = bytesInFirstBlock;
      }
      else if (index == endIndex) {
        bytesInThisBlock = bytesInLastBlock;
      }
      else {
        bytesInThisBlock = blkLocations[index].getLength();
      }
      
      allTopos = blkLocations[index].getTopologyPaths();

      // If no topology information is available, just
      // prefix a fakeRack
      if (allTopos.length == 0) {
        allTopos = fakeRacks(blkLocations, index);
      }

      // NOTE: This code currently works only for one level of
      // hierarchy (rack/host). However, it is relatively easy
      // to extend this to support aggregation at different
      // levels 
      
      for (String topo: allTopos) {

        Node node, parentNode;
        NodeInfo nodeInfo, parentNodeInfo;

        node = clusterMap.getNode(topo);

        if (node == null) {
          node = new NodeBase(topo);
          clusterMap.add(node);
        }
        
        nodeInfo = hostsMap.get(node);
        
        if (nodeInfo == null) {
          nodeInfo = new NodeInfo(node);
          hostsMap.put(node,nodeInfo);
          parentNode = node.getParent();
          parentNodeInfo = racksMap.get(parentNode);
          if (parentNodeInfo == null) {
            parentNodeInfo = new NodeInfo(parentNode);
            racksMap.put(parentNode,parentNodeInfo);
          }
          parentNodeInfo.addLeaf(nodeInfo);
        }
        else {
          nodeInfo = hostsMap.get(node);
          parentNode = node.getParent();
          parentNodeInfo = racksMap.get(parentNode);
        }

        nodeInfo.addValue(index, bytesInThisBlock);
        parentNodeInfo.addValue(index, bytesInThisBlock);

      } // for all topos
    
    } // for all indices

    // We don't yet support cached hosts when bytesInThisBlock > splitSize
    return new String[][] { identifyHosts(allTopos.length, racksMap),
        new String[0]};
  }
  
  private String[] identifyHosts(int replicationFactor, 
                                 Map<Node,NodeInfo> racksMap) {
    
    String [] retVal = new String[replicationFactor];
   
    List <NodeInfo> rackList = new LinkedList<NodeInfo>(); 

    rackList.addAll(racksMap.values());
    
    // Sort the racks based on their contribution to this split
    sortInDescendingOrder(rackList);
    
    boolean done = false;
    int index = 0;
    
    // Get the host list for all our aggregated items, sort
    // them and return the top entries
    for (NodeInfo ni: rackList) {

      Set<NodeInfo> hostSet = ni.getLeaves();

      List<NodeInfo>hostList = new LinkedList<NodeInfo>();
      hostList.addAll(hostSet);
    
      // Sort the hosts in this rack based on their contribution
      sortInDescendingOrder(hostList);

      for (NodeInfo host: hostList) {
        // Strip out the port number from the host name
        retVal[index++] = host.node.getName().split(":")[0];
        if (index == replicationFactor) {
          done = true;
          break;
        }
      }
      
      if (done == true) {
        break;
      }
    }
    return retVal;
  }
  
  private String[] fakeRacks(BlockLocation[] blkLocations, int index) 
  throws IOException {
    String[] allHosts = blkLocations[index].getHosts();
    String[] allTopos = new String[allHosts.length];
    for (int i = 0; i < allHosts.length; i++) {
      allTopos[i] = NetworkTopology.DEFAULT_RACK + "/" + allHosts[i];
    }
    return allTopos;
  }


  private static class NodeInfo {
    final Node node;
    final Set<Integer> blockIds;
    final Set<NodeInfo> leaves;

    private long value;
    
    NodeInfo(Node node) {
      this.node = node;
      blockIds = new HashSet<Integer>();
      leaves = new HashSet<NodeInfo>();
    }

    long getValue() {return value;}

    void addValue(int blockIndex, long value) {
      if (blockIds.add(blockIndex) == true) {
        this.value += value;
      }
    }

    Set<NodeInfo> getLeaves() { return leaves;}

    void addLeaf(NodeInfo nodeInfo) {
      leaves.add(nodeInfo);
    }
  }
}
