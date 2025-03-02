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

package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.security.Credentials;

/**
 * 这段代码是Hadoop MapReduce框架中的JobContext接口定义，它为运行中的任务提供了对作业配置和元数据的只读访问。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface JobContext extends MRJobConfig {

  /** 返回作业的共享配置对象（Configuration），包含用户设置的参数（如输入路径、资源限制等）。
   */
  public Configuration getConfiguration();

  /**
   * 获取作业的安全凭证（Credentials），用于认证访问HDFS或其他受保护资源。
   * Kerberos票据或令牌
   */
  public Credentials getCredentials();

  /**
   * 返回作业的唯一标识符（JobID），用于跟踪作业状态。
   */
  public JobID getJobID();
  
  /**
   * 获取Reduce任务数量，控制作业并行度。
   */
  public int getNumReduceTasks();
  
  /**
   * 返回作业的HDFS工作目录，用于存储中间数据和结果。
   * 
   * @return the directory name.
   */
  public Path getWorkingDirectory() throws IOException;

  /**
   * 最终输出键的类
   * @return the key class for the job output data.
   */
  public Class<?> getOutputKeyClass();
  
  /**
   * 最终输出值的类
   * @return the value class for job outputs.
   */
  public Class<?> getOutputValueClass();

  /**
   * Map阶段中间输出键的类，可独立于最终输出。
   * @return the map output key class.
   */
  public Class<?> getMapOutputKeyClass();

  /**
   * Get the value class for the map output data. If it is not set, use the
   * (final) output value class This allows the map output value class to be
   * different than the final output value class.
   *  
   * @return the map output value class.
   */
  public Class<?> getMapOutputValueClass();

  /**
   * Get the user-specified job name. This is only used to identify the 
   * job to the user.
   * 
   * @return the job's name, defaulting to "".
   */
  public String getJobName();

  /**
   * 输入格式类（如TextInputFormat），控制输入分片方式。	
   * 
   * @return the {@link InputFormat} class for the job.
   */
  public Class<? extends InputFormat<?,?>> getInputFormatClass() 
     throws ClassNotFoundException;

  /**
   * 获取用户定义的Mapper类，实现map()逻辑。
   * 
   * @return the {@link Mapper} class for the job.
   */
  public Class<? extends Mapper<?,?,?,?>> getMapperClass() 
     throws ClassNotFoundException;

  /**
   * Get the combiner class for the job.
   * 
   * @return the combiner class for the job.
   */
  public Class<? extends Reducer<?,?,?,?>> getCombinerClass() 
     throws ClassNotFoundException;

  /**
   * 获取用户定义的Reducer类，实现reduce()逻辑。
   * 
   * @return the {@link Reducer} class for the job.
   */
  public Class<? extends Reducer<?,?,?,?>> getReducerClass() 
     throws ClassNotFoundException;

  /**
   * Get the {@link OutputFormat} class for the job.
   * 
   * @return the {@link OutputFormat} class for the job.
   */
  public Class<? extends OutputFormat<?,?>> getOutputFormatClass() 
     throws ClassNotFoundException;

  /**
   * Get the {@link Partitioner} class for the job.
   * 
   * @return the {@link Partitioner} class for the job.
   */
  public Class<? extends Partitioner<?,?>> getPartitionerClass() 
     throws ClassNotFoundException;

  /**
   * 返回键排序的RawComparator，影响Shuffle阶段的排序顺序。
   * 
   * @return the {@link RawComparator} comparator used to compare keys.
   */
  public RawComparator<?> getSortComparator();

  /**
   * 获取用户代码的JAR路径，框架将其分发到集群节点。
   * @return the pathname
   */
  public String getJar();

  /**
   * Get the user defined {@link RawComparator} comparator for
   * grouping keys of inputs to the combiner.
   *
   * @return comparator set by the user for grouping values.
   * @see Job#setCombinerKeyGroupingComparatorClass(Class)
   */
  public RawComparator<?> getCombinerKeyGroupingComparator();

    /**
     * 定义Reduce阶段的分组逻辑，决定哪些键进入同一reduce()调用。
     *
     * @return comparator set by the user for grouping values.
     * @see Job#setGroupingComparatorClass(Class)
     * @see #getCombinerKeyGroupingComparator()
     */
  public RawComparator<?> getGroupingComparator();
  
  /**
   * 是否执行作业级别的初始化/清理操作。
   * 
   * @return boolean 
   */
  public boolean getJobSetupCleanupNeeded();
  
  /**
   * 	是否在任务完成后清理临时文件。
   * 
   * @return boolean 
   */
  public boolean getTaskCleanupNeeded();

  /**
   * 	是否启用任务性能分析（如CPU、内存使用）。
   * @return true if some tasks will be profiled
   */
  public boolean getProfileEnabled();

  /**
   * 	返回性能分析工具参数（如hprof配置）。
   * 
   * @return the parameters to pass to the task child to configure profiling
   */
  public String getProfileParams();

  /**
   * Get the range of maps or reduces to profile.
   * @param isMap is the task a map?
   * @return the task ranges
   */
  public IntegerRanges getProfileTaskRange(boolean isMap);

  /**
   * 返回提交作业的用户名，用于权限控制和日志记录。
   * 
   * @return the username
   */
  public String getUser();
  
  /**
   * Originally intended to check if symlinks should be used, but currently
   * symlinks cannot be disabled.
   * @return true
   */
  @Deprecated
  public boolean getSymlink();
  
  /**
   * Get the archive entries in classpath as an array of Path
   */
  public Path[] getArchiveClassPaths();

  /**
   * 获取需缓存的压缩包URI列表（如JAR、ZIP），自动解压到任务工作目录。
   * @throws IOException
   */
  public URI[] getCacheArchives() throws IOException;

  /**
   * 获取需缓存的文件URI列表，直接本地化到任务节点。
   * @throws IOException
   */

  public URI[] getCacheFiles() throws IOException;

  /**
   * Return the path array of the localized caches
   * @return A path array of localized caches
   * @throws IOException
   * @deprecated the array returned only includes the items the were 
   * downloaded. There is no way to map this to what is returned by
   * {@link #getCacheArchives()}.
   */
  @Deprecated
  public Path[] getLocalCacheArchives() throws IOException;

  /**
   * Return the path array of the localized files
   * @return A path array of localized files
   * @throws IOException
   * @deprecated the array returned only includes the items the were 
   * downloaded. There is no way to map this to what is returned by
   * {@link #getCacheFiles()}.
   */
  @Deprecated
  public Path[] getLocalCacheFiles() throws IOException;

  /**
   * Get the file entries in classpath as an array of Path
   */
  public Path[] getFileClassPaths();
  
  /**
   * Get the timestamps of the archives.  Used by internal
   * DistributedCache and MapReduce code.
   * @return a string array of timestamps 
   */
  public String[] getArchiveTimestamps();

  /**
   * Get the timestamps of the files.  Used by internal
   * DistributedCache and MapReduce code.
   * @return a string array of timestamps 
   */
  public String[] getFileTimestamps();

  /** 
   * Map任务最大重试次数，提高容错性。
   *  
   * @return the max number of attempts per map task.
   */
  public int getMaxMapAttempts();

  /** 
   * 	Reduce任务最大重试次数。
   * 
   * @return the max number of attempts per reduce task.
   */
  public int getMaxReduceAttempts();

}
