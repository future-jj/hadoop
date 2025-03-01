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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.mapred.SplitLocationInfo;

/**
 * 这段代码是Hadoop 框架中的InputSplit 抽象类，用于表示输入数据的分片，
 * 以便在MapReduce 中并行处理。以下是关键部分的解释：
 * 作用：Input代表输入数据的一个逻辑分片，每一个分片由一个Map任务去处理。
 * 它提供了分片的大小、存储的位置信息，帮助Hadoop优化任务调度
 * @see InputFormat
 * @see RecordReader
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class InputSplit {
  /**
   * 返回分片的大小（如字节数），用于排序和资源分配。
   * @return the number of bytes in the split
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract long getLength() throws IOException, InterruptedException;

  /**
   * 返回存储该分片数据的节点名称（HDFS的DataNode）,利用数据本地减少网络传输
   * @return a new array of the node nodes.
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract String[] getLocations() throws IOException, InterruptedException;
  
  /**
   * 返回分片存储位置的详细信息（如磁盘或内存），默认返回null表示所有数据在磁盘
   * @return list of <code>SplitLocationInfo</code>s describing how the split
   *    data is stored at each location. A null value indicates that all the
   *    locations have the data stored on disk.
   * @throws IOException
   */
  //  @Evolving表明该API可能仍在演进中，未来版本可能调整
  @Evolving
  public SplitLocationInfo[] getLocationInfo() throws IOException {
    return null;
  }
}