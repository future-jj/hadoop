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

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * 描述文件的分片信息（如路径、起始偏移、长度、存储节点），
 * 支持Hadoop的数据本地性优化和分布式计算。
 * 为MapReduce任务提供文件分片的元数据，帮助框架将任务调度到数据所在节点。
 * */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FileSplit extends InputSplit implements Writable {


  private Path file;  //  文件路径（如HDFS路径），标识分片所属文件。
  private long start; //  分片在文件中的起始字节偏移量。
  private long length;  //  分片长度（字节数）。
  private String[] hosts; //  存储该分片的节点列表（如DataNode主机名）。
  private SplitLocationInfo[] hostInfos;  //  分片存储位置的详细信息（如是否在内存中）。

  public FileSplit() {}

  /** Constructs a split with host information
   *
   * @param file the file name
   * @param start the position of the first byte in the file to process
   * @param length the number of bytes in the file to process
   * @param hosts the list of hosts containing the block, possibly null
   */
  public FileSplit(Path file, long start, long length, String[] hosts) {
    this.file = file;
    this.start = start;
    this.length = length;
    this.hosts = hosts;
  }
  
  /** Constructs a split with host and cached-blocks information
   * 
  */
 public FileSplit(Path file, long start, long length, String[] hosts, String[] inMemoryHosts) {
  //  调用基础构造函数
   this(file, start, length, hosts);
   // 构建hostInfos 标记那些节点在内存中缓存了数据
   hostInfos = new SplitLocationInfo[hosts.length];
   for (int i = 0; i < hosts.length; i++) {
     // because N will be tiny, scanning is probably faster than a HashSet
     boolean inMemory = false;
     for (String inMemoryHost : inMemoryHosts) {
       if (inMemoryHost.equals(hosts[i])) {
         inMemory = true;
         break;
       }
     }
     hostInfos[i] = new SplitLocationInfo(hosts[i], inMemory);
   }
 }
 
  /** The file containing this split's data. */
  public Path getPath() { return file; }
  
  /** The position of the first byte in the file to process. */
  public long getStart() { return start; }
  
  /** The number of bytes in the file to process. */
  @Override
  public long getLength() { return length; }

  @Override
  public String toString() { return file + ":" + start + "+" + length; }

  ////////////////////////////////////////////
  // Writable methods
  ////////////////////////////////////////////

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, file.toString()); //  写入文件路径
    out.writeLong(start); //  起始偏移
    out.writeLong(length);  //  分片长度
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    file = new Path(Text.readString(in)); //  读取路径
    start = in.readLong();  //  起始偏移
    length = in.readLong(); //  分片长度
    hosts = null; //  反序列化后需重新设置（可能从文件系统获取）
  }

  @Override
  public String[] getLocations() throws IOException {
    if (this.hosts == null) {
      return new String[]{};
    } else {
      return this.hosts;
    }
  }
  
  @Override
  @Evolving
  public SplitLocationInfo[] getLocationInfo() throws IOException {
    return hostInfos;
  }
}
