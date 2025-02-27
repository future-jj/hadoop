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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;

/** 
 * InputFormat 描述了一个MapReduce作业的输入规范
 * MapReduce 框架依赖作业的 InputFormat 完成以下工作：
 * 1. 验证作业的输入规范（例如输入路径是否存在、格式是否正确）
 * 2. 将输入文件分割为逻辑的 InputSplit，每个 InputSplit 会被分配给一个 Mapper 处理。
 * 3. 提供 RecordReader 实现，用于从 InputSplit 中读取数据记录，供 Mapper 处理。  
 * 基于文件的 InputFormat（如 FileInputFormat 的子类）的默认行为是：
 * 根据输入文件的总大小（字节数）生成逻辑 InputSplit。
 * 输入文件的 HDFS 块大小（blockSize）是 InputSplit 的上限。
 * 可以通过配置参数 mapreduce.input.fileinputformat.split.minsize 设置 InputSplit 的最小值。
 * 
 * ​验证输入：检查输入路径是否合法、文件是否存在等。
 * ​逻辑分片：将输入数据划分为多个 InputSplit，每个分片由一个 Mapper 处理。
 * ​数据读取：通过 RecordReader 将 InputSplit 转换为键值对（<K, V>），供 Mapper 处理。
 * @see InputSplit
 * @see RecordReader
 * @see JobClient
 * @see FileInputFormat
 */
@InterfaceAudience.Public //  表示该接口是公开的，可供开发者使用
@InterfaceStability.Stable  //  表示该接口是稳定的，后续版本不会修改或者删除
public interface InputFormat<K, V> {

  /** 
   * 对作业的输入文件进行逻辑分片。
   * 每个 InputSplit 会被分配给一个 Mapper 处理。
   * 分片是逻辑上的，不会物理切割文件。例如，一个分片可以是 <文件路径, 起始偏移量, 长度>。
   * 
   * @param job 作业配置信息（如输入路径、文件格式等）。
   * @param numSplits  期望的分片数量（仅为建议值，实际分片数可能不同）。
   * @return 返回 InputSplit 数组，表示所有分片。
   */
  InputSplit[] getSplits(JobConf job, int numSplits) throws IOException;

  /** 
   * 为给定的 InputSplit 创建 RecordReader。
   *
   * RecordReader 的责任是从逻辑分片中读取数据，并正确处理记录边界，
   * 为 Mapper 提供按记录处理的数据视图。
   * 
   * @param split  对应的 InputSplit。
   * @param job 作业配置信息
   * @param reporter 用于报告任务进度的对象（如进度百分比）。
   * @return 回 RecordReader 实例，用于读取分片中的数据。
   */
  RecordReader<K, V> getRecordReader(InputSplit split,
                                     JobConf job, 
                                     Reporter reporter) throws IOException;
}

