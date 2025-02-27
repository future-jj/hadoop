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
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/** 
 * 输入验证：确保作业的输入路径正确且数据可访问
 * 逻辑分片：将输入数据切分成多个逻辑分片，每一个分片由一个Mapper处理。
 * 分片不一定是物理切割，而是记录数据的位置和范围（如文件路径、起始偏移量、长度）
 * 记录读取：提供读取分片数据的机制，将原始数据转换为键值对共Mapper处理。
 * @see InputSplit
 * @see RecordReader
 * @see FileInputFormat
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class InputFormat<K, V> {

  /** 
   * 生成逻辑分片列表，每一个分片对应一个Mapper任务的输入
   * 关键点：
   * 分片是逻辑的，不实际切割数据文件
   * 分片信息可能包含元数据（如文件路径，偏移量）
   * 
   * 实现细节：
   * 默认基于文件大小分片（FileInputFormat），分片大小上限为HDFS块大小
   * 可通过配置mapreduce.input.fileinputformat.split.minsize设置最小分片大小
   * @param context 包含作业配置信息（如输入路径、文件块大小）
   * @return an array of {@link InputSplit}s for the job.
   */
  public abstract 
    List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException;
  
  /**
   * 创建RecordReader 实例，用于从分片中读取记录
   * RecordReader 负责解析分片数据，按记录边界生成键值对
   * @param split 当前处理的分片
   * @param context 任务执行上下文（配置、计数器）
   * @return a new record reader
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract 
    RecordReader<K,V> createRecordReader(InputSplit split,
                                         TaskAttemptContext context
                                        ) throws IOException, 
                                                 InterruptedException;

}

