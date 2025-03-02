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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 是Hadoop MapReduce 框架中负责从输入分片（InputSplit）逐条读取键值对的核心组件
 * 作为输入处理的关键抽象，它定义了如何将原始数据（文件块）解析为可提供Mapper处理的结构化记录
 * 
 * @param <KEYIN>
 * @param <VALUEIN>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class RecordReader<KEYIN, VALUEIN> implements Closeable {

  /**
   * 初始化读取器
   * 打开文件、连接数据库、定位起始偏移量的时候会使用
   * @param split   输入分片
   * @param context 任务上下文（配置，状态）
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract void initialize(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException;

  /**
   * 读取下一跳记录
   * 遍历文件行、数据库记录，逐条处理。
   * @return true 表示有数据；false 表示结束
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract boolean nextKeyValue() throws IOException, InterruptedException;

  /**
   * 获取当前键	
   * 当前记录的键（如行偏移、ID）
   * 在 Mapper 中作为输入键（如 LongWritable）
   * @return the current key or null if there is no current key
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract KEYIN getCurrentKey() throws IOException, InterruptedException;

  /**
   * Get the current value.
   * 当前记录的值（如文本行、JSON 对象）。
   * 在 Mapper 中作为输入值（如 Text）。
   * @return the object that was read
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract VALUEIN getCurrentValue() throws IOException, InterruptedException;

  /**
   * 报告读取进度
   * 完成度百分比（0.0~1.0）。
   * 任务监控、推测执行决策。
   * @return a number between 0.0 and 1.0 that is the fraction of the data read
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract float getProgress() throws IOException, InterruptedException;

  /**
   * 释放资源	
   * 关闭文件句柄、网络连接等。
   * 任务结束时自动调用，清理资源。
   */
  public abstract void close() throws IOException;
}
