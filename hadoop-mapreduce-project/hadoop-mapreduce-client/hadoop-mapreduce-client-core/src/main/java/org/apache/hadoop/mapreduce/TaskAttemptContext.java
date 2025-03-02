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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Progressable;

/**
 * 这段代码是 Hadoop 框架中 TaskAttemptContext 接口的定义，用于为 MapReduce 任务的单次尝试（如一个 Map 或 Reduce 任务的一次执行）提供上下文信息
 * 继承自 JobContext
 * 提供作业级别的配置信息（如 Configuration 对象）和作业 ID（JobID），允许任务访问全局配置。
 * 继承自 Progressable
 * 定义 progress() 方法（未显式写出），用于任务定期报告存活状态，防止被系统误判为超时。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface TaskAttemptContext extends JobContext, Progressable {

  /**
   * 获取当前任务尝试的唯一标识符。	
   */
  public TaskAttemptID getTaskAttemptID();

  /**
   * 	设置任务尝试的当前状态信息。
   */
  public void setStatus(String msg);

  /**
   * 获取最后一次设置的任务状态信息。
   */
  public String getStatus();
  
  /**
   * 获取任务尝试的进度。
   */
  public abstract float getProgress();

  /**
   * 通过枚举类型获取计数器实例。
   */
  public Counter getCounter(Enum<?> counterName);

  /**
   * 通过组名和计数器名获取自定义计数器实例。
   */
  public Counter getCounter(String groupName, String counterName);

}