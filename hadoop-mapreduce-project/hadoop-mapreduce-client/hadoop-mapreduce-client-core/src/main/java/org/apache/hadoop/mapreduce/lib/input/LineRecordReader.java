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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.functional.FutureIO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_SPLIT_END;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_SPLIT_START;

/**
 * Treats keys as offset in file and value as line.
 */
@InterfaceAudience.LimitedPrivate({ "MapReduce", "Pig" })
@InterfaceStability.Evolving
/**
 * 逐行读取文本文件的RecordReader实现，将文件偏移量作为键
 */
public class LineRecordReader extends RecordReader<LongWritable, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(LineRecordReader.class);
  public static final String MAX_LINE_LENGTH = "mapreduce.input.linerecordreader.line.maxlength";

  // 输入分片的起始偏移量
  private long start;
  // 当前读取位置
  private long pos;
  // 输入分片的结束偏移量
  private long end;
  // 行读取器，处理压缩与非压缩数据的读取逻辑。
  private SplitLineReader in;
  // 文件输入流，用于访问HDFS文件。
  private FSDataInputStream fileIn;
  //
  private Seekable filePosition;
  // 单行最大长度（由mapreduce.input.linerecordreader.line.maxlength配置）
  private int maxLineLength;
  // 当前记录的键（文件偏移量）。
  private LongWritable key;
  // 当前记录的值（行内容）。
  private Text value;
  // 标识输入是否为压缩格式。
  private boolean isCompressedInput;
  // 解压缩器实例（用于压缩文件）。
  private Decompressor decompressor;

  private byte[] recordDelimiterBytes;

  public LineRecordReader() {
  }

  public LineRecordReader(byte[] recordDelimiter) {
    this.recordDelimiterBytes = recordDelimiter;
  }

  /**
   * 初始化一个输入分片，处理压缩、分片边界对齐、记录跳转等逻辑，确保数据正确分割
   * 1、解析输入分片信息
   * 2、处理压缩文件
   * 3、调整分片边界以对齐压缩块或边界行
   * 4、跳过非起始分片的首条记录
   * 
   */
  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
    start = split.getStart(); //  分片起始字节位置
    end = start + split.getLength();  //  分片的结束位置
    final Path file = split.getPath();  //  文件路径
    //  通过FileSystem打开文件流，设置分片范围选项。
    final FutureDataInputStreamBuilder builder = file.getFileSystem(job).openFile(file);
    //  设置分片范围（start 到 end），提示底层文件系统优化读取。允许底层存储系统优化读取操作
    builder.optLong(FS_OPTION_OPENFILE_SPLIT_START, start);
    builder.optLong(FS_OPTION_OPENFILE_SPLIT_END, end);
    FutureIO.propagateOptions(builder, job, MRJobConfig.INPUT_FILE_OPTION_PREFIX, MRJobConfig.INPUT_FILE_MANDATORY_PREFIX);
    //  通过异步方式构建输入流，等待其完成后返回 fileIn（实际类型为 FSDataInputStream）。
    fileIn = FutureIO.awaitFuture(builder.build());
    try {
      //  检测文件是否压缩（通过CompressionCodecFactory）。
      CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
      if (null != codec) {
        isCompressedInput = true;
        //  从对象池获取解压器，避免重复创建，提升性能。
        decompressor = CodecPool.getDecompressor(codec);
        // 若为可分割压缩格式（如BZip2），创建CompressedSplitLineReader并调整分片边界。
        if (codec instanceof SplittableCompressionCodec) {
          //  createInputStream 根据分片范围（start, end） 创建支持分割的压缩流
          final SplitCompressionInputStream cIn = ((SplittableCompressionCodec) codec).createInputStream(
              fileIn, decompressor, start, end, SplittableCompressionCodec.READ_MODE.BYBLOCK);
          //  专为可分割压缩设计的行读取器，处理跨压缩块的记录
          in = new CompressedSplitLineReader(cIn, job, this.recordDelimiterBytes);
          start = cIn.getAdjustedStart(); //  调整后的实际起始位置
          end = cIn.getAdjustedEnd(); // 调整后的实际位置
          filePosition = cIn;
        } else {
          //  若为不可分割压缩格式（如GZIP），仅在分片起始位置为0时允许读取，否则抛出异常。
          if (start != 0) {
            // So we have a split that is only part of a file stored using
            // a Compression codec that cannot be split.
            throw new IOException("Cannot seek in " + codec.getClass().getSimpleName() + " compressed stream");
          }
          in = new SplitLineReader(codec.createInputStream(fileIn, decompressor), job, this.recordDelimiterBytes);
          filePosition = fileIn;
        }
      } else {
        fileIn.seek(start);
        in = new UncompressedSplitLineReader(fileIn, job, this.recordDelimiterBytes, split.getLength());
        filePosition = fileIn;
      }
      // If this is not the first split, we always throw away first record
      // because we always (except the last split) read one extra line in
      // next() method.
      if (start != 0) {
        start += in.readLine(new Text(), 0, maxBytesToConsume(start));
      }
      this.pos = start;
    } catch (Exception e) {
      fileIn.close();
      throw e;
    }
  }

  /**
   * 计算单次读取的最大字节数
   * 根据输入是否为压缩格式，决定单次读取的最大字节数，避免内存溢出或无效读取。
   * @param pos
   * @return
   */
  private int maxBytesToConsume(long pos) {
    return isCompressedInput
        ? Integer.MAX_VALUE //  压缩文件，允许读取尽可能多的数据
        : (int) Math.max(Math.min(Integer.MAX_VALUE, end - pos), maxLineLength);  // 非压缩文件：取剩余分片长度和maxLineLength的最大值
  }

  /**
   * 返回当前处理位置（字节偏移量），用于进度跟踪、错误恢复或日志记录。
   * 
   * @return
   * @throws IOException
   */
  private long getFilePosition() throws IOException {
    long retVal;
    if (isCompressedInput && null != filePosition) {
      //  可分割压缩格式：SplitCompressionInputStream（如 BZip2）。
      //  不可分割压缩格式：原始文件流 fileIn（如 GZIP）
      retVal = filePosition.getPos(); // 压缩文件：从压缩流获取实际位置
    } else {
      retVal = pos; // 非压缩文件：直接返回记录的pos变量
    }
    return retVal;
  }

  /**
   * 跳过文件开头的 UTF-8 BOM（3字节 0xEFBBBF），避免其污染数据内容。
   * ​扩展行长度限制：临时允许读取 maxLineLength + 3 字节，确保 BOM 能被完整读取。
   * 读取并检查 BOM：若发现 BOM，调整 value 内容，移除前3字节。
   * 更新位置：pos 始终记录文件原始偏移（包含 BOM），但后续读取会跳过 BOM。
   * 文件开头有 BOM，readLine 读取前3字节，pos 增加3。处理后，value 内容为空，newSize 返回0。
   * @return
   * @throws IOException
   */
  private int skipUtfByteOrderMark() throws IOException {
    //  临时扩展最大行的限制，确保能够正确地读取包含UTF-8 的文件首行
    //  为什么要与Integer.MAX_VALUE取最小值，
    // 若 maxLineLength 接近或等于该值，3L + maxLineLength 可能超过 Integer.MAX_VALUE，导致溢出为负数。
    int newMaxLineLength = (int) Math.min(3L + (long) maxLineLength, Integer.MAX_VALUE);
    //  读取一行（可能包含BOM）
    int newSize = in.readLine(value, newMaxLineLength, maxBytesToConsume(pos));
    pos += newSize; //  更新位置
    int textLength = value.getLength();
    //  检查是否为BOM （0xEF, 0xBB, 0xBF）
    byte[] textBytes = value.getBytes();
    if ((textLength >= 3) && (textBytes[0] == (byte) 0xEF) &&
        (textBytes[1] == (byte) 0xBB) && (textBytes[2] == (byte) 0xBF)) {

      LOG.info("Found UTF-8 BOM and skipped it");
      textLength -= 3;
      newSize -= 3; //  实际有效的数据长度
      if (textLength > 0) {
        // It may work to use the same buffer and not do the copyBytes
        textBytes = value.copyBytes();
        //  调整内容，跳过前3字节
        value.set(textBytes, 3, textLength);
      } else {
        value.clear();
      }
    }
    return newSize;
  }

  /**
   * 读取下一个键值对
   */
  public boolean nextKeyValue() throws IOException {
    //  初始化键值对
    if (key == null) {
      key = new LongWritable();
    }
    //  key为起始偏移量
    key.set(pos);
    if (value == null) {
      value = new Text();
    }
    int newSize = 0;
    // 循环读取，指导满足条件
    while (getFilePosition() <= end || in.needAdditionalRecordAfterSplit()) {
      if (pos == 0) {
        //  处理BOM
        newSize = skipUtfByteOrderMark();
      } else {
        newSize = in.readLine(value, maxLineLength, maxBytesToConsume(pos));
        pos += newSize;
      }

      if ((newSize == 0) || (newSize < maxLineLength)) {
        break;
      }

      // line too long. try again
      LOG.info("Skipped line of size {} at pos {} ", + newSize , (pos - newSize));
    }
    if (newSize == 0) {
      key = null;
      value = null;
      return false;
    } else {
      return true;
    }
  }

  @Override
  public LongWritable getCurrentKey() {
    return key;
  }

  @Override
  public Text getCurrentValue() {
    return value;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (getFilePosition() - start) / (float) (end - start));
    }
  }

  public synchronized void close() throws IOException {
    try {
      if (in != null) {
        in.close();
      }
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
        decompressor = null;
      }
    }
  }
}
