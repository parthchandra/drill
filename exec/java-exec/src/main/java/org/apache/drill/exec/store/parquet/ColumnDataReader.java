/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.parquet;

import com.google.common.base.Stopwatch;
import io.netty.buffer.DrillBuf;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.parquet.ShouldNeverHappenException;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.util.CompatibilityUtil;

public class ColumnDataReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ColumnDataReader.class);

  private final long endPosition;
  public final FSDataInputStream input;

  public ColumnDataReader(FSDataInputStream input, long start, long length) throws IOException{
    this.input = input;
    this.input.seek(start);
    this.endPosition = start + length;
  }

  public PageHeader readPageHeader() throws IOException{
    return Util.readPageHeader(input);
  }

  public FSDataInputStream getInputStream() {
    return input;
  }

  public BytesInput getPageAsBytesInput(int pageLength) throws IOException{
    byte[] b = new byte[pageLength];
    input.read(b);
    return new HadoopBytesInput(b);
  }

  public void loadPage(DrillBuf target, int pageLength) throws IOException {
    target.clear();
    ByteBuffer directBuffer = target.nioBuffer(0, pageLength);
    int lengthLeftToRead = pageLength;
    while (lengthLeftToRead > 0) {
      //lengthLeftToRead -= CompatibilityUtil.getBuf(input, directBuffer, lengthLeftToRead);
      lengthLeftToRead -= this.getBuf(input, directBuffer, lengthLeftToRead);
      //lengthLeftToRead -= input.read(directBuffer);
    }
    target.writerIndex(pageLength);
  }

  public void clear(){
    try{
      input.close();
    }catch(IOException ex){
      logger.warn("Error while closing input stream.", ex);
    }
  }

  public boolean hasRemainder() throws IOException{
    return input.getPos() < endPosition;
  }

  private static boolean useByteBufferRead=true;
  public static int getBuf(FSDataInputStream f, ByteBuffer readBuf, int maxSize) throws IOException {
    int res;
    long timeToRead = 0;
    Stopwatch timer = new Stopwatch();
    long offset = f.getPos();
    if(useByteBufferRead) {
      try {
        timer.start();
        res = f.read(readBuf);
        timeToRead = timer.elapsed(TimeUnit.MICROSECONDS);
        logger.trace("ParquetTrace,DataRead,ByteBuffer,,,{},{},{},{}", offset, maxSize, maxSize, timeToRead);
      } catch (Exception var5) {
        if(var5 instanceof UnsupportedOperationException) {
          useByteBufferRead = false;
          return getBuf(f, readBuf, maxSize);
        }
        if(var5.getCause() instanceof IOException) {
          throw (IOException)var5.getCause();
        }
        throw new IOException("Error reading out of an FSDataInputStream using the Hadoop 2 ByteBuffer based read method.", var5.getCause());
      }
    } else {
      byte[] buf = new byte[maxSize];
      timer.start();
      res = f.read(buf);
      timeToRead = timer.elapsed(TimeUnit.MICROSECONDS);
      timer.reset();
      logger.trace("ParquetTrace,DataRead,ByteArray,,,{},{},{},{}", offset, maxSize, maxSize, timeToRead);
      timer.start();
      readBuf.put(buf, 0, res);
      timeToRead = timer.elapsed(TimeUnit.MICROSECONDS);
      logger.trace("ParquetTrace,DataCopy,ByteArray,,,{},{},{},{}", offset, maxSize, maxSize, timeToRead);
    }

    return res;
  }

  public class HadoopBytesInput extends BytesInput{

    private final byte[] pageBytes;

    public HadoopBytesInput(byte[] pageBytes) {
      super();
      this.pageBytes = pageBytes;
    }

    @Override
    public byte[] toByteArray() throws IOException {
      return pageBytes;
    }

    @Override
    public long size() {
      return pageBytes.length;
    }

    @Override
    public void writeAllTo(OutputStream out) throws IOException {
      out.write(pageBytes);
    }

  }

}
