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
package parquet.FileReader;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by pchandra on 5/5/16.
 * Reads (in one thread) an entire column, one block of data at a time.
 * Block size is 8 MB
 */
public class RunnableBlockReader extends RunnableReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RunnableBlockReader.class);


  private boolean shutdown = false;

  public RunnableBlockReader(BufferAllocator allocator, Configuration dfsConfig, FileStatus fileStatus,
      ParquetTableReader.ColumnInfo columnInfo, int bufsize, boolean enableHints) throws IOException {
    super(allocator, dfsConfig, fileStatus, columnInfo, bufsize, enableHints);
  }

  @Override public void run() {
    String fileName = fileStatus.getPath().toString();
    Thread.currentThread().setName("[" + fileName + "]." + columnInfo.columnName);
    stopwatch.start();
    reader.init();
    while (!shutdown && true) {
      try {
        DrillBuf buf = reader.getNext(BUFSZ - 1);
        if (buf == null) {
          break;
        }
        buf.release();
      } catch (Exception e) {
        e.printStackTrace();
        break;
      }
    }
    elapsedTime = stopwatch.elapsed(TimeUnit.MICROSECONDS);
    logger.info("[COMPLETED]\t{}\t{}\t{}\t{}\t{}", fileName, columnInfo.columnName, columnInfo.totalSize,
        elapsedTime, (columnInfo.totalSize*1000000)/(elapsedTime*1024*1024));
    try {
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void shutdown(){
    this.shutdown=true;
  }


}
