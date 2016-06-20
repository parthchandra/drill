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
package org.apache.drill.exec.util.filereader;

import io.netty.buffer.DrillBuf;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Seekable;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by pchandra on 5/5/16.
 */
public abstract class BufferedDirectBufInputStream extends FilterInputStream {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BufferedDirectBufInputStream.class);

  protected boolean enableHints = true;
  BufferedDirectBufInputStream(InputStream in, boolean enableHints){
    super(in);
    this.enableHints = enableHints;
  }

  public abstract void init();

  public abstract DrillBuf getNext(int bytes) throws IOException;

  @Override public abstract int read(byte[] b) throws IOException;

  @Override public abstract int read(byte[] b, int off, int len) throws IOException;

  protected static void fadviseIfAvailable(FSDataInputStream inputStream, long off, long n) {
    Method readAhead;
    final Class adviceType;

    try {
      adviceType = Class.forName("org.apache.hadoop.fs.FSDataInputStream$FadviseType");
    } catch (ClassNotFoundException e) {
      logger.info("Unable to call fadvise due to: {}", e.toString());
      readAhead = null;
      return;
    }
    try {
      Class<? extends FSDataInputStream> inputStreamClass = inputStream.getClass();
      readAhead = inputStreamClass.getMethod("adviseFile", new Class[] {adviceType, long.class, long.class});
    } catch (NoSuchMethodException e) {
      logger.info("Unable to call fadvise due to: {}", e.toString());
      readAhead = null;
      return;
    }
    if (readAhead != null) {
      Object[] adviceTypeValues = adviceType.getEnumConstants();
      for(int idx = 0; idx < adviceTypeValues.length; idx++) {
        if((adviceTypeValues[idx]).toString().contains("SEQUENTIAL")) {
          try {
            readAhead.invoke(inputStream, adviceTypeValues[idx], off, n);
          } catch (IllegalAccessException e) {
            logger.info("Unable to call fadvise due to: {}", e.toString());
          } catch (InvocationTargetException e) {
            logger.info("Unable to call fadvise due to: {}", e.toString());
          }
          break;
        }
      }
    }
    return;
  }


}
