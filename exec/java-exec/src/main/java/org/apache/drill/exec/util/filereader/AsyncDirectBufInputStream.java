/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.util.filereader;

import io.netty.buffer.DrillBuf;
import org.apache.hadoop.fs.ByteBufferReadable;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Created by pchandra on 6/29/16.
 */
public class AsyncDirectBufInputStream extends DirectBufInputStream {

  private ExecutorService executor;


  AsyncDirectBufInputStream(ExecutorService e, InputStream in, boolean enableHints) {
    super(in, enableHints);
    this.executor = e;
  }

  @Override public void init() throws IOException, UnsupportedOperationException {
    if (!(in instanceof ByteBufferReadable)) {
      throw new UnsupportedOperationException("The input stream is not ByteBuffer readable.");
    }
    if (!(in instanceof DirectBufInputStream)) {
      throw new UnsupportedOperationException("The input stream is not Direct Buffer readable.");
    }
    ((DirectBufInputStream)in).init();

  }

  @Override public int read() throws IOException {
    return ((DirectBufInputStream)in).read();
  }

  @Override public int read(DrillBuf buf, int off, int len) throws IOException {
    return ((DirectBufInputStream)in).read(buf, off, len);
  }

  class GetNextCallable implements Callable<DrillBuf> {
      private int bytes;
      GetNextCallable(int bytes){
        this.bytes = bytes;
      }
    @Override public DrillBuf call() throws Exception {
      return ((DirectBufInputStream)in).getNext(bytes);
    }
  }

  /*
  public Future<DrillBuf> getNextAsync(int bytes) throws IOException {
    return executor.submit(new GetNextCallable(bytes));
  }
  */
  public Future<DrillBuf> getNextAsync(final int bytes) throws IOException {
    return executor.submit(new Callable<DrillBuf>(){
      @Override public DrillBuf call() throws Exception {
        return ((DirectBufInputStream)in).getNext(bytes);
      }
    });
  }

  @Override public DrillBuf getNext(int bytes) throws IOException {
    return ((DirectBufInputStream)in).getNext(bytes);
  }

  @Override public long getPos() throws IOException {
    return ((DirectBufInputStream)in).getPos();
  }

  @Override public boolean hasRemainder() throws IOException {
    return ((DirectBufInputStream)in).hasRemainder();
  }


}
