/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.record;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.RecordBatchDef;
import org.apache.drill.exec.vector.ValueVector;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Map;

public class RecordBatchLoaderFactory {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RecordBatchLoaderFactory.class);

  private final BufferAllocator allocator;
  private final ReferenceQueue<ValueVector> queue = new ReferenceQueue<>();
  private final Map<WeakReference<ValueVector>, DrillBuf[]> buffersList = Maps.newConcurrentMap();
  private boolean waitingForCompletion = false;

  private final Thread cleaningThread = new Thread(new Runnable() {
    @Override
    public void run() {
      while(!waitingForCompletion || buffersList.size() != 0) {
        try {
          WeakReference<ValueVector> ref = (WeakReference<ValueVector>) queue.remove();
          if (ref != null) {
            DrillBuf[] buffers = buffersList.remove(ref);
            for(DrillBuf buf : buffers) {
              buf.release();
            }
          }
        } catch (Exception e) {
          logger.error("Failed to release ValueVector", e);
        }
      }
    }
  });

  public RecordBatchLoaderFactory(BufferAllocator allocator) {
    this.allocator = allocator;
    cleaningThread.start();
  }

  public RecordBatchLoader load(RecordBatchDef def, DrillBuf buf) throws SchemaChangeException {
    if (waitingForCompletion) {
      throw new SchemaChangeException("RecordBatchLoaderFactory is already closed. Create a new one.");
    }
    RecordBatchLoader loader = new RecordBatchLoader(allocator);
    loader.load(def, buf);

    for(VectorWrapper vw : loader.getContainer()) {
      buffersList.put(new WeakReference<>(vw.getValueVector(), queue), vw.getValueVector().getBuffers(false));
    }

    return loader;
  }

  public void close() throws InterruptedException {
    waitingForCompletion = true;
    // Need to call the GC twice in order to make sure unreferenced ValueVectors are GCed.
    System.gc();
    System.gc();
    cleaningThread.join();
  }
}
