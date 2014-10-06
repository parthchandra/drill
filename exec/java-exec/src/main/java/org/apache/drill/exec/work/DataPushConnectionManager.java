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
package org.apache.drill.exec.work;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.record.RawFragmentBatch;
import org.apache.drill.exec.work.batch.UnlimitedRawBatchBufferNoAck;
import org.apache.drill.exec.work.fragment.FragmentExecutor;

import com.google.common.collect.Maps;

/**
 * Singleton class responsible for maintaining map between FragmentHandle and RawBatchBuffer
 * that is a queue based on the data received from DrillClient
 *
 */
public class DataPushConnectionManager {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DataPushConnectionManager.class);
  
  private static DataPushConnectionManager s_instance = new DataPushConnectionManager();
  
  private final Map<FragmentHandle, UnlimitedRawBatchBufferNoAck> requestHeaders = Maps.newConcurrentMap();
  /**
   * Map of fragmenthandle to lock to wait till fragment is available
   */
  private final Map<FragmentHandle, CountDownLatch> locks = Maps.newConcurrentMap();
  private final Object lock = new Object();
  
  private DataPushConnectionManager() {
    
  }
  
  public static DataPushConnectionManager getInstance() {
    return s_instance;
  }
  
  public UnlimitedRawBatchBufferNoAck createIfNotExistRawBatchBuffer(FragmentContext context) {
    CountDownLatch lockObject = null;
    UnlimitedRawBatchBufferNoAck rawBatchBuffer = requestHeaders.get(context.getHandle());
    if ( rawBatchBuffer == null) {
      // TODO need to get correct number of partitions
      synchronized(lock) {
        rawBatchBuffer = new UnlimitedRawBatchBufferNoAck(context, 1);
        requestHeaders.put(context.getHandle(), rawBatchBuffer);
        lockObject = locks.remove(context.getHandle());
      }
      if ( lockObject != null) {
        lockObject.countDown();
      }
    }
    return rawBatchBuffer;
  }
    
  
  public UnlimitedRawBatchBufferNoAck getRawBatchBuffer(FragmentHandle handle) {
    UnlimitedRawBatchBufferNoAck rawBatchBuffer;
    CountDownLatch lockObject = null;
    while(true) { 
      synchronized(lock) {
        rawBatchBuffer = requestHeaders.get(handle);
        if ( rawBatchBuffer != null ) {
          break;
        }
        lockObject = new CountDownLatch(1);
        locks.put(handle, lockObject);
      }
      try {
        lockObject.await();
      } catch (InterruptedException e) {
        logger.info("Ignoring InterruptedException on this thread");
      }
    }

    return rawBatchBuffer;
  }

  public void remove(FragmentHandle handle) {
    synchronized(lock) {
      requestHeaders.remove(handle);
      CountDownLatch currentLock = locks.remove(handle);
      if ( currentLock != null ) {
        currentLock.countDown();
      }
    }
  }
  
  public void enqueueData(FragmentHandle handle, RawFragmentBatch rawBatch) {
    UnlimitedRawBatchBufferNoAck rawBatchBuffer = getRawBatchBuffer(handle);
    rawBatchBuffer.enqueue(rawBatch);
  }
  
  public void cleanRawBatchBuffer(FragmentHandle handle) {
    requestHeaders.remove(handle);
  }
  
}
