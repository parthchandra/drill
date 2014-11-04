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

import java.util.concurrent.ConcurrentMap;

import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.record.RawFragmentBatch;
import org.apache.drill.exec.work.batch.UnlimitedRawBatchBufferNoAck;

import com.google.common.collect.Maps;

/**
 * Singleton class responsible for maintaining map between FragmentHandle and RawBatchBuffer
 * that is a queue based on the data received from DrillClient
 *
 */
public class DataPushConnectionManager {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DataPushConnectionManager.class);
  
  private static DataPushConnectionManager s_instance = new DataPushConnectionManager();
  
  private final ConcurrentMap<FragmentHandle, UnlimitedRawBatchBufferNoAck> requestHeaders = Maps.newConcurrentMap();
  
  private DataPushConnectionManager() {
    
  }
  
//  @VisibleForTesting
  public int getHeadersCount() {
    return requestHeaders.size();
  }
  public static DataPushConnectionManager getInstance() {
    return s_instance;
  }
  
  
  public UnlimitedRawBatchBufferNoAck getRawBatchBuffer(FragmentHandle handle) {
    logger.info("DataPushConnection: getRawBatchBuffer");
    UnlimitedRawBatchBufferNoAck rawBatchBuffer = requestHeaders.get(handle);
    if ( rawBatchBuffer != null ) {
      return rawBatchBuffer;
    }
    rawBatchBuffer = new UnlimitedRawBatchBufferNoAck(1);
    logger.info("DataPushConnection: getRawBatchBuffer - new");
    UnlimitedRawBatchBufferNoAck retValue = requestHeaders.putIfAbsent(handle, rawBatchBuffer);
    if ( retValue != null ) {
      rawBatchBuffer.cleanup();
    }

    return rawBatchBuffer;
  }

  public void enqueueData(FragmentHandle handle, RawFragmentBatch rawBatch) {
    UnlimitedRawBatchBufferNoAck rawBatchBuffer = getRawBatchBuffer(handle);
    rawBatchBuffer.enqueue(rawBatch);
  }
  
  public void cleanRawBatchBuffer(FragmentHandle handle) {
    logger.info("DataPushConnectionManager: cleanRawBatchBuffer");    
    requestHeaders.remove(handle);
  }
  
}
