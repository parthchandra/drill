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
package org.apache.drill.exec.inputformat;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;

import com.google.common.collect.Queues;

/**
 * Listener on getting QueryResultBatch
 * queues those results so consumers of this Listener 
 * can get QueryResultBatch for it's consumption
 *
 */
public class StreamingListener implements UserResultsListener {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StreamingListener.class);
  private static final int MAX = 100;
  private volatile RpcException ex;
  private volatile boolean completed = false;
  private volatile boolean autoread = true;
  private volatile ConnectionThrottle throttle;
  private volatile boolean closed = false;
  private QueryId queryId;

  final LinkedBlockingDeque<QueryResultBatch> queue = Queues.newLinkedBlockingDeque();

  public boolean isCompleted() {
    return completed;
  }

  @Override
  public void submissionFailed(RpcException ex) {
    this.ex = ex;
    completed = true;
    close();
    System.out.println("Query failed: " + ex.getMessage());
  }

  @Override
  public void resultArrived(QueryResultBatch result, ConnectionThrottle throttle) {
    logger.debug("Result arrived {}", result);

    if (result.getHeader().hasQueryState() && 
        result.getHeader().getQueryState() == QueryState.COMPLETED && 
        result.getHeader().getRowCount() == 0) {
      if (result.getHeader().getIsLastChunk()) {
        completed = true;
      }
      result.release();
      return;
    }

    // if we're in a closed state, just release the message.
    if (closed) {
      result.release();
      completed = true;
      return;
    }

    // Do not add 0 results to the queue - may need to change
    if (result != null && 
        result.getHeader().getRowCount() == 0) {
      result.release();
      return;
    }
    // we're active, let's add to the queue.
    queue.add(result);
    if (queue.size() >= MAX - 1) {
      throttle.setAutoRead(false);
      this.throttle = throttle;
      autoread = false;
    }

    if (result.getHeader().getIsLastChunk()) {
      completed = true;
    }

    if (result.getHeader().getErrorCount() > 0) {
      submissionFailed(new RpcException(String.format("%s", result.getHeader().getErrorList())));
    }
  }

  public QueryResultBatch getNext() throws RpcException, InterruptedException {
    while (true) {
      if (ex != null)
        throw ex;
      if (completed && queue.isEmpty()) {
        return null;
      } else {
        QueryResultBatch q = queue.poll(50, TimeUnit.MILLISECONDS);
        if (q != null) {
          if (!autoread && queue.size() < MAX / 2) {
            autoread = true;
            throttle.setAutoRead(true);
            throttle = null;
          }
          return q;
        }

      }

    }
  }

  void close() {
    closed = true;
    while (!queue.isEmpty()) {
      QueryResultBatch qrb = queue.poll();
      if(qrb != null && qrb.getData() != null) qrb.getData().release();
    }
    completed = true;
  }

  @Override
  public void queryIdArrived(QueryId queryId) {
    this.queryId = queryId;
  }
}
