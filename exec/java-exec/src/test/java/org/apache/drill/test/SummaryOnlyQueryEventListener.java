/*
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
package org.apache.drill.test;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.rpc.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;

/**
 * Listener used to retrieve the query summary (only) asynchronously
 * using a {@link QuerySummaryFuture}.
 */

public class SummaryOnlyQueryEventListener implements UserResultsListener {

  /**
   * The future to be notified. Created here and returned by the
   * query builder.
   */

  private final QuerySummaryFuture future;
  private QueryId queryId;
  private int recordCount;
  private int batchCount;
  private long startTime;

  public SummaryOnlyQueryEventListener(QuerySummaryFuture future) {
    this.future = future;
    startTime = System.currentTimeMillis();
  }

  @Override
  public void queryIdArrived(QueryId queryId) {
    this.queryId = queryId;
  }

  @Override
  public void submissionFailed(UserException ex) {
    future.completed(
        new QuerySummary(queryId, recordCount, batchCount,
                         System.currentTimeMillis() - startTime, ex));
  }

  @Override
  public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
    batchCount++;
    recordCount += result.getHeader().getRowCount();
    result.release();
  }

  @Override
  public void queryCompleted(QueryState state) {
    future.completed(
        new QuerySummary(queryId, recordCount, batchCount,
                         System.currentTimeMillis() - startTime, state));
  }
}
