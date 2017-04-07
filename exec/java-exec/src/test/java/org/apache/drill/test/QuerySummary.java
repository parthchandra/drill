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

import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.helper.QueryIdHelper;

/**
 * Summary results of a query: records, batches, run time.
 */

public class QuerySummary {
  private final QueryId queryId;
  private final int records;
  private final int batches;
  private final long ms;
  private final QueryState finalState;
  private final Exception error;

  public QuerySummary(QueryId queryId, int recordCount, int batchCount, long elapsed, QueryState state) {
    this.queryId = queryId;
    records = recordCount;
    batches = batchCount;
    ms = elapsed;
    finalState = state;
    error = null;
  }

  public QuerySummary(QueryId queryId, int recordCount, int batchCount, long elapsed, Exception ex) {
    this.queryId = queryId;
    records = recordCount;
    batches = batchCount;
    ms = elapsed;
    finalState = null;
    error = ex;
  }

  public boolean failed() { return error != null; }
  public boolean succeeded() { return error == null; }
  public long recordCount() { return records; }
  public int batchCount() { return batches; }
  public long runTimeMs() { return ms; }
  public QueryId queryId() { return queryId; }
  public String queryIdString() { return QueryIdHelper.getQueryId(queryId); }
  public Exception error() { return error; }
  public QueryState finalState() { return finalState; }
}
