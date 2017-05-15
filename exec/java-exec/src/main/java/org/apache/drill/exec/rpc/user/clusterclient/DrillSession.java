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
package org.apache.drill.exec.rpc.user.clusterclient;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.user.UserResultsListener;

public interface DrillSession {

  /**
   * Gets the {@link BufferAllocator buffer allocator} used by this session.
   *
   * @return buffer allocator
   */
  BufferAllocator getAllocator();

  /**
   * Execute a SQL query, and register the result listener for that query.
   *
   * @param sql sql query
   * @param listener user results listener
   */
  void executeStatement(String sql, UserResultsListener listener);

  /**
   * Cancel query with the given query identifier.
   *
   * @param queryId query identifier
   */
  DrillRpcFuture<Ack> cancelQuery(QueryId queryId);

  /**
   * Release resources, and fail all {@link UserResultsListener listeners}
   * for queries that are still executing within this session.
   **/
  void close();
}
