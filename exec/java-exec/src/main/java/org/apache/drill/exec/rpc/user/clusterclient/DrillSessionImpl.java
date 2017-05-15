/*
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
package org.apache.drill.exec.rpc.user.clusterclient;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.proto.UserProtos.CancelQueryWithSessionHandle;
import org.apache.drill.exec.proto.UserProtos.RpcType;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.proto.UserProtos.RunQueryWithSessionHandle;
import org.apache.drill.exec.proto.UserProtos.SessionHandle;
import org.apache.drill.exec.rpc.ConnectionThrottle;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;

import java.util.concurrent.ConcurrentMap;

public class DrillSessionImpl implements DrillSession {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSessionImpl.class);

  private final SessionHandle sessionHandle;
  private final DrillConnectionImpl connection;
  private final ConcurrentMap<QueryId, QueryId> queries = Maps.newConcurrentMap();

  DrillSessionImpl(DrillConnectionImpl connection, SessionHandle sessionHandle) {
    this.connection = connection;
    this.sessionHandle = sessionHandle;
  }

  @Override
  public BufferAllocator getAllocator() {
    return connection.getAllocator();
  }

  @Override
  public void executeStatement(final String sql, final UserResultsListener resultsListener) {
    connection.send(connection.getResultHandler()
            .getWrappedListener(wrapUserResultListener(resultsListener)),
        RpcType.RUN_QUERY_WITH_SESSION,
        RunQueryWithSessionHandle.newBuilder()
            .setRunQuery(RunQuery.newBuilder()
                .setType(QueryType.SQL)
                .setPlan(sql)
                .build())
            .setSessionHandle(sessionHandle)
            .build(),
        QueryId.class);
  }

  private UserResultsListener wrapUserResultListener(final UserResultsListener resultsListener) {
    return new UserResultsListener() {
      QueryId queryId;

      @Override
      public void queryIdArrived(QueryId queryId) {
        this.queryId = queryId;
        queries.putIfAbsent(queryId, queryId);
        resultsListener.queryIdArrived(queryId);
      }

      @Override
      public void submissionFailed(UserException ex) {
        queries.remove(queryId);
        resultsListener.submissionFailed(ex);
      }

      @Override
      public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
        resultsListener.dataArrived(result, throttle);
      }

      @Override
      public void queryCompleted(QueryState state) {
        queries.remove(queryId);
        resultsListener.queryCompleted(state);
      }
    };
  }

  @Override
  public DrillRpcFuture<Ack> cancelQuery(final QueryId queryId) {
    return connection.send(RpcType.CANCEL_QUERY_WITH_SESSION,
        CancelQueryWithSessionHandle.newBuilder()
            .setSessionHandle(sessionHandle)
            .setQueryId(queryId)
            .build(),
        Ack.class);
  }

  public void close(boolean invokedDirectly) {
    if (invokedDirectly) {
      connection.sessionClosedDirectly(sessionHandle);
    }

    connection.getResultHandler()
        .failAndRemoveListeners(
            new Predicate<QueryId>() {
              @Override
              public boolean apply(final QueryId input) {
                return queries.containsKey(input);
              }
            },
            UserException.connectionError()
                .message("Session closed."));
  }

  @Override
  public void close() {
    close(true);
  }
}
