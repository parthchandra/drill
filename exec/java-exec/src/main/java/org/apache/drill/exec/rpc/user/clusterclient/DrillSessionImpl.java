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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
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

import java.util.Set;

public class DrillSessionImpl implements DrillSession {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSessionImpl.class);

  private final SessionHandle sessionHandle;
  private final DrillConnectionImpl connection;
  private final Set<QueryId> queries = Sets.newConcurrentHashSet();

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
    Preconditions.checkArgument(StringUtils.isNotBlank(sql), "sql should not be null or blank");
    Preconditions.checkArgument(resultsListener != null, "resultsListener can not be null");
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
        Preconditions.checkNotNull(queryId);
        this.queryId = queryId;
        String wasAdded = queries.add(queryId) ?  "was" : "was not";
        logger.debug("query {} {} added to known queries", queryId, wasAdded);
        resultsListener.queryIdArrived(queryId);
      }

      @Override
      public void submissionFailed(UserException ex) {
        // It is possible that queryId was not yet assigned, see SubmissionListener.failed()
        String wasRemoved = (queryId != null) && queries.remove(queryId) ? "was" : "was not";
        logger.debug("query {} {} removed from known queries", queryId, wasRemoved, ex);
        resultsListener.submissionFailed(ex);
      }

      @Override
      public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
        logger.debug("data arrived for query {}", queryId);
        // TODO: replace queryId == null check with Preconditions.checkNotNull(queryId);
        if (queryId == null || queries.contains(queryId)) {
          resultsListener.dataArrived(result, throttle);
        }
      }

      @Override
      public void queryCompleted(QueryState state) {
        // TODO: replace queryId == null check with Preconditions.checkNotNull(queryId);
        boolean notify = queryId == null || queries.remove(queryId);
        logger.debug("query {} was completed with state {}, listener {} {} be notified", queryId, state, resultsListener,
            notify ? "will" : "will not");
        if (notify) {
          resultsListener.queryCompleted(state);
        }
      }
    };
  }

  @Override
  public DrillRpcFuture<Ack> cancelQuery(final QueryId queryId) {
    return cancelQuery(queryId, false);
  }

  private DrillRpcFuture<Ack> cancelQuery(final QueryId queryId, boolean notifyWhenCompleted) {
    Preconditions.checkArgument(queryId != null, "queryId can not be null");
    logger.debug("cancelling query {}", queryId);
    if (!notifyWhenCompleted && !queries.remove(queryId) || notifyWhenCompleted && !queries.contains(queryId)) {
      logger.warn("Trying to cancel unknown query {}, query already completed or cancelled?", queryId);
    }
    // TODO: do not send cancellation request for unknown queries to the server
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
                return queries.contains(input);
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
