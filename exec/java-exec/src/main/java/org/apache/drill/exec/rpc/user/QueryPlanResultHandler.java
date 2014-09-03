/**
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
package org.apache.drill.exec.rpc.user;

import io.netty.buffer.ByteBuf;

import java.util.concurrent.ConcurrentMap;

import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserProtos.QueryPlanFragments;
import org.apache.drill.exec.rpc.BaseRpcOutcomeListener;
import org.apache.drill.exec.rpc.RpcBus;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;

import com.google.common.collect.Maps;

public class QueryPlanResultHandler {
  private ConcurrentMap<QueryId, UserQueryPlanResultListener> resultsListener = Maps.newConcurrentMap();


  public RpcOutcomeListener<QueryId> getWrappedListener(UserQueryPlanResultListener listener){
    return new SubmissionListener(listener);
  }

  public void batchArrived(ConnectionThrottle throttle, ByteBuf pBody, ByteBuf dBody) throws RpcException {
    final QueryPlanFragments result = RpcBus.get(pBody, QueryPlanFragments.PARSER);

    UserQueryPlanResultListener l = resultsListener.get(result.getQueryId());
    l.resultsArrived(result);
    resultsListener.remove(result.getQueryId(), l);
  }

  private class SubmissionListener extends BaseRpcOutcomeListener<QueryId> {
    private UserQueryPlanResultListener listener;

    public SubmissionListener(UserQueryPlanResultListener listener) {
      super();
      this.listener = listener;
    }

    @Override
    public void failed(RpcException ex) {
      // listener.submissionFailed(ex);
    }

    @Override
    public void success(QueryId queryId, ByteBuf buf) {
      //listener.queryIdArrived(queryId);
      resultsListener.putIfAbsent(queryId, listener);
    }
  }
}
