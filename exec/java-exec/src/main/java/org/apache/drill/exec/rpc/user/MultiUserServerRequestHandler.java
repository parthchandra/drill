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
package org.apache.drill.exec.rpc.user;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.exec.physical.impl.materialize.QueryWritableBatch;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult;
import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.proto.UserProtos.CancelQueryWithSessionHandle;
import org.apache.drill.exec.proto.UserProtos.NewSessionRequest;
import org.apache.drill.exec.proto.UserProtos.RpcType;
import org.apache.drill.exec.proto.UserProtos.RunQueryWithSessionHandle;
import org.apache.drill.exec.proto.UserProtos.SessionHandle;
import org.apache.drill.exec.rpc.Acks;
import org.apache.drill.exec.rpc.RequestHandler;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.ResponseSender;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.rpc.UserClientConnection;
import org.apache.drill.exec.work.user.UserWorker;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

// note that changing from this handler to another will destroy the state
public class MultiUserServerRequestHandler implements RequestHandler<UserServer.BitToUserConnection> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MultiUserServerRequestHandler.class);

  private final UserWorker worker;
  private final UserConnectionConfig config;

  private final Map<SessionHandle, UserClientConnection> sessions = new HashMap<>();

  public MultiUserServerRequestHandler(UserWorker worker, UserConnectionConfig config) {
    this.worker = worker;
    this.config = config;
  }

  @Override
  public void handle(final UserServer.BitToUserConnection connection, int rpcType, ByteBuf pBody,
                     ByteBuf dBody, ResponseSender sender) throws RpcException {

    switch (rpcType) {

    case RpcType.NEW_SESSION_VALUE: {
      final NewSessionRequest request;
      try {
        request = NewSessionRequest.PARSER.parseFrom(new ByteBufInputStream(pBody));
      } catch (final InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding NewSessionRequest body.", e);
      }

      final DrillProperties properties = DrillProperties.createEmpty();
      final UserSession connectionSession = connection.getSession();
      properties.merge(connectionSession.getProperties());
      properties.merge(DrillProperties.createFromProperties(request.getProperties(), false));

      final UserSession userSession = UserSession.Builder.newBuilder()
          .withCredentials(UserCredentials.newBuilder()
              .setUserName(connectionSession.getCredentials()
                  .getUserName())
              .build())
          .withOptionManager(worker.getSystemOptions())
          .withUserProperties(properties)
          .setSupportComplexTypes(connectionSession.isSupportComplexTypes())
          .build();

      if (config.getImpersonationManager() != null && userSession.getTargetUserName() != null) {
        config.getImpersonationManager()
            .replaceUserOnSession(userSession.getTargetUserName(), userSession);
      }

      final SessionHandle sessionHandle = SessionHandle.newBuilder()
          .setSessionId(userSession.getSessionId())
          .build();

      sessions.put(sessionHandle, newSession(connection, userSession));
      logger.debug("New session created: {}", sessionHandle.getSessionId());
      sender.send(new Response(RpcType.SESSION_HANDLE, sessionHandle));
      break;
    }

    case RpcType.RUN_QUERY_WITH_SESSION_VALUE: {
      logger.debug("Received query to run. Returning query handle.");

      final RunQueryWithSessionHandle query;
      try {
        query = RunQueryWithSessionHandle.PARSER.parseFrom(new ByteBufInputStream(pBody));
      } catch (final InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding RunQueryWithSessionHandle body.", e);
      }

      final UserClientConnection currentConnection = sessions.get(query.getSessionHandle());
      if (currentConnection == null) {
        throw new RpcException("Unexpected message. Received a query on non-existent session.");
      }

      logger.debug("Received query on session: {}", query.getSessionHandle());
      final QueryId queryId = worker.submitWork(currentConnection, query.getRunQuery());
      sender.send(new Response(RpcType.QUERY_HANDLE, queryId));
      break;
    }

    case RpcType.CLOSE_SESSION_VALUE: {
      final SessionHandle handle;
      try {
        handle = SessionHandle.PARSER.parseFrom(new ByteBufInputStream(pBody));
      } catch (final InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding SessionHandle body.", e);
      }

      final UserClientConnection removedConnection = sessions.remove(handle);
      if (removedConnection != null) {
        if (removedConnection instanceof AutoCloseable) {
          try {
            ((AutoCloseable) removedConnection).close();
          } catch (Exception e) {
            throw new RpcException("Failure while decoding SessionHandle body.", e);
          }
        }
        sender.send(new Response(RpcType.ACK, Acks.OK));
      } else {
        sender.send(new Response(RpcType.ACK, Acks.FAIL));
      }
      break;
    }

    case RpcType.CANCEL_QUERY_WITH_SESSION_VALUE: {
      final CancelQueryWithSessionHandle request;
      try {
        request = CancelQueryWithSessionHandle.PARSER.parseFrom(new ByteBufInputStream(pBody));
      } catch (final InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding CancelQueryWithSessionHandle body.", e);
      }

      if (sessions.get(request.getSessionHandle()) == null) {
        throw new RpcException("Unexpected message. Received a cancellation on query in non-existent session.");
      }

      sender.send(new Response(RpcType.ACK, worker.cancelQuery(request.getQueryId())));
      break;
    }

    default:
      throw new UnsupportedOperationException(
          String.format("MultiUserServerRequestHandler received rpc of unknown type. Type was %d.", rpcType));
    }
  }

  private static class SessionConnection implements UserClientConnection, AutoCloseable {
    final UserServer.BitToUserConnection underlyingConnection;
    final UserSession userSession;

    public SessionConnection(final UserServer.BitToUserConnection underlyingConnection,
                             final UserSession userSession) {
      this.underlyingConnection = underlyingConnection;
      this.userSession = userSession;
    }

    @Override
    public UserSession getSession() {
      return userSession;
    }

    @Override
    public void sendResult(RpcOutcomeListener<Ack> listener, QueryResult result) {
      underlyingConnection.sendResult(listener, result);
    }

    @Override
    public void sendData(RpcOutcomeListener<Ack> listener, QueryWritableBatch result) {
      underlyingConnection.sendData(listener, result);
    }

    @Override
    public ChannelFuture getChannelClosureFuture() {
      return underlyingConnection.getChannelClosureFuture()
          .addListener(new GenericFutureListener<Future<? super Void>>() {
            @Override
            public void operationComplete(Future<? super Void> future) throws Exception {
              userSession.close(); // when the underlying connection is closed
            }
          });
    }

    @Override
    public SocketAddress getRemoteAddress() {
      return underlyingConnection.getRemoteAddress();
    }

    @Override
    public void close() {
      userSession.close(); // when this specific session is closed
    }
  }

  private static UserClientConnection newSession(final UserServer.BitToUserConnection underlyingConnection,
                                                 final UserSession userSession) {
    return new SessionConnection(underlyingConnection, userSession);
  }
}
