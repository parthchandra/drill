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

import com.google.common.collect.Maps;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.exceptions.DrillIOException;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserProtos.NewSessionRequest;
import org.apache.drill.exec.proto.UserProtos.RpcType;
import org.apache.drill.exec.proto.UserProtos.SessionHandle;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryResultHandler;
import org.apache.drill.exec.rpc.user.UserClient;

import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

public class DrillConnectionImpl extends UserClient implements DrillConnection {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillConnection.class);

  private final AbstractDrillClusterClient clusterClient;
  private final DrillbitEndpoint endpoint;

  private final ConcurrentMap<SessionHandle, DrillSessionImpl> sessions = Maps.newConcurrentMap();

  DrillConnectionImpl(AbstractDrillClusterClient clusterClient, DrillbitEndpoint endpoint) {
    super(clusterClient.clientName, clusterClient.config, clusterClient.supportComplexTypes,
        clusterClient.allocator, clusterClient.eventLoopGroup, clusterClient.executor);
    this.clusterClient = clusterClient;
    this.endpoint = endpoint;
  }

  @Override
  public DrillSession newSession(final Properties properties) throws DrillIOException {
    final NewSessionRequest request = NewSessionRequest.newBuilder()
        .setProperties(DrillProperties.createFromProperties(properties)
            .serializeForServer())
        .build();

    final SessionHandle handle = send(RpcType.NEW_SESSION, request, SessionHandle.class)
          .checkedGet();
    if (!handle.hasSessionId()) {
      throw new DrillIOException("Server could not create a new session.");
    }

    final DrillSessionImpl newSession = new DrillSessionImpl(this, handle);
    final DrillSessionImpl oldSession = sessions.putIfAbsent(handle, newSession);
    if (oldSession != null) {
      throw new IllegalStateException("Two sessions with the same handle.");
    }
    return newSession;
  }

  protected final QueryResultHandler getResultHandler() {
    return queryResultHandler;
  }

  /**
   * Invoked when a session is closed directly, instead of being invoked through this object. Any state
   * maintained for the session must be cleared.
   *
   * @param handle session handle associated with the closed session
   */
  protected void sessionClosedDirectly(final SessionHandle handle) {
    final DrillSession session = sessions.remove(handle);
    if (session != null) {
      try {
        send(RpcType.CLOSE_SESSION, handle, Ack.class).checkedGet();
      } catch (RpcException e) {
        logger.warn("failed to close session", e);
      }
    } // else, ignore
  }

  public void close(boolean invokedDirectly) {
    if (invokedDirectly) {
      clusterClient.connectionClosedDirectly(endpoint);
    }

    for (final DrillSessionImpl session : sessions.values()) {
      session.close(false);
      // no need to send RpcType.CLOSE_SESSION for each session as all session states will be cleared when
      // connection is closed
    }
    sessions.clear();
    super.close();
  }

  @Override
  public void close() {
    close(true);
  }
}
