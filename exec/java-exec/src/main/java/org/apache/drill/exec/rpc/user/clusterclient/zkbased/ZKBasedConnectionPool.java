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
package org.apache.drill.exec.rpc.user.clusterclient.zkbased;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.exceptions.DrillIOException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.rpc.NonTransientRpcException;
import org.apache.drill.exec.rpc.user.clusterclient.AbstractDrillClusterClient;
import org.apache.drill.exec.rpc.user.clusterclient.DrillConnectionImpl;
import org.apache.drill.exec.rpc.user.clusterclient.DrillConnectionPool;
import org.apache.drill.exec.rpc.user.clusterclient.DrillSession;
import org.apache.drill.exec.rpc.user.clusterclient.EndpointProvider;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class ZKBasedConnectionPool extends AbstractDrillClusterClient implements DrillConnectionPool {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ZKBasedConnectionPool.class);

  private final EndpointProvider endpoints;
  private final DrillProperties connectionProperties;
  private final Cache<DrillbitEndpoint, DrillConnectionImpl> connections;

  private final boolean ownsEndpointProvider;

  ZKBasedConnectionPool(ZKBasedConnectionPoolBuilder builder) {
    super(builder);
    this.endpoints = builder.endpointProvider;
    this.connectionProperties = builder.connectionProperties;
    this.ownsEndpointProvider = builder.ownsEndpointProvider;
    connectionProperties.setProperty(DrillProperties.MULTIPLEX, "true");
    this.connections = CacheBuilder.newBuilder()
        .removalListener(new RemovalListener<DrillbitEndpoint, DrillConnectionImpl>() {
          @Override
          @ParametersAreNonnullByDefault
          public void onRemoval(RemovalNotification<DrillbitEndpoint, DrillConnectionImpl> notification) {
            if (notification.wasEvicted()) {
              logger.warn("Connection was evicted. Closing connection, and failing all running queries.");
              final DrillConnectionImpl connection = notification.getValue();
              if (connection != null) {
                connection.close(false);
              }
            }
          }
        }).build();
  }

  @Override
  public BufferAllocator getAllocator() {
    return super.getAllocator();
  }

  @Override
  public DrillSession newSession(final Properties info) throws DrillIOException {
    final DrillbitEndpoint endpoint = endpoints.getEndpoint();
    if (endpoint == null) {
      throw new NonTransientRpcException("Invalid endpoint information; did not expect 'null'");
    }
    final DrillConnectionImpl connection;
    try {
      connection = connections.get(endpoint, new Callable<DrillConnectionImpl>() {
        @Override
        public DrillConnectionImpl call() throws Exception {
          final DrillConnectionImpl newConnection = newPhysicalConnection(endpoint, info);
          newConnection.connect(endpoint, connectionProperties, getUserCredentials(connectionProperties));
          return newConnection;
        }
      });
    } catch (ExecutionException e) {
      if (e.getCause() instanceof DrillIOException) {
        throw (DrillIOException) e.getCause();
      }
      throw new DrillIOException("Unexpected error: " + e.getCause().getMessage());
    }
    return connection.newSession(info);
  }

  @Override
  protected void connectionClosedDirectly(DrillbitEndpoint endpoint) {
    connections.invalidate(endpoint);
  }

  @Override
  protected void closeAllConnections() {
    for (final DrillConnectionImpl connection : connections.asMap().values()) {
      connection.close(false);
    }
    connections.invalidateAll();
  }

  @Override
  public void close() {
    if (ownsEndpointProvider) {
      endpoints.close();
    }
    super.close();
  }
}
