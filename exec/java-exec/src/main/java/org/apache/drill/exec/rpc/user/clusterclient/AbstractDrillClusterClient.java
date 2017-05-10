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

import com.google.common.base.Strings;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.channel.EventLoopGroup;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.rpc.NamedThreadFactory;
import org.apache.drill.exec.rpc.TransportCheck;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class AbstractDrillClusterClient implements DrillClusterClient {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractDrillClusterClient.class);

  final String clientName;
  final DrillConfig config;
  final boolean supportComplexTypes;

  // resources used in connections created using this client
  final BufferAllocator allocator;
  final EventLoopGroup eventLoopGroup;
  final ExecutorService executor;

  // checks if this client owns these resources (used when closing)
  private final boolean ownsAllocator;
  private final boolean ownsEventLoopGroup;
  private final boolean ownsExecutor;

  protected AbstractDrillClusterClient(final AbstractDrillClusterClientBuilder<?, ?> builder) {
    this.clientName = builder.name;

    this.config = builder.config != null ? builder.config :
        DrillConfig.create();

    this.supportComplexTypes = builder.supportComplexTypes;


    this.ownsAllocator = builder.allocator == null;
    this.allocator = !ownsAllocator ? builder.allocator :
        RootAllocatorFactory.newRoot(config);

    this.ownsEventLoopGroup = builder.eventLoopGroup == null;
    this.eventLoopGroup = !ownsEventLoopGroup ? builder.eventLoopGroup :
        TransportCheck.createEventLoopGroup(builder.eventLoopGroupSize, "Client-");

    this.ownsExecutor = builder.executor == null;
    this.executor = !ownsExecutor ? builder.executor :
        new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>(),
            new NamedThreadFactory("drill-client-executor-")) {
          @Override
          protected void afterExecute(final Runnable r, final Throwable t) {
            if (t != null) {
              logger.error(String.format("%s.run() leaked an exception.", r.getClass().getName()), t);
            }
            super.afterExecute(r, t);
          }
        };
  }

  @Override
  public BufferAllocator getAllocator() {
    return allocator;
  }

  protected static UserCredentials getUserCredentials(final DrillProperties properties) {
    String userName = properties.getProperty(DrillProperties.USER);
    if (Strings.isNullOrEmpty(userName)) {
      userName = "anonymous"; // if username is not propagated as one of the properties
    }
    return UserCredentials.newBuilder()
        .setUserName(userName)
        .build();
  }

  protected final DrillConnectionImpl newPhysicalConnection(final DrillbitEndpoint endpoint) {
    return new DrillConnectionImpl(this, endpoint);
  }

  /**
   * Invoked when a connection is closed directly, instead of being invoked through this object. Any state
   * maintained for the connection must be cleared.
   *
   * @param endpoint endpoint associated with the connection
   */
  protected abstract void connectionClosedDirectly(final DrillbitEndpoint endpoint);

  /**
   * Close all connections. Invoked when this client is closed directly. On every connection, invoked close
   * indirectly i.e. {@link DrillConnectionImpl#close(boolean) with false}.
   */
  protected abstract void closeAllConnections();

  @Override
  public void close() {
    closeAllConnections();
    if (ownsAllocator) {
      try {
        AutoCloseables.close(allocator);
      } catch (final Exception e) {
        logger.error("Could not close allocator cleanly", e);
      }
    }

    if (ownsEventLoopGroup) {
      TransportCheck.shutDownEventLoopGroup(eventLoopGroup, "Client-", logger);
    }
    if (ownsExecutor) {
      if (!MoreExecutors.shutdownAndAwaitTermination(executor, 1, TimeUnit.SECONDS)) {
        logger.error("Executor could not terminate.");
      }
    }
  }
}
