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

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.exec.coord.DrillServiceInstanceHelper;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.rpc.user.clusterclient.EndpointProvider;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZKBasedEndpointProvider implements EndpointProvider {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ZKBasedEndpointProvider.class);

  // same as in ZKClusterCoordinator
  private static final Pattern ZK_COMPLEX_STRING = Pattern.compile("(^.*?)/(.*)/([^/]*)$");

  public static Builder newBuilder() {
    return new Builder();
  }

  private final CuratorFramework curator;
  private final ServiceDiscovery<DrillbitEndpoint> discovery;
  private final ServiceCache<DrillbitEndpoint> serviceCache;
  private final Random random = new Random();

  // synchronized on "this"
  private volatile List<DrillbitEndpoint> endpoints;

  ZKBasedEndpointProvider(final Builder builder) throws Exception {
    curator = CuratorFrameworkFactory.builder()
        .namespace(builder.zkRoot)
        .connectionTimeoutMs(builder.timeoutMs)
        .retryPolicy(new RetryNTimes(builder.retryTimes, builder.retryDelay))
        .connectString(builder.connectionString)
        .build();

    final CountDownLatch latch = new CountDownLatch(1);
    curator.getConnectionStateListenable().addListener(new ConnectionStateListener() {
      @Override
      public void stateChanged(CuratorFramework client, ConnectionState newState) {
        if (newState == ConnectionState.CONNECTED) {
          latch.countDown();
          client.getConnectionStateListenable().removeListener(this);
        }
      }
    });
    curator.start();

    discovery = ServiceDiscoveryBuilder.builder(DrillbitEndpoint.class)
        .basePath("/")
        .client(curator)
        .serializer(DrillServiceInstanceHelper.SERIALIZER)
        .build();
    discovery.start();

    latch.await();

    serviceCache = discovery.serviceCacheBuilder()
        .name(builder.clusterId)
        .build();
    serviceCache.addListener(new ServiceCacheListener() {
      final String clusterId = builder.clusterId;
      @Override
      public void cacheChanged() {
        updateEndpoints(clusterId);
      }

      @Override
      public void stateChanged(CuratorFramework client, ConnectionState newState) {
      }
    });
    serviceCache.start();
    updateEndpoints(builder.clusterId);
  }

  private void updateEndpoints(final String clusterId) {
    try {
      final List<DrillbitEndpoint> list = Lists.newArrayList(
          Collections2.transform(discovery.queryForInstances(clusterId),
              new Function<ServiceInstance<DrillbitEndpoint>, DrillbitEndpoint>() {
                @Override
                public DrillbitEndpoint apply(ServiceInstance<DrillbitEndpoint> input) {
                  return input.getPayload();
                }
              }));
      synchronized (ZKBasedEndpointProvider.this) {
        endpoints = list;
      }
    } catch (final Exception e) {
      logger.error("Unexpected failure while requesting ZooKeeper for list of bits.", e);
    }
  }

  @Override
  public DrillbitEndpoint getEndpoint() {
    synchronized (this) {
      if (endpoints.size() == 0) {
        return null;
      }
      return endpoints.get(random.nextInt(endpoints.size()));
    }
  }

  @Override
  public void close() {
    try {
      AutoCloseables.close(serviceCache, discovery, curator);
    } catch (Exception e) {
      logger.warn("Ignoring exception(s) while closing ZK resources", e);
    }
  }

  public static class Builder {
    int retryTimes = 5;
    int retryDelay = 500;
    int timeoutMs = 5000;

    String connectionString;
    String zkRoot;
    String clusterId;

    /**
     * Connection URL to ZooKeeper.
     *
     * @param url connection url
     * @return this builder
     */
    public Builder withUrl(final String url) {
      final Matcher matcher = ZK_COMPLEX_STRING.matcher(url);
      if (matcher.matches()) {
        connectionString = matcher.group(1);
        zkRoot = matcher.group(2);
        clusterId = matcher.group(3);
      }
      return this;
    }

    /**
     * Number of times to retry connecting.
     *
     * @param retryTimes number of times to retry
     * @return this builder
     */
    public Builder withRetryTimes(final int retryTimes) {
      this.retryTimes = retryTimes;
      return this;
    }

    /**
     * Delay between each retry in milliseconds.
     *
     * @param retryDelay delay between each retry
     * @return this builder
     */
    public Builder withRetryDelay(final int retryDelay) {
      this.retryDelay = retryDelay;
      return this;
    }

    /**
     * Connection timeout in milliseconds.
     *
     * @param timeoutMs  Connection timeout
     * @return this builder
     */
    public Builder withTimeoutMs(final int timeoutMs) {
      this.timeoutMs = timeoutMs;
      return this;
    }

    /**
     * ZooKeeper connection string.
     *
     * @param connectionString ZooKeeper connection string
     * @return this builder
     */
    public Builder withConnectionString(final String connectionString) {
      this.connectionString = connectionString;
      return this;
    }

    /**
     * Root path of the namespace of the Drill cluster.
     *
     * @param zkRoot root path of the namespace of the Drill cluster
     * @return this builder
     */
    public Builder withZKRoot(final String zkRoot) {
      this.zkRoot = zkRoot;
      return this;
    }

    /**
     * Drill Cluster ID.
     * @param clusterId Drill Cluster ID
     * @return this builder
     */
    public Builder withClusterId(final String clusterId) {
      this.clusterId = clusterId;
      return this;
    }

    public ZKBasedEndpointProvider build() throws Exception {
      if (connectionString == null || zkRoot == null || clusterId == null) {
        throw new IllegalArgumentException();
      }
      return new ZKBasedEndpointProvider(this);
    }
  }
}
