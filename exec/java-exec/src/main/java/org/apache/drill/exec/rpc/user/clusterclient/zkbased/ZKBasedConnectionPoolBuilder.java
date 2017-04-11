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

import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.exec.rpc.user.clusterclient.AbstractDrillClusterClientBuilder;

public class ZKBasedConnectionPoolBuilder extends
    AbstractDrillClusterClientBuilder<ZKBasedConnectionPoolBuilder, ZKBasedConnectionPool> {
//  private static final org.slf4j.Logger logger =
//      org.slf4j.LoggerFactory.getLogger(ZKBasedConnectionPoolBuilder.class);

  DrillProperties connectionProperties = DrillProperties.createEmpty();

  private ZKBasedEndpointProvider.Builder epBuilder;
  ZKBasedEndpointProvider endpointProvider;

  boolean ownsEndpointProvider = true;

  @Override
  protected ZKBasedConnectionPoolBuilder getThis() {
    return this;
  }

  /**
   * Set connection URL to ZooKeeper.
   *
   * @param url connection url
   * @return this builder
   */
  public ZKBasedConnectionPoolBuilder setZKUrl(String url) {
    this.epBuilder = ZKBasedEndpointProvider.newBuilder().withUrl(url);
    return getThis();
  }

  /**
   * Set ZooKeeper endpoint provider builder.
   *
   * @param builder endpoint provider builder
   * @return this builder
   */
  public ZKBasedConnectionPoolBuilder setZKEndpointProviderBuilder(ZKBasedEndpointProvider.Builder builder) {
    this.epBuilder = builder;
    ownsEndpointProvider = false;
    return getThis();
  }

  /**
   * Set connection properties to Drill.
   *
   * @param connectionProperties connection properties
   * @return this builder
   */
  public ZKBasedConnectionPoolBuilder setConnectionProperties(DrillProperties connectionProperties) {
    this.connectionProperties = connectionProperties;
    return getThis();
  }

  @Override
  public ZKBasedConnectionPool build() {
    if (epBuilder == null) {
      throw new IllegalArgumentException();
    }
    try {
      endpointProvider = epBuilder.build();
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
    return new ZKBasedConnectionPool(this);
  }
}
