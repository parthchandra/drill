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

import io.netty.channel.EventLoopGroup;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.memory.BufferAllocator;

import java.util.concurrent.ExecutorService;

/**
 * @param <B> builder type
 * @param <C> cluster client type
 */
public abstract class AbstractDrillClusterClientBuilder<B extends AbstractDrillClusterClientBuilder<B, C>,
    C extends DrillClusterClient> {
//  private static final org.slf4j.Logger logger =
//      org.slf4j.LoggerFactory.getLogger(AbstractDrillClusterClientBuilder.class);

  String name = "Apache Drill Java client";
  DrillConfig config;
  BufferAllocator allocator;
  EventLoopGroup eventLoopGroup;
  ExecutorService executor;

  int eventLoopGroupSize = 5;
  boolean supportComplexTypes = true;

  protected abstract B getThis();

  /**
     * Sets the client name.
     *
     * @param clientName name of client
     * @return this builder
     */
  public B setName(final String clientName) {
    this.name = clientName;
    return getThis();
  }

  /**
     * Sets the {@link DrillConfig configuration} for this client.
     *
     * @param drillConfig drill configuration
     * @return this builder
     */
  public B setConfig(final DrillConfig drillConfig) {
    this.config = drillConfig;
    return getThis();
  }

  /**
     * Sets the {@link DrillConfig configuration} for this client based on the given file.
     *
     * @param fileName configuration file name
     * @return this builder
     */
  public B setConfigFromFile(final String fileName) {
    this.config = DrillConfig.create(fileName);
    return getThis();
  }

  /**
     * Sets the {@link BufferAllocator buffer allocator} to be used by this client.
     * If this is not set, an allocator will be created based on the configuration.
     *
     * If this is set, the caller is responsible for closing the given allocator.
     *
     * @param allocator buffer allocator
     * @return this builder
     */
  public B setAllocator(final BufferAllocator allocator) {
    this.allocator = allocator;
    return getThis();
  }

  /**
     * Sets the event loop group that to be used by the client. If this is not set,
     * an event loop group will be created.
     *
     * If this is set, the caller is responsible for closing the given event loop group.
     *
     * @param eventLoopGroup event loop group
     * @return this builder
     */
  public B setEventLoopGroup(final EventLoopGroup eventLoopGroup) {
    this.eventLoopGroup = eventLoopGroup;
    return getThis();
  }

  /**
   * Sets the event loop group size that to be used by the client. An event loop group
   * will be created.
   *
   * @param eventLoopGroupSize event loop group size
   * @return this builder
   */
  public B setEventLoopGroupSize(final int eventLoopGroupSize) {
    this.eventLoopGroupSize = eventLoopGroupSize;
    return getThis();
  }

  /**
     * Sets the executor service to be used by the client. If this is not set,
     * an executor will be created based on the configuration.
     *
     * If this is set, the caller is responsible for closing the given executor.
     *
     * @param executor executor service
     * @return this builder
     */
  public B setExecutorService(final ExecutorService executor) {
    this.executor = executor;
    return getThis();
  }

  /**
     * Sets whether the application is willing to accept complex types (Map, Arrays)
     * in the returned result set. Default is {@code true}. If set to {@code false},
     * the complex types are returned as JSON encoded VARCHAR type.
     *
     * @param supportComplexTypes if client accepts complex types
     * @return this builder
     */
  public B setSupportsComplexTypes(final boolean supportComplexTypes) {
    this.supportComplexTypes = supportComplexTypes;
    return getThis();
  }

  /**
   * Builds the drill client.
   *
   * @return a new drill client
   */
  public abstract C build();
}
