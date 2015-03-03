/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.ops;

import io.netty.buffer.DrillBuf;

/**
 * Defines the query state and shared resources available to UDFs through
 * injectables. For use in a function, include a {@see javax.inject.Inject}
 * annotation on a UDF class member with any of the types available through
 * this interface.
 */
public interface UdfUtilities {

  public static final Class[] INJECTABLE_TYPES = {DrillBuf.class, QueryDateTimeInfo.class};

  /**
   * Get the query start time and timezone recorded by the head node during
   * planning. This allows for SQL functions like now() to return a stable
   * result within the context of a distributed query.
   *
   * @return - object wrapping the raw time and timezone values
   */
  QueryDateTimeInfo getQueryDateTimeInfo();

  /**
   * For UDFs to allocate general purpose intermediate buffers we provide the
   * DrillBuf type as an injectable, which provides access to an off-heap
   * buffer that can be tracked by Drill and re-allocated as needed.
   *
   * @return - a buffer managed by Drill, connected to the fragment allocator
   *           for memory management
   */
  DrillBuf getManagedBuffer();
}
