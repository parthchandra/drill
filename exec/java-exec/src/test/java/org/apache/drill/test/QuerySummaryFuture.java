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
package org.apache.drill.test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The future used to wait for the completion of an async query. Returns
 * just the summary of the query.
 */

public class QuerySummaryFuture implements Future<QuerySummary> {

  /**
   * Synchronizes the listener thread and the test thread that
   * launched the query.
   */

  private CountDownLatch lock = new CountDownLatch(1);
  private QuerySummary summary;

  /**
   * Unsupported at present.
   */

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    throw new UnsupportedOperationException();
  }

  /**
   * Always returns false.
   */

  @Override
  public boolean isCancelled() { return false; }

  @Override
  public boolean isDone() { return summary != null; }

  @Override
  public QuerySummary get() throws InterruptedException, ExecutionException {
    lock.await();
    return summary;
  }

  /**
   * Not supported at present, just does a non-timeout get.
   */

  @Override
  public QuerySummary get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return get();
  }

  protected void completed(QuerySummary querySummary) {
    summary = querySummary;
    lock.countDown();
  }
}
