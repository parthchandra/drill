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
package org.apache.drill.exec.client;

import org.apache.drill.common.exceptions.DrillIOException;
import org.apache.drill.exec.DrillSystemTestBase;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.rpc.user.clusterclient.ClusterClientBuilders;
import org.apache.drill.exec.rpc.user.clusterclient.DrillConnection;
import org.apache.drill.exec.rpc.user.clusterclient.DrillSession;
import org.apache.drill.exec.rpc.user.clusterclient.zkbased.ZKBasedConnectionPool;
import org.apache.drill.exec.rpc.user.clusterclient.zkbased.ZKBasedEndpointProvider;
import org.apache.drill.test.QuerySummaryFuture;
import org.apache.drill.test.SummaryOnlyQueryEventListener;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestDrillClusterClient extends DrillSystemTestBase {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestDrillClusterClient.class);

  @Test
  public void zkPool() throws Exception {
    startCluster(1);
    ZKBasedConnectionPool pool = null;
    try {
      pool = ClusterClientBuilders.newZKBasedPool()
          .setZKEndpointProviderBuilder(ZKBasedEndpointProvider.newBuilder()
              .withConnectionString(getConfig().getString(ExecConstants.ZK_CONNECTION))
              .withClusterId(getConfig().getString(ExecConstants.SERVICE_NAME))
              .withZKRoot(getConfig().getString(ExecConstants.ZK_ROOT)))
          .build();
      createSessionAndTryQuery(pool);
      createSessionAndTryQuery(pool);
      createSessionAndTryQuery(pool);
      createSessionAndTryQuery(pool);
    } finally {
      try {
        if (pool != null) {
          pool.close();
        }
      } finally {
        stopCluster();
      }
    }
  }

  @Test
  public void zkPoolMultiSession() throws Exception {
    startCluster(1);
    ZKBasedConnectionPool pool = null;
    try {
      pool = ClusterClientBuilders.newZKBasedPool()
          .setZKEndpointProviderBuilder(ZKBasedEndpointProvider.newBuilder()
              .withConnectionString(getConfig().getString(ExecConstants.ZK_CONNECTION))
              .withClusterId(getConfig().getString(ExecConstants.SERVICE_NAME))
              .withZKRoot(getConfig().getString(ExecConstants.ZK_ROOT)))
          // .setConnectionProperties(null) username, password
          .build();

      final Random random = new Random();
      final List<DrillSession> sessions = new ArrayList<>();
      try {
        int numSessions = 3;
        final int numOperations = 50;
        for (int i = 0; i < numSessions; i++) {
          sessions.add(pool.newSession(null /* impersonation_target */));
        }

        final List<QuerySummaryFuture> results = new ArrayList<>();
        for (int i = 0; i < numOperations; i++) {
          switch (random.nextInt(4)) {
          case 0 :
          case 1 :
            results.add(runSampleQuery(sessions.get(random.nextInt(numSessions))));
            break;
          case 2 :
            sessions.add(pool.newSession(null /* impersonation_target */));
            numSessions++;
            break;
          case 3:
            if (numSessions - 1 != 0) {
              sessions.remove(random.nextInt(numSessions))
                  .close();
              numSessions--;
            }
            break;
          }
        }

//        for (final QuerySummaryFuture result : results) {
//          System.out.println("Final state: " + result.get().finalState()
//              + " or failed? " + result.get().failed());
//        }

      } finally {
        for (DrillSession session : sessions) {
          session.close();
        }
      }
    } finally {
      try {
        if (pool != null) {
          pool.close();
        }
      } finally {
        stopCluster();
      }
    }
  }

  private static void createSessionAndTryQuery(final DrillConnection connection)
      throws DrillIOException, InterruptedException {
    DrillSession session = null;
    try {
      session = connection.newSession(null);
      assertTrue(runSampleQuery(session)
          .get()
          .succeeded());
    } catch (ExecutionException e) {
      fail(e.getMessage());
    } finally {
      if (session != null) {
        session.close();
      }
    }
  }

  private static QuerySummaryFuture runSampleQuery(final DrillSession session)
      throws ExecutionException, InterruptedException {
    final QuerySummaryFuture future = new QuerySummaryFuture();
    session.executeStatement("select * from sys.drillbits", new SummaryOnlyQueryEventListener(future));
    return future;
  }
}
