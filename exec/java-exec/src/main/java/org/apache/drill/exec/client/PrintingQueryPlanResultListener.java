/**
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
package org.apache.drill.exec.client;

import java.util.concurrent.CountDownLatch;

import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserProtos.QueryPlanFragments;
import org.apache.drill.exec.rpc.user.UserQueryPlanResultListener;

public class PrintingQueryPlanResultListener implements UserQueryPlanResultListener {
  private CountDownLatch latch = new CountDownLatch(1);


  @Override
  public void resultsArrived(QueryPlanFragments fragments) {
    System.out.println("QueryId: " + fragments.getQueryId());
    for(int i = 0; i < fragments.getFragmentsList().size(); i++) {
      FragmentHandle handle = fragments.getFragments(i).getHandle();
      System.out.println(handle.getMajorFragmentId() + ":" + handle.getMinorFragmentId());
      System.out.println(fragments.getFragments(i).getFragmentJson());
      System.out.println("-------");
    }

    latch.countDown();
  }

  public void await() throws Exception {
    latch.await();
  }
}