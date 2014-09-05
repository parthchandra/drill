/**
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
package org.apache.drill.exec.work.user;

import com.google.common.collect.Lists;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.ExternalSort;
import org.apache.drill.exec.planner.fragment.Fragment;
import org.apache.drill.exec.planner.fragment.MakeFragmentsVisitor;
import org.apache.drill.exec.planner.fragment.PlanningSet;
import org.apache.drill.exec.planner.fragment.SimpleParallelizer;
import org.apache.drill.exec.planner.fragment.StatsCollector;
import org.apache.drill.exec.planner.sql.DrillParallelSqlWorker;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserProtos.GetQueryPlanFragments;
import org.apache.drill.exec.proto.UserProtos.QueryPlanFragments;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.work.ErrorHelper;
import org.apache.drill.exec.work.QueryWorkUnit;

import java.util.List;

/**
 * Provides utility methods for parsing and planning a query
 */
public class QueryPlannerHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryPlannerHelper.class);


  // Plan the query and send plan fragments as response to client
  public static QueryPlanFragments planQuery(DrillbitContext dContext, UserClientConnection connection,
      QueryId queryId, GetQueryPlanFragments req) {
    QueryPlanFragments.Builder responseBuilder = QueryPlanFragments.newBuilder();
    QueryContext context = new QueryContext(connection.getSession(), queryId, dContext);
    try {
      DrillParallelSqlWorker sqlWorker = new DrillParallelSqlWorker(context);
      PhysicalPlan plan = sqlWorker.getPlan(req.getQuery());
      List<PlanFragment> fragments = getPlanFragments(plan, context, queryId);

      responseBuilder.setQueryId(queryId);
      for(int i = 0; i < fragments.size(); i++) {
        responseBuilder.addFragments(fragments.get(i));
      }

      responseBuilder.setStatus(QueryState.COMPLETED);
    }catch(Exception e){
      responseBuilder.setStatus(QueryState.COMPLETED);
      boolean verbose = context.getOptions().getOption(ExecConstants.ENABLE_VERBOSE_ERRORS_KEY).bool_val;
      DrillPBError error =
          ErrorHelper.logAndConvertError(context.getCurrentEndpoint(), "Failed to plan query", e, logger, verbose);

      responseBuilder.setStatus(QueryState.FAILED);
      responseBuilder.setError(error);
    }

    return responseBuilder.build();
  }

  private static List<PlanFragment> getPlanFragments(PhysicalPlan plan, QueryContext context, QueryId queryId)
      throws ExecutionSetupException {
    PhysicalOperator rootOperator = plan.getSortedOperators(false).iterator().next();

    MakeFragmentsVisitor makeFragmentsVisitor = new MakeFragmentsVisitor();
    Fragment rootFragment = rootOperator.accept(makeFragmentsVisitor, null);

    int sortCount = 0;
    for (PhysicalOperator op : plan.getSortedOperators()) {
      if (op instanceof ExternalSort) sortCount++;
    }

    if (sortCount > 0) {
      long maxWidthPerNode = context.getOptions().getOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY).num_val;
      long maxAllocPerNode = Math.min(DrillConfig.getMaxDirectMemory(),
          context.getConfig().getLong(ExecConstants.TOP_LEVEL_MAX_ALLOC));
      maxAllocPerNode = Math.min(maxAllocPerNode, context.getOptions().getOption(
          ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY).num_val);
      long maxSortAlloc = maxAllocPerNode / (sortCount * maxWidthPerNode);

      for (PhysicalOperator op : plan.getSortedOperators()) {
        if (op instanceof  ExternalSort) {
          ((ExternalSort)op).setMaxAllocation(maxSortAlloc);
        }
      }
    }

    PlanningSet planningSet = StatsCollector.collectStats(rootFragment);
    SimpleParallelizer parallelizer = new SimpleParallelizer(context);
    QueryWorkUnit work = parallelizer.getFragments(context.getOptions().getOptionList(), context.getCurrentEndpoint(),
        queryId, context.getActiveEndpoints(), context.getPlanReader(), rootFragment, planningSet);

    List<PlanFragment> fragments = Lists.newArrayList();
    fragments.add(work.getRootFragment());
    fragments.addAll(work.getFragments());

    return fragments;
  }
}
