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
package org.apache.drill.exec.planner.index;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.DrillSortRel;

import java.util.List;
import java.util.Set;

public class IndexPlanCallContext {
  final public RelOptRuleCall call;
  final public DrillSortRel sort;
  final public DrillProjectRel upperProject;
  final public DrillFilterRel filter;
  final public DrillProjectRel lowerProject;
  final public DrillScanRel scan;
  final public String indexHint;

  public Set<LogicalExpression> leftOutPathsInFunctions;

  public IndexableExprMarker origMarker;
  public List<LogicalExpression> sortExprs;

  public RexNode origPushedCondition;

  IndexPlanCallContext(RelOptRuleCall call,
                       DrillProjectRel capProject,
                       DrillFilterRel filter,
                       DrillProjectRel project,
                       DrillScanRel scan) {
    this(call, null, capProject, filter, project, scan);
  }

  IndexPlanCallContext(RelOptRuleCall call,
      DrillSortRel sort,
      DrillProjectRel capProject,
      DrillFilterRel filter,
      DrillProjectRel project,
      DrillScanRel scan) {
    this.call = call;
    this.sort = sort;
    this.upperProject = capProject;
    this.filter = filter;
    this.lowerProject = project;
    this.scan = scan;
    this.indexHint = ((DbGroupScan)this.scan.getGroupScan()).getIndexHint();
  }

  IndexPlanCallContext(RelOptRuleCall call,
                       DrillSortRel sort,
                       DrillProjectRel project,
                       DrillScanRel scan) {
    this.call = call;
    this.sort = sort;
    this.upperProject = null;
    this.filter = null;
    this.lowerProject = project;
    this.scan = scan;
    this.indexHint = ((DbGroupScan)this.scan.getGroupScan()).getIndexHint();
  }
}
