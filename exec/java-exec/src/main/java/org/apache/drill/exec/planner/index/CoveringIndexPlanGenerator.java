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

package org.apache.drill.exec.planner.index;

import com.google.common.collect.Lists;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.Prule;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Generate a covering index plan that is equivalent to the original plan.
 *
 * This plan will be further optimized by the filter pushdown rule of the Index plugin which should
 * push this filter into the index scan.
 */
public class CoveringIndexPlanGenerator extends AbstractIndexPlanGenerator {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CoveringIndexPlanGenerator.class);

  public CoveringIndexPlanGenerator(RelOptRuleCall call,
      ProjectPrel origProject,
      ScanPrel origScan,
      IndexGroupScan indexGroupScan,
      RexNode indexCondition,
      RexNode remainderCondition,
      RexBuilder builder) {
    super(call, origProject, origScan, indexGroupScan, indexCondition, remainderCondition, builder);
  }

  @Override
  public RelNode convertChild(final FilterPrel filter, final RelNode input) throws InvalidRelException {

    if (indexGroupScan == null) {
      logger.error("Null indexgroupScan in CoveringIndexPlanGenerator.convertChild");
      return null;
    }

    indexGroupScan.setColumns(((DbGroupScan)origScan.getGroupScan()).getColumns());

    ScanPrel indexScanPrel = new ScanPrel(origScan.getCluster(),
        origScan.getTraitSet(), indexGroupScan, origScan.getRowType() /* use the same row type as the original scan */);

    FilterPrel indexFilterPrel = new FilterPrel(indexScanPrel.getCluster(), indexScanPrel.getTraitSet(),
        indexScanPrel, indexCondition);

    ProjectPrel indexProjectPrel = null;
    if (origProject != null) {
      indexProjectPrel = new ProjectPrel(origScan.getCluster(), origScan.getTraitSet(),
          indexFilterPrel, origProject.getProjects(), origProject.getRowType());
    }
    final RelNode finalRel;

    if (remainderCondition != null && !remainderCondition.isAlwaysTrue()) {
      // create a Filter corresponding to the remainder condition
      FilterPrel remainderFilterPrel = new FilterPrel(origScan.getCluster(), indexProjectPrel.getTraitSet(),
          indexProjectPrel, remainderCondition);
      finalRel = Prule.convert(remainderFilterPrel, remainderFilterPrel.getTraitSet());
    } else if (indexProjectPrel != null) {
      finalRel = Prule.convert(indexProjectPrel, indexProjectPrel.getTraitSet());
    }
    else {
      finalRel = Prule.convert(indexFilterPrel, indexFilterPrel.getTraitSet());
    }

    return finalRel;
  }
}
