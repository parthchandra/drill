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

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.logical.DrillMergeProjectRule;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.DrillSortRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.Prule;
import org.apache.drill.exec.planner.physical.ScanPrel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class PlainDbScanToIndexScanPrule extends Prule {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlainDbScanToIndexScanPrule.class);


  protected IndexPlanCallContext indexContext;

  private PlainDbScanToIndexScanPrule(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  public static final RelOptRule SORT_PROJECT_SCAN = new PlainDbScanToIndexScanPrule(
      RelOptHelper.some(DrillSortRel.class, RelOptHelper.some(DrillProjectRel.class, RelOptHelper.any(DrillScanRel.class))),
      "IndexScanWithSortOnlyPrule:Sort_Project_Scan") {
    @Override
    public boolean matches(RelOptRuleCall call) {
      DrillSortRel sort = call.rel(0);
      DrillProjectRel project = call.rel(1);
      DrillScanRel scan = call.rel(2);

      if (IndexPlanUtils.checkScan(scan) == true) {
        indexContext = new IndexPlanCallContext(call, sort, project, scan);
        return true;
      }
      return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      doOnMatch();
    }
  };

  public static final RelOptRule SORT_SCAN = new PlainDbScanToIndexScanPrule(
      RelOptHelper.some(DrillSortRel.class, RelOptHelper.any(DrillScanRel.class)),
      "IndexScanWithSortOnlyPrule:Sort_Scan") {
    @Override
    public boolean matches(RelOptRuleCall call) {
      DrillSortRel sort = call.rel(0);
      DrillProjectRel project = null;
      DrillScanRel scan = call.rel(1);

      if (IndexPlanUtils.checkScan(scan) == true) {
        indexContext = new IndexPlanCallContext(call, sort, project, scan);
        return true;
      }
      return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      doOnMatch();
    }
  };

  public void doOnMatch() {
    Stopwatch indexPlanTimer = Stopwatch.createStarted();
    final PlannerSettings settings = PrelUtil.getPlannerSettings(indexContext.call.getPlanner());

    DbGroupScan groupScan = (DbGroupScan)indexContext.scan.getGroupScan();
    final IndexCollection indexCollection =  groupScan.getSecondaryIndexCollection(indexContext.scan);
    if( indexCollection == null ) {
      return;
    }

    IndexPlanUtils.updateSortExpression(indexContext);

    IndexSelector selector = new IndexSelector(indexContext);

    for (IndexDescriptor indexDesc : indexCollection) {

      FunctionalIndexInfo functionInfo = indexDesc.getFunctionalInfo();
      if (IndexPlanUtils.isCoveringIndex(indexContext, functionInfo) ) {
        selector.addIndex(indexDesc, true,
            indexContext.lowerProject != null ? indexContext.lowerProject.getRowType().getFieldCount() :
                indexContext.scan.getRowType().getFieldCount());
        //coveringIndexes.add(functionInfo);
      }
    }

    IndexSelector.IndexProperties idxProp = selector.getBestIndexNoFilter();

    if (idxProp != null) {
      try {
        //generate a covering plan
        CoveringPlanNoFilterGenerator planGen = new CoveringPlanNoFilterGenerator(indexContext, idxProp.getIndexDesc().getFunctionalInfo(), settings);
        planGen.go();
      } catch (Exception e) {
        logger.warn("Exception while trying to generate covering-no-filter index plan", e);
      }
    }
    indexPlanTimer.stop();
    logger.debug("Index Planning took {} ms", indexPlanTimer.elapsed(TimeUnit.MILLISECONDS));
  }


  public static class CoveringPlanNoFilterGenerator extends AbstractIndexPlanGenerator {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CoveringIndexPlanGenerator.class);
    final protected IndexGroupScan indexGroupScan;
    final protected IndexDescriptor indexDesc;

    // Ideally This functionInfo should be cached along with indexDesc.
    final protected FunctionalIndexInfo functionInfo;

    public CoveringPlanNoFilterGenerator(IndexPlanCallContext indexContext,
                                      FunctionalIndexInfo functionInfo,
                                      PlannerSettings settings) {
      super(indexContext, null, null, null, settings);
      this.functionInfo = functionInfo;
      this.indexDesc = functionInfo.getIndexDesc();
      this.indexGroupScan = functionInfo.getIndexDesc().getIndexGroupScan();
    }

    public RelNode convertChild(final RelNode filter, final RelNode input) throws InvalidRelException {

      if (indexGroupScan == null) {
        logger.error("Null indexgroupScan in CoveringIndexPlanGenerator.convertChild");
        return null;
      }
      //update sort expressions in context
      IndexPlanUtils.updateSortExpression(indexContext);

      ScanPrel indexScanPrel =
          IndexPlanUtils.buildCoveringIndexScan(origScan, indexGroupScan, indexContext, indexDesc);
      RelTraitSet indexScanTraitSet = indexScanPrel.getTraitSet();

      RelNode finalRel = indexScanPrel;
      if (indexContext.lowerProject != null) {

        RelCollation collation = IndexPlanUtils.buildCollationProject(indexContext.lowerProject.getProjects(), null,
            indexContext.scan, functionInfo, indexContext);
        finalRel = new ProjectPrel(indexContext.scan.getCluster(), indexScanTraitSet.plus(collation),
            indexScanPrel, indexContext.lowerProject.getProjects(), indexContext.lowerProject.getRowType());

        if (functionInfo.hasFunctional()) {
          //if there is functional index field, then a rewrite may be needed in upperProject/indexProject
          //merge upperProject with indexProjectPrel(from origProject) if both exist,
          ProjectPrel newProject = (ProjectPrel)finalRel;

          // then rewrite functional expressions in new project.
          List<RexNode> newProjects = Lists.newArrayList();
          DrillParseContext parseContxt = new DrillParseContext(PrelUtil.getPlannerSettings(newProject.getCluster()));
          for(RexNode projectRex: newProject.getProjects()) {
            RexNode newRex = IndexPlanUtils.rewriteFunctionalRex(indexContext, parseContxt, null, origScan, projectRex, indexScanPrel.getRowType(), functionInfo);
            newProjects.add(newRex);
          }

          ProjectPrel rewrittenProject = new ProjectPrel(newProject.getCluster(),
              collation==null? newProject.getTraitSet() : newProject.getTraitSet().plus(collation),
              indexScanPrel, newProjects, newProject.getRowType());

          finalRel = rewrittenProject;
        }
      }

      if (indexContext.sort != null) {
        finalRel = getSortNode(indexContext, finalRel);
      }

      finalRel = Prule.convert(finalRel, finalRel.getTraitSet().plus(Prel.DRILL_PHYSICAL));

      logger.debug("CoveringPlanNoFilterGenerator got finalRel {} from origScan {}, original digest {}, new digest {}.",
          finalRel.toString(), indexContext.scan.toString(),
          indexContext.lowerProject!=null?indexContext.lowerProject.getDigest(): indexContext.scan.getDigest(),
          finalRel.getDigest());
      return finalRel;

    }
  }
}
