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

import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.logical.partition.RewriteAsBinaryOperators;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.Prule;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.Lists;

public abstract class DbScanToIndexScanPrule extends Prule {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DbScanToIndexScanPrule.class);

  /**
   * Selectivity threshold below which an index scan plan would be generated.
   */
  static final double INDEX_SELECTIVITY_THRESHOLD = 0.1;

  /**
   * Return the index collection relevant for the underlying data source
   * @param settings
   * @param scan
   */
  public abstract IndexCollection getIndexCollection(PlannerSettings settings, ScanPrel scan);

  final OptimizerRulesContext optimizerContext;

  public DbScanToIndexScanPrule(RelOptRuleOperand operand, String id, OptimizerRulesContext optimizerContext) {
    super(operand, id);
    this.optimizerContext = optimizerContext;
  }

  public static final Prule getFilterOnProject(OptimizerRulesContext optimizerRulesContext) {
    return new DbScanToIndexScanPrule(
        RelOptHelper.some(FilterPrel.class, RelOptHelper.some(ProjectPrel.class, RelOptHelper.any(ScanPrel.class))),
        "DbScanToIndexScanPrule:Filter_On_Project",
        optimizerRulesContext) {

      @Override
      public IndexCollection getIndexCollection(PlannerSettings settings, ScanPrel scan) {
        DbGroupScan groupScan = (DbGroupScan)scan.getGroupScan();
        return groupScan.getSecondaryIndexCollection(scan);
      }

      @Override
      public boolean matches(RelOptRuleCall call) {
        final ScanPrel scan = (ScanPrel) call.rel(2);
        GroupScan groupScan = scan.getGroupScan();
        if (groupScan instanceof DbGroupScan) {
          DbGroupScan dbscan = ((DbGroupScan)groupScan);
          //if we already applied index convert rule, and this scan is indexScan or restricted scan already,
          //no more trying index convert rule
          return dbscan.supportsSecondaryIndex() && (!dbscan.isIndexScan()) && (!dbscan.isRestrictedScan());
        }
        return false;
      }

      @Override
      public void onMatch(RelOptRuleCall call) {
        final FilterPrel filter = (FilterPrel) call.rel(0);
        final ProjectPrel project = (ProjectPrel) call.rel(1);
        final ScanPrel scan = (ScanPrel) call.rel(2);
        doOnMatch(call, filter, project, scan);
      }
    };
  }

  public static final Prule getFilterOnScan(OptimizerRulesContext optimizerRulesContext) {
    return new DbScanToIndexScanPrule(
        RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)),
        "DbScanToIndexScanPrule:Filter_On_Scan", optimizerRulesContext) {

      @Override
      public IndexCollection getIndexCollection(PlannerSettings settings, ScanPrel scan) {
        DbGroupScan dbGroupScan = (DbGroupScan)scan.getGroupScan();
        return dbGroupScan.getSecondaryIndexCollection(scan);
      }

      @Override
      public boolean matches(RelOptRuleCall call) {
        final ScanPrel scan = (ScanPrel) call.rel(1);
        GroupScan groupScan = scan.getGroupScan();
        if (groupScan instanceof DbGroupScan) {
          DbGroupScan dbscan = ((DbGroupScan)groupScan);
          //if we already applied index convert rule, and this scan is indexScan or restricted scan already,
          //no more trying index convert rule
          return dbscan.supportsSecondaryIndex() && (!dbscan.isIndexScan()) && (!dbscan.isRestrictedScan());
        }
        return false;
      }

      @Override
      public void onMatch(RelOptRuleCall call) {
        final FilterPrel filter = (FilterPrel) call.rel(0);
        final ScanPrel scan = (ScanPrel) call.rel(1);
        doOnMatch(call, filter, null, scan);
      }
    };
  }


  protected void doOnMatch(RelOptRuleCall call, FilterPrel filter, ProjectPrel project, ScanPrel scan) {
    final PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());
    final IndexCollection indexCollection = getIndexCollection(settings, scan);
    if( indexCollection == null ) {
      return;
    }

    RexBuilder builder = filter.getCluster().getRexBuilder();

    RexNode condition = null;
    if (project == null) {
      condition = filter.getCondition();
    } else {
      // get the filter as if it were below the projection.
      condition = RelOptUtil.pushFilterPastProject(filter.getCondition(), project);
    }

    RewriteAsBinaryOperators visitor = new RewriteAsBinaryOperators(true, builder);
    condition = condition.accept(visitor);

    if (indexCollection.supportsIndexSelection()) {
      processWithoutIndexSelection(call, settings, condition,
          indexCollection, builder, filter, project, scan);
    } else {
      processWithIndexSelection(call, settings, condition,
          indexCollection, builder, filter, scan);
    }
  }


  /**
   *
   * @param rowType
   * @param indexCondition
   * @param indexDesc
   * @return If all fields involved in this condition are in the indexed fields of the single IndexDescriptor, return true
   */
  static private boolean conditionIndexed(RelDataType rowType, RexNode indexCondition, IndexDescriptor indexDesc) {
    List<RexNode> conditions = Lists.newArrayList();
    conditions.add(indexCondition);
    PrelUtil.ProjectPushInfo info = PrelUtil.getColumns(rowType, conditions);
    return indexDesc.allColumnsIndexed(info.columns);
  }

  private IndexDescriptor selectIndexForNonCoveringPlan(ScanPrel scan, Iterable<IndexDescriptor> indexes) {
    IndexDescriptor ret = null;
    int maxIndexedNum = 0;
    //XXX the implement here should make decision based on selectivity, for now, we pick the index cover
    //the most index fields.
    for(IndexDescriptor index: indexes) {
      if(index.getIndexColumns().size() > maxIndexedNum) {
        maxIndexedNum = index.getIndexColumns().size();
        ret = index;
      }
    }
    return ret;
  }
  /**
   *
   */
  private void processWithoutIndexSelection(
      RelOptRuleCall call,
      PlannerSettings settings,
      RexNode condition,
      IndexCollection collection,
      RexBuilder builder,
      FilterPrel filter,
      ProjectPrel project,
      ScanPrel scan) {
    if (! (scan.getGroupScan() instanceof DbGroupScan) ) {
      return;
    }
    IndexConditionInfo.Builder infoBuilder = IndexConditionInfo.newBuilder(condition, collection, builder, scan);
    IndexConditionInfo cInfo = infoBuilder.getCollectiveInfo();

    if (!cInfo.hasIndexCol) {
      logger.debug("No index columns are projected from the scan..continue.");
      return;
    }

    if (cInfo.indexCondition == null) {
      logger.debug("No conditions were found eligible for applying index lookup.");
      return;
    }

    RexNode indexCondition = cInfo.indexCondition;
    RexNode remainderCondition = cInfo.remainderCondition;

    List<IndexDescriptor> coveringIndexes = Lists.newArrayList();
    List<IndexDescriptor> nonCoveringIndexes = Lists.newArrayList();

    // get the list of covering and non-covering indexes for this collection
    for (IndexDescriptor indexDesc : collection) {
      if(conditionIndexed(scan.getRowType(), indexCondition, indexDesc)) {
        if (isCoveringIndex(scan, indexDesc)) {
          coveringIndexes.add(indexDesc);
        } else {
          nonCoveringIndexes.add(indexDesc);
        }
      }
    }

    if (logger.isDebugEnabled()) {
      StringBuffer strBuf = new StringBuffer();
      strBuf.append("Split  indexes:");
      if (coveringIndexes.size() > 0) {
        for (IndexDescriptor coveringIdx : coveringIndexes) {
          strBuf.append(coveringIdx.getIndexName()).append(",");
        }
      }
      if(nonCoveringIndexes.size() > 0) {
        strBuf.append("non-covering indexes:");
        for (IndexDescriptor coveringIdx : nonCoveringIndexes) {
          strBuf.append(coveringIdx.getIndexName()).append(",");
        }
      }
      logger.debug(strBuf.toString());
    }
    //Not a single covering index plan or non-covering index plan is sufficient.
    // either 1) there is no usable index at all, 2) or the chop of original condition to
    // indexCondition + remainderCondition is not working for any single index.
    if (coveringIndexes.size() == 0 && nonCoveringIndexes.size() == 0) {
      Map<IndexDescriptor, IndexConditionInfo> indexInfoMap = infoBuilder.getIndexConditionMap();

      //no usable index
      if (indexInfoMap == null || indexInfoMap.size() == 0) {
        return;
      }

      //if there is only one index found, no need to o intersect, but just a regular non-covering plan
      //some part of filter condition needs to apply on primary table.
      if(indexInfoMap.size() == 1) {
        IndexDescriptor idx = indexInfoMap.keySet().iterator().next();
        nonCoveringIndexes.add(idx);
        indexCondition = indexInfoMap.get(idx).indexCondition;
        remainderCondition = indexInfoMap.get(idx).remainderCondition;
      }
      else {
      /*
      //multiple indexes, let us try to intersect results from multiple index tables
      IndexScanIntersectGenerator planGen = new IndexScanIntersectGenerator(
          call, project, scan, indexInfoMap, builder);
      try {
        planGen.go(filter, convert(scan, scan.getTraitSet()));
      } catch (Exception e) {
        logger.warn("Exception while trying to generate intersect index plan", e);
        return;
      }
      //TODO:we may generate some more non-covering plans(each uses a single index) from the indexes of smallest selectivity
      */
        return;
      }

    }

    boolean createdCovering = false;
    try {
      for (IndexDescriptor indexDesc : coveringIndexes) {
        IndexGroupScan idxScan = indexDesc.getIndexGroupScan();
        logger.debug("Generating covering index plan for query condition {}", indexCondition.toString());

        CoveringIndexPlanGenerator planGen = new CoveringIndexPlanGenerator(call, project, scan, idxScan, indexCondition,
            remainderCondition, builder);
        planGen.go(filter, convert(scan, scan.getTraitSet()));
        createdCovering = true;
      }
    } catch (Exception e) {
      logger.warn("Exception while trying to generate covering index plan", e);
      return;
    }

    if (createdCovering) {
      return;
    }

    // Create non-covering index plans. First, check if the primary table scan supports creating a
    // restricted scan
    GroupScan primaryTableScan = scan.getGroupScan();
    if (primaryTableScan instanceof DbGroupScan &&
        (((DbGroupScan) primaryTableScan).supportsRestrictedScan())) {
      try {
        if (nonCoveringIndexes.size() > 0) {
          IndexDescriptor index = selectIndexForNonCoveringPlan(scan, nonCoveringIndexes);
          IndexGroupScan idxScan = nonCoveringIndexes.get(0).getIndexGroupScan();
          logger.debug("Generating non-covering index plan for query condition {}", indexCondition.toString());
          NonCoveringIndexPlanGenerator planGen = new NonCoveringIndexPlanGenerator(call, project, scan, idxScan, indexCondition,
              remainderCondition, builder);
          planGen.go(filter, convert(scan, scan.getTraitSet()));
        }
      } catch (Exception e) {
        logger.warn("Exception while trying to generate non-covering index access plan", e);
        return;
      }
    }

  }

  private void processWithIndexSelection(
      RelOptRuleCall call,
      PlannerSettings settings,
      RexNode condition,
      IndexCollection collection,
      RexBuilder builder,
      FilterPrel filter,
      ScanPrel scan) {

    IndexConditionInfo cInfo = IndexConditionInfo.newBuilder(condition, collection, builder, scan).getCollectiveInfo();

  }

  /**
   * For a particular table scan for table T1 and an index on that table, find out if it is a covering index
   * @return
   */
  private boolean isCoveringIndex(ScanPrel scan, IndexDescriptor index) {
    DbGroupScan groupScan = (DbGroupScan)scan.getGroupScan();
    List<SchemaPath> tableCols = groupScan.getColumns();
    return index.isCoveringIndex(tableCols);
  }

}

