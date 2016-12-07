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

import java.util.BitSet;
import java.util.List;

import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.logical.partition.FindPartitionConditions;
import org.apache.drill.exec.planner.logical.partition.RewriteAsBinaryOperators;
import org.apache.drill.exec.planner.logical.partition.RewriteCombineBinaryOperators;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.Prule;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.planner.physical.SubsetTransformer;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.commons.lang3.tuple.Pair;

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
          return dbscan.supportsSecondaryIndex() && (!dbscan.isIndexScan());
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
        DbGroupScan hbGroupScan = (DbGroupScan)scan.getGroupScan();
        return hbGroupScan.getSecondaryIndexCollection(scan);
      }

      @Override
      public boolean matches(RelOptRuleCall call) {
        final ScanPrel scan = (ScanPrel) call.rel(1);
        GroupScan groupScan = scan.getGroupScan();
        if (groupScan instanceof DbGroupScan) {
          DbGroupScan dbscan = ((DbGroupScan)groupScan);
          return dbscan.supportsSecondaryIndex() && (!dbscan.isIndexScan());
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

  private IndexConditionInfo getIndexConditionInfo(
      RexNode condition,
      IndexCollection collection,
      RexBuilder builder,
      ScanPrel scan
      ) {
    DbGroupScan dbScan = (DbGroupScan)scan.getGroupScan();

    List<SchemaPath> listPath = dbScan.getColumns();
    List<String> fieldNames = scan.getRowType().getFieldNames();
    BitSet columnBitSet = new BitSet();

    int relColIndex = 0; // index into the rowtype for the indexed columns
    boolean indexedCol = false;
    for (SchemaPath path : listPath) {
      if (collection.isColumnIndexed(path)) {
        for (int i = 0; i < fieldNames.size(); ++i) {
          if (fieldNames.get(i).equals(path.getRootSegment().getPath())) {
            columnBitSet.set(i);
            indexedCol = true;
            break;
          }
        }
      }
      relColIndex++;
    }

    if (indexedCol) {
      // Use the same filter analyzer that is used for partitioning columns
      FindPartitionConditions c = new FindPartitionConditions(columnBitSet, builder);
      c.analyze(condition);
      RexNode indexCondition = c.getFinalCondition();

      if (indexCondition != null) {
        List<RexNode> conjuncts = RelOptUtil.conjunctions(condition);
        List<RexNode> indexConjuncts = RelOptUtil.conjunctions(indexCondition);
        conjuncts.removeAll(indexConjuncts);
        RexNode remainderCondition = RexUtil.composeConjunction(builder, conjuncts, false);

        RewriteCombineBinaryOperators reverseVisitor =
            new RewriteCombineBinaryOperators(true, builder);

        condition = condition.accept(reverseVisitor);
        indexCondition = indexCondition.accept(reverseVisitor);

        return new IndexConditionInfo(indexCondition, remainderCondition, true);
      }
    }

    return new IndexConditionInfo(null, null, indexedCol);
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

    IndexConditionInfo cInfo = getIndexConditionInfo(condition, collection, builder, scan);

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
      if (isCoveringIndex(scan, indexDesc)) {
        coveringIndexes.add(indexDesc);
      } else {
        nonCoveringIndexes.add(indexDesc);
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
      logger.warn("Exception while trying to generate covering index access plan", e);
      return;
    }

    if (createdCovering) {
      return;
    }

    try {
      if (collection.supportsRowCountStats()) {

        double origRowCount = scan.getRows();
        IndexGroupScan idxScan = collection.getGroupScan();
        double indexRows = collection.getRows(indexCondition);

        // set the estimated row count for the index scan
        idxScan.setRowCount(indexCondition, Math.round(indexRows), Math.round(origRowCount));

        // get the selectivity of the predicates on the index columns
        double selectivity = indexRows/origRowCount;

        if (selectivity < INDEX_SELECTIVITY_THRESHOLD &&
            indexRows < settings.getBroadcastThreshold()) {
          logger.debug("Generating non-covering index plan for query condition {}", indexCondition.toString());
          NonCoveringIndexPlanGenerator planGen = new NonCoveringIndexPlanGenerator(call, project, scan, idxScan, indexCondition,
              remainderCondition, builder);
          planGen.go(filter, convert(scan, scan.getTraitSet()));
        }
        else {
          logger.debug("Index plan was not chosen since selectivity {} was higher than threshold", selectivity);
        }
      }
    } catch (Exception e) {
      logger.warn("Exception while trying to generate non-covering index access plan", e);
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

    IndexConditionInfo cInfo = getIndexConditionInfo(condition, collection, builder, scan);

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

