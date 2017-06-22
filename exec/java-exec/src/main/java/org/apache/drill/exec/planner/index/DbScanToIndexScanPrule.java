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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.common.DrillRelNode;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.DrillSortRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.logical.partition.RewriteAsBinaryOperators;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.Prule;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DbScanToIndexScanPrule extends Prule {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DbScanToIndexScanPrule.class);
  final public MatchFunction match;

  public static final RelOptRule REL_FILTER_SCAN = new DbScanToIndexScanPrule(
      RelOptHelper.some(DrillRelNode.class, RelOptHelper.some(DrillFilterRel.class, RelOptHelper.any(DrillScanRel.class))),
      "DbScanToIndexScanPrule:Rel_Filter_Scan", new MatchRelFS());

  public static final RelOptRule PROJECT_FILTER_PROJECT_SCAN = new DbScanToIndexScanPrule(
      RelOptHelper.some(DrillProjectRel.class, RelOptHelper.some(DrillFilterRel.class,
         RelOptHelper.some(DrillProjectRel.class, RelOptHelper.any(DrillScanRel.class)))),
     "DbScanToIndexScanPrule:Project_Filter_Project_Scan", new MatchPFPS());

  public static final RelOptRule SORT_FILTER_PROJECT_SCAN = new DbScanToIndexScanPrule(
     RelOptHelper.some(DrillSortRel.class, RelOptHelper.some(DrillFilterRel.class,
        RelOptHelper.some(DrillProjectRel.class, RelOptHelper.any(DrillScanRel.class)))),
    "DbScanToIndexScanPrule:Sort_Filter_Project_Scan", new MatchSFPS());

  public static final RelOptRule SORT_PROJECT_FILTER_PROJECT_SCAN = new DbScanToIndexScanPrule(
      RelOptHelper.some(DrillSortRel.class, RelOptHelper.some(DrillProjectRel.class, RelOptHelper.some(DrillFilterRel.class,
          RelOptHelper.some(DrillProjectRel.class, RelOptHelper.any(DrillScanRel.class))))),
      "DbScanToIndexScanPrule:Sort_Project_Filter_Project_Scan", new MatchSPFPS());

  public static final RelOptRule SORT_PROJECT_FILTER_SCAN = new DbScanToIndexScanPrule(
      RelOptHelper.some(DrillSortRel.class, RelOptHelper.some(DrillProjectRel.class, RelOptHelper.some(DrillFilterRel.class,
          RelOptHelper.any(DrillScanRel.class)))),
      "DbScanToIndexScanPrule:Sort_Project_Filter_Scan", new MatchSPFS());

  public static final RelOptRule FILTER_SCAN = new DbScanToIndexScanPrule(
      RelOptHelper.some(DrillFilterRel.class, RelOptHelper.any(DrillScanRel.class)),
      "DbScanToIndexScanPrule:Filter_On_Scan", new MatchFS());

  public static final RelOptRule FILTER_PROJECT_SCAN = new DbScanToIndexScanPrule(
      RelOptHelper.some(DrillFilterRel.class,
          RelOptHelper.some(DrillProjectRel.class, RelOptHelper.any(DrillScanRel.class))),
      "DbScanToIndexScanPrule:Filter_Project_Scan", new MatchFPS());

  public enum ConditionIndexed {
    NONE,
    PARTIAL,
    FULL}

  private DbScanToIndexScanPrule(RelOptRuleOperand operand, String description, MatchFunction match) {
    super(operand, description);
    this.match = match;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (getMatchIfRoot(call) != null) {
      return true;
    }
    return match.match(call);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    if (getMatchIfRoot(call) != null) {
      getMatchIfRoot(call).onMatch(call);
      return;
    }
    doOnMatch(match.onMatch(call));
  }

  private MatchFunction getMatchIfRoot(RelOptRuleCall call) {
    List<RelNode> rels = call.getRelList();
    if (call.getPlanner().getRoot().equals(call.rel(0))) {
      if (rels.size() == 2) {
        if ((rels.get(0) instanceof DrillFilterRel) && (rels.get(1) instanceof DrillScanRel)) {
          return ((DbScanToIndexScanPrule)FILTER_SCAN).match;
        }
      }
      else if (rels.size() == 3) {
        if ((rels.get(0) instanceof DrillFilterRel) && (rels.get(1) instanceof DrillProjectRel) && (rels.get(2) instanceof DrillScanRel)) {
          return ((DbScanToIndexScanPrule)FILTER_PROJECT_SCAN).match;
        }
      }
    }
    return null;
  }

  private interface MatchFunction {
    boolean match(RelOptRuleCall call);
    IndexPlanCallContext onMatch(RelOptRuleCall call);
  }

  private static abstract class AbstractMatchFunction implements MatchFunction {
    boolean checkScan(DrillScanRel scanRel) {
      GroupScan groupScan = scanRel.getGroupScan();
      if (groupScan instanceof DbGroupScan) {
        DbGroupScan dbscan = ((DbGroupScan) groupScan);
        //if we already applied index convert rule, and this scan is indexScan or restricted scan already,
        //no more trying index convert rule
        return dbscan.supportsSecondaryIndex() && (!dbscan.isIndexScan()) && (!dbscan.isRestrictedScan());
      }
      return false;
    }
  }

  private static class MatchFPS extends AbstractMatchFunction {

    public boolean match(RelOptRuleCall call) {
      final DrillScanRel scan = (DrillScanRel) call.rel(2);
      return checkScan(scan);
    }

    public IndexPlanCallContext onMatch(RelOptRuleCall call) {
      final DrillFilterRel filter = call.rel(0);
      final DrillProjectRel project = call.rel(1);
      final DrillScanRel scan = call.rel(2);
      return new IndexPlanCallContext(call, null, filter, project, scan);
    }

  }

  private static class MatchFS extends AbstractMatchFunction {
    public boolean match(RelOptRuleCall call) {
      final DrillScanRel scan = call.rel(1);
      return checkScan(scan);
    }

    public IndexPlanCallContext onMatch(RelOptRuleCall call) {
      final DrillFilterRel filter = call.rel(0);
      final DrillScanRel scan = call.rel(1);
      return new IndexPlanCallContext(call, null, filter, null, scan);
    }
  }

  private static class MatchRelFS extends AbstractMatchFunction {
    public boolean match(RelOptRuleCall call) {
      if (call.rel(0) instanceof DrillProjectRel ||
          call.rel(0) instanceof DrillSortRel) {
        final DrillScanRel scan = call.rel(2);
        return checkScan(scan);
      }
      return false;
    }

    public IndexPlanCallContext onMatch(RelOptRuleCall call) {
      DrillProjectRel capProject = null;
      DrillSortRel sort = null;
      if (call.rel(0) instanceof DrillProjectRel) {
        capProject = call.rel(0);
      } else if (call.rel(0) instanceof DrillSortRel) {
        sort = call.rel(0);
      }
      final DrillFilterRel filter = call.rel(1);
      final DrillScanRel scan = call.rel(2);
      return new IndexPlanCallContext(call, sort, capProject, filter, null, scan);
    }
  }

  private static class MatchPFPS extends AbstractMatchFunction {
    public boolean match(RelOptRuleCall call) {
      final DrillScanRel scan = call.rel(3);
      return checkScan(scan);
    }

    public IndexPlanCallContext onMatch(RelOptRuleCall call) {
      final DrillProjectRel capProject = call.rel(0);
      final DrillFilterRel filter = call.rel(1);
      final DrillProjectRel project = call.rel(2);
      final DrillScanRel scan = call.rel(3);
      return new IndexPlanCallContext(call, null, capProject, filter, project, scan);
    }
  }

  private static class MatchSFPS extends AbstractMatchFunction {
    public boolean match(RelOptRuleCall call) {
      final DrillScanRel scan = call.rel(3);
      return checkScan(scan);
    }

    public IndexPlanCallContext onMatch(RelOptRuleCall call) {
      final DrillSortRel sort = call.rel(0);
      final DrillFilterRel filter = call.rel(1);
      final DrillProjectRel project = call.rel(2);
      final DrillScanRel scan = call.rel(3);
      return new IndexPlanCallContext(call, sort, null, filter, project, scan);
    }
  }

  private static class MatchSPFPS extends AbstractMatchFunction {
    public boolean match(RelOptRuleCall call) {
      final DrillScanRel scan = call.rel(4);
      return checkScan(scan);
    }

    public IndexPlanCallContext onMatch(RelOptRuleCall call) {
      final DrillSortRel sort = call.rel(0);
      final DrillProjectRel capProject = call.rel(1);
      final DrillFilterRel filter = call.rel(2);
      final DrillProjectRel project = call.rel(3);
      final DrillScanRel scan = call.rel(4);
      return new IndexPlanCallContext(call, sort, capProject, filter, project, scan);
    }
  }

  private static class MatchSPFS extends AbstractMatchFunction {
    public boolean match(RelOptRuleCall call) {
      final DrillScanRel scan = call.rel(3);
      return checkScan(scan);
    }

    public IndexPlanCallContext onMatch(RelOptRuleCall call) {
      final DrillSortRel sort = call.rel(0);
      final DrillProjectRel capProject = call.rel(1);
      final DrillFilterRel filter = call.rel(2);
      final DrillScanRel scan = call.rel(3);
      return new IndexPlanCallContext(call, sort, capProject, filter, null, scan);
    }
  }

  protected void doOnMatch(IndexPlanCallContext indexContext) {

    Stopwatch indexPlanTimer = Stopwatch.createStarted();
    final PlannerSettings settings = PrelUtil.getPlannerSettings(indexContext.call.getPlanner());
    final IndexCollection indexCollection = getIndexCollection(settings, indexContext.scan);
    if( indexCollection == null ) {
      return;
    }

    RexBuilder builder = indexContext.filter.getCluster().getRexBuilder();

    RexNode condition = null;
    if (indexContext.lowerProject == null) {
      condition = indexContext.filter.getCondition();
    } else {
      // get the filter as if it were below the projection.
      condition = RelOptUtil.pushFilterPastProject(indexContext.filter.getCondition(), indexContext.lowerProject);
    }

    RewriteAsBinaryOperators visitor = new RewriteAsBinaryOperators(true, builder);
    condition = condition.accept(visitor);

    if (indexCollection.supportsIndexSelection()) {
      processWithIndexSelection(indexContext, settings, condition,
          indexCollection, builder);
    } else {
      throw new UnsupportedOperationException("Index collection must support index selection");
    }
    indexPlanTimer.stop();
    logger.info("Index Planning took {} ms", indexPlanTimer.elapsed(TimeUnit.MILLISECONDS));
  }
  /**
   * Return the index collection relevant for the underlying data source
   * @param settings
   * @param scan
   */
  public IndexCollection getIndexCollection(PlannerSettings settings, DrillScanRel scan) {
    DbGroupScan groupScan = (DbGroupScan)scan.getGroupScan();
    return groupScan.getSecondaryIndexCollection(scan);
  }
  /**
   *
   * @param inputRel  the rel node provide input for filter to operate upon
   * @param indexCondition
   * @param indexDesc
   * @return If all indexable expressions involved in this condition are in the indexed fields of
   * the single IndexDescriptor, return true
   */
  static private ConditionIndexed conditionIndexed(RelNode inputRel, RexNode indexCondition, IndexDescriptor indexDesc) {
    return conditionIndexedHelper(inputRel, indexCondition, indexDesc);
  }

  static private ConditionIndexed conditionIndexedHelper(RelNode inputRel, RexNode indexCondition, IndexDescriptor indexDesc) {
    IndexableExprMarker exprMarker = new IndexableExprMarker(inputRel);
    indexCondition.accept(exprMarker);
    Map<RexNode, LogicalExpression> mapRexExpr = exprMarker.getIndexableExpression();
    List<LogicalExpression> infoCols = Lists.newArrayList();
    infoCols.addAll(mapRexExpr.values());
    if (indexDesc.allColumnsIndexed(infoCols)) {
      return ConditionIndexed.FULL;
    } else if (indexDesc.someColumnsIndexed(infoCols)) {
      return ConditionIndexed.PARTIAL;
    } else {
      return ConditionIndexed.NONE;
    }
  }

  private boolean isConditionPrefix(IndexDescriptor indexDesc, RexNode initCondition, IndexConditionInfo.Builder infoBuilder) {
    List<LogicalExpression> indexCols = indexDesc.getIndexColumns();
    boolean prefix = true;
    if (indexCols.size() > 0 && initCondition != null) {
      int i=0;
      while (prefix && i < indexCols.size()) {
        LogicalExpression p = indexCols.get(i++);
        List<LogicalExpression> prefixCol = ImmutableList.of(p);
        IndexConditionInfo info = infoBuilder.indexConditionRelatedToFields(prefixCol, initCondition);
        if(info != null && info.hasIndexCol) {
          initCondition = info.remainderCondition;
          if (initCondition.isAlwaysTrue()) {
            // all filter conditions are accounted for, so if the remainder is TRUE, set it to NULL because
            // we don't need to keep track of it for rest of the index selection
            initCondition = null;
            break;
          }
        } else {
          prefix = false;
        }
      }
    }
    return prefix;
  }

  /**
   *
   */
  private void processWithIndexSelection(
      IndexPlanCallContext indexContext,
      PlannerSettings settings,
      RexNode condition,
      IndexCollection collection,
      RexBuilder builder) {
    double totalRows = 0;
    double filterRows = totalRows;
    DrillScanRel scan = indexContext.scan;
    if (! (indexContext.scan.getGroupScan() instanceof DbGroupScan) ) {
      return;
    }
    IndexConditionInfo.Builder infoBuilder = IndexConditionInfo.newBuilder(condition, collection, builder, indexContext.scan);
    IndexConditionInfo cInfo = infoBuilder.getCollectiveInfo();

    if (!cInfo.hasIndexCol) {
      logger.info("No index columns are projected from the scan..continue.");
      return;
    }

    if (cInfo.indexCondition == null) {
      logger.info("No conditions were found eligible for applying index lookup.");
      return;
    }

    if (scan.getGroupScan() instanceof DbGroupScan) {
      // Initialize statistics
      DbGroupScan dbScan = ((DbGroupScan) scan.getGroupScan());
      dbScan.getStatistics().initialize(condition, scan, indexContext);
      totalRows = dbScan.getRowCount(null, scan);
      filterRows = dbScan.getRowCount(condition, scan);
      if (filterRows/totalRows > Math.min(1.0, 2.0 * settings.getIndexSelectivityFactor())) {
        // Generate full table scan only plans for selectivity > 2*index_selectivity_factor
        return;
      }
    }

    RexNode indexCondition = cInfo.indexCondition;
    RexNode remainderCondition = cInfo.remainderCondition;

    List<IndexDescriptor> coveringIndexes = Lists.newArrayList();
    List<IndexDescriptor> nonCoveringIndexes = Lists.newArrayList();

    if (settings.isCostBasedIndexSelectionEnabled()) {
      IndexSelector selector = new IndexSelector(indexCondition,
          indexContext,
          collection,
          builder,
          totalRows);

      for (IndexDescriptor indexDesc : collection) {
        if (conditionIndexed(indexContext.scan, indexCondition, indexDesc) != ConditionIndexed.NONE) {
          FunctionalIndexInfo functionInfo = indexDesc.getFunctionalInfo();
          selector.addIndex(indexDesc, isCoveringIndex(indexContext, functionInfo),
              indexContext.lowerProject != null ? indexContext.lowerProject.getRowType().getFieldCount() :
                scan.getRowType().getFieldCount());
        }
      }
      // get the candidate indexes based on selection
      selector.getCandidateIndexes(coveringIndexes, nonCoveringIndexes);
    } else {
      // get the list of covering and non-covering indexes for this collection
      for (IndexDescriptor indexDesc : collection) {
        if(conditionIndexed(indexContext.scan, indexCondition, indexDesc) != ConditionIndexed.NONE &&
            isConditionPrefix(indexDesc, indexCondition, infoBuilder)) {
          FunctionalIndexInfo functionInfo = indexDesc.getFunctionalInfo();
          if (isCoveringIndex(indexContext, functionInfo)) {
            coveringIndexes.add(indexDesc);
          } else {
            nonCoveringIndexes.add(indexDesc);
          }
        }
      }
    }

    if (logger.isDebugEnabled()) {
      StringBuffer strBuf = new StringBuffer();
      strBuf.append("Split  indexes:");
      if (coveringIndexes.size() > 0) {
        for (IndexDescriptor indexDesc : coveringIndexes) {
          strBuf.append(indexDesc.getIndexName()).append(",");
        }
      }
      if(nonCoveringIndexes.size() > 0) {
        strBuf.append("non-covering indexes:");
        for (IndexDescriptor indexDesc : nonCoveringIndexes) {
          strBuf.append(indexDesc.getIndexName()).append(",");
        }
      }
      logger.debug(strBuf.toString());
    }
    //Not a single covering index plan or non-covering index plan is sufficient.
    // either 1) there is no usable index at all, 2) or the chop of original condition to
    // indexCondition + remainderCondition is not working for any single index.
    if (coveringIndexes.size() == 0 && nonCoveringIndexes.size() > 1) {
      Map<IndexDescriptor, IndexConditionInfo> indexInfoMap = infoBuilder.getIndexConditionMap();

      //no usable index
      if (indexInfoMap == null || indexInfoMap.size() == 0) {
        return;
      }

      //if there is only one index found, no need to do intersect, but just a regular non-covering plan
      //some part of filter condition needs to apply on primary table.
      if(indexInfoMap.size() == 1) {
        IndexDescriptor idx = indexInfoMap.keySet().iterator().next();
        nonCoveringIndexes.add(idx);
        indexCondition = indexInfoMap.get(idx).indexCondition;
        remainderCondition = indexInfoMap.get(idx).remainderCondition;
      }
      else {
        //multiple indexes, let us try to intersect results from multiple index tables
        //TODO: make sure the smallest selectivity of these indexes times rowcount smaller than broadcast threshold

        IndexIntersectPlanGenerator planGen = new IndexIntersectPlanGenerator(
            indexContext, indexInfoMap, builder, settings);
        try {
          planGen.go();
        } catch (Exception e) {
          logger.warn("Exception while trying to generate intersect index plan", e);
          return;
        }
        //TODO:we may generate some more non-covering plans(each uses a single index) from the indexes of smallest selectivity
        return;
      }
    }

    boolean createdCovering = false;
    try {
      for (IndexDescriptor indexDesc : coveringIndexes) {
        IndexGroupScan idxScan = indexDesc.getIndexGroupScan();
        FunctionalIndexInfo indexInfo = indexDesc.getFunctionalInfo();
        //Copy primary table statistics to index table
        idxScan.setStatistics(((DbGroupScan) scan.getGroupScan()).getStatistics());
        logger.info("Generating covering index plan for query condition {}", indexCondition.toString());

        CoveringIndexPlanGenerator planGen = new CoveringIndexPlanGenerator(indexContext, indexInfo, idxScan,
            indexCondition, remainderCondition, builder, settings);

        planGen.go();
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
    GroupScan primaryTableScan = indexContext.scan.getGroupScan();
    if (primaryTableScan instanceof DbGroupScan &&
        (((DbGroupScan) primaryTableScan).supportsRestrictedScan())) {
      try {
        for (IndexDescriptor index : nonCoveringIndexes) {
          IndexGroupScan idxScan = index.getIndexGroupScan();
          //Copy primary table statistics to index table
          idxScan.setStatistics(((DbGroupScan) primaryTableScan).getStatistics());
          logger.info("Generating non-covering index plan for query condition {}", indexCondition.toString());
          NonCoveringIndexPlanGenerator planGen = new NonCoveringIndexPlanGenerator(indexContext, index,
            idxScan, indexCondition, remainderCondition, builder, settings);
          planGen.go();
        }
      } catch (Exception e) {
        logger.warn("Exception while trying to generate non-covering index access plan", e);
        return;
      }
    }
  }

  /**
   * For a particular table scan for table T1 and an index on that table, find out if it is a covering index
   * @return
   */
  private boolean isCoveringIndex(IndexPlanCallContext indexContext, FunctionalIndexInfo functionInfo) {
    if(functionInfo.hasFunctional()) {
      //need info from full query
      return queryCoveredByIndex(indexContext, functionInfo);
    }
    DbGroupScan groupScan = (DbGroupScan) indexContext.scan.getGroupScan();
    List<LogicalExpression> tableCols = Lists.newArrayList();
    tableCols.addAll(groupScan.getColumns());
    return functionInfo.getIndexDesc().isCoveringIndex(tableCols);
  }

  //this check queryCoveredByIndex is needed only when there is a function based index field.
  //If there is no function based index field, we don't need to worry whether there could be
  //expressions not covered by the index existing somewhere out of the visible scope of this rule(project-filter-scan).

  /**
   * For the given index with functional field, e.g. cast(a.b as INT), use the knowledge of the whole query(cached in optimizerContext from previous convert process)
   * to check if there are expressions other than indexed function cast(a.b as INT) has field a.b and a.b is not in the index
   * @param functionInfo
   * @return false if the query could not be covered by the index (should not create covering index plan)
   */
  boolean queryCoveredByIndex(IndexPlanCallContext indexContext,
                              FunctionalIndexInfo functionInfo) {
    //for indexed functions, if relevant schemapaths are included in index(in indexed fields or non-indexed fields),
    // check covering based on the local information we have:
    //   if references to schema paths in functional indexes disappear beyond capProject

    if (indexContext.upperProject == null) {
      if( !isFullQuery(indexContext)) {
        return false;
      }
    }

    DrillParseContext parserContext =
        new DrillParseContext(PrelUtil.getPlannerSettings(indexContext.call.rel(0).getCluster()));

    Set<LogicalExpression> exprs = Sets.newHashSet();
    if (indexContext.upperProject != null) {
      if (indexContext.lowerProject == null) {
        for (RexNode rex : indexContext.upperProject.getProjects()) {
          LogicalExpression expr = DrillOptiq.toDrill(parserContext, indexContext.scan, rex);
          exprs.add(expr);
        }
      } else {
        //we have underneath project, so we have to do more to convert expressions
        for (RexNode rex : indexContext.upperProject.getProjects()) {
          LogicalExpression expr = RexToExpression.toDrill(parserContext, indexContext.lowerProject, indexContext.scan, rex);
          exprs.add(expr);
        }
      }
    }
    else {//capProject == null
      if (indexContext.lowerProject != null) {
        for (RexNode rex : indexContext.lowerProject.getProjects()) {
          LogicalExpression expr = DrillOptiq.toDrill(parserContext, indexContext.scan, rex);
          exprs.add(expr);
        }
      }
    }

    Map<LogicalExpression, Set<SchemaPath>> exprPathMap = functionInfo.getPathsInFunctionExpr();
    PathInExpr exprSearch = new PathInExpr(exprPathMap);

    for(LogicalExpression expr: exprs) {
      if(expr.accept(exprSearch, null) == false) {
        return false;
      }
    }
    //if we come to here, paths in indexed function expressions are covered in capProject.
    //now we check other paths.

    //check the leftout paths (appear in capProject other than functional index expression) are covered by other index fields or not
    List<LogicalExpression> leftPaths = Lists.newArrayList(exprSearch.getRemainderPaths());

    indexContext.leftOutPathsInFunctions = exprSearch.getRemainderPathsInFunctions();
    return functionInfo.getIndexDesc().isCoveringIndex(leftPaths);
  }

  private boolean isFullQuery(IndexPlanCallContext indexContext) {
    RelNode rootInCall = indexContext.call.rel(0);
    //check if the tip of the operator stack we have is also the top of the whole query, if yes, return true
    if (indexContext.call.getPlanner().getRoot() instanceof RelSubset) {
      final RelSubset rootSet = (RelSubset) indexContext.call.getPlanner().getRoot();
      if (rootSet.getRelList().contains(rootInCall)) {
        return true;
      }
    } else {
      if (indexContext.call.getPlanner().getRoot().equals(rootInCall)) {
        return true;
      }
    }

    return false;
  }

}
