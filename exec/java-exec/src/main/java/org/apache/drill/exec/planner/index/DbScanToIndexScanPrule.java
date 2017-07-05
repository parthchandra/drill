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
import com.google.common.collect.Sets;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
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
   * generate logical expressions for sort rexNodes in SortRel
   * @param indexContext
   */
  private void updateSortExpression(IndexPlanCallContext indexContext) {
    if(indexContext.sort == null) {
      return;
    }

    DrillParseContext parserContext =
        new DrillParseContext(PrelUtil.getPlannerSettings(indexContext.call.rel(0).getCluster()));

    indexContext.sortExprs = Lists.newArrayList();
    for (RelFieldCollation collation : indexContext.sort.getCollation().getFieldCollations()) {
      int idx = collation.getFieldIndex();
      DrillProjectRel oneProject;
      if (indexContext.upperProject != null && indexContext.lowerProject != null) {
        LogicalExpression expr = RexToExpression.toDrill(parserContext, indexContext.lowerProject, indexContext.scan,
            indexContext.upperProject.getProjects().get(idx));
        indexContext.sortExprs.add(expr);
      }
      else {//one project is null now
        oneProject = (indexContext.upperProject != null)? indexContext.upperProject : indexContext.lowerProject;
        if(oneProject != null) {
          LogicalExpression expr = RexToExpression.toDrill(parserContext, null, indexContext.scan,
              oneProject.getProjects().get(idx));
          indexContext.sortExprs.add(expr);
        }
        else {//two projects are null
          SchemaPath path;
          RelDataTypeField f = indexContext.scan.getRowType().getFieldList().get(idx);
          String pathSeg = f.getName().replaceAll("`", "");
          final String[] segs = pathSeg.split("\\.");
          path = SchemaPath.getCompoundPath(segs);
          indexContext.sortExprs.add(path);
        }
      }
    }
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
    RexNode indexCondition = cInfo.indexCondition;
    RexNode remainderCondition = cInfo.remainderCondition;

    IndexableExprMarker indexableExprMarker = new IndexableExprMarker(indexContext.scan);
    indexCondition.accept(indexableExprMarker);
    indexContext.origMarker = indexableExprMarker;

    if (scan.getGroupScan() instanceof DbGroupScan) {
      // Initialize statistics
      DbGroupScan dbScan = ((DbGroupScan) scan.getGroupScan());
      dbScan.getStatistics().initialize(condition, scan, indexContext);
      totalRows = dbScan.getRowCount(null, scan);
      filterRows = dbScan.getRowCount(condition, scan);
      double sel = filterRows/totalRows;
      if (totalRows != Statistics.ROWCOUNT_UNKNOWN &&
          filterRows != Statistics.ROWCOUNT_UNKNOWN &&
          !settings.isDisableFullTableScan() &&
          sel > Math.max(settings.getCoveringIndexSelectivityFactor(),
              settings.getNonCoveringIndexSelectivityFactor() )) {
        // If full table scan is not disabled, generate full table scan only plans if selectivity
        // is greater than covering and non-covering selectivity thresholds
        logger.info("Skip index planning because filter selectivity: {} is greater than index thresholds", sel);
        return;
      }
    }

    if (totalRows == Statistics.ROWCOUNT_UNKNOWN ||
        totalRows == 0) {
      logger.warn("Total row count is UNKNOWN or 0; skip index planning");
      return;
    }

    List<IndexDescriptor> coveringIndexes = Lists.newArrayList();
    List<IndexDescriptor> nonCoveringIndexes = Lists.newArrayList();

    //update sort expressions in context
    updateSortExpression(indexContext);

    IndexSelector selector = new IndexSelector(indexCondition,
        indexContext,
        collection,
        builder,
        totalRows);

    for (IndexDescriptor indexDesc : collection) {
      // check if any of the indexed fields of the index are present in the filter condition
      if (IndexPlanUtils.conditionIndexed(indexableExprMarker, indexDesc) != IndexPlanUtils.ConditionIndexed.NONE) {
        FunctionalIndexInfo functionInfo = indexDesc.getFunctionalInfo();
        selector.addIndex(indexDesc, isCoveringIndex(indexContext, functionInfo),
            indexContext.lowerProject != null ? indexContext.lowerProject.getRowType().getFieldCount() :
                scan.getRowType().getFieldCount());
      }
    }
    // get the candidate indexes based on selection
    selector.getCandidateIndexes(coveringIndexes, nonCoveringIndexes);

    if (logger.isDebugEnabled()) {
      StringBuffer strBuf = new StringBuffer();
      if (coveringIndexes.size() > 0) {
        strBuf.append("Covering indexes:");
        for (IndexDescriptor indexDesc : coveringIndexes) {
          strBuf.append(indexDesc.getIndexName()).append(", ");
        }
      }
      if(nonCoveringIndexes.size() > 0) {
        strBuf.append("Non-covering indexes:");
        for (IndexDescriptor indexDesc : nonCoveringIndexes) {
          strBuf.append(indexDesc.getIndexName()).append(", ");
        }
      }
      logger.debug(strBuf.toString());
    }

    //Not a single covering index plan or non-covering index plan is sufficient.
    // either 1) there is no usable index at all, 2) or the chop of original condition to
    // indexCondition + remainderCondition is not working for any single index.
    if (coveringIndexes.size() == 0 && nonCoveringIndexes.size() > 1) {

      Map<IndexDescriptor, IndexConditionInfo> indexInfoMap = infoBuilder.getIndexConditionMap(nonCoveringIndexes);

      //no usable index
      if (indexInfoMap == null || indexInfoMap.size() == 0) {
        return;
      }

      //if there is only one index found, no need to do intersect, but just a regular non-covering plan
      //some part of filter condition needs to apply on primary table.
      if(indexInfoMap.size() == 1) {
        IndexDescriptor idx = indexInfoMap.keySet().iterator().next();
        if (infoBuilder.isConditionPrefix(idx, indexCondition) && !nonCoveringIndexes.contains(idx)) {
          nonCoveringIndexes.add(idx);
          //indexCondition = indexInfoMap.get(idx).indexCondition;
          //remainderCondition = indexInfoMap.get(idx).remainderCondition;
        }
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
        }
        //TODO:we may generate some more non-covering plans(each uses a single index) from the indexes of smallest selectivity
        return;
      }
    }

    try {
      for (IndexDescriptor indexDesc : coveringIndexes) {
        IndexGroupScan idxScan = indexDesc.getIndexGroupScan();
        FunctionalIndexInfo indexInfo = indexDesc.getFunctionalInfo();
        //Copy primary table statistics to index table
        idxScan.setStatistics(((DbGroupScan) scan.getGroupScan()).getStatistics());
        logger.info("Generating covering index plan for index: {}, query condition {}", indexDesc.getIndexName(), indexCondition.toString());

        CoveringIndexPlanGenerator planGen = new CoveringIndexPlanGenerator(indexContext, indexInfo, idxScan,
            indexCondition, remainderCondition, builder, settings);

        planGen.go();
      }
    } catch (Exception e) {
      logger.warn("Exception while trying to generate covering index plan", e);
    }

    // Create non-covering index plans.
    GroupScan primaryTableScan = indexContext.scan.getGroupScan();

    //First, check if the primary table scan supports creating a restricted scan
    if (primaryTableScan instanceof DbGroupScan &&
        (((DbGroupScan) primaryTableScan).supportsRestrictedScan())) {
      try {
        for (IndexDescriptor index : nonCoveringIndexes) {
          IndexGroupScan idxScan = index.getIndexGroupScan();

          //get indexCondition, remainderCondition for this index, ideally we should share the conditions with IndexSelector
          IndexConditionInfo idxInfo = infoBuilder.getIndexConditionInfo(index);
          indexCondition = idxInfo.indexCondition;
          remainderCondition = idxInfo.remainderCondition;
          //Copy primary table statistics to index table
          idxScan.setStatistics(((DbGroupScan) primaryTableScan).getStatistics());
          logger.info("Generating non-covering index plan for index: {}, query condition {}", index.getIndexName(), indexCondition.toString());
          NonCoveringIndexPlanGenerator planGen = new NonCoveringIndexPlanGenerator(indexContext, index,
            idxScan, indexCondition, remainderCondition, builder, settings);
          planGen.go();
        }
      } catch (Exception e) {
        logger.warn("Exception while trying to generate non-covering index access plan", e);
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