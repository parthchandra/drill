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

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.common.DrillProjectRelBase;
import org.apache.drill.exec.planner.fragment.DistributionAffinity;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

public class IndexPlanUtils {

  public enum ConditionIndexed {
    NONE,
    PARTIAL,
    FULL}

  /**
   * Check if any of the fields of the index are present in a list of LogicalExpressions supplied
   * as part of IndexableExprMarker
   * @param exprMarker, the marker that has analyzed original index condition on top of original scan
   * @param indexDesc
   * @return ConditionIndexed.FULL, PARTIAL or NONE depending on whether all, some or no columns
   * of the indexDesc are present in the list of LogicalExpressions supplied as part of exprMarker
   *
   */
  static public ConditionIndexed conditionIndexed(IndexableExprMarker exprMarker, IndexDescriptor indexDesc) {
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

  /**
   * check if we want to apply index rules on this scan,
   * if group scan is not instance of DbGroupScan, or this DbGroupScan instance does not support secondary index, or
   *    this scan is already an index scan or Restricted Scan, do not apply index plan rules on it.
   * @param scanRel
   * @return
   */
  static public boolean checkScan(DrillScanRel scanRel) {
    GroupScan groupScan = scanRel.getGroupScan();
    if (groupScan instanceof DbGroupScan) {
      DbGroupScan dbscan = ((DbGroupScan) groupScan);
      //if we already applied index convert rule, and this scan is indexScan or restricted scan already,
      //no more trying index convert rule
      return dbscan.supportsSecondaryIndex() && (!dbscan.isIndexScan()) && (!dbscan.isRestrictedScan());
    }
    return false;
  }

  /**
   * For a particular table scan for table T1 and an index on that table, find out if it is a covering index
   * @return
   */
  static public boolean isCoveringIndex(IndexPlanCallContext indexContext, FunctionalIndexInfo functionInfo) {
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
  static private boolean queryCoveredByIndex(IndexPlanCallContext indexContext,
                              FunctionalIndexInfo functionInfo) {
    //for indexed functions, if relevant schemapaths are included in index(in indexed fields or non-indexed fields),
    // check covering based on the local information we have:
    //   if references to schema paths in functional indexes disappear beyond capProject

    if (indexContext.filter != null && indexContext.upperProject == null) {
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
    else if (indexContext.lowerProject != null) {
      for (RexNode rex : indexContext.lowerProject.getProjects()) {
        LogicalExpression expr = DrillOptiq.toDrill(parserContext, indexContext.scan, rex);
        exprs.add(expr);
      }
    }
    else {//upperProject and lowerProject both are null, the only place to find columns being used in query is scan
      exprs.addAll(indexContext.scan.getColumns());
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

  static private boolean isFullQuery(IndexPlanCallContext indexContext) {
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

  /**
   * Build collation property for the 'lower' project, the one closer to the Scan
   * @param projectRexs
   * @param input
   * @param indexInfo
   * @return the output RelCollation
   */
  public static RelCollation buildCollationLowerProject(List<RexNode> projectRexs, RelNode input, FunctionalIndexInfo indexInfo) {
    //if leading fields of index are here, add them to RelCollation
    List<RelFieldCollation> newFields = Lists.newArrayList();
    if (!indexInfo.hasFunctional()) {
      Map<LogicalExpression, Integer> projectExprs = Maps.newLinkedHashMap();
      DrillParseContext parserContext = new DrillParseContext(PrelUtil.getPlannerSettings(input.getCluster()));
      int idx=0;
      for(RexNode rex : projectRexs) {
        projectExprs.put(DrillOptiq.toDrill(parserContext, input, rex), idx);
        idx++;
      }
      int idxFieldCount = 0;
      for (LogicalExpression expr : indexInfo.getIndexDesc().getIndexColumns()) {
        if (!projectExprs.containsKey(expr)) {
          break;
        }
        RelFieldCollation.Direction dir = indexInfo.getIndexDesc().getCollation().getFieldCollations().get(idxFieldCount).direction;
        if ( dir == null) {
          break;
        }
        newFields.add(new RelFieldCollation(projectExprs.get(expr), dir,
            RelFieldCollation.NullDirection.UNSPECIFIED));
      }
      idxFieldCount++;
    } else {
      // TODO: handle functional index
    }

    return RelCollations.of(newFields);
  }

  /**
   * Build collation property for the 'upper' project, the one above the filter
   * @param projectRexs
   * @param inputCollation
   * @param indexInfo
   * @param collationFilterMap
   * @return the output RelCollation
   */
  public static RelCollation buildCollationUpperProject(List<RexNode> projectRexs,
                                                        RelCollation inputCollation, FunctionalIndexInfo indexInfo,
                                                        Map<Integer, List<RexNode>> collationFilterMap) {
    List<RelFieldCollation> outputFieldCollations = Lists.newArrayList();

    if (inputCollation != null) {
      List<RelFieldCollation> inputFieldCollations = inputCollation.getFieldCollations();
      if (!indexInfo.hasFunctional()) {
        for (int projectExprIdx = 0; projectExprIdx < projectRexs.size(); projectExprIdx++) {
          RexNode n = projectRexs.get(projectExprIdx);
          if (n instanceof RexInputRef) {
            RexInputRef ref = (RexInputRef)n;
            boolean eligibleForCollation = true;
            int maxIndex = getIndexFromCollation(ref.getIndex(), inputFieldCollations);
            if (maxIndex < 0) {
              eligibleForCollation = false;
              continue;
            }
            // check if the prefix has equality conditions
            for (int i = 0; i < maxIndex; i++) {
              int fieldIdx = inputFieldCollations.get(i).getFieldIndex();
              List<RexNode> conditions = collationFilterMap != null ? collationFilterMap.get(fieldIdx) : null;
              if ((conditions == null || conditions.size() == 0) &&
                  i < maxIndex-1) {
                // if an intermediate column has no filter condition, it would select all values
                // of that column, so a subsequent column cannot be eligible for collation
                eligibleForCollation = false;
                break;
              } else {
                for (RexNode r : conditions) {
                  if (!(r.getKind() == SqlKind.EQUALS)) {
                    eligibleForCollation = false;
                    break;
                  }
                }
              }
            }
            // for every projected expr, if it is eligible for collation, get the
            // corresponding field collation from the input
            if (eligibleForCollation) {
              for (RelFieldCollation c : inputFieldCollations) {
                if (ref.getIndex() == c.getFieldIndex()) {
                  RelFieldCollation outFieldCollation = new RelFieldCollation(projectExprIdx, c.getDirection(), c.nullDirection);
                  outputFieldCollations.add(outFieldCollation);
                }
              }
            }
          }
        }
      } else {
        // TODO: handle functional index
      }
    }
    return RelCollations.of(outputFieldCollations);
  }

  public static int getIndexFromCollation(int refIndex, List<RelFieldCollation> inputFieldCollations) {
    for (int i=0; i < inputFieldCollations.size(); i++) {
      if (refIndex == inputFieldCollations.get(i).getFieldIndex()) {
        return i;
      }
    }
    return -1;
  }


  /**
   * generate logical expressions for sort rexNodes in SortRel, the result is store to IndexPlanCallContext
   * @param indexContext
   */
  public static void updateSortExpression(IndexPlanCallContext indexContext) {
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
   * @param expr
   * @param context
   * @return if there is filter and expr is only in equality condition of the filter, return true
   */
  private static boolean exprOnlyInEquality(LogicalExpression expr, IndexPlanCallContext context) {
    //if there is no filter, expr wont be in equality
    if(context.filter == null) {
      return false;
    }
    final Set<LogicalExpression> onlyInEquality = context.origMarker.getExpressionsOnlyInEquality();
    return onlyInEquality.contains(expr);

  }
  /**
   * Build collation property for project, the one closer to the Scan
   * @param projectRexs the expressions to project
   * @param project the project between projectRexs and input, it could be null if no such intermediate project(lower project)
   * @param input  the input RelNode to the project, usually it is the scan operator.
   * @param indexInfo the index for which we are building index plan
   * @param context the context of this index planning process
   * @return the output RelCollation
   */
  public static RelCollation buildCollationProject(List<RexNode> projectRexs,
                                                   DrillProjectRel project,
                                                   RelNode input,
                                                   FunctionalIndexInfo indexInfo,
                                                   IndexPlanCallContext context) {
    Map<LogicalExpression, Integer> projectExprs = getProjectExprs(projectRexs, project, input);
    return buildCollationForExpressions(projectExprs, indexInfo.getIndexDesc(), context);
  }

  /**
   * Build the collation property for index scan
   * @param indexDesc the index for which we are building index plan
   * @param context the context of this index planning process
   * @return the output RelCollation for the scan on index
   */
  public static RelCollation buildCollationCoveringIndexScan(IndexDescriptor indexDesc,
      IndexPlanCallContext context) {
    Map<LogicalExpression, Integer> rowTypeExprs = getExprsFromRowType(context.scan.getRowType());
    return buildCollationForExpressions(rowTypeExprs, indexDesc, context);
  }

  public static Map<LogicalExpression, Integer> getProjectExprs(List<RexNode> projectRexs,
                                                                DrillProjectRel project,
                                                                RelNode input) {
    Map<LogicalExpression, Integer> projectExprs = Maps.newLinkedHashMap();
    DrillParseContext parserContext = new DrillParseContext(PrelUtil.getPlannerSettings(input.getCluster()));
    int idx=0;
    for(RexNode rex : projectRexs) {
      LogicalExpression expr;
      expr = RexToExpression.toDrill(parserContext, project, input, rex);
      projectExprs.put(expr, idx);
      idx++;
    }
    return projectExprs;
  }

  public static Map<LogicalExpression, Integer> getExprsFromRowType( RelDataType indexScanRowType) {

    Map<LogicalExpression, Integer> rowTypeExprs = Maps.newLinkedHashMap();
    int idx = 0;
    for (RelDataTypeField field : indexScanRowType.getFieldList()) {
      rowTypeExprs.put(FieldReference.getWithQuotedRef(field.getName()), idx++);
    }
    return rowTypeExprs;
  }

  /**
   * Given index, compute the collations for a list of projected expressions(from Scan's rowType or Project's )
   * in the context
   * @param projectExprs the output expression list of a RelNode
   * @param indexDesc  the index for which we are building index plan
   * @param context  the context of this index planning process
   * @return the collation provided by index that will be exposed by the expression list
   */
  public static RelCollation buildCollationForExpressions(Map<LogicalExpression, Integer> projectExprs,
                                                        IndexDescriptor indexDesc,
                                                        IndexPlanCallContext context) {

    assert projectExprs != null;
    final List<LogicalExpression> sortExpressions = context.sortExprs;
    // if leading fields of index are here, add them to RelCollation
    List<RelFieldCollation> newFields = Lists.newArrayList();
    if (sortExpressions == null) {
      return RelCollations.of(newFields);
    }

    // go through indexed fields to build collation
    // break out of the loop when found first indexed field [not projected && not _only_ in equality condition of filter]
    // or the leading field is not projected
    List<LogicalExpression> indexedCols = indexDesc.getIndexColumns();
    for (int idxFieldCount=0; idxFieldCount<indexedCols.size(); ++idxFieldCount) {
      LogicalExpression expr = indexedCols.get(idxFieldCount);

      if (!projectExprs.containsKey(expr)) {
        // leading indexed field is not projected
        // but it is only-in-equality field, -- we continue to next indexed field, but we don't generate collation for this field
        if(exprOnlyInEquality(expr, context)) {
          continue;
        }
        // else no more collation is needed to be generated, since we now have one leading field which is not in equality condition
        break;
      }

      // leading indexed field is projected,

      // if this field is not in sort expression && only-in-equality, we don't need to generate collation for this field
      // and we are okay to continue: generate collation for next indexed field.
      if (!sortExpressions.contains(expr) && exprOnlyInEquality(expr, context) ) {
        continue;
      }

      RelCollation idxCollation = indexDesc.getCollation();
      RelFieldCollation.Direction dir = (idxCollation == null)?
          null : idxCollation.getFieldCollations().get(idxFieldCount).direction;
      if ( dir == null) {
        break;
      }
      newFields.add(new RelFieldCollation(projectExprs.get(expr), dir,
          RelFieldCollation.NullDirection.UNSPECIFIED));
    }

    return RelCollations.of(newFields);
  }

  // TODO: proper implementation
  public static boolean pathOnlyInIndexedFunction(SchemaPath path) {
    return true;
  }

  public static RelCollation buildCollationNonCoveringIndexScan(IndexDescriptor indexDesc,
      RelDataType indexScanRowType,
      RelDataType restrictedScanRowType, IndexPlanCallContext context) {

    if (context.sortExprs == null) {
      return RelCollations.of(RelCollations.EMPTY.getFieldCollations());
    }

    final List<RelDataTypeField> indexFields = indexScanRowType.getFieldList();
    final List<RelDataTypeField> rsFields = restrictedScanRowType.getFieldList();
    final Map<LogicalExpression, RelFieldCollation> collationMap = indexDesc.getCollationMap();

    assert collationMap != null : "Invalid collation map for index";

    List<RelFieldCollation> fieldCollations = Lists.newArrayList();

    Map<Integer, RelFieldCollation> rsScanCollationMap = Maps.newTreeMap();

    // for each index field that is projected from the indexScan, find the corresponding
    // field in the restricted scan's row type and keep track of the ordinal # in the
    // restricted scan's row type.
    for (int i = 0; i < indexScanRowType.getFieldCount(); i++) {
      RelDataTypeField f1 = indexFields.get(i);
      for (int j = 0; j < rsFields.size(); j++) {
        RelDataTypeField f2 = rsFields.get(j);
        if (f1.getName().equals(f2.getName())) {
          FieldReference ref = FieldReference.getWithQuotedRef(f1.getName());
          RelFieldCollation origCollation = collationMap.get(ref);
          if (origCollation != null) {
            RelFieldCollation fc = new RelFieldCollation(j,//origCollation.getFieldIndex(),
                origCollation.direction, origCollation.nullDirection);
            rsScanCollationMap.put(origCollation.getFieldIndex(), fc);
          }
        }
      }
    }

    // should sort by the order of these fields in indexDesc
    for (Map.Entry<Integer, RelFieldCollation> entry : rsScanCollationMap.entrySet()) {
      RelFieldCollation fc = entry.getValue();
      if (fc != null) {
        fieldCollations.add(fc);
      }
    }

    final RelCollation collation = RelCollations.of(fieldCollations);
    return collation;
  }

  public static boolean scanIsPartition(GroupScan scan) {
    return (scan.isDistributed() || scan.getDistributionAffinity() == DistributionAffinity.HARD);
  }

  public static ScanPrel buildCoveringIndexScan(DrillScanRel origScan,
      IndexGroupScan indexGroupScan,
      IndexPlanCallContext indexContext,
      IndexDescriptor indexDesc) {

    FunctionalIndexInfo functionInfo = indexDesc.getFunctionalInfo();
    indexGroupScan.setColumns(
        rewriteFunctionColumn(((DbGroupScan)origScan.getGroupScan()).getColumns(),
            functionInfo));

    DrillDistributionTrait partition = scanIsPartition(origScan.getGroupScan())?
        DrillDistributionTrait.RANDOM_DISTRIBUTED : DrillDistributionTrait.SINGLETON;
    RelDataType newRowType = FunctionalIndexHelper.rewriteFunctionalRowType(origScan, indexContext, functionInfo);

    // add a default collation trait otherwise Calcite runs into a ClassCastException, which at first glance
    // seems like a Calcite bug
    RelTraitSet indexScanTraitSet = origScan.getTraitSet().plus(Prel.DRILL_PHYSICAL).
        plus(RelCollationTraitDef.INSTANCE.getDefault()).plus(partition);

    // Create the collation traits for index scan based on the index columns under the
    // condition that the index actually has collation property (e.g hash indexes don't)
    if (indexDesc.getCollation() != null) {
      RelCollation collationTrait = buildCollationCoveringIndexScan(indexDesc, indexContext);
      indexScanTraitSet = indexScanTraitSet.plus(collationTrait);
    }

    ScanPrel indexScanPrel = new ScanPrel(origScan.getCluster(),
        indexScanTraitSet, indexGroupScan,
        newRowType);

    return indexScanPrel;
  }

  /**
   * For IndexGroupScan, if a column is only appeared in the should-be-renamed function,
   * this column is to-be-replaced column, we replace that column(schemaPath) from 'a.b'
   * to '$1' in the list of SchemaPath.
   * @param paths
   * @param functionInfo functional index information that may impact rewrite
   * @return
   */
  public static List<SchemaPath> rewriteFunctionColumn(List<SchemaPath> paths, FunctionalIndexInfo functionInfo) {
    if (!functionInfo.hasFunctional()) {
      return paths;
    }

    List<SchemaPath> newPaths = Lists.newArrayList(paths);
    for (int i=0; i<paths.size(); ++i) {
      SchemaPath newPath = functionInfo.getNewPath(paths.get(i));
      if(newPath == null) {
        continue;
      }

      // if this path only in indexed function, we are safe to replace it
      if(pathOnlyInIndexedFunction(paths.get(i))) {
        newPaths.set(i, newPath);
      }
      else {// we should not replace this column, instead we add a new "$N" field.
        newPaths.add(newPath);
      }
    }
    return newPaths;
  }

  /**
   *A RexNode forest with three RexNodes for expressions "cast(a.q as int) * 2, b+c, concat(a.q, " world")"
   * on Scan RowType('a', 'b', 'c') will be like this:
   *
   *          (0)Call:"*"                                       Call:"concat"
   *           /         \                                    /           \
   *    (1)Call:CAST     2            Call:"+"        (5)Call:ITEM     ' world'
   *      /        \                   /     \          /   \
   * (2)Call:ITEM  TYPE:INT       (3)$1    (4)$2       $0    'q'
   *   /      \
   *  $0     'q'
   *
   * So for above expressions, when visiting the RexNode trees using PathInExpr, we could mark indexed expressions in the trees,
   * as shown in the diagram above are the node (1),
   * then collect the schema paths in the indexed expression but found out of the indexed expression -- node (5),
   * and other regular schema paths (3) (4)
   *
   * @param parseContext
   * @param project
   * @param scan
   * @param toRewriteRex  the RexNode to be converted if it contain a functional index expression.
   * @param newRowType
   * @param functionInfo
   * @return
   */
  public static RexNode rewriteFunctionalRex(IndexPlanCallContext indexContext,
                                       DrillParseContext parseContext,
                                       DrillProjectRelBase project,
                                       RelNode scan,
                                       RexNode toRewriteRex,
                                       RelDataType newRowType,
                                       FunctionalIndexInfo functionInfo) {
    if (!functionInfo.hasFunctional()) {
      return toRewriteRex;
    }
    RexToExpression.RexToDrillExt rexToDrill = new RexToExpression.RexToDrillExt(parseContext, project, scan);
    LogicalExpression expr = toRewriteRex.accept(rexToDrill);

    final Map<LogicalExpression, Set<SchemaPath>> exprPathMap = functionInfo.getPathsInFunctionExpr();
    PathInExpr exprSearch = new PathInExpr(exprPathMap);
    expr.accept(exprSearch, null);
    Set<LogicalExpression> remainderPaths = exprSearch.getRemainderPaths();

    // now build the rex->logical expression map for SimpleRexRemap
    // left out schema paths
    Map<LogicalExpression, Set<RexNode>> exprToRex = rexToDrill.getMapExprToRex();
    final Map<RexNode, LogicalExpression> mapRexExpr = Maps.newHashMap();
    for (LogicalExpression leftExpr: remainderPaths) {
      if (exprToRex.containsKey(leftExpr)) {
        Set<RexNode> rexs = exprToRex.get(leftExpr);
        for (RexNode rex: rexs) {
          mapRexExpr.put(rex, leftExpr);
        }
      }
    }

    // functional expressions e.g. cast(a.b as int)
    for (LogicalExpression functionExpr: functionInfo.getExprMap().keySet()) {
      if (exprToRex.containsKey(functionExpr)) {
        Set<RexNode> rexs = exprToRex.get(functionExpr);
        for (RexNode rex: rexs) {
          mapRexExpr.put(rex, functionExpr);
        }
      }

    }

    SimpleRexRemap remap = new SimpleRexRemap(indexContext.scan, newRowType, indexContext.scan.getCluster().getRexBuilder());
    remap.setExpressionMap(functionInfo.getExprMap());
    return remap.rewriteWithMap(toRewriteRex, mapRexExpr);
  }

}
