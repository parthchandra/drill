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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.common.DrillProjectRelBase;
import org.apache.drill.exec.planner.logical.DrillMergeProjectRule;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillSortRel;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ProjectPrule;
import org.apache.drill.exec.planner.physical.Prule;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.planner.physical.SingleMergeExchangePrel;
import org.apache.drill.exec.planner.physical.SortPrel;
import org.apache.drill.exec.planner.physical.SortPrule;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Generate a covering index plan that is equivalent to the original plan.
 *
 * This plan will be further optimized by the filter pushdown rule of the Index plugin which should
 * push this filter into the index scan.
 */
public class CoveringIndexPlanGenerator extends AbstractIndexPlanGenerator {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CoveringIndexPlanGenerator.class);
  final protected IndexGroupScan indexGroupScan;
  final protected IndexDescriptor indexDesc;

  // Ideally This functionInfo should be cached along with indexDesc.
  final protected FunctionalIndexInfo functionInfo;

  public CoveringIndexPlanGenerator(IndexPlanCallContext indexContext,
                                    FunctionalIndexInfo functionInfo,
                                    IndexGroupScan indexGroupScan,
                                    RexNode indexCondition,
                                    RexNode remainderCondition,
                                    RexBuilder builder,
                                    PlannerSettings settings) {
    super(indexContext, indexCondition, remainderCondition, builder, settings);
    this.indexGroupScan = indexGroupScan;
    this.functionInfo = functionInfo;
    this.indexDesc = functionInfo.getIndexDesc();
  }

  private RelDataType rewriteFunctionalRowType(RelDataType origRowType, FunctionalIndexInfo functionInfo) {
    return FunctionalIndexHelper.rewriteFunctionalRowType(origScan, indexContext, functionInfo);
  }

  boolean pathOnlyInIndexedFunction(SchemaPath path) {
    return true;
  }
  /**
   * For IndexGroupScan, if a column is only appeared in the should-be-renamed function,
   * this column is to-be-replaced column, we replace that column(schemaPath) from 'a.b'
   * to '$1' in the list of SchemaPath.
   * @param paths
   * @param functionInfo functional index information that may impact rewrite
   * @return
   */
  private List<SchemaPath> rewriteFunctionColumn(List<SchemaPath> paths, FunctionalIndexInfo functionInfo) {
    if (!functionInfo.hasFunctional()) {
      return paths;
    }

    List<SchemaPath> newPaths = Lists.newArrayList(paths);
    for (int i=0; i<paths.size(); ++i) {
      SchemaPath newPath = functionInfo.getNewPath(paths.get(i));
      if(newPath == null) {
        continue;
      }

      //if this path only in indexed function, we are safe to replace it
      if(pathOnlyInIndexedFunction(paths.get(i))) {
        newPaths.set(i, newPath);
      }
      else {//we should not replace this column, instead we add a new "$N" field.
        newPaths.add(newPath);
      }
    }
    return newPaths;
  }

  /**
   *
   * @param inputIndex
   * @param functionInfo functional index information that may impact rewrite
   * @return
   */
  private RexNode rewriteFunctionalCondition(RexNode inputIndex, RelDataType newRowType,
                                             FunctionalIndexInfo functionInfo) {
    if (!functionInfo.hasFunctional()) {
      return inputIndex;
    }
    return FunctionalIndexHelper.convertConditionForIndexScan(indexCondition,
        origScan, newRowType, builder, functionInfo);
  }

  /**
   *A RexNode forest with two RexNode for expressions "cast(a.q as int) * 2, b+c, concat(a.q, " world")"
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
  private RexNode rewriteFunctionalRex(DrillParseContext parseContext,
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

    //now build the rex->logical expression map for SimpleRexRemap
    //left out schema paths
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

    //functional expressions e.g. cast(a.b as int)
    for (LogicalExpression functionExpr: functionInfo.getExprMap().keySet()) {
      if (exprToRex.containsKey(functionExpr)) {
        Set<RexNode> rexs = exprToRex.get(functionExpr);
        for (RexNode rex: rexs) {
          mapRexExpr.put(rex, functionExpr);
        }
      }

    }

    SimpleRexRemap remap = new SimpleRexRemap(origScan, newRowType, builder);
    remap.setExpressionMap(functionInfo.getExprMap());
    return remap.rewriteWithMap(toRewriteRex, mapRexExpr);
  }

  private RelCollation buildCollation(List<RexNode> projectRexs, RelNode input) {
    //if leading fields of index are here, add them to RelCollation
    List<RelFieldCollation> newFields = Lists.newArrayList();
    if (!functionInfo.hasFunctional()) {
      Map<LogicalExpression, Integer> projectExprs = Maps.newLinkedHashMap();
      DrillParseContext parserContext = new DrillParseContext(PrelUtil.getPlannerSettings(input.getCluster()));
      int idx=0;
      for(RexNode rex : projectRexs) {
        projectExprs.put(DrillOptiq.toDrill(parserContext, input, rex), idx);
        idx++;
      }
      int idxFieldCount = 0;
      for (LogicalExpression expr : indexDesc.getIndexColumns()) {
        if (!projectExprs.containsKey(expr)) {
          break;
        }
        RelFieldCollation.Direction dir = indexDesc.getCollation().getFieldCollations().get(idxFieldCount).direction;
        if ( dir == null) {
          break;
        }
        newFields.add(new RelFieldCollation(projectExprs.get(expr), dir,
            RelFieldCollation.NullDirection.UNSPECIFIED));
      }
      idxFieldCount++;
    }
    return RelCollationImpl.of(newFields);
  }

  private boolean toRemoveSort(DrillSortRel sort, RelCollation inputCollation) {
    if ( inputCollation.satisfies(sort.getCollation())) {
      return true;
    }
    return false;
  }

  @Override
  public RelNode convertChild(final RelNode filter, final RelNode input) throws InvalidRelException {

    if (indexGroupScan == null) {
      logger.error("Null indexgroupScan in CoveringIndexPlanGenerator.convertChild");
      return null;
    }

    indexGroupScan.setColumns(
        rewriteFunctionColumn(((DbGroupScan)origScan.getGroupScan()).getColumns(),
        this.functionInfo));

    RelDataType newRowType = FunctionalIndexHelper.rewriteFunctionalRowType(origScan, indexContext, functionInfo);

    RelTraitSet indexScanTraitSet = origScan.getTraitSet().plus(Prel.DRILL_PHYSICAL);

    // Create the collation traits for index scan based on the index columns under the
    // condition that the index actually has collation property (e.g hash indexes don't)
    if (indexDesc.getCollation() != null) {
      RelCollation collationTrait = buildCollationTraits(indexDesc, newRowType);
      indexScanTraitSet = indexScanTraitSet.plus(collationTrait);
    }

    ScanPrel indexScanPrel = new ScanPrel(origScan.getCluster(),
        indexScanTraitSet, indexGroupScan,
        newRowType);

    RexNode newIndexCondition =
        rewriteFunctionalCondition(indexCondition, newRowType, functionInfo);
    FilterPrel indexFilterPrel = new FilterPrel(indexScanPrel.getCluster(), indexScanPrel.getTraitSet(),
        indexScanPrel, newIndexCondition);

    ProjectPrel indexProjectPrel = null;
    if (origProject != null) {
      RelCollation collation = buildCollation(origProject.getProjects(), indexScanPrel);
      indexProjectPrel = new ProjectPrel(origScan.getCluster(), indexScanPrel.getTraitSet().plus(collation),
          indexFilterPrel, origProject.getProjects(), origProject.getRowType());
    }
    RelNode finalRel;

    if (remainderCondition != null && !remainderCondition.isAlwaysTrue()) {
      // create a Filter corresponding to the remainder condition
      FilterPrel remainderFilterPrel = new FilterPrel(origScan.getCluster(), indexProjectPrel.getTraitSet(),
          indexProjectPrel, remainderCondition);
      finalRel = remainderFilterPrel;
    } else if (indexProjectPrel != null) {
      finalRel = indexProjectPrel;
    }
    else {
      finalRel = indexFilterPrel;
    }

    if ( capProject != null) {

      final Map<Integer, Integer> collationMap = ProjectPrule.getCollationMap(capProject);
      RelCollation newCollation = null;

      if ( origProject != null) {//so we already built collation there
        RelCollation collation = finalRel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
        newCollation = ProjectPrule.convertRelCollation(collation, collationMap);
      }
      else {
        newCollation = buildCollation(capProject.getProjects(), indexScanPrel);
      }

      ProjectPrel cap = new ProjectPrel(capProject.getCluster(),
          newCollation==null?finalRel.getTraitSet() : finalRel.getTraitSet().plus(newCollation),
          finalRel, capProject.getProjects(), capProject.getRowType());

      if (functionInfo.hasFunctional()) {
        //if there is functional index field, then a rewrite may be needed in capProject/indexProject
        //merge capProject with indexProjectPrel(from origProject) if both exist,
        ProjectPrel newProject = cap;
        if (indexProjectPrel != null) {
          newProject = (ProjectPrel) DrillMergeProjectRule.replace(newProject, indexProjectPrel);
        }
        // then rewrite functional expressions in new project.
        List<RexNode> newProjects = Lists.newArrayList();
        DrillParseContext parseContxt = new DrillParseContext(PrelUtil.getPlannerSettings(newProject.getCluster()));
        for(RexNode projectRex: newProject.getProjects()) {
          RexNode newRex = rewriteFunctionalRex(parseContxt, null, origScan, projectRex, newRowType, functionInfo);
          newProjects.add(newRex);
        }

        ProjectPrel rewrittenProject = new ProjectPrel(newProject.getCluster(), newProject.getTraitSet(),
            indexFilterPrel, newProjects, newProject.getRowType());

        cap = rewrittenProject;
      }

      finalRel = cap;
    }

    if (indexContext.sort != null) {
      DrillSortRel rel = indexContext.sort;
      DrillDistributionTrait hashDistribution =
          new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED,
              ImmutableList.copyOf(SortPrule.getDistributionField(rel)));

      if ( !toRemoveSort(indexContext.sort, finalRel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE))) {
        //create sort's prel, also let us add distribution trait as SortPrule will do
        final RelTraitSet traits = rel.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(hashDistribution);
        RelNode convertedInput = Prule.convert(finalRel, traits);
        //final RelNode convertedInput = Prule.convert(rel.getInput(), traits);

        if (Prule.isSingleMode(indexContext.call)) {
          indexContext.call.transformTo(convertedInput);
        } else {
          RelNode exch = new SingleMergeExchangePrel(rel.getCluster(),
              rel.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON),
              convertedInput, rel.getCollation());
          indexContext.call.transformTo(exch);  // transform logical "sort" into "SingleMergeExchange".
        }
      }
      else {
        //we are going to remove sort
        logger.debug("Not generating SortPrel since we have the required collation");

        RelTraitSet traits = finalRel.getTraitSet().plus(rel.getCollation()).plus(Prel.DRILL_PHYSICAL);
        RelNode convertedInput = Prule.convert(finalRel, traits);
        RelNode exch = new SingleMergeExchangePrel(finalRel.getCluster(),
            traits.replace(DrillDistributionTrait.SINGLETON),
            convertedInput,//finalRel,//
            indexContext.sort.getCollation());
        finalRel = exch;
      }
    }

    finalRel = Prule.convert(finalRel, finalRel.getTraitSet().plus(Prel.DRILL_PHYSICAL));

    logger.debug("CoveringIndexPlanGenerator got finalRel {} from origScan {}, original digest {}, new digest {}.",
        finalRel.toString(), origScan.toString(), capProject==null?indexContext.filter.getDigest(): capProject.getDigest(), finalRel.getDigest());
    return finalRel;
  }

  private RelCollation buildCollationTraits(IndexDescriptor indexDesc,
      RelDataType indexScanRowType) {

    final List<RelDataTypeField> indexFields = indexScanRowType.getFieldList();
    final Map<SchemaPath, RelFieldCollation> collationMap = indexDesc.getCollationMap();

    assert collationMap != null : "Invalid collation map for index";

    List<RelFieldCollation> fieldCollations = Lists.newArrayList();

    for (int i = 0; i < indexScanRowType.getFieldCount(); i++) {
      RelDataTypeField f1 = indexFields.get(i);
      FieldReference ref = FieldReference.getWithQuotedRef(f1.getName());
      RelFieldCollation origCollation = collationMap.get(ref);
      if (origCollation != null) {
        RelFieldCollation fc = new RelFieldCollation(i, origCollation.direction,
            origCollation.nullDirection);
        fieldCollations.add(fc);
      }
    }

    final RelCollation collation = RelCollations.of(fieldCollations);
    return collation;
  }

}
