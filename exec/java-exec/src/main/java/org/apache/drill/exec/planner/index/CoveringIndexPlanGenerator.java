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
import com.google.common.collect.Maps;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.common.DrillProjectRelBase;
import org.apache.drill.exec.planner.logical.DrillMergeProjectRule;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.Prule;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.calcite.rel.InvalidRelException;

import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.HashMap;
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

  @Override
  public RelNode convertChild(final RelNode filter, final RelNode input) throws InvalidRelException {

    if (indexGroupScan == null) {
      logger.error("Null indexgroupScan in CoveringIndexPlanGenerator.convertChild");
      return null;
    }

    RexNode coveringCondition = indexCondition;
    ScanPrel indexScanPrel =
        IndexPlanUtils.buildCoveringIndexScan(origScan, indexGroupScan, indexContext, indexDesc);

    // If remainder condition, then combine the index and remainder conditions. This is a covering plan so we can
    // pushed the entire condition into the index.
    if (remainderCondition != null && !remainderCondition.isAlwaysTrue()) {
      List<RexNode> conditions = new ArrayList<RexNode>();
      conditions.add(indexCondition);
      conditions.add(remainderCondition);
      coveringCondition = RexUtil.composeConjunction(indexScanPrel.getCluster().getRexBuilder(), conditions, true);
    }
    RexNode newIndexCondition =
        rewriteFunctionalCondition(coveringCondition, indexScanPrel.getRowType(), functionInfo);

    // build collation for filter
    RelTraitSet indexFilterTraitSet = indexScanPrel.getTraitSet();

    FilterPrel indexFilterPrel = new FilterPrel(indexScanPrel.getCluster(), indexFilterTraitSet,
        indexScanPrel, newIndexCondition);

    ProjectPrel indexProjectPrel = null;
    if (origProject != null) {
      RelCollation collation = IndexPlanUtils.buildCollationProject(origProject.getProjects(), null,
          origScan, functionInfo, indexContext);
      indexProjectPrel = new ProjectPrel(origScan.getCluster(), indexFilterTraitSet.plus(collation),
          indexFilterPrel, origProject.getProjects(), origProject.getRowType());
    }

    RelNode finalRel;
    if (indexProjectPrel != null) {
      finalRel = indexProjectPrel;
    } else {
      finalRel = indexFilterPrel;
    }

    if (upperProject != null) {
      RelCollation newCollation =
          IndexPlanUtils.buildCollationProject(upperProject.getProjects(), origProject,
              origScan, functionInfo, indexContext);

      ProjectPrel cap = new ProjectPrel(upperProject.getCluster(),
          newCollation==null?finalRel.getTraitSet() : finalRel.getTraitSet().plus(newCollation),
          finalRel, upperProject.getProjects(), upperProject.getRowType());

      if (functionInfo.hasFunctional()) {
        //if there is functional index field, then a rewrite may be needed in upperProject/indexProject
        //merge upperProject with indexProjectPrel(from origProject) if both exist,
        ProjectPrel newProject = cap;
        if (indexProjectPrel != null) {
          newProject = (ProjectPrel) DrillMergeProjectRule.replace(newProject, indexProjectPrel);
        }
        // then rewrite functional expressions in new project.
        List<RexNode> newProjects = Lists.newArrayList();
        DrillParseContext parseContxt = new DrillParseContext(PrelUtil.getPlannerSettings(newProject.getCluster()));
        for(RexNode projectRex: newProject.getProjects()) {
          RexNode newRex = rewriteFunctionalRex(parseContxt, null, origScan, projectRex, indexScanPrel.getRowType(), functionInfo);
          newProjects.add(newRex);
        }

        ProjectPrel rewrittenProject = new ProjectPrel(newProject.getCluster(),
            newCollation==null? newProject.getTraitSet() : newProject.getTraitSet().plus(newCollation),
            indexFilterPrel, newProjects, newProject.getRowType());

        cap = rewrittenProject;
      }

      finalRel = cap;
    }

    if (indexContext.sort != null) {
      finalRel = getSortNode(indexContext, finalRel);
    }

    finalRel = Prule.convert(finalRel, finalRel.getTraitSet().plus(Prel.DRILL_PHYSICAL));

    logger.debug("CoveringIndexPlanGenerator got finalRel {} from origScan {}, original digest {}, new digest {}.",
        finalRel.toString(), origScan.toString(),
        upperProject==null?indexContext.filter.getDigest(): upperProject.getDigest(), finalRel.getDigest());
    return finalRel;
  }

  private RexNode rewriteConditionForProject(RexNode condition, List<RexNode> projects) {
    Map<RexNode, RexNode> mapping = new HashMap<>();
    rewriteConditionForProjectInternal(condition, projects, mapping);
    SimpleRexRemap.RexReplace replacer = new SimpleRexRemap.RexReplace(mapping);
    return condition.accept(replacer);
  }

  private void rewriteConditionForProjectInternal(RexNode condition, List<RexNode> projects, Map<RexNode, RexNode> mapping) {
    if (condition instanceof RexCall) {
      if ("ITEM".equals(((RexCall) condition).getOperator().getName().toUpperCase())) {
        int index = 0;
        for (RexNode project : projects) {
          if (project.toString().equals(condition.toString())) {
            // Map it to the corresponding RexInputRef for the project
            mapping.put(condition, new RexInputRef(index, project.getType()));
          }
          ++index;
        }
      } else {
        for (RexNode child : ((RexCall) condition).getOperands()) {
          rewriteConditionForProjectInternal(child, projects, mapping);
        }
      }
    }
  }
}
