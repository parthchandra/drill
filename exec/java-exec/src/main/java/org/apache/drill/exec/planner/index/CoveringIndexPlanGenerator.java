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
import com.google.common.collect.Sets;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.common.DrillProjectRelBase;
import org.apache.drill.exec.planner.logical.DrillMergeProjectRule;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.Prule;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

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
                                    RexBuilder builder) {
    super(indexContext, indexCondition, remainderCondition, builder);
    this.indexGroupScan = indexGroupScan;
    this.functionInfo = functionInfo;
    this.indexDesc = this.functionInfo.getIndexDesc();
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

  private String getRootSeg(String fullPathString) {
    String pathSeg = fullPathString.replaceAll("`", "");
    final String[] segs = pathSeg.split("\\.");
    return segs[0];
  }
  /**
   * if a field in rowType serves only the to-be-replaced column(s), we should replace it with new name "$1", otherwise
   * we should keep this dataTypeField and add a new one for "$1"
   * @param origRowType  original rowtype to rewrite
   * @param functionInfo functional index information that may impact rewrite
   * @return
   */
  private RelDataType rewriteFunctionalRowType(RelDataType origRowType, FunctionalIndexInfo functionInfo) {
    if (!functionInfo.hasFunctional()) {
      return origRowType;
    }
    List<RelDataTypeField> fields = Lists.newArrayList();

    Set<String> leftOutFieldNames  = Sets.newHashSet();
    for (LogicalExpression expr : indexContext.leftOutPathsInFunctions) {
      leftOutFieldNames.add(getRootSeg(((SchemaPath)expr).getAsUnescapedPath()));
    }

    Set<String> fieldInFunctions  = Sets.newHashSet();
    for (SchemaPath path: functionInfo.allPathsInFunction()) {
      fieldInFunctions.add(getRootSeg(path.getAsUnescapedPath()));
    }

    RelDataTypeFactory typeFactory = origScan.getCluster().getTypeFactory();

    for ( RelDataTypeField field: origRowType.getFieldList()) {
      final String fieldName = field.getName();
      if (fieldInFunctions.contains(fieldName)) {
        if (!leftOutFieldNames.contains(fieldName)) {
          continue;
        }
      }
      //this should be preserved
      String pathSeg = fieldName.replaceAll("`", "");
      final String[] segs = pathSeg.split("\\.");

      fields.add(new RelDataTypeFieldImpl(
          segs[0], fields.size(),
          typeFactory.createSqlType(SqlTypeName.ANY)));
    }

    //TODO: we should have the information about which $N was needed
    for(SchemaPath dollarPath: functionInfo.allNewSchemaPaths()) {
      fields.add(
          new RelDataTypeFieldImpl(dollarPath.getAsUnescapedPath(), fields.size(),
          origScan.getCluster().getTypeFactory().createSqlType(SqlTypeName.ANY)));
    }
    return new RelRecordType(fields);
  }

  /**
   *
   * @param inputIndex
   * @param functionInfo functional index information that may impact rewrite
   * @return
   */
  private RexNode rewriteFunctionalCondition(RexNode inputIndex, RelDataType origRowType, RelDataType newRowType,
                                             FunctionalIndexInfo functionInfo) {
    if (!functionInfo.hasFunctional()) {
      return inputIndex;
    }
    return convertConditionForIndexScan(inputIndex, newRowType, functionInfo);
  }

  /**
   *
   *A RexNode forest with two RexNode for expressions "cast(a.q as int) * 2, b+c, concat(a.q, " world")"
   * on Scan RowType('a', 'b', 'c') will be like this:
   *          (0)Call:"*"                                       Call:"concat"
   *           /         \                                    /           \
   *    (1)Call:CAST     2            Call:"+"        (5)Call:ITEM     ' world'
   *      /        \                   /     \          /   \
   * (2)Call:ITEM  TYPE:INT       (3)$1    (4)$2       $0    'q'
   *   /      \
   *  $0     'q'
   *
   * Class PathInExpr is to recursively analyze a RexNode trees with a map of indexed expression collected from indexDescriptor,
   * e.g. Map 'cast(a.q as int)' -> '$0' means the expression 'cast(a.q as int)' is named as '$0' in index table
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

    //TODO: functional index case
    return toRewriteRex;
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

    RelDataType newRowType = rewriteFunctionalRowType(origScan.getRowType(), functionInfo);
    ScanPrel indexScanPrel = new ScanPrel(origScan.getCluster(),
        origScan.getTraitSet().plus(Prel.DRILL_PHYSICAL), indexGroupScan,
        newRowType);

    RexNode newIndexCondition =
        rewriteFunctionalCondition(indexCondition, origScan.getRowType(), newRowType, functionInfo);
    FilterPrel indexFilterPrel = new FilterPrel(indexScanPrel.getCluster(), indexScanPrel.getTraitSet(),
        indexScanPrel, newIndexCondition);

    ProjectPrel indexProjectPrel = null;
    if (origProject != null) {
      indexProjectPrel = new ProjectPrel(origScan.getCluster(), indexScanPrel.getTraitSet(),
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
      ProjectPrel cap = new ProjectPrel(capProject.getCluster(), finalRel.getTraitSet(),
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

    finalRel = Prule.convert(finalRel, finalRel.getTraitSet().plus(Prel.DRILL_PHYSICAL));

    logger.trace("CoveringIndexPlanGenerator got finalRel {} from origScan {}, original disgest {}, new digest {}.",
        finalRel.toString(), origScan.toString(), capProject==null?indexContext.filter.getDigest(): capProject.getDigest(), finalRel.getDigest());
    return finalRel;
  }
}
