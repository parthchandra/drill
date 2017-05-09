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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.expression.SchemaPath;
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

import java.util.ArrayList;
import java.util.List;

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

  public CoveringIndexPlanGenerator(RelOptRuleCall call,
                                    FunctionalIndexInfo functionInfo,
                                    ProjectPrel origProject,
                                    ScanPrel origScan,
                                    IndexGroupScan indexGroupScan,
                                    RexNode indexCondition,
                                    RexNode remainderCondition,
                                    RexBuilder builder) {
    super(call, origProject, origScan, indexCondition, remainderCondition, builder);
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
    List<RelDataTypeField> fields = new ArrayList<>();

    //TODO: check if we should replace some row fields, for now we simply append to the end
    fields.addAll(origRowType.getFieldList());
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
    return convertConditionForIndexScan(indexCondition, newRowType, functionInfo);
  }

  @Override
  public RelNode convertChild(final FilterPrel filter, final RelNode input) throws InvalidRelException {

    if (indexGroupScan == null) {
      logger.error("Null indexgroupScan in CoveringIndexPlanGenerator.convertChild");
      return null;
    }

    indexGroupScan.setColumns(
        rewriteFunctionColumn(((DbGroupScan)origScan.getGroupScan()).getColumns(),
        this.functionInfo));

    RelDataType newRowType = rewriteFunctionalRowType(origScan.getRowType(), functionInfo);
    ScanPrel indexScanPrel = new ScanPrel(origScan.getCluster(),
        origScan.getTraitSet(), indexGroupScan,
        newRowType);

    RexNode newIndexCondition =
        rewriteFunctionalCondition(indexCondition, origScan.getRowType(), newRowType, functionInfo);
    FilterPrel indexFilterPrel = new FilterPrel(indexScanPrel.getCluster(), indexScanPrel.getTraitSet(),
        indexScanPrel, newIndexCondition);

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

    logger.trace("CoveringIndexPlanGenerator got finalRel {} from origScan {}",
        finalRel.toString(), origScan.toString());
    return finalRel;
  }
}
