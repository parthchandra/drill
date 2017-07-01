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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.calcite.plan.RelTraitSet;
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
import org.apache.drill.exec.planner.fragment.DistributionAffinity;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
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
   * @param input
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

  // TODO: proper implementation
  public static boolean pathOnlyInIndexedFunction(SchemaPath path) {
    return true;
  }

  /**
   * Build the collation property for the index scan
   * @param indexDesc
   * @param indexScanRowType
   * @return the output RelCollation
   */
  public static RelCollation buildCollationCoveringIndexScan(IndexDescriptor indexDesc,
      RelDataType indexScanRowType) {

    final List<RelDataTypeField> indexFields = indexScanRowType.getFieldList();

    // The collationMap has SchemaPath as key.  This works fine for top level JSON fields.  For
    // nested JSON fields e.g a.b.c, the Project above the Scan will create an ITEM expr to produce
    // the field, so the RelCollation property has to be build separately for that Project.
    final Map<SchemaPath, RelFieldCollation> collationMap = indexDesc.getCollationMap();

    assert collationMap != null : "Invalid collation map for index";

    List<RelFieldCollation> fieldCollations = Lists.newArrayList();

    for (int i = 0; i < indexScanRowType.getFieldCount(); i++) {
      RelDataTypeField f1 = indexFields.get(i);
      FieldReference ref = FieldReference.getWithQuotedRef(f1.getName());
      RelFieldCollation origCollation = collationMap.get(ref);
      if (origCollation != null) {
        RelFieldCollation fc = new RelFieldCollation(origCollation.getFieldIndex(),
            origCollation.direction, origCollation.nullDirection);
        fieldCollations.add(fc);
      }
    }
    final RelCollation collation = RelCollations.of(fieldCollations);
    return collation;
  }

  public static RelCollation buildCollationNonCoveringIndexScan(IndexDescriptor indexDesc,
      RelDataType indexScanRowType,
      RelDataType restrictedScanRowType) {

    final List<RelDataTypeField> indexFields = indexScanRowType.getFieldList();
    final List<RelDataTypeField> rsFields = restrictedScanRowType.getFieldList();
    final Map<SchemaPath, RelFieldCollation> collationMap = indexDesc.getCollationMap();

    assert collationMap != null : "Invalid collation map for index";

    List<RelFieldCollation> fieldCollations = Lists.newArrayList();
    Map<Integer, RelFieldCollation> rsScanCollationMap = Maps.newHashMap();

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
            RelFieldCollation fc = new RelFieldCollation(origCollation.getFieldIndex(),
                origCollation.direction, origCollation.nullDirection);
            rsScanCollationMap.put(j, fc);
          }
        }
      }
    }

    if (rsScanCollationMap.size() > 0) {
      for (int j = 0; j < rsFields.size(); j++) {
        RelFieldCollation fc = rsScanCollationMap.get(j);
        if (fc != null) {
          fieldCollations.add(fc);
        }
      }
    }

    final RelCollation collation = RelCollations.of(fieldCollations);
    return collation;
  }

  public static boolean scanIsPartition(GroupScan scan) {
    return (scan.getMaxParallelizationWidth() > 1
        || scan.getDistributionAffinity() == DistributionAffinity.HARD);
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
      RelCollation collationTrait = buildCollationCoveringIndexScan(indexDesc, newRowType);
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

}
