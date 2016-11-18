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

import java.util.ArrayList;
import java.util.List;


import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractDbGroupScan;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.HashJoinPrel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.Prule;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;


/**
 * Generate a non-covering index plan that is equivalent to the original plan. The non-covering plan
 *
 * The right child of hash join consists of the index group scan followed by filter containing the index condition.
 * Left child of hash join consists of the table scan followed by the same filter containing the index condition.
 *
 * Original Plan:
 *               Filter
 *                 |
 *            DBGroupScan
 *
 * New Plan:
 *            Filter (Original filter minus filters on index columns)
 *               |
 *            HashJoin
 *          /          \
 *   DbGroupScan   Exchange (e.g BroadcastExchange)
 *                      |
 *              Filter (with index columns only)
 *                      |
 *                  IndexGroupScan
 *
 * This plan will be further optimized by the filter pushdown rule of the Index plugin which should
 * push this filter into the index scan.
 */
public class NonCoveringIndexPlanGenerator extends AbstractIndexPlanGenerator {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NonCoveringIndexPlanGenerator.class);

  public NonCoveringIndexPlanGenerator(RelOptRuleCall call,
      ProjectPrel origProject,
      ScanPrel origScan,
      IndexGroupScan indexGroupScan,
      RexNode indexCondition,
      RexNode remainderCondition,
      RexBuilder builder) {
    super(call, origProject, origScan, indexGroupScan, indexCondition, remainderCondition, builder);
  }

  @Override
  public RelNode convertChild(final FilterPrel filter, final RelNode input) throws InvalidRelException {

    if (indexGroupScan == null) {
      logger.error("Null indexgroupScan in NonCoveringIndexPlanGenerator.convertChild");
      return null;
    }

    RelDataType hbscanRowType = convertRowType(origScan.getRowType(), origScan.getCluster().getTypeFactory());

    ScanPrel indexScanPrel = new ScanPrel(origScan.getCluster(),
        origScan.getTraitSet(), indexGroupScan, hbscanRowType /* use the same row type as the hbase scan */);


    // right (build) side of the hash join: broadcast the project-filter-indexscan subplan
    FilterPrel rightIndexFilterPrel = new FilterPrel(indexScanPrel.getCluster(), indexScanPrel.getTraitSet(),
        indexScanPrel, indexCondition);

    // project the rowkey column from the index scan
    List<RexNode> rightProjectExprs = Lists.newArrayList();
    int rightRowKeyIndex = getRowKeyIndex(indexScanPrel.getRowType());//indexGroupScan.getRowKeyOrdinal();
    assert rightRowKeyIndex >= 0;

    rightProjectExprs.add(RexInputRef.of(rightRowKeyIndex, indexScanPrel.getRowType()));

    final List<RelDataTypeField> indexScanFields = indexScanPrel.getRowType().getFieldList();

    final RelDataTypeFactory.FieldInfoBuilder rightFieldTypeBuilder =
        indexScanPrel.getCluster().getTypeFactory().builder();

    // build the row type for the right Project
    final RelDataTypeField rightRowKeyField = indexScanFields.get(rightRowKeyIndex);
    rightFieldTypeBuilder.add(rightRowKeyField);
    final RelDataType rightProjectRowType = rightFieldTypeBuilder.build();

    final ProjectPrel rightIndexProjectPrel = new ProjectPrel(indexScanPrel.getCluster(), indexScanPrel.getTraitSet(),
        rightIndexFilterPrel, rightProjectExprs, rightProjectRowType);

    final DrillDistributionTrait distBroadcastRight = new DrillDistributionTrait(DrillDistributionTrait.DistributionType.BROADCAST_DISTRIBUTED);
    RelTraitSet rightTraits = newTraitSet(distBroadcastRight).plus(Prel.DRILL_PHYSICAL);
    RelNode convertedRight = Prule.convert(rightIndexProjectPrel, rightTraits);

    // left (probe) side of the hash join

    DbGroupScan origHbGroupScan = (DbGroupScan)origScan.getGroupScan();
    List<SchemaPath> cols = new ArrayList<SchemaPath>(origHbGroupScan.getColumns());
    if (!checkRowKey(cols)) {
      cols.add(AbstractDbGroupScan.ROW_KEY_PATH);
    }

    DbGroupScan newHbGroupScan  = (DbGroupScan)origHbGroupScan.clone(cols);
    newHbGroupScan.setCostFactor(0.00001);
    ScanPrel hbaseScan = new ScanPrel(origScan.getCluster(),
        origScan.getTraitSet(), newHbGroupScan, hbscanRowType);

    // build the row type for the left Project
    List<RexNode> leftProjectExprs = Lists.newArrayList();
    int leftRowKeyIndex = getRowKeyIndex(hbaseScan.getRowType());
    final RelDataTypeField leftRowKeyField = hbaseScan.getRowType().getFieldList().get(leftRowKeyIndex);
    final RelDataTypeFactory.FieldInfoBuilder leftFieldTypeBuilder =
        hbaseScan.getCluster().getTypeFactory().builder();

    // new Project's rowtype is original Project's rowtype [plus rowkey if rowkey is not in original rowtype]
    List<RelDataTypeField> origProjFields = origProject.getRowType().getFieldList();
    leftFieldTypeBuilder.addAll(origProjFields);
    // get the exprs from the original Project
    leftProjectExprs.addAll(origProject.getProjects());
    // add the rowkey IFF rowkey is not in orig scan
    if( getRowKeyIndex(origProject.getRowType()) < 0 ) {
      leftFieldTypeBuilder.add(leftRowKeyField);
      leftProjectExprs.add(RexInputRef.of(leftRowKeyIndex, hbaseScan.getRowType()));
    }

    final RelDataType leftProjectRowType = leftFieldTypeBuilder.build();
    final ProjectPrel leftIndexProjectPrel = new ProjectPrel(hbaseScan.getCluster(), hbaseScan.getTraitSet(),
        hbaseScan, leftProjectExprs, leftProjectRowType);

    FilterPrel leftIndexFilterPrel = new FilterPrel(hbaseScan.getCluster(), hbaseScan.getTraitSet(),
        leftIndexProjectPrel, filter.getCondition());

    final RelTraitSet leftTraits = hbaseScan.getTraitSet().plus(Prel.DRILL_PHYSICAL);
    // final RelNode convertedLeft = convert(leftIndexProjectPrel, leftTraits);
    final RelNode convertedLeft = Prule.convert(leftIndexFilterPrel, leftTraits);

    // find the rowkey column on the left side of join
    // TODO: is there a shortcut way to do this ?
    final int leftRowKeyIdx = getRowKeyIndex(convertedLeft.getRowType());
    final int rightRowKeyIdx = 0; // only rowkey field is being projected from right side

    assert leftRowKeyIdx >= 0;

    List<Integer> leftJoinKeys = ImmutableList.of(leftRowKeyIdx);
    List<Integer> rightJoinKeys = ImmutableList.of(rightRowKeyIdx);

    RexNode joinCondition =
        RelOptUtil.createEquiJoinCondition(convertedLeft, leftJoinKeys,
            convertedRight, rightJoinKeys, builder);

    HashJoinPrel hjPrel = new HashJoinPrel(filter.getCluster(), leftTraits, convertedLeft,
        convertedRight, joinCondition, JoinRelType.INNER, false,
        newHbGroupScan /* useful for join-restricted scans */);

    RelNode newRel = hjPrel;

/*  don't need to evaluate remainder since filter is already pushed to left side of hash join
    if (remainderCondition != null && !remainderCondition.isAlwaysTrue()) {
      // create a Filter corresponding to the remainder condition
      FilterPrel remainderFilterPrel = new FilterPrel(hjPrel.getCluster(), hjPrel.getTraitSet(),
          hjPrel, remainderCondition);

      newRel = remainderFilterPrel;
    }
*/

    final RelDataTypeFactory.FieldInfoBuilder finalFieldTypeBuilder =
        origScan.getCluster().getTypeFactory().builder();

    List<RelDataTypeField> hjRowFields = newRel.getRowType().getFieldList();
    int toRemoveRowKeyCount = 1;
    if (getRowKeyIndex(origProject.getRowType())  < 0 ) {
      toRemoveRowKeyCount = 2;
    }
    finalFieldTypeBuilder.addAll(hjRowFields.subList(0, hjRowFields.size()-toRemoveRowKeyCount));
    final RelDataType finalProjectRowType = finalFieldTypeBuilder.build();

    List<RexNode> resetExprs = Lists.newArrayList();
    for (int idx=0; idx<hjRowFields.size()-toRemoveRowKeyCount; ++idx) {
      resetExprs.add(RexInputRef.of(idx, newRel.getRowType()));
    }

    final ProjectPrel resetProjectPrel = new ProjectPrel(newRel.getCluster(), newRel.getTraitSet(),
        newRel, resetExprs, finalProjectRowType);
    newRel = resetProjectPrel;

    RelNode finalRel = Prule.convert(newRel, newRel.getTraitSet());


    return finalRel;
  }
}
