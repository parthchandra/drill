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
import java.util.Set;


import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.HashJoinPrel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.PrelUtil;
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
 * Generate a non-covering index plan that is equivalent to the original plan. The non-covering plan consists
 * of a join-back between an index lookup and the primary table. This join-back is performed using a hash join.
 * For the primary table, we use a restricted scan that allows doing skip-scan instead of sequential scan.
 *
 * Original Plan:
 *               Filter
 *                 |
 *            DBGroupScan
 *
 * New Plan:
 *
 *            HashJoin (on rowkey)
 *          /         \
 * Remainder Filter  Exchange
 *         |            |
 *   Restricted    Filter (with index columns only)
 *   DBGroupScan        |
 *                  IndexGroupScan
 *
 * This plan will be further optimized by the filter pushdown rule of the Index plugin which should
 * push the index column filters into the index scan.
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

  /**
   * For IndexScan in non-covering case, rowType to return contains only row_key('_id') of primary table.
   * so the rowType for IndexScan should be converted from [Primary_table.row_key, primary_table.indexed_col]
   * to [indexTable.row_key(primary_table.indexed_col), indexTable.<primary_key.row_key> (Primary_table.row_key)]
   * This will impact the columns of scan, the rowType of ScanRel
   *
   * @param primaryRowType
   * @param typeFactory
   * @return
   */
  private RelDataType convertRowTypeForIndexScan(RelDataType primaryRowType, RelDataTypeFactory typeFactory) {
    List<RelDataTypeField> fields = new ArrayList<>();
    //TODO: we are not handling the case where row_key of index is composed of 2 or more columns of primary table

    //row_key in the rowType of scan on primary table
    RelDataTypeField rowkey_primary;

    RelRecordType newRowType = null;

      //first add row_key of primary table,
      rowkey_primary = new RelDataTypeFieldImpl(
          ((DbGroupScan)origScan.getGroupScan()).getRowKeyName(), fields.size(),
          typeFactory.createSqlType(SqlTypeName.ANY));
      fields.add(rowkey_primary);
      //then add indexed cols
    List<RexNode> conditions = Lists.newArrayList();
    conditions.add(indexCondition);
    PrelUtil.ProjectPushInfo info = PrelUtil.getColumns(origScan.getRowType(), conditions);

    for (SchemaPath indexedPath: info.columns) {
      fields.add(new RelDataTypeFieldImpl(
          indexedPath.getAsUnescapedPath(), fields.size(),
          typeFactory.createSqlType(SqlTypeName.ANY)));
    }

    //update columns of groupscan accordingly
    Set<RelDataTypeField> rowfields = Sets.newLinkedHashSet();
    final List<SchemaPath> columns = Lists.newArrayList();
    for (RelDataTypeField f : fields) {
      SchemaPath path;
      String pathSeg = f.getName().replaceAll("`", "");
      final String[] segs = pathSeg.split("\\.");
      path = SchemaPath.getCompoundPath(segs);
      rowfields.add(new RelDataTypeFieldImpl(
          segs[0], rowfields.size(),
          typeFactory.createMapType(typeFactory.createSqlType(SqlTypeName.VARCHAR),
              typeFactory.createSqlType(SqlTypeName.ANY))
      ));
      columns.add(path);
    }
    indexGroupScan.setColumns(columns);

    //rowtype does not take the whole path, but only the rootSegment of the SchemaPath
    newRowType = new RelRecordType(Lists.newArrayList(rowfields));
    return newRowType;
  }

  @Override
  public RelNode convertChild(final FilterPrel filter, final RelNode input) throws InvalidRelException {

    if (indexGroupScan == null) {
      logger.error("Null indexgroupScan in NonCoveringIndexPlanGenerator.convertChild");
      return null;
    }

    RelDataType dbscanRowType = convertRowType(origScan.getRowType(), origScan.getCluster().getTypeFactory());
    RelDataType indexScanRowType = convertRowTypeForIndexScan(origScan.getRowType(), origScan.getCluster().getTypeFactory());
    ScanPrel indexScanPrel = new ScanPrel(origScan.getCluster(),
        origScan.getTraitSet(), indexGroupScan, indexScanRowType);

    // right (build) side of the hash join: broadcast the project-filter-indexscan subplan
    FilterPrel  rightIndexFilterPrel = new FilterPrel(indexScanPrel.getCluster(), indexScanPrel.getTraitSet(),
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

    DbGroupScan origDbGroupScan = (DbGroupScan)origScan.getGroupScan();
    List<SchemaPath> cols = new ArrayList<SchemaPath>(origDbGroupScan.getColumns());
    if (!checkRowKey(cols)) {
      cols.add(origDbGroupScan.getRowKeyPath());
    }

    // Create a restricted groupscan from the primary table's groupscan
    DbGroupScan restrictedGroupScan  = (DbGroupScan)origDbGroupScan.getRestrictedScan(cols);
    if (restrictedGroupScan == null) {
      logger.error("Null restricted groupscan in NonCoveringIndexPlanGenerator.convertChild");
      return null;
    }

    ScanPrel dbScan = new ScanPrel(origScan.getCluster(),
        origScan.getTraitSet(), restrictedGroupScan, dbscanRowType);

    // build the row type for the left Project
    List<RexNode> leftProjectExprs = Lists.newArrayList();
    int leftRowKeyIndex = getRowKeyIndex(dbScan.getRowType());
    final RelDataTypeField leftRowKeyField = dbScan.getRowType().getFieldList().get(leftRowKeyIndex);
    final RelDataTypeFactory.FieldInfoBuilder leftFieldTypeBuilder =
        dbScan.getCluster().getTypeFactory().builder();

    // new Project's rowtype is original Project's rowtype [plus rowkey if rowkey is not in original rowtype]
    List<RelDataTypeField> origProjFields = origProject.getRowType().getFieldList();
    leftFieldTypeBuilder.addAll(origProjFields);
    // get the exprs from the original Project
    leftProjectExprs.addAll(origProject.getProjects());
    // add the rowkey IFF rowkey is not in orig scan
    if( getRowKeyIndex(origProject.getRowType()) < 0 ) {
      leftFieldTypeBuilder.add(leftRowKeyField);
      leftProjectExprs.add(RexInputRef.of(leftRowKeyIndex, dbScan.getRowType()));
    }

    final RelDataType leftProjectRowType = leftFieldTypeBuilder.build();
    final ProjectPrel leftIndexProjectPrel = new ProjectPrel(dbScan.getCluster(), dbScan.getTraitSet(),
        dbScan, leftProjectExprs, leftProjectRowType);

    FilterPrel leftIndexFilterPrel = null;
    if(remainderCondition != null && !remainderCondition.isAlwaysTrue()) {
      leftIndexFilterPrel = new FilterPrel(dbScan.getCluster(), dbScan.getTraitSet(),
          leftIndexProjectPrel, remainderCondition);
    }

    final RelTraitSet leftTraits = dbScan.getTraitSet().plus(Prel.DRILL_PHYSICAL);
    // final RelNode convertedLeft = convert(leftIndexProjectPrel, leftTraits);
    final RelNode convertedLeft = Prule.convert(leftIndexFilterPrel==null?leftIndexProjectPrel:leftIndexFilterPrel, leftTraits);

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
        restrictedGroupScan /* useful for join-restricted scans */);

    RelNode newRel = hjPrel;

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

    logger.trace("DbScanToIndexScanPrule got finalRel {} from origScan {}",
        finalRel.toString(), origScan.toString());
    return finalRel;
  }
}
