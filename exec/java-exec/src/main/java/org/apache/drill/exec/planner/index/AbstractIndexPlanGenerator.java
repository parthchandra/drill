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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.DrillSortRel;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.Prule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.planner.physical.SingleMergeExchangePrel;
import org.apache.drill.exec.planner.physical.SortPrel;
import org.apache.drill.exec.planner.physical.SortPrule;
import org.apache.drill.exec.planner.physical.SubsetTransformer;

public abstract class AbstractIndexPlanGenerator extends SubsetTransformer<RelNode, InvalidRelException>{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractIndexPlanGenerator.class);

  final protected DrillProjectRel origProject;
  final protected DrillScanRel origScan;
  final protected DrillProjectRel upperProject;
  final protected DrillSortRel origSort;

  final protected RexNode indexCondition;
  final protected RexNode remainderCondition;
  final protected RexBuilder builder;
  final protected IndexPlanCallContext indexContext;
  final protected PlannerSettings settings;

  public AbstractIndexPlanGenerator(IndexPlanCallContext indexContext,
      RexNode indexCondition,
      RexNode remainderCondition,
      RexBuilder builder,
      PlannerSettings settings) {
    super(indexContext.call);
    this.origProject = indexContext.lowerProject;
    this.origScan = indexContext.scan;
    this.upperProject = indexContext.upperProject;
    this.origSort = indexContext.sort;
    this.indexCondition = indexCondition;
    this.remainderCondition = remainderCondition;
    this.indexContext = indexContext;
    this.builder = builder;
    this.settings = settings;
  }

  //This class provides the utility functions that don't rely on index(one or multiple) or final plan (covering or not),
  //but those helper functions that focus on serving building index plan (project-filter-indexscan)

  public static int getRowKeyIndex(RelDataType rowType, DrillScanRel origScan) {
    List<String> fieldNames = rowType.getFieldNames();
    int idx = 0;
    for (String field : fieldNames) {
      if (field.equalsIgnoreCase(((DbGroupScan)origScan.getGroupScan()).getRowKeyName())) {
        return idx;
      }
      idx++;
    }
    return -1;
  }

  protected RelDataType convertRowType(RelDataType origRowType, RelDataTypeFactory typeFactory) {
    if ( getRowKeyIndex(origRowType, origScan)>=0 ) { // row key already present
      return origRowType;
    }
    List<RelDataTypeField> fields = new ArrayList<>();

    fields.addAll(origRowType.getFieldList());
    fields.add(new RelDataTypeFieldImpl(
        ((DbGroupScan)origScan.getGroupScan()).getRowKeyName(), 0,
            typeFactory.createSqlType(SqlTypeName.ANY)));
    return new RelRecordType(fields);
  }

  protected boolean checkRowKey(List<SchemaPath> columns) {
    for (SchemaPath s : columns) {
      if (s.equals(((DbGroupScan)origScan.getGroupScan()).getRowKeyPath())) {
        return true;
      }
    }
    return false;
  }

  // Range distribute the right side of the join, on row keys using a range partitioning function
  protected RelNode createRangeDistRight(final RelNode rightPrel,
                                         final RelDataTypeField rightRowKeyField,
                                         final DbGroupScan origDbGroupScan) {

    List<DrillDistributionTrait.DistributionField> rangeDistFields =
        Lists.newArrayList(new DrillDistributionTrait.DistributionField(0 /* rowkey ordinal on the right side */));

    FieldReference rangeDistRef = FieldReference.getWithQuotedRef(rightRowKeyField.getName());
    List<FieldReference> rangeDistRefList = Lists.newArrayList();
    rangeDistRefList.add(rangeDistRef);

    final DrillDistributionTrait distRight;
    if (IndexPlanUtils.scanIsPartition(origDbGroupScan)) {
      distRight = new DrillDistributionTrait(
          DrillDistributionTrait.DistributionType.RANGE_DISTRIBUTED,
          ImmutableList.copyOf(rangeDistFields),
          origDbGroupScan.getRangePartitionFunction(rangeDistRefList));
    }
    else {
      distRight = DrillDistributionTrait.SINGLETON;
    }

    RelTraitSet rightTraits = newTraitSet(distRight).plus(Prel.DRILL_PHYSICAL);
    RelNode convertedRight = Prule.convert(rightPrel, rightTraits);

    return convertedRight;
  }

  public RelTraitSet newTraitSet(RelTrait... traits) {
    RelTraitSet set = indexContext.call.getPlanner().emptyTraitSet();
    for (RelTrait t : traits) {
      if(t != null) {
        set = set.plus(t);
      }
    }
    return set;
  }

  protected boolean toRemoveSort(DrillSortRel sort, RelCollation inputCollation) {
    if ( (inputCollation != null) && inputCollation.satisfies(sort.getCollation())) {
      return true;
    }
    return false;
  }

  public RelNode getSortNode(IndexPlanCallContext indexContext, RelNode newRel) {
    DrillSortRel rel = indexContext.sort;
    DrillDistributionTrait hashDistribution =
        new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED,
            ImmutableList.copyOf(SortPrule.getDistributionField(rel)));

    if ( toRemoveSort(indexContext.sort, newRel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE))) {
      //we are going to remove sort
      logger.debug("Not generating SortPrel since we have the required collation");

      RelTraitSet traits = newRel.getTraitSet().plus(rel.getCollation()).plus(Prel.DRILL_PHYSICAL);
      RelNode convertedInput = Prule.convert(newRel, traits);
      RelNode exch = new SingleMergeExchangePrel(newRel.getCluster(),
          traits.replace(DrillDistributionTrait.SINGLETON),
          convertedInput,//finalRel,//
          indexContext.sort.getCollation());
      newRel = exch;
    }
    else {
      RelTraitSet traits = newRel.getTraitSet().plus(rel.getCollation()).plus(Prel.DRILL_PHYSICAL);
      RelNode convertedInput = Prule.convert(newRel, traits);
      SortPrel sortPrel = new SortPrel(rel.getCluster(),
          rel.getTraitSet().replace(Prel.DRILL_PHYSICAL).plus(hashDistribution).plus(rel.getCollation()),
          Prule.convert(newRel, newRel.getTraitSet().replace(Prel.DRILL_PHYSICAL)),
          rel.getCollation());
      //newRel = sortPrel;
      RelNode exch = new SingleMergeExchangePrel(newRel.getCluster(),
          traits.replace(DrillDistributionTrait.SINGLETON),
          sortPrel,//finalRel,//
          indexContext.sort.getCollation());
      newRel = exch;
    }
    return newRel;
  }



  public abstract RelNode convertChild(RelNode current, RelNode child) throws InvalidRelException;

  public void go() throws InvalidRelException {
    RelNode top = indexContext.call.rel(0);
    final RelNode input;
    if (top instanceof DrillProjectRel) {
      DrillProjectRel topProject = (DrillProjectRel) top;
      input = topProject.getInput();
    }
    else if (top instanceof DrillFilterRel) {
      DrillFilterRel topFilter = (DrillFilterRel)top;
      input = topFilter.getInput();
    } else if (top instanceof DrillSortRel) {
      DrillSortRel topSort = (DrillSortRel)top;
      input = topSort.getInput();
    }
    else if ( top instanceof DrillSortRel) {
      DrillSortRel topSort = (DrillSortRel) top;
      input = topSort.getInput();
    }
    else {
      return;
    }
    RelTraitSet traits = input.getTraitSet().plus(Prel.DRILL_PHYSICAL);
    RelNode convertedInput = Prule.convert(input, traits);
    this.go(top, convertedInput);
  }
}
