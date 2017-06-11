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
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.DrillSortRel;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.Prule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.planner.physical.SingleMergeExchangePrel;
import org.apache.drill.exec.planner.physical.SortPrule;
import org.apache.drill.exec.planner.physical.SubsetTransformer;

public abstract class AbstractIndexPlanGenerator extends SubsetTransformer<RelNode, InvalidRelException>{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractIndexPlanGenerator.class);

  final protected DrillProjectRel origProject;
  final protected DrillScanRel origScan;
  final protected DrillProjectRel capProject;
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
    this.origProject = indexContext.project;
    this.origScan = indexContext.scan;
    this.capProject = indexContext.capProject;
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

    final DrillDistributionTrait distRangeRight = new DrillDistributionTrait(
        DrillDistributionTrait.DistributionType.RANGE_DISTRIBUTED,
        ImmutableList.copyOf(rangeDistFields),
        origDbGroupScan.getRangePartitionFunction(rangeDistRefList));

    RelTraitSet rightTraits = newTraitSet(distRangeRight).plus(Prel.DRILL_PHYSICAL);
    RelNode convertedRight = Prule.convert(rightPrel, rightTraits);

    return convertedRight;
  }

  protected RelCollation buildCollation(List<RexNode> projectRexs, RelNode input, FunctionalIndexInfo indexInfo) {
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
    }
    return RelCollationImpl.of(newFields);
  }

  /**
   * For IndexScan in non-covering case, rowType to return contains only row_key('_id') of primary table.
   * so the rowType for IndexScan should be converted from [Primary_table.row_key, primary_table.indexed_col]
   * to [indexTable.row_key(primary_table.indexed_col), indexTable.<primary_key.row_key> (Primary_table.row_key)]
   * This will impact the columns of scan, the rowType of ScanRel
   *
   * @param origScan
   * @param indexCondition
   * @param idxScan
   * @return
   */
  public static RelDataType convertRowTypeForIndexScan(DrillScanRel origScan,
                                                       RexNode indexCondition,
                                                       IndexGroupScan idxScan,
                                                       FunctionalIndexInfo functionInfo) {
    return FunctionalIndexHelper.convertRowTypeForIndexScan(origScan, indexCondition, idxScan, functionInfo);
  }

  protected RexNode convertConditionForIndexScan(RexNode idxCondition,
                                                 RelDataType idxRowType,
                                                 FunctionalIndexInfo functionInfo) {
    return FunctionalIndexHelper.convertConditionForIndexScan(idxCondition, origScan, idxRowType,
        builder, functionInfo);
  }

  public RelTraitSet newTraitSet(RelTrait... traits) {
    RelTraitSet set = indexContext.call.getPlanner().emptyTraitSet();
    for (RelTrait t : traits) {
      set = set.plus(t);
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

    if ( !toRemoveSort(indexContext.sort, newRel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE))) {
      //create sort's prel, also let us add distribution trait as SortPrule will do
      final RelTraitSet traits = rel.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(hashDistribution);
      RelNode convertedInput = Prule.convert(newRel, traits);

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

      RelTraitSet traits = newRel.getTraitSet().plus(rel.getCollation()).plus(Prel.DRILL_PHYSICAL);
      RelNode convertedInput = Prule.convert(newRel, traits);
      RelNode exch = new SingleMergeExchangePrel(newRel.getCluster(),
          traits.replace(DrillDistributionTrait.SINGLETON),
          convertedInput,//finalRel,//
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
