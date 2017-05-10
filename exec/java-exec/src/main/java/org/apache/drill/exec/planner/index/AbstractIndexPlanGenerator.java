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
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.Prule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.planner.physical.SubsetTransformer;

public abstract class AbstractIndexPlanGenerator extends SubsetTransformer<RelNode, InvalidRelException>{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractIndexPlanGenerator.class);

  final protected DrillProjectRel origProject;
  final protected DrillScanRel origScan;
  final protected DrillProjectRel capProject;

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
    RelDataTypeFactory typeFactory = origScan.getCluster().getTypeFactory();
    List<RelDataTypeField> fields = new ArrayList<>();

    //row_key in the rowType of scan on primary table
    RelDataTypeField rowkey_primary;

    RelRecordType newRowType = null;

    //first add row_key of primary table,
    rowkey_primary = new RelDataTypeFieldImpl(
        ((DbGroupScan)origScan.getGroupScan()).getRowKeyName(), fields.size(),
        typeFactory.createSqlType(SqlTypeName.ANY));
    fields.add(rowkey_primary);

    //then add indexed cols
    IndexableExprMarker idxMarker = new IndexableExprMarker(origScan);
    indexCondition.accept(idxMarker);
    Map<RexNode, LogicalExpression> idxExprMap = idxMarker.getIndexableExpression();

    for (LogicalExpression indexedExpr : idxExprMap.values()) {
      if (indexedExpr instanceof SchemaPath) {
        fields.add(new RelDataTypeFieldImpl(
            ((SchemaPath)indexedExpr).getAsUnescapedPath(), fields.size(),
            typeFactory.createSqlType(SqlTypeName.ANY)));
      }
      else if(indexedExpr instanceof CastExpression) {
        SchemaPath newPath = functionInfo.getNewPathFromExpr(indexedExpr);
        fields.add(new RelDataTypeFieldImpl(
            newPath.getAsUnescapedPath(), fields.size(),
            typeFactory.createSqlType(SqlTypeName.ANY)));
      }
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
    idxScan.setColumns(columns);

    //rowtype does not take the whole path, but only the rootSegment of the SchemaPath
    newRowType = new RelRecordType(Lists.newArrayList(rowfields));
    return newRowType;
  }

  protected RexNode convertConditionForIndexScan(RexNode idxCondition,
                                                 RelDataType idxRowType,
                                                 FunctionalIndexInfo functionInfo) {
    IndexableExprMarker marker = new IndexableExprMarker(origScan);
    idxCondition.accept(marker);
    SimpleRexRemap remap = new SimpleRexRemap(origScan, idxRowType, builder);
    remap.setExpressionMap(functionInfo.getExprMap());

    if (functionInfo.supportEqualCharConvertToLike()) {
      final Map<LogicalExpression, LogicalExpression> indexedExprs = functionInfo.getExprMap();

      final Map<RexNode, LogicalExpression> equalCastMap = marker.getEqualOnCastChar();

      Map<RexNode, LogicalExpression> toRewriteEqualCastMap = Maps.newHashMap();

      // the marker collected all equal-cast-varchar, now check which one we should replace
      for (Map.Entry<RexNode, LogicalExpression> entry : equalCastMap.entrySet()) {
        CastExpression expr = (CastExpression) entry.getValue();
        //whether this cast varchar/char expression is indexed even the length is not the same
        for (LogicalExpression indexed : indexedExprs.keySet()) {
          if (indexed instanceof CastExpression) {
            final CastExpression indexedCast = (CastExpression) indexed;
            if (expr.getInput().equals(indexedCast.getInput())
                && expr.getMajorType().getMinorType().equals(indexedCast.getMajorType().getMinorType())
                //if expr's length < indexedCast's length, we should convert equal to LIKE for this condition
                && expr.getMajorType().getWidth() < indexedCast.getMajorType().getWidth()) {
              toRewriteEqualCastMap.put(entry.getKey(), entry.getValue());
            }
          }
        }
      }
      if (toRewriteEqualCastMap.size() > 0) {
        idxCondition = remap.rewriteEqualOnCharToLike(idxCondition, toRewriteEqualCastMap);
      }
    }

    return remap.rewriteWithMap(idxCondition, marker.getIndexableExpression());
  }

  public RelTraitSet newTraitSet(RelTrait... traits) {
    RelTraitSet set = indexContext.call.getPlanner().emptyTraitSet();
    for (RelTrait t : traits) {
      set = set.plus(t);
    }
    return set;
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
    }
    else {
      return;
    }
    RelTraitSet traits = input.getTraitSet().plus(Prel.DRILL_PHYSICAL);
    RelNode convertedInput = Prule.convert(input, traits);
    this.go(top, convertedInput);
  }

}
