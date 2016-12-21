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

import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractDbGroupScan;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.planner.physical.SubsetTransformer;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

public abstract class AbstractIndexPlanGenerator extends SubsetTransformer<FilterPrel, InvalidRelException> {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractIndexPlanGenerator.class);

  final protected ProjectPrel origProject;
  final protected ScanPrel origScan;
  final protected IndexGroupScan indexGroupScan;
  final protected RexNode indexCondition;
  final protected RexNode remainderCondition;
  final protected RexBuilder builder;

  public AbstractIndexPlanGenerator(RelOptRuleCall call,
      ProjectPrel origProject,
      ScanPrel origScan,
      IndexGroupScan indexGroupScan,
      RexNode indexCondition,
      RexNode remainderCondition,
      RexBuilder builder) {
    super(call);
    this.origProject = origProject;
    this.origScan = origScan;
    this.indexGroupScan = indexGroupScan;
    this.indexCondition = indexCondition;
    this.remainderCondition = remainderCondition;
    this.builder = builder;
  }

  protected int getRowKeyIndex(RelDataType rowType) {
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
    if ( getRowKeyIndex(origRowType)>=0 ) { // row key already present
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

}
