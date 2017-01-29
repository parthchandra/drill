/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.index;


import com.google.common.collect.Maps;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.planner.logical.partition.FindPartitionConditions;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class RexSeparator {

  final private List<SchemaPath> relatedPaths;
  final private RelDataType rowType;
  final private RexBuilder builder;

  public RexSeparator(List<SchemaPath> relatedPaths, RelDataType rowType, RexBuilder builder) {
    this.relatedPaths = relatedPaths;
    this.rowType = rowType;
    this.builder = builder;
  }

  public RexNode getSeparatedCondition(RexNode expr) {
    SimpleRexRemap.FieldsMarker marker = new SimpleRexRemap.FieldsMarker(rowType);
    expr.accept(marker);

    final Map<RexNode, String> markMap = Maps.newHashMap();
    final Map<RexNode, String> relevantRexMap = marker.getFieldAndPos();
    for(Map.Entry<RexNode, String> entry : relevantRexMap.entrySet()) {
      //for the schemaPath found in expr, only these in relatedPaths is related
      if(relatedPaths.contains(SchemaPath.getCompoundPath(entry.getValue().split("\\.")))) {
        markMap.put(entry.getKey(), entry.getValue());
      }
    }

    ConditionSeparator separator = new ConditionSeparator(markMap, builder);
    separator.analyze(expr);
    return separator.getFinalCondition();
  }

  private static class ConditionSeparator extends  FindPartitionConditions {

    final private Map<RexNode, String> markMap;
    private boolean inAcceptedPath;

    public ConditionSeparator(Map<RexNode, String> markMap, RexBuilder builder) {
      super(new BitSet(), builder);
      this.markMap = markMap;
      inAcceptedPath = false;
    }

    @Override
    protected boolean inputRefToPush(RexInputRef inputRef) {
      //this class will based on the schemaPath to decide what to push
      if (markMap.containsKey(inputRef) || inAcceptedPath) {
        return true;
      }
      return false;
    }

    @Override
    public Void visitCall(RexCall call) {
      boolean oldValue = inAcceptedPath;
      try {
        if (markMap.containsKey(call)) {
          inAcceptedPath = true;

        }
        return super.visitCall(call);
      } finally {
        inAcceptedPath = oldValue;
      }
    }
  }
}
