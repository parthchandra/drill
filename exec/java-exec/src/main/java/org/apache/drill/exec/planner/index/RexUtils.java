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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;

import java.util.List;
import java.util.Map;

public class RexUtils {

  private static PathSegment convertLiteral(RexLiteral literal) {
    switch (literal.getType().getSqlTypeName()) {
      case CHAR:
        return new PathSegment.NameSegment(RexLiteral.stringValue(literal));
      case INTEGER:
        return new PathSegment.ArraySegment(RexLiteral.intValue(literal));
      default:
        return null;
    }
  }

  public static SchemaPath getFullPath(PathSegment pathSeg) {
    return new SchemaPath((PathSegment.NameSegment)pathSeg);
  }

  /**
   * This class go through the RexNode, collect all the fieldNames, mark starting positions(RexNode) of fields
   * so this information can be used later e,.g. replaced with a substitute node; check if these fields are all indexed
   * in a single index table.
   */
  public static class FieldsMarker extends RexVisitorImpl<PathSegment> {
    final List<String> fieldNames;
    final List<RelDataTypeField> fields;
    final Map<RexNode, SchemaPath> desiredFields = Maps.newHashMap();

    int stackDepth;

    public FieldsMarker(RelDataType rowType) {
      super(true);
      this.fieldNames = rowType.getFieldNames();
      this.fields = rowType.getFieldList();
      this.stackDepth = 0;
    }

    public PathSegment newPath(PathSegment segment, RexNode node) {
      if (stackDepth == 0) {
        desiredFields.put(node, getFullPath(segment));
      }
      return segment;
    }
    public PathSegment newPath(String path, RexNode node) {
      PathSegment segment = new PathSegment.NameSegment(path);
      if (stackDepth == 0) {
        desiredFields.put(node, getFullPath(segment));
      }
      return segment;
    }

    public Map<RexNode, SchemaPath> getFieldAndPos() {
      return ImmutableMap.copyOf(desiredFields);
    }

    @Override
    public PathSegment visitInputRef(RexInputRef inputRef) {
      int index = inputRef.getIndex();
      String name = fieldNames.get(index);
      return newPath(name, inputRef);
    }

    @Override
    public PathSegment visitCall(RexCall call) {
      if ("ITEM".equals(call.getOperator().getName())) {
        stackDepth++;
        PathSegment mapOrArray = call.operands.get(0).accept(this);
        stackDepth--;
        if (mapOrArray != null) {
          if (call.operands.get(1) instanceof RexLiteral) {
            PathSegment newFieldPath = newPath(
                mapOrArray.cloneWithNewChild(convertLiteral((RexLiteral) call.operands.get(1))),
                call);
            return newFieldPath;
          }
          return mapOrArray;
        }
      } else {
        for (RexNode operand : call.operands) {
          operand.accept(this);
        }
      }
      return null;
    }
  }
}
