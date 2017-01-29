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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;

import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitorImpl;

import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.drill.common.expression.PathSegment;

import java.util.List;
import java.util.Map;

/**
 * Rewrite RexNode with these policies:
 * 1) field renamed. The input field was named differently in index table,
 * 2) field is in different position of underlying rowtype
 *
 * TODO: 3) certain operator needs rewriting.
 * This class for now applies to only filter on scan, for filter-on-project-on-scan. A stack of
 * rowType is required.
 */
public class SimpleRexRemap {
  final RelDataType origRowType;
  final RelDataType newRowType;
  private Map<RexNode, String> markMap;//path-->rexNode in expr(indexCondition)

  private RexBuilder builder;

  public SimpleRexRemap(RelDataType origRowType,
                        RelDataType newRowType, RexBuilder builder) {
    super();
    this.origRowType = origRowType;
    this.newRowType = newRowType;
    this.builder = builder;
  }

  public RexNode rewrite(RexNode expr) {
    FieldsMarker marker = new FieldsMarker(origRowType);
    expr.accept(marker);
    this.markMap = marker.getFieldAndPos();

    Map<String, RexNode> destNodeMap = Maps.newHashMap();
    for(Map.Entry<RexNode, String> entry: this.markMap.entrySet()) {
      //if this path is required to replace(in fieldMap), replace the correspondent rexNode to reflect the new field in newRowType
      String entryPath = entry.getValue();

      //then build rexNode from the path
      RexNode destRex = buildRexForField(entryPath, newRowType);
      destNodeMap.put(entryPath, destRex);
    }

    //we marked the nodes could be replaced in pathMap, and we prepared nodes to replace in destNodeMap
    //now we visit through the nodes and replace the marked nodes if destNode is available for it.
    RexReplace replacer = new RexReplace(markMap, destNodeMap);
    RexNode resultRex = expr.accept(replacer);
    return resultRex;
  }

  public static RelDataTypeField findField(String fieldName, RelDataType rowType) {
    final String[] parts = fieldName.replaceAll("`", "").split("\\.");
    final String rootPart = parts[0];

    for (RelDataTypeField f : rowType.getFieldList()) {
      if (rootPart.equalsIgnoreCase(f.getName())) {
        return f;
      }
    }
    return null;
  }

  public static String getFullPath(PathSegment pathSeg) {
    PathSegment.NameSegment nameSeg = (PathSegment.NameSegment)pathSeg;
    if(nameSeg.isLastPath()) {
      return nameSeg.getPath();
    }
    return String.format("%s.%s",
        nameSeg.getPath(),
        getFullPath(nameSeg.getChild()));
  }

  //reverse
  private RexNode makeItemOperator(String[] paths, int index, RelDataType rowType) {
    if( index == 0 ) {//last one, return ITEM([0]-inputRef, [1] Literal)
      final RelDataTypeField field = findField(paths[0], rowType);
      return builder.makeInputRef(field.getType(), field.getIndex());
    }
    return builder.makeCall(SqlStdOperatorTable.ITEM, makeItemOperator(paths,index-1, rowType), builder.makeLiteral(paths[index]));
  }

  private RexNode buildRexForField(String strPath, RelDataType rowType) {
    //if size is 0, return InputRef directly, else return ITEM(ITEM($1, 'seg1'), 'seg2')
    if( ! strPath.contains(".") ) {
      final RelDataTypeField field = findField(strPath, rowType);
      return field == null ? null : builder.makeInputRef(field.getType(), field.getIndex());
    }
    String[] paths = strPath.split("\\.");

    return makeItemOperator(paths, paths.length-1, rowType);
  }

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

  /**
   * This class go through the RexNode, collect all the fieldNames, mark starting positions(RexNode) of fields
   * so this information can be used later e,.g. replaced with a substitute node
   */
  public static class FieldsMarker extends RexVisitorImpl<PathSegment> {
    final List<String> fieldNames;
    final List<RelDataTypeField> fields;
    final Map<RexNode, String> desiredFields = Maps.newHashMap();

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
    public Map<RexNode, String> getFieldAndPos() {
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

  public static class RexReplace extends RexShuttle {

    final Map<RexNode, String> marks;
    final Map<String, RexNode> substitutes;

    public RexReplace( Map<RexNode, String> markNodes,
                       Map<String, RexNode> substituteNodes ) {
      this.marks = markNodes;
      this.substitutes = substituteNodes;
    }

    boolean toReplace(RexNode node) {
      return marks.containsKey(node);
    }

    RexNode replace(RexNode node) {
      return substitutes.get(marks.get(node));
    }

    public RexNode visitOver(RexOver over) {
      return toReplace(over) ? replace(over) : super.visitOver(over);
    }

    public RexNode visitCall(final RexCall call) {
      return toReplace(call) ? replace(call) : super.visitCall(call);
    }

    public RexNode visitCorrelVariable(RexCorrelVariable variable) {
      return variable;
    }

    public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      return toReplace(fieldAccess) ? replace(fieldAccess) : super.visitFieldAccess(fieldAccess);
    }

    public RexNode visitInputRef(RexInputRef inputRef) {
      return toReplace(inputRef) ? replace(inputRef) : super.visitInputRef(inputRef);
    }

    public RexNode visitLocalRef(RexLocalRef localRef) {
      return toReplace(localRef) ? replace(localRef) : super.visitLocalRef(localRef);
    }

    public RexNode visitLiteral(RexLiteral literal) {
      return literal;
    }

    public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
      return dynamicParam;
    }

    public RexNode visitRangeRef(RexRangeRef rangeRef) {
      return toReplace(rangeRef) ? replace(rangeRef) : super.visitRangeRef(rangeRef);
    }
  }
}

