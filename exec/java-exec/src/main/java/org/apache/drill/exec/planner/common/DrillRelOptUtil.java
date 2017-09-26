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
package org.apache.drill.exec.planner.common;

import java.util.AbstractList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
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
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.resolver.TypeCastRules;

/**
 * Utility class that is a subset of the RelOptUtil class and is a placeholder for Drill specific
 * static methods that are needed during either logical or physical planning.
 */
public abstract class DrillRelOptUtil {

  // Similar to RelOptUtil.areRowTypesEqual() with the additional check for allowSubstring
  public static boolean areRowTypesCompatible(
      RelDataType rowType1,
      RelDataType rowType2,
      boolean compareNames,
      boolean allowSubstring) {
    if (rowType1 == rowType2) {
      return true;
    }
    if (compareNames) {
      // if types are not identity-equal, then either the names or
      // the types must be different
      return false;
    }
    if (rowType2.getFieldCount() != rowType1.getFieldCount()) {
      return false;
    }
    final List<RelDataTypeField> f1 = rowType1.getFieldList();
    final List<RelDataTypeField> f2 = rowType2.getFieldList();
    for (Pair<RelDataTypeField, RelDataTypeField> pair : Pair.zip(f1, f2)) {
      final RelDataType type1 = pair.left.getType();
      final RelDataType type2 = pair.right.getType();
      // If one of the types is ANY comparison should succeed
      if (type1.getSqlTypeName() == SqlTypeName.ANY
        || type2.getSqlTypeName() == SqlTypeName.ANY) {
        continue;
      }
      if (type1.getSqlTypeName() != type2.getSqlTypeName()) {
        if (allowSubstring
            && (type1.getSqlTypeName() == SqlTypeName.CHAR && type2.getSqlTypeName() == SqlTypeName.CHAR)
            && (type1.getPrecision() <= type2.getPrecision())) {
          return true;
        }

        // Check if Drill implicit casting can resolve the incompatibility
        List<TypeProtos.MinorType> types = Lists.newArrayListWithCapacity(2);
        types.add(Types.getMinorTypeFromName(type1.getSqlTypeName().getName()));
        types.add(Types.getMinorTypeFromName(type2.getSqlTypeName().getName()));
        if(TypeCastRules.getLeastRestrictiveType(types) != null) {
          return true;
        }

        return false;
      }
    }
    return true;
  }

  /**
   * Returns a relational expression which has the same fields as the
   * underlying expression, but the fields have different names.
   *
   *
   * @param rel        Relational expression
   * @param fieldNames Field names
   * @return Renamed relational expression
   */
  public static RelNode createRename(
      RelNode rel,
      final List<String> fieldNames) {
    final List<RelDataTypeField> fields = rel.getRowType().getFieldList();
    assert fieldNames.size() == fields.size();
    final List<RexNode> refs =
        new AbstractList<RexNode>() {
          public int size() {
            return fields.size();
          }

          public RexNode get(int index) {
            return RexInputRef.of(index, fields);
          }
        };

    return RelOptUtil.createProject(rel, refs, fieldNames, false);
  }

  public static boolean isTrivialProject(Project project, boolean useNamesInIdentityProjCalc) {
    if (!useNamesInIdentityProjCalc) {
      return ProjectRemoveRule.isTrivial(project);
    }  else {
      return containIdentity(project.getProjects(), project.getRowType(), project.getInput().getRowType());
    }
  }

  /** Returns a rowType having all unique field name.
   *
   * @param rowType : input rowType
   * @param typeFactory : type factory used to create a new row type.
   * @return
   */
  public static RelDataType uniqifyFieldName(final RelDataType rowType, final RelDataTypeFactory typeFactory) {
    return typeFactory.createStructType(RelOptUtil.getFieldTypeList(rowType),
        SqlValidatorUtil.uniquify(rowType.getFieldNames()));
  }

  /**
   * Returns whether the leading edge of a given array of expressions is
   * wholly {@link RexInputRef} objects with types and names corresponding
   * to the underlying row type. */
  private static boolean containIdentity(List<? extends RexNode> exps,
                                        RelDataType rowType, RelDataType childRowType) {
    List<RelDataTypeField> fields = rowType.getFieldList();
    List<RelDataTypeField> childFields = childRowType.getFieldList();
    int fieldCount = childFields.size();
    if (exps.size() != fieldCount) {
      return false;
    }
    for (int i = 0; i < exps.size(); i++) {
      RexNode exp = exps.get(i);
      if (!(exp instanceof RexInputRef)) {
        return false;
      }
      RexInputRef var = (RexInputRef) exp;
      if (var.getIndex() != i) {
        return false;
      }
      if (!fields.get(i).getName().equals(childFields.get(i).getName())) {
        return false;
      }
      if (!fields.get(i).getType().equals(childFields.get(i).getType())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Travesal RexNode to find the item/flattern operator. Continue search if RexNode has a
   * RexInputRef which refers to a RexNode in project expressions.
   *
   * @param node : RexNode to search
   * @param projExprs : the list of project expressions. Empty list means there is No project operator underneath.
   * @return : Return null if there is NONE; return the first appearance of item/flatten RexCall.
   */
  public static RexCall findItemOrFlatten(
      final RexNode node,
      final List<RexNode> projExprs) {
    try {
      RexVisitor<Void> visitor =
          new RexVisitorImpl<Void>(true) {
            public Void visitCall(RexCall call) {
              if ("item".equals(call.getOperator().getName().toLowerCase()) ||
                  "flatten".equals(call.getOperator().getName().toLowerCase())) {
                throw new Util.FoundOne(call); /* throw exception to interrupt tree walk (this is similar to
                                              other utility methods in RexUtil.java */
              }
              return super.visitCall(call);
            }

            public Void visitInputRef(RexInputRef inputRef) {
              if (projExprs.size() == 0 ) {
                return super.visitInputRef(inputRef);
              } else {
                final int index = inputRef.getIndex();
                RexNode n = projExprs.get(index);
                if (n instanceof RexCall) {
                  RexCall r = (RexCall) n;
                  if ("item".equals(r.getOperator().getName().toLowerCase()) ||
                      "flatten".equals(r.getOperator().getName().toLowerCase())) {
                    throw new Util.FoundOne(r);
                  }
                }

                return super.visitInputRef(inputRef);
              }
            }
          };
      node.accept(visitor);
      return null;
    } catch (Util.FoundOne e) {
      Util.swallow(e, null);
      return (RexCall) e.getNode();
    }
  }

  /**
   * Converts a collection of expressions into an AND.
   * If there are zero expressions, returns TRUE.
   * If there is one expression, returns just that expression.
   * Removes expressions that always evaluate to TRUE.
   * Returns null only if {@code nullOnEmpty} and expression is TRUE.
   * @deprecated Will be superseded by {@link RexUtil} methods post migration to latest Calcite version
   */
  @Deprecated
  public static RexNode composeConjunction(RexBuilder rexBuilder,
                                           Iterable<? extends RexNode> nodes, boolean nullOnEmpty) {
    ImmutableList<RexNode> list = flattenAnd(nodes);
    switch (list.size()) {
      case 0:
        return nullOnEmpty
            ? null
            : rexBuilder.makeLiteral(true);
      case 1:
        return list.get(0);
      default:
        return rexBuilder.makeCall(SqlStdOperatorTable.AND, list);
    }
  }

  /**
   * Flattens a list of AND nodes.
   * <p>Treats null nodes as literal TRUE (i.e. ignores them).
   * @param nodes
   * @return
   * @deprecated Will be superseded by {@link RexUtil} methods post migration to latest Calcite version
   */
  /** Flattens a list of AND nodes.
   *
   * <p>Treats null nodes as literal TRUE (i.e. ignores them). */
  @Deprecated
  public static ImmutableList<RexNode> flattenAnd(
      Iterable<? extends RexNode> nodes) {
    boolean flatten = flattenIsValid(nodes);
    if (nodes instanceof Collection && ((Collection) nodes).isEmpty()) {
      // Optimize common case
      return ImmutableList.of();
    }
    final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
    final Set<String> digests = Sets.newHashSet(); // to eliminate duplicates
    for (RexNode node : nodes) {
      if (node != null && (!flatten || digests.add(node.toString()))) {
        addAnd(builder, node);
      }
    }
    return builder.build();
  }

  @Deprecated
  private static void addAnd(ImmutableList.Builder<RexNode> builder,
                             RexNode node) {
    switch (node.getKind()) {
      case AND:
        for (RexNode operand : ((RexCall) node).getOperands()) {
          addAnd(builder, operand);
        }
        return;
      default:
        if (!node.isAlwaysTrue()) {
          builder.add(node);
        }
    }
  }

  /**
   * Converts a collection of expressions into an OR.
   * If there are zero expressions, returns FALSE.
   * If there is one expression, returns just that expression.
   * Removes expressions that always evaluate to FALSE.
   * Flattens expressions that are ORs.
   * @deprecated Will be superseded by {@link RexUtil} methods post migration to latest Calcite version
   */
  @Deprecated
  public static RexNode composeDisjunction(RexBuilder rexBuilder,
                                           Iterable<? extends RexNode> nodes, boolean nullOnEmpty) {
    ImmutableList<RexNode> list = flattenOr(nodes);
    switch (list.size()) {
      case 0:
        return nullOnEmpty
            ? null
            : rexBuilder.makeLiteral(false);
      case 1:
        return list.get(0);
      default:
        return rexBuilder.makeCall(SqlStdOperatorTable.OR, list);
    }
  }

  /**
   * Flattens a list of OR nodes.
   * <p>Treats null nodes as literal TRUE (i.e. ignores them).
   * @param nodes
   * @return
   * @deprecated Will be superseded by {@link RexUtil} methods post migration to latest Calcite version
   */
  @Deprecated
  public static ImmutableList<RexNode> flattenOr(
      Iterable<? extends RexNode> nodes) {
    boolean flatten = flattenIsValid(nodes);
    if (nodes instanceof Collection && ((Collection) nodes).isEmpty()) {
      // Optimize common case
      return ImmutableList.of();
    }
    final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
    final Set<String> digests = Sets.newHashSet(); // to eliminate duplicates
    for (RexNode node : nodes) {
      if (node != null && (!flatten || digests.add(node.toString()))) {
        addOr(builder, node);
      }
    }
    return builder.build();
  }

  @Deprecated
  private static void addOr(ImmutableList.Builder<RexNode> builder,
                            RexNode node) {
    switch (node.getKind()) {
      case OR:
        for (RexNode operand : ((RexCall) node).getOperands()) {
          addOr(builder, operand);
        }
        return;
      default:
        if (!node.isAlwaysFalse()) {
          builder.add(node);
        }
    }
  }

  @Deprecated
  private static boolean flattenIsValid(Iterable<? extends RexNode> nodes) {
    FlattenValidityRexVisitor visitor = new FlattenValidityRexVisitor();
    for (RexNode node : nodes) {
      node.accept(visitor);
    }
    return visitor.isFlattenValid();
  }

  private static class FlattenValidityRexVisitor extends RexVisitorImpl<RexNode> {

    protected boolean isFlattenValid = true;

    public FlattenValidityRexVisitor() {
      super(true);
    }

    public boolean isFlattenValid() {
      return isFlattenValid;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      return inputRef;
    }

    @Override
    public RexNode visitLocalRef(RexLocalRef localRef) {
      return localRef;
    }

    @Override
    public RexNode visitLiteral(RexLiteral literal) {
      if (literal.getTypeName() == SqlTypeName.TIME
          || literal.getTypeName() == SqlTypeName.TIMESTAMP) {
        isFlattenValid = false;
      }
      return literal;
    }

    @Override
    public RexNode visitOver(RexOver over) {
      return over;
    }

    @Override
    public RexNode visitCorrelVariable(RexCorrelVariable correlVariable) {
      return correlVariable;
    }

    @Override
    public RexNode visitCall(RexCall call) {

      for (RexNode operand : call.operands) {
        operand.accept(this);
      }
      return call;
    }

    @Override
    public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
      return dynamicParam;
    }

    @Override
    public RexNode visitRangeRef(RexRangeRef rangeRef) {
      return rangeRef;
    }

    @Override
    public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      return fieldAccess;
    }
  }
}
