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
package org.apache.drill.exec.planner.sql.handlers;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.drill.exec.planner.common.DrillAggregateRelBase;
import org.apache.drill.exec.planner.common.DrillJoinRelBase;
import org.apache.drill.exec.planner.common.DrillUnionRelBase;
import org.apache.drill.exec.planner.logical.DrillLimitRel;
import org.apache.drill.exec.planner.logical.DrillRel;

import java.math.BigDecimal;

/**
 * Visitor that will identify whether the root portion of the RelNode tree contains a limit 0 pattern. In this case, we
 * inform the planner settings that this plan should be run as a single node plan to reduce the overhead associated with
 * executing a schema-only query.
 */
public class FindLimit0Visitor extends RelShuttleImpl {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FindLimit0Visitor.class);

  private boolean contains = false;

  public static boolean containsLimit0(RelNode rel) {
    FindLimit0Visitor visitor = new FindLimit0Visitor();
    rel.accept(visitor);
    return visitor.isContains();
  }

  /**
   * TODO(DRILL-3993): Use RelBuilder to create a limit node to allow for applying this optimization in potentially
   * any of the transformations, but currently this can be applied after Drill logical transformation, and before
   * Drill physical transformation.
   */
  public static DrillRel addLimitOnTopOfLeafNodes(final RelNode rel) {
    final RelShuttleImpl shuttle = new RelShuttleImpl() {

      private RelNode addLimitAsParent(RelNode node) {
        final RexBuilder builder = node.getCluster().getRexBuilder();
        final RexLiteral offset = builder.makeExactLiteral(BigDecimal.ZERO);
        final RexLiteral fetch = builder.makeExactLiteral(BigDecimal.ZERO);
        return new DrillLimitRel(node.getCluster(), node.getTraitSet(), node, offset, fetch);
      }

      @Override
      public RelNode visit(LogicalValues values) {
        return addLimitAsParent(values);
      }

      @Override
      public RelNode visit(TableScan scan) {
        return addLimitAsParent(scan);
      }

      @Override
      public RelNode visit(RelNode other) {
        if (other.getInputs().size() == 0) { // leaf operator
          return addLimitAsParent(other);
        }
        return super.visit(other);
      }
    };
    return (DrillRel) rel.accept(shuttle);
  }

  private FindLimit0Visitor() {
  }

  boolean isContains() {
    return contains;
  }

  private static boolean isLimit0(RexNode fetch) {
    if (fetch != null && fetch.isA(SqlKind.LITERAL)) {
      RexLiteral l = (RexLiteral) fetch;
      switch (l.getTypeName()) {
      case BIGINT:
      case INTEGER:
      case DECIMAL:
        if (((long) l.getValue2()) == 0) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public RelNode visit(LogicalSort sort) {
    if (isLimit0(sort.fetch)) {
      contains = true;
      return sort;
    }

    return super.visit(sort);
  }

  @Override
  public RelNode visit(RelNode other) {
    if (other instanceof DrillJoinRelBase ||
        other instanceof DrillAggregateRelBase ||
        other instanceof DrillUnionRelBase) {
      return other;
    }
    if (other instanceof DrillLimitRel) {
      if (isLimit0(((DrillLimitRel) other).getFetch())) {
        contains = true;
        return other;
      }
    }

    return super.visit(other);
  }

  // TODO: The following nodes are never visited because this visitor is used after logical transformation!
  // The following set of RelNodes should terminate a search for the limit 0 pattern as they want convey its meaning.

  @Override
  public RelNode visit(LogicalAggregate aggregate) {
    return aggregate;
  }

  @Override
  public RelNode visit(LogicalIntersect intersect) {
    return intersect;
  }

  @Override
  public RelNode visit(LogicalJoin join) {
    return join;
  }

  @Override
  public RelNode visit(LogicalMinus minus) {
    return minus;
  }

  @Override
  public RelNode visit(LogicalUnion union) {
    return union;
  }
}
