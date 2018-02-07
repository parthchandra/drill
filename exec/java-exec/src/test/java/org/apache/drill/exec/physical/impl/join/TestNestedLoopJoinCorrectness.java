/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl.join;

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.drill.categories.OperatorTest;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.NestedLoopJoinPOP;
import org.apache.drill.exec.physical.impl.MockRecordBatch;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.ExpandableHyperContainer;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.store.mock.MockStorePOP;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertTrue;

@Category(OperatorTest.class)
public class TestNestedLoopJoinCorrectness extends SubOperatorTest {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestNestedNotYet.class);

  // Operator Context for mock batch
  public static OperatorContext operatorContext;

  // For now using MockStorePop for MockRecordBatch
  public static PhysicalOperator mockPopConfig;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    mockPopConfig = new MockStorePOP(null);
    operatorContext = fixture.newOperatorContext(mockPopConfig);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    operatorContext.close();
  }

  @Test
  public void testNotYetOutcomeUsingMockBatch() throws Exception {

    // Create left input schema
    TupleMetadata leftSchema = new SchemaBuilder().add("id_left", TypeProtos.MinorType.INT).add("cost_left", TypeProtos.MinorType.INT).add("name_left", TypeProtos.MinorType.VARCHAR).buildSchema();

    // Create right input schema
    TupleMetadata rightSchema = new SchemaBuilder().add("id_right", TypeProtos.MinorType.INT).add("cost_right", TypeProtos.MinorType.INT).add("name_right", TypeProtos.MinorType.VARCHAR).buildSchema();

    // Create data for left input
    final RowSet.SingleRowSet leftRowSet = fixture.rowSetBuilder(leftSchema).addRow(1, 10, "item1").addRow(2, 20, "item2").addRow(3, 30, "item3").build();

    // Create data for right input
    final RowSet.SingleRowSet rightRowSet = fixture.rowSetBuilder(rightSchema).addRow(1, 11, "item11").addRow(2, 21, "item21").addRow(3, 31, "item31").build();

    final RowSet.SingleRowSet rightRowSet2 = fixture.rowSetBuilder(rightSchema).addRow(2, 111, "item111").addRow(3, 211, "item211").addRow(1, 311, "item311").build();

    // Create NLJ Condition
    final FieldReference leftField = FieldReference.getWithQuotedRef("id_left");
    final FieldReference rightField = FieldReference.getWithQuotedRef("id_right");
    final List<LogicalExpression> args = new ArrayList<>(2);
    args.add(leftField);
    args.add(rightField);
    final LogicalExpression nljExpr = new FunctionCall("equal", args, ExpressionPosition.UNKNOWN);

    // Get the NestedLoopJoinPOPConfig
    final NestedLoopJoinPOP nljPopConfig = new NestedLoopJoinPOP(null, null, JoinRelType.INNER, nljExpr);

    // Get the left container with dummy data for NLJ
    final List<VectorContainer> leftContainer = new ArrayList<>(2);
    leftContainer.add(leftRowSet.container());

    // Get the left IterOutcomes for NLJ
    final List<RecordBatch.IterOutcome> leftOutcomes = new ArrayList<>(2);
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext, leftContainer, leftOutcomes);

    // Get the right container with dummy data
    final List<VectorContainer> rightContainer = new ArrayList<>(5);
    rightContainer.add(rightRowSet.container());
    rightContainer.add(rightRowSet2.container());

    final List<RecordBatch.IterOutcome> rightOutcomes = new ArrayList<>(5);
    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    rightOutcomes.add(RecordBatch.IterOutcome.OK);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext, rightContainer, rightOutcomes);

    final NestedLoopJoinBatch nljBatch = new NestedLoopJoinBatch(nljPopConfig, fixture.getFragmentContext(), leftMockBatch, rightMockBatch);

    final ExpandableHyperContainer results = new ExpandableHyperContainer();
    try {
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == nljBatch.next());
      results.addBatch(nljBatch);

      while (!isTerminal(nljBatch.next())) {
        results.addBatch(nljBatch);
        //VectorUtil.showVectorAccessibleContent(nljBatch);
      }

      // TODO: We can add check for output correctness as well
      assert (((MockRecordBatch) leftMockBatch).isCompleted());
      assertTrue(((MockRecordBatch) rightMockBatch).isCompleted());
      fail();

    } catch (AssertionError error){
      // expected since currently NestedLoopJoin doesn't handle NOT_YET correctly
    } finally {
      // Close all the resources for this test case
      nljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();

      results.clear();
      leftRowSet.clear();
      rightRowSet.clear();
      rightRowSet2.clear();
    }
  }

  private boolean isTerminal(RecordBatch.IterOutcome outcome) {
    return (outcome == RecordBatch.IterOutcome.NONE || outcome == RecordBatch.IterOutcome.STOP) || (outcome == RecordBatch.IterOutcome.OUT_OF_MEMORY);
  }

}