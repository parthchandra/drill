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
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.LateralJoinPOP;
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
public class TestLateralJoinCorrectness extends SubOperatorTest {
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

  /**
   * With both left and right batch being empty, the {@link LateralJoinBatch#buildSchema()} phase
   * should still build the output container schema and return empty batch
   * @throws Exception
   */
  @Test
  public void testBuildSchemaEmptyLRBatch() throws Exception {
    // Create left input schema
    TupleMetadata leftSchema = new SchemaBuilder()
      .add("id_left", TypeProtos.MinorType.INT)
      .add("cost_left", TypeProtos.MinorType.INT)
      .add("name_left", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    // Create right input schema
    TupleMetadata rightSchema = new SchemaBuilder()
      .add("id_right", TypeProtos.MinorType.INT)
      .add("cost_right", TypeProtos.MinorType.INT)
      .add("name_right", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    // Create data for left input
    final RowSet.SingleRowSet leftRowSet = fixture.rowSetBuilder(leftSchema).build();

    // Create data for right input
    final RowSet.SingleRowSet rightRowSet = fixture.rowSetBuilder(rightSchema).build();

    final LateralJoinPOP ljPopConfig = new LateralJoinPOP(null, null, JoinRelType.FULL);

    // Get the left container with dummy data for NLJ
    final List<VectorContainer> leftContainer = new ArrayList<>(2);
    leftContainer.add(leftRowSet.container());

    // Get the left IterOutcomes for NLJ
    final List<RecordBatch.IterOutcome> leftOutcomes = new ArrayList<>(2);
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftRowSet.container().getSchema());

    // Get the right container with dummy data
    final List<VectorContainer> rightContainer = new ArrayList<>(5);
    rightContainer.add(rightRowSet.container());

    final List<RecordBatch.IterOutcome> rightOutcomes = new ArrayList<>(5);
    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, rightRowSet.container().getSchema());

    final LateralJoinBatch ljBatch = new LateralJoinBatch(ljPopConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    final ExpandableHyperContainer results = new ExpandableHyperContainer();

    try {
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());
      assertTrue(ljBatch.getRecordCount() == 0);
      results.addBatch(ljBatch);

      while (!isTerminal(ljBatch.next())) {
        results.addBatch(ljBatch);
      }

      // TODO: We can add check for output correctness as well
      assertTrue (((MockRecordBatch) leftMockBatch).isCompleted());
      assertTrue(((MockRecordBatch) rightMockBatch).isCompleted());
    } catch (AssertionError | Exception error){
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();

      results.clear();
      leftRowSet.clear();
      rightRowSet.clear();
    }
  }

  /**
   * With both left and right batch being non-empty, the {@link LateralJoinBatch#buildSchema()} phase
   * will fail with {@link IllegalStateException} since it expects first batch from right branch of LATERAL
   * to be always empty
   * @throws Exception
   */
  @Test
  public void testBuildSchemaNonEmptyLRBatch() throws Exception {
    // Create left input schema
    TupleMetadata leftSchema = new SchemaBuilder()
      .add("id_left", TypeProtos.MinorType.INT)
      .add("cost_left", TypeProtos.MinorType.INT)
      .add("name_left", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    // Create right input schema
    TupleMetadata rightSchema = new SchemaBuilder()
      .add("id_right", TypeProtos.MinorType.INT)
      .add("cost_right", TypeProtos.MinorType.INT)
      .add("name_right", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    // Create data for left input
    final RowSet.SingleRowSet leftRowSet = fixture.rowSetBuilder(leftSchema)
      .addRow(1, 10, "item1")
      .addRow(2, 20, "item2")
      .addRow(3, 30, "item3")
      .build();

    // Create data for right input
    final RowSet.SingleRowSet rightRowSet = fixture.rowSetBuilder(rightSchema)
      .addRow(1, 11, "item11")
      .addRow(2, 21, "item21")
      .addRow(3, 31, "item31")
      .build();

    final LateralJoinPOP ljPopConfig = new LateralJoinPOP(null, null, JoinRelType.FULL);

    // Get the left container with dummy data for NLJ
    final List<VectorContainer> leftContainer = new ArrayList<>(2);
    leftContainer.add(leftRowSet.container());

    // Get the left IterOutcomes for NLJ
    final List<RecordBatch.IterOutcome> leftOutcomes = new ArrayList<>(2);
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    final List<VectorContainer> rightContainer = new ArrayList<>(5);
    rightContainer.add(rightRowSet.container());

    final List<RecordBatch.IterOutcome> rightOutcomes = new ArrayList<>(5);
    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, rightContainer.get(0).getSchema());

    final LateralJoinBatch ljBatch = new LateralJoinBatch(ljPopConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    try {
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());
      fail();
    } catch (AssertionError | Exception error){
      // Expected since first right batch is supposed to be empty
      assertTrue(error instanceof IllegalStateException);
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      leftRowSet.clear();
      rightRowSet.clear();
    }
  }

  /**
   * With non-empty left and empty right batch, the {@link LateralJoinBatch#buildSchema()} phase
   * should still build the output container schema and return empty batch
   * @throws Exception
   */
  @Test
  public void testBuildSchemaNonEmptyLEmptyRBatch() throws Exception {
    // Create left input schema
    TupleMetadata leftSchema = new SchemaBuilder()
      .add("id_left", TypeProtos.MinorType.INT)
      .add("cost_left", TypeProtos.MinorType.INT)
      .add("name_left", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    // Create right input schema
    TupleMetadata rightSchema = new SchemaBuilder()
      .add("id_right", TypeProtos.MinorType.INT)
      .add("cost_right", TypeProtos.MinorType.INT)
      .add("name_right", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    // Create data for left input
    final RowSet.SingleRowSet leftRowSet = fixture.rowSetBuilder(leftSchema)
      .addRow(1, 10, "item1")
      .addRow(2, 20, "item2")
      .addRow(3, 30, "item3")
      .build();

    // Create data for right input
    final RowSet.SingleRowSet rightRowSet = fixture.rowSetBuilder(rightSchema)
      .build();

    final LateralJoinPOP ljPopConfig = new LateralJoinPOP(null, null, JoinRelType.FULL);

    // Get the left container with dummy data for NLJ
    final List<VectorContainer> leftContainer = new ArrayList<>(2);
    leftContainer.add(leftRowSet.container());

    // Get the left IterOutcomes for NLJ
    final List<RecordBatch.IterOutcome> leftOutcomes = new ArrayList<>(2);
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    final List<VectorContainer> rightContainer = new ArrayList<>(5);
    rightContainer.add(rightRowSet.container());

    final List<RecordBatch.IterOutcome> rightOutcomes = new ArrayList<>(5);
    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, rightContainer.get(0).getSchema());

    final LateralJoinBatch ljBatch = new LateralJoinBatch(ljPopConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    try {
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());
      assertTrue(ljBatch.getRecordCount() == 0);

      // Since Right batch is empty it should drain left and return NONE
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());

    } catch (AssertionError | Exception error){
      // Expected since first right batch is supposed to be empty
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      leftRowSet.clear();
      rightRowSet.clear();
    }
  }

  /**
   * This case should never happen since With empty left there cannot be non-empty right batch, the
   * {@link LateralJoinBatch#buildSchema()} phase should fail() with {@link IllegalStateException}
   * @throws Exception
   */
  @Test
  public void testBuildSchemaEmptyLNonEmptyRBatch() throws Exception {
    // Create left input schema
    TupleMetadata leftSchema = new SchemaBuilder()
      .add("id_left", TypeProtos.MinorType.INT)
      .add("cost_left", TypeProtos.MinorType.INT)
      .add("name_left", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    // Create right input schema
    TupleMetadata rightSchema = new SchemaBuilder()
      .add("id_right", TypeProtos.MinorType.INT)
      .add("cost_right", TypeProtos.MinorType.INT)
      .add("name_right", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    // Create data for left input
    final RowSet.SingleRowSet leftRowSet = fixture.rowSetBuilder(leftSchema)
      .build();

    // Create data for right input
    final RowSet.SingleRowSet rightRowSet = fixture.rowSetBuilder(rightSchema)
      .addRow(1, 11, "item11")
      .addRow(2, 21, "item21")
      .addRow(3, 31, "item31")
      .build();

    final LateralJoinPOP ljPopConfig = new LateralJoinPOP(null, null, JoinRelType.FULL);

    // Get the left container with dummy data for NLJ
    final List<VectorContainer> leftContainer = new ArrayList<>(2);
    leftContainer.add(leftRowSet.container());

    // Get the left IterOutcomes for NLJ
    final List<RecordBatch.IterOutcome> leftOutcomes = new ArrayList<>(2);
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    final List<VectorContainer> rightContainer = new ArrayList<>(5);
    rightContainer.add(rightRowSet.container());

    final List<RecordBatch.IterOutcome> rightOutcomes = new ArrayList<>(5);
    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, rightContainer.get(0).getSchema());

    final LateralJoinBatch ljBatch = new LateralJoinBatch(ljPopConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    try {
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());
      fail();

    } catch (AssertionError | Exception error){
      // Expected since first right batch is supposed to be empty
      assertTrue(error instanceof IllegalStateException);
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      leftRowSet.clear();
      rightRowSet.clear();
    }
  }


  @Test
  public void testLateral1RecordLeftBatchTo1RightRecordBatch() throws Exception {
    // Create left input schema
    TupleMetadata leftSchema = new SchemaBuilder()
      .add("id_left", TypeProtos.MinorType.INT)
      .add("cost_left", TypeProtos.MinorType.INT)
      .add("name_left", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    // Create right input schema
    TupleMetadata rightSchema = new SchemaBuilder()
      .add("id_right", TypeProtos.MinorType.INT)
      .add("cost_right", TypeProtos.MinorType.INT)
      .add("name_right", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    // Create data for left input
    final RowSet.SingleRowSet leftRowSet = fixture.rowSetBuilder(leftSchema)
      .addRow(1, 10, "item10")
      .build();

    // Create data for right input
    final RowSet.SingleRowSet emptyRightRowSet = fixture.rowSetBuilder(rightSchema)
      .build();

    final RowSet.SingleRowSet nonEmptyRightRowSet = fixture.rowSetBuilder(rightSchema)
      .addRow(1, 11, "item11")
      .addRow(2, 21, "item21")
      .addRow(3, 31, "item31")
      .build();

    final LateralJoinPOP ljPopConfig = new LateralJoinPOP(null, null, JoinRelType.FULL);

    // Get the left container with dummy data for NLJ
    final List<VectorContainer> leftContainer = new ArrayList<>(2);
    leftContainer.add(leftRowSet.container());

    // Get the left IterOutcomes for NLJ
    final List<RecordBatch.IterOutcome> leftOutcomes = new ArrayList<>(2);
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    final List<VectorContainer> rightContainer = new ArrayList<>(5);
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet.container());

    final List<RecordBatch.IterOutcome> rightOutcomes = new ArrayList<>(5);
    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, rightContainer.get(0).getSchema());

    final LateralJoinBatch ljBatch = new LateralJoinBatch(ljPopConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    try {
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());
      assertTrue(RecordBatch.IterOutcome.OK == ljBatch.next());
      assertTrue(ljBatch.getRecordCount() == (leftRowSet.rowCount() * nonEmptyRightRowSet.rowCount()));
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error){
      // Expected since first right batch is supposed to be empty
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      leftRowSet.clear();
      emptyRightRowSet.clear();
      nonEmptyRightRowSet.clear();
    }
  }

  @Test
  public void testLateral1RecordLeftBatchTo2RightRecordBatch() throws Exception {
    // Create left input schema
    TupleMetadata leftSchema = new SchemaBuilder()
      .add("id_left", TypeProtos.MinorType.INT)
      .add("cost_left", TypeProtos.MinorType.INT)
      .add("name_left", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    // Create right input schema
    TupleMetadata rightSchema = new SchemaBuilder()
      .add("id_right", TypeProtos.MinorType.INT)
      .add("cost_right", TypeProtos.MinorType.INT)
      .add("name_right", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    // Create data for left input
    final RowSet.SingleRowSet leftRowSet = fixture.rowSetBuilder(leftSchema)
      .addRow(1, 10, "item10")
      .build();

    // Create data for right input
    final RowSet.SingleRowSet emptyRightRowSet = fixture.rowSetBuilder(rightSchema)
      .build();

    final RowSet.SingleRowSet nonEmptyRightRowSet1 = fixture.rowSetBuilder(rightSchema)
      .addRow(1, 11, "item11")
      .addRow(2, 21, "item21")
      .addRow(3, 31, "item31")
      .build();

    final RowSet.SingleRowSet nonEmptyRightRowSet2 = fixture.rowSetBuilder(rightSchema)
      .addRow(4, 41, "item41")
      .addRow(5, 51, "item51")
      .build();

    final LateralJoinPOP ljPopConfig = new LateralJoinPOP(null, null, JoinRelType.FULL);

    // Get the left container with dummy data for NLJ
    final List<VectorContainer> leftContainer = new ArrayList<>(2);
    leftContainer.add(leftRowSet.container());

    // Get the left IterOutcomes for NLJ
    final List<RecordBatch.IterOutcome> leftOutcomes = new ArrayList<>(2);
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    final List<VectorContainer> rightContainer = new ArrayList<>(5);
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet1.container());
    rightContainer.add(nonEmptyRightRowSet2.container());

    final List<RecordBatch.IterOutcome> rightOutcomes = new ArrayList<>(5);
    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    rightOutcomes.add(RecordBatch.IterOutcome.OK);
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, rightContainer.get(0).getSchema());

    final LateralJoinBatch ljBatch = new LateralJoinBatch(ljPopConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    try {
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());
      assertTrue(RecordBatch.IterOutcome.OK == ljBatch.next());
      assertTrue(ljBatch.getRecordCount() ==
        (leftRowSet.rowCount() * (nonEmptyRightRowSet1.rowCount() + nonEmptyRightRowSet2.rowCount())));
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error){
      // Expected since first right batch is supposed to be empty
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      leftRowSet.clear();
      emptyRightRowSet.clear();
      nonEmptyRightRowSet1.clear();
      nonEmptyRightRowSet2.clear();
    }
  }

  /**
   * Test to show that if the result of cross join between left batch and empty right batch is an empty
   * batch and next batch on left side result in NONE outcome, then there is no output produced and result
   * returned to downstream is NONE.
   * @throws Exception
   */
  @Test
  public void testLateral1RecordLeftBatchToEmptyRightBatch() throws Exception {
    // Create left input schema
    TupleMetadata leftSchema = new SchemaBuilder()
      .add("id_left", TypeProtos.MinorType.INT)
      .add("cost_left", TypeProtos.MinorType.INT)
      .add("name_left", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    // Create right input schema
    TupleMetadata rightSchema = new SchemaBuilder()
      .add("id_right", TypeProtos.MinorType.INT)
      .add("cost_right", TypeProtos.MinorType.INT)
      .add("name_right", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    // Create data for left input
    final RowSet.SingleRowSet leftRowSet = fixture.rowSetBuilder(leftSchema)
      .addRow(1, 10, "item10")
      .build();

    // Create data for right input
    final RowSet.SingleRowSet emptyRightRowSet = fixture.rowSetBuilder(rightSchema)
      .build();

    final RowSet.SingleRowSet nonEmptyRightRowSet1 = fixture.rowSetBuilder(rightSchema)
      .build();

    final LateralJoinPOP ljPopConfig = new LateralJoinPOP(null, null, JoinRelType.FULL);

    // Get the left container with dummy data for NLJ
    final List<VectorContainer> leftContainer = new ArrayList<>(2);
    leftContainer.add(leftRowSet.container());

    // Get the left IterOutcomes for NLJ
    final List<RecordBatch.IterOutcome> leftOutcomes = new ArrayList<>(2);
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    final List<VectorContainer> rightContainer = new ArrayList<>(5);
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet1.container());

    final List<RecordBatch.IterOutcome> rightOutcomes = new ArrayList<>(5);
    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, rightContainer.get(0).getSchema());

    final LateralJoinBatch ljBatch = new LateralJoinBatch(ljPopConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    try {
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error){
      // Expected since first right batch is supposed to be empty
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      leftRowSet.clear();
      emptyRightRowSet.clear();
      nonEmptyRightRowSet1.clear();
    }
  }



  private boolean isTerminal(RecordBatch.IterOutcome outcome) {
    return (outcome == RecordBatch.IterOutcome.NONE || outcome == RecordBatch.IterOutcome.STOP) || (outcome == RecordBatch.IterOutcome.OUT_OF_MEMORY);
  }

}