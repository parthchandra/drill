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
  private static OperatorContext operatorContext;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    PhysicalOperator mockPopConfig = new MockStorePOP(null);
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
      //assertTrue (((MockRecordBatch) leftMockBatch).isCompleted());
      //assertTrue(((MockRecordBatch) rightMockBatch).isCompleted());
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

  /**
   * Test to show correct IterOutcome produced by LATERAL when one record in left batch produces only 1 right batch
   * with EMIT outcome. Then output of LATERAL should be produced by OK outcome after the join. It verifies the number
   * of records in the output batch based on left and right input batches.
   * @throws Exception
   */
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

  /**
   * Test to show correct IterOutcome & output produced by LATERAL when one record in left batch produces 2 right
   * batches with OK and EMIT outcome respectively. Then output of LATERAL should be produced with OK outcome after the
   * join is done using both right batch with the left batch. It verifies the number of records in the output batch
   * based on left and right input batches.
   * @throws Exception
   */
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

  /**
   * Test to show LATERAL tries to pack the output batch until it's full or all the data is consumed from left and
   * right side. We have multiple left and right batches which fits after join inside the same output batch, hence
   * LATERAL only generates one output batch.
   * @throws Exception
   */
  @Test
  public void testLateralFillingUpOutputBatch() throws Exception {
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
    final RowSet.SingleRowSet leftRowSet1 = fixture.rowSetBuilder(leftSchema)
      .addRow(1, 10, "item10")
      .build();

    final RowSet.SingleRowSet leftRowSet2 = fixture.rowSetBuilder(leftSchema)
      .addRow(2, 20, "item20")
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
    leftContainer.add(leftRowSet1.container());
    leftContainer.add(leftRowSet2.container());

    // Get the left IterOutcomes for NLJ
    final List<RecordBatch.IterOutcome> leftOutcomes = new ArrayList<>(2);
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    leftOutcomes.add(RecordBatch.IterOutcome.OK);

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
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, rightContainer.get(0).getSchema());

    final LateralJoinBatch ljBatch = new LateralJoinBatch(ljPopConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    try {
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());
      assertTrue(RecordBatch.IterOutcome.OK == ljBatch.next());
      assertTrue(ljBatch.getRecordCount() ==
        (leftRowSet1.rowCount() * nonEmptyRightRowSet1.rowCount() +
          leftRowSet2.rowCount() * nonEmptyRightRowSet2.rowCount()));
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error){
      // Expected since first right batch is supposed to be empty
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      leftRowSet1.clear();
      leftRowSet2.clear();
      emptyRightRowSet.clear();
      nonEmptyRightRowSet1.clear();
      nonEmptyRightRowSet2.clear();
    }
  }

  /**
   * When multiple left batch is received with different schema, then LATERAL produces output for each schema type
   * separately (even though output batch is not filled completely) and handles the schema change in left batch.
   * Moreover in this case the schema change was only for columns which are not produced by the UNNEST or right branch.
   * @throws Exception
   */
  @Test
  public void testLateralHandlingSchemaChangeForNonUnnestField() throws Exception {
    // Create left input schema
    TupleMetadata leftSchema1 = new SchemaBuilder()
      .add("id_left", TypeProtos.MinorType.INT)
      .add("cost_left", TypeProtos.MinorType.INT)
      .add("name_left", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    // Create left input schema 2
    TupleMetadata leftSchema2 = new SchemaBuilder()
      .add("id_left", TypeProtos.MinorType.INT)
      .add("cost_left", TypeProtos.MinorType.VARCHAR)
      .add("name_left", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    // Create right input schema
    TupleMetadata rightSchema = new SchemaBuilder()
      .add("id_right", TypeProtos.MinorType.INT)
      .add("cost_right", TypeProtos.MinorType.INT)
      .add("name_right", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    // Create data for left input
    final RowSet.SingleRowSet leftRowSet1 = fixture.rowSetBuilder(leftSchema1)
      .addRow(1, 10, "item10")
      .build();

    final RowSet.SingleRowSet leftRowSet2 = fixture.rowSetBuilder(leftSchema2)
      .addRow(2, "20", "item20")
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
    leftContainer.add(leftRowSet1.container());
    leftContainer.add(leftRowSet2.container());

    // Get the left IterOutcomes for NLJ
    final List<RecordBatch.IterOutcome> leftOutcomes = new ArrayList<>(2);
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
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
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, rightContainer.get(0).getSchema());

    final LateralJoinBatch ljBatch = new LateralJoinBatch(ljPopConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    try {
      int totalRecordCount = 0;
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());
      assertTrue(RecordBatch.IterOutcome.OK == ljBatch.next());
      totalRecordCount += ljBatch.getRecordCount();
      // This means 2 output record batches were received because of Schema change
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());
      totalRecordCount += ljBatch.getRecordCount();
      assertTrue(totalRecordCount ==
        (leftRowSet1.rowCount() * nonEmptyRightRowSet1.rowCount() +
          leftRowSet2.rowCount() * nonEmptyRightRowSet2.rowCount()));
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error){
      // Expected since first right batch is supposed to be empty
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      leftRowSet1.clear();
      leftRowSet2.clear();
      emptyRightRowSet.clear();
      nonEmptyRightRowSet1.clear();
      nonEmptyRightRowSet2.clear();
    }
  }

  /**
   * When multiple left batch is received with same schema but with OK_NEW_SCHEMA, then LATERAL detects that
   * correctly and suppresses schema change operation by producing output in same batch created with initial schema.
   * The schema change was only for columns which are not produced by the UNNEST or right branch.
   * @throws Exception
   */
  @Test
  public void testLateralHandlingOK_NEW_SCHEMA_WithNoActualSchemaChange() throws Exception {
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
    final RowSet.SingleRowSet leftRowSet1 = fixture.rowSetBuilder(leftSchema)
      .addRow(1, 10, "item10")
      .build();

    final RowSet.SingleRowSet leftRowSet2 = fixture.rowSetBuilder(leftSchema)
      .addRow(2, 20, "item20")
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
    leftContainer.add(leftRowSet1.container());
    leftContainer.add(leftRowSet2.container());

    // Get the left IterOutcomes for NLJ
    final List<RecordBatch.IterOutcome> leftOutcomes = new ArrayList<>(2);
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
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
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, rightContainer.get(0).getSchema());

    final LateralJoinBatch ljBatch = new LateralJoinBatch(ljPopConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    try {
      int totalRecordCount = 0;
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());
      assertTrue(RecordBatch.IterOutcome.OK == ljBatch.next());
      totalRecordCount += ljBatch.getRecordCount();
      assertTrue(totalRecordCount ==
        (leftRowSet1.rowCount() * nonEmptyRightRowSet1.rowCount() +
          leftRowSet2.rowCount() * nonEmptyRightRowSet2.rowCount()));
      // This means only 1 output record batch was received.
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error){
      // Expected since first right batch is supposed to be empty
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      leftRowSet1.clear();
      leftRowSet2.clear();
      emptyRightRowSet.clear();
      nonEmptyRightRowSet1.clear();
      nonEmptyRightRowSet2.clear();
    }
  }


  /**
   * When multiple left batch is received with same schema but with OK_NEW_SCHEMA, then LATERAL detects that
   * correctly and suppresses schema change operation by producing output in same batch created with initial schema.
   * The schema change was only for columns which are not produced by the UNNEST or right branch.
   * @throws Exception
   */
  @Test
  public void testLateralHandlingEMITFromLeft() throws Exception {
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
    final RowSet.SingleRowSet emptyLeftRowSet = fixture.rowSetBuilder(leftSchema)
      .build();

    final RowSet.SingleRowSet leftRowSet1 = fixture.rowSetBuilder(leftSchema)
      .addRow(1, 10, "item10")
      .build();

    final RowSet.SingleRowSet leftRowSet2 = fixture.rowSetBuilder(leftSchema)
      .addRow(3, 30, "item30")
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
    final List<VectorContainer> leftContainer = new ArrayList<>(3);
    leftContainer.add(emptyLeftRowSet.container());
    leftContainer.add(leftRowSet1.container());
    leftContainer.add(leftRowSet2.container());

    // Get the left IterOutcomes for NLJ
    final List<RecordBatch.IterOutcome> leftOutcomes = new ArrayList<>(3);
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    leftOutcomes.add(RecordBatch.IterOutcome.EMIT);
    leftOutcomes.add(RecordBatch.IterOutcome.EMIT);

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
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, rightContainer.get(0).getSchema());

    final LateralJoinBatch ljBatch = new LateralJoinBatch(ljPopConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    try {
      int totalRecordCount = 0;
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());

      // 1st output batch is received for first EMIT from LEFT side
      assertTrue(RecordBatch.IterOutcome.EMIT == ljBatch.next());
      totalRecordCount += ljBatch.getRecordCount();

      // 2nd output batch is received for second EMIT from LEFT side
      assertTrue(RecordBatch.IterOutcome.EMIT == ljBatch.next());
      totalRecordCount += ljBatch.getRecordCount();

      // Compare the total records generated in 2 output batches with expected count.
      assertTrue(totalRecordCount ==
        (leftRowSet1.rowCount() * nonEmptyRightRowSet1.rowCount() +
          leftRowSet2.rowCount() * nonEmptyRightRowSet2.rowCount()));
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error){
      // Expected since first right batch is supposed to be empty
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      emptyLeftRowSet.clear();
      leftRowSet1.clear();
      leftRowSet2.clear();
      emptyRightRowSet.clear();
      nonEmptyRightRowSet1.clear();
      nonEmptyRightRowSet2.clear();
    }
  }

  /**
   * Test for the case where LATERAL received a left batch with OK outcome and then populate the Join output in the
   * outgoing batch. There is still some space left in output batch so LATERAL call's next() on left side and receive
   * NONE outcome from left side. Then in this case LATERAL should produce the previous output batch with OK outcome
   * and then handle NONE outcome in future next() call.
   * @throws Exception
   */
  @Test
  public void testLateralHandlingNoneAfterOK() throws Exception {
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
    final RowSet.SingleRowSet leftRowSet1 = fixture.rowSetBuilder(leftSchema)
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

    final LateralJoinPOP ljPopConfig = new LateralJoinPOP(null, null, JoinRelType.FULL);

    // Get the left container with dummy data for NLJ
    final List<VectorContainer> leftContainer = new ArrayList<>(3);
    leftContainer.add(leftRowSet1.container());

    // Get the left IterOutcomes for NLJ
    final List<RecordBatch.IterOutcome> leftOutcomes = new ArrayList<>(3);
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
      int totalRecordCount = 0;
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());

      // 1st output batch is received for first EMIT from LEFT side
      assertTrue(RecordBatch.IterOutcome.OK == ljBatch.next());
      totalRecordCount += ljBatch.getRecordCount();

      // Compare the total records generated in 2 output batches with expected count.
      assertTrue(totalRecordCount ==
        (leftRowSet1.rowCount() * nonEmptyRightRowSet1.rowCount()));
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error){
      // Expected since first right batch is supposed to be empty
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      leftRowSet1.clear();
      emptyRightRowSet.clear();
      nonEmptyRightRowSet1.clear();
    }
  }

  /**
   * Test for the case when LATERAL received a left batch with OK outcome and then populate the Join output in the
   * outgoing batch. There is still some space left in output batch so LATERAL call's next() on left side and receive
   * EMIT outcome from left side with empty batch. Then in this case LATERAL should produce the previous output batch
   * with EMIT outcome.
   * @throws Exception
   */
  @Test
  public void testLateralHandlingEmptyEMITAfterOK() throws Exception {
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
    final RowSet.SingleRowSet leftRowSet1 = fixture.rowSetBuilder(leftSchema)
      .addRow(1, 10, "item10")
      .build();

    final RowSet.SingleRowSet emptyLeftRowSet = fixture.rowSetBuilder(leftSchema)
      .build();

    // Create data for right input
    final RowSet.SingleRowSet emptyRightRowSet = fixture.rowSetBuilder(rightSchema)
      .build();

    final RowSet.SingleRowSet nonEmptyRightRowSet1 = fixture.rowSetBuilder(rightSchema)
      .addRow(1, 11, "item11")
      .addRow(2, 21, "item21")
      .addRow(3, 31, "item31")
      .build();

    final LateralJoinPOP ljPopConfig = new LateralJoinPOP(null, null, JoinRelType.FULL);

    // Get the left container with dummy data for NLJ
    final List<VectorContainer> leftContainer = new ArrayList<>(3);
    leftContainer.add(leftRowSet1.container());
    leftContainer.add(emptyLeftRowSet.container());

    // Get the left IterOutcomes for NLJ
    final List<RecordBatch.IterOutcome> leftOutcomes = new ArrayList<>(3);
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    leftOutcomes.add(RecordBatch.IterOutcome.EMIT);

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
      int totalRecordCount = 0;
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());

      // 1st output batch is received for first EMIT from LEFT side
      assertTrue(RecordBatch.IterOutcome.EMIT == ljBatch.next());
      totalRecordCount += ljBatch.getRecordCount();

      // Compare the total records generated in 2 output batches with expected count.
      assertTrue(totalRecordCount ==
        (leftRowSet1.rowCount() * nonEmptyRightRowSet1.rowCount()));
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error){
      // Expected since first right batch is supposed to be empty
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      emptyLeftRowSet.clear();
      leftRowSet1.clear();
      emptyRightRowSet.clear();
      nonEmptyRightRowSet1.clear();
    }
  }

  /**
   * Test for the case when LATERAL received a left batch with OK outcome and then populate the Join output in the
   * outgoing batch. There is still some space left in output batch so LATERAL call's next() on left side and receive
   * EMIT outcome from left side with non-empty batch. Then in this case LATERAL should produce the output batch with
   * EMIT outcome if second left and right batches are also consumed entirely in same output batch.
   * @throws Exception
   */
  @Test
  public void testLateralHandlingNonEmptyEMITAfterOK() throws Exception {
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
    final RowSet.SingleRowSet leftRowSet1 = fixture.rowSetBuilder(leftSchema)
      .addRow(1, 10, "item10")
      .build();

    final RowSet.SingleRowSet leftRowSet2 = fixture.rowSetBuilder(leftSchema)
      .addRow(2, 20, "item20")
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
    final List<VectorContainer> leftContainer = new ArrayList<>(3);
    leftContainer.add(leftRowSet1.container());
    leftContainer.add(leftRowSet2.container());

    // Get the left IterOutcomes for NLJ
    final List<RecordBatch.IterOutcome> leftOutcomes = new ArrayList<>(3);
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    leftOutcomes.add(RecordBatch.IterOutcome.EMIT);

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
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, rightContainer.get(0).getSchema());

    final LateralJoinBatch ljBatch = new LateralJoinBatch(ljPopConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    try {
      int totalRecordCount = 0;
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());

      // 1st output batch is received for first EMIT from LEFT side
      assertTrue(RecordBatch.IterOutcome.EMIT == ljBatch.next());
      totalRecordCount += ljBatch.getRecordCount();

      // Compare the total records generated in 2 output batches with expected count.
      assertTrue(totalRecordCount ==
        (leftRowSet1.rowCount() * nonEmptyRightRowSet1.rowCount()) +
          (leftRowSet2.rowCount() * nonEmptyRightRowSet2.rowCount()));
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error){
      // Expected since first right batch is supposed to be empty
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      leftRowSet1.clear();
      leftRowSet2.clear();
      emptyRightRowSet.clear();
      nonEmptyRightRowSet1.clear();
      nonEmptyRightRowSet2.clear();
    }
  }

  /**
   * Temporary test to validate LATERAL handling output batch getting filled without consuming full output from left
   * and right batch join.
   *
   * For this test we are updating {@link LateralJoinBatch#MAX_BATCH_SIZE} by making it public, which might not expected
   * after including the BatchSizing logic
   * @throws Exception
   */
  @Test
  public void testHandlingNonEmpty_EMITAfterOK_WithMultipleOutput() throws Exception {
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
    final RowSet.SingleRowSet leftRowSet1 = fixture.rowSetBuilder(leftSchema)
      .addRow(1, 10, "item10")
      .build();

    final RowSet.SingleRowSet leftRowSet2 = fixture.rowSetBuilder(leftSchema)
      .addRow(2, 20, "item20")
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
    final List<VectorContainer> leftContainer = new ArrayList<>(3);
    leftContainer.add(leftRowSet1.container());
    leftContainer.add(leftRowSet2.container());

    // Get the left IterOutcomes for NLJ
    final List<RecordBatch.IterOutcome> leftOutcomes = new ArrayList<>(3);
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    leftOutcomes.add(RecordBatch.IterOutcome.EMIT);

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
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, rightContainer.get(0).getSchema());

    final LateralJoinBatch ljBatch = new LateralJoinBatch(ljPopConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    int originalMaxBatchSize = LateralJoinBatch.MAX_BATCH_SIZE;
    LateralJoinBatch.MAX_BATCH_SIZE = 2;

    try {
      int totalRecordCount = 0;
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());

      // 1st output batch
      assertTrue(RecordBatch.IterOutcome.OK == ljBatch.next());
      totalRecordCount += ljBatch.getRecordCount();
      assertTrue(ljBatch.getRecordCount() == LateralJoinBatch.MAX_BATCH_SIZE);

      // 2nd output batch
      assertTrue(RecordBatch.IterOutcome.OK == ljBatch.next());
      assertTrue(ljBatch.getRecordCount() == LateralJoinBatch.MAX_BATCH_SIZE);
      totalRecordCount += ljBatch.getRecordCount();

      // 3rd output batch
      assertTrue(RecordBatch.IterOutcome.EMIT == ljBatch.next());
      totalRecordCount += ljBatch.getRecordCount();

      // Compare the total records generated in 2 output batches with expected count.
      assertTrue(totalRecordCount ==
        (leftRowSet1.rowCount() * nonEmptyRightRowSet1.rowCount()) +
          (leftRowSet2.rowCount() * nonEmptyRightRowSet2.rowCount()));
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error){
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      leftRowSet1.clear();
      leftRowSet2.clear();
      emptyRightRowSet.clear();
      nonEmptyRightRowSet1.clear();
      nonEmptyRightRowSet2.clear();
      LateralJoinBatch.MAX_BATCH_SIZE = originalMaxBatchSize;
    }
  }

  @Test
  public void testHandlingOOMFromLeft() throws Exception {
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
    final RowSet.SingleRowSet leftRowSet1 = fixture.rowSetBuilder(leftSchema)
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

    final LateralJoinPOP ljPopConfig = new LateralJoinPOP(null, null, JoinRelType.FULL);

    // Get the left container with dummy data for NLJ
    final List<VectorContainer> leftContainer = new ArrayList<>(3);
    leftContainer.add(leftRowSet1.container());

    // Get the left IterOutcomes for NLJ
    final List<RecordBatch.IterOutcome> leftOutcomes = new ArrayList<>(3);
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    leftOutcomes.add(RecordBatch.IterOutcome.OUT_OF_MEMORY);

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
      int totalRecordCount = 0;
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());

      // 1st output batch
      assertTrue(RecordBatch.IterOutcome.OK == ljBatch.next());
      totalRecordCount += ljBatch.getRecordCount();

      // 2nd output batch
      assertTrue(RecordBatch.IterOutcome.OUT_OF_MEMORY == ljBatch.next());

      // Compare the total records generated in 2 output batches with expected count.
      assertTrue(totalRecordCount ==
        (leftRowSet1.rowCount() * nonEmptyRightRowSet1.rowCount()));

      // TODO: We are not draining left or right batch anymore on receiving terminal outcome from either branch
      // TODO: since not sure if that's the right behavior
      //assertTrue(((MockRecordBatch) leftMockBatch).isCompleted());
      //assertTrue(((MockRecordBatch) rightMockBatch).isCompleted());

    } catch (AssertionError | Exception error){
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      leftRowSet1.clear();
      emptyRightRowSet.clear();
      nonEmptyRightRowSet1.clear();
    }
  }

  @Test
  public void testHandlingOOMFromRight() throws Exception {
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
    final RowSet.SingleRowSet leftRowSet1 = fixture.rowSetBuilder(leftSchema)
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

    final LateralJoinPOP ljPopConfig = new LateralJoinPOP(null, null, JoinRelType.FULL);

    // Get the left container with dummy data for NLJ
    final List<VectorContainer> leftContainer = new ArrayList<>(3);
    leftContainer.add(leftRowSet1.container());

    // Get the left IterOutcomes for NLJ
    final List<RecordBatch.IterOutcome> leftOutcomes = new ArrayList<>(3);
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    leftOutcomes.add(RecordBatch.IterOutcome.OK);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    final List<VectorContainer> rightContainer = new ArrayList<>(5);
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet1.container());

    final List<RecordBatch.IterOutcome> rightOutcomes = new ArrayList<>(5);
    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    rightOutcomes.add(RecordBatch.IterOutcome.OUT_OF_MEMORY);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, rightContainer.get(0).getSchema());

    final LateralJoinBatch ljBatch = new LateralJoinBatch(ljPopConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    try {
      // int totalRecordCount = 0;
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());

      // 2nd output batch
      assertTrue(RecordBatch.IterOutcome.OUT_OF_MEMORY == ljBatch.next());

      // Compare the total records generated in 2 output batches with expected count.
      //assertTrue(totalRecordCount ==
      //  (leftRowSet1.rowCount() * nonEmptyRightRowSet1.rowCount()));
      // TODO: We are not draining left or right batch anymore on receiving terminal outcome from either branch
      // TODO: since not sure if that's the right behavior
      //assertTrue(((MockRecordBatch) leftMockBatch).isCompleted());
      //assertTrue(((MockRecordBatch) rightMockBatch).isCompleted());

    } catch (AssertionError | Exception error){
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      leftRowSet1.clear();
      emptyRightRowSet.clear();
      nonEmptyRightRowSet1.clear();
    }
  }

  private boolean isTerminal(RecordBatch.IterOutcome outcome) {
    return (outcome == RecordBatch.IterOutcome.NONE || outcome == RecordBatch.IterOutcome.STOP) || (outcome == RecordBatch.IterOutcome.OUT_OF_MEMORY);
  }

}