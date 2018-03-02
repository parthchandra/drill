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
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.store.mock.MockStorePOP;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
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

  // Left Batch Schema
  private static TupleMetadata leftSchema;

  // Right Batch Schema
  private static TupleMetadata rightSchema;

  // Empty left RowSet
  private static RowSet.SingleRowSet emptyLeftRowSet;

  // Non-Empty left RowSet
  private static RowSet.SingleRowSet nonEmptyLeftRowSet;

  // List of left incoming containers
  private static final List<VectorContainer> leftContainer = new ArrayList<>(5);

  // List of left IterOutcomes
  private static final List<RecordBatch.IterOutcome> leftOutcomes = new ArrayList<>(5);

  // Empty right RowSet
  private static RowSet.SingleRowSet emptyRightRowSet;

  // Non-Empty right RowSet
  private static RowSet.SingleRowSet nonEmptyRightRowSet;

  // List of right incoming containers
  private static final List<VectorContainer> rightContainer = new ArrayList<>(5);

  // List of right IterOutcomes
  private static final List<RecordBatch.IterOutcome> rightOutcomes = new ArrayList<>(5);

  // Lateral Join POP Config
  private static LateralJoinPOP ljPopConfig;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    PhysicalOperator mockPopConfig = new MockStorePOP(null);
    operatorContext = fixture.newOperatorContext(mockPopConfig);

    leftSchema = new SchemaBuilder()
      .add("id_left", TypeProtos.MinorType.INT)
      .add("cost_left", TypeProtos.MinorType.INT)
      .add("name_left", TypeProtos.MinorType.VARCHAR)
      .buildSchema();
    emptyLeftRowSet = fixture.rowSetBuilder(leftSchema).build();

    rightSchema = new SchemaBuilder()
      .add("id_right", TypeProtos.MinorType.INT)
      .add("cost_right", TypeProtos.MinorType.INT)
      .add("name_right", TypeProtos.MinorType.VARCHAR)
      .buildSchema();
    emptyRightRowSet = fixture.rowSetBuilder(rightSchema).build();

    ljPopConfig = new LateralJoinPOP(null, null, JoinRelType.FULL);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    operatorContext.close();
    emptyLeftRowSet.clear();
    emptyRightRowSet.clear();
  }


  @Before
  public void beforeTest() throws Exception {
    nonEmptyLeftRowSet = fixture.rowSetBuilder(leftSchema)
      .addRow(1, 10, "item1")
      .build();

    nonEmptyRightRowSet = fixture.rowSetBuilder(rightSchema)
      .addRow(1, 11, "item11")
      .addRow(2, 21, "item21")
      .addRow(3, 31, "item31")
      .build();
  }

  @After
  public void afterTest() throws Exception {
    nonEmptyLeftRowSet.clear();
    nonEmptyRightRowSet.clear();
    leftContainer.clear();
    leftOutcomes.clear();
    rightContainer.clear();
    rightOutcomes.clear();
  }

  /**
   * Helper method to check if input outcome is one of terminal state or not
   *
   * @param outcome - input IterOutcome state to check for
   * @return
   */
  private boolean isTerminal(RecordBatch.IterOutcome outcome) {
    return (outcome == RecordBatch.IterOutcome.NONE || outcome == RecordBatch.IterOutcome.STOP)
      || (outcome == RecordBatch.IterOutcome.OUT_OF_MEMORY);
  }

  /**
   * With both left and right batch being empty, the {@link LateralJoinBatch#buildSchema()} phase
   * should still build the output container schema and return empty batch
   *
   * @throws Exception
   */
  @Test
  public void testBuildSchemaEmptyLRBatch() throws Exception {

    // Get the left container with dummy data for Lateral Join
    leftContainer.add(emptyLeftRowSet.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, emptyLeftRowSet.container().getSchema());

    // Get the right container with dummy data
    rightContainer.add(emptyRightRowSet.container());

    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, emptyRightRowSet.container().getSchema());

    final LateralJoinBatch ljBatch = new LateralJoinBatch(ljPopConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    try {
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());
      assertTrue(ljBatch.getRecordCount() == 0);

      while (!isTerminal(ljBatch.next())) {
        // do nothing
      }

      // TODO: We can add check for output correctness as well
      //assertTrue (((MockRecordBatch) leftMockBatch).isCompleted());
      //assertTrue(((MockRecordBatch) rightMockBatch).isCompleted());
    } catch (AssertionError | Exception error) {
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
    }
  }

  /**
   * With both left and right batch being non-empty, the {@link LateralJoinBatch#buildSchema()} phase
   * will fail with {@link IllegalStateException} since it expects first batch from right branch of LATERAL
   * to be always empty
   *
   * @throws Exception
   */
  @Test
  public void testBuildSchemaNonEmptyLRBatch() throws Exception {

    // Get the left container with dummy data
    leftContainer.add(nonEmptyLeftRowSet.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    rightContainer.add(nonEmptyRightRowSet.container());
    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, rightContainer.get(0).getSchema());

    final LateralJoinBatch ljBatch = new LateralJoinBatch(ljPopConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    try {
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());
      fail();
    } catch (AssertionError | Exception error) {
      // Expected since first right batch is supposed to be empty
      assertTrue(error instanceof IllegalStateException);
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
    }
  }

  /**
   * With non-empty left and empty right batch, the {@link LateralJoinBatch#buildSchema()} phase
   * should still build the output container schema and return empty batch
   *
   * @throws Exception
   */
  @Test
  public void testBuildSchemaNonEmptyLEmptyRBatch() throws Exception {

    // Get the left container with dummy data for Lateral Join
    leftContainer.add(nonEmptyLeftRowSet.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    rightContainer.add(emptyRightRowSet.container());
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

    } catch (AssertionError | Exception error) {
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
    }
  }

  /**
   * This case should never happen since With empty left there cannot be non-empty right batch, the
   * {@link LateralJoinBatch#buildSchema()} phase should fail() with {@link IllegalStateException}
   *
   * @throws Exception
   */
  @Test
  public void testBuildSchemaEmptyLNonEmptyRBatch() throws Exception {

    // Get the left container with dummy data for Lateral Join
    leftContainer.add(emptyLeftRowSet.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    rightContainer.add(nonEmptyRightRowSet.container());
    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, rightContainer.get(0).getSchema());

    final LateralJoinBatch ljBatch = new LateralJoinBatch(ljPopConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    try {
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());
      fail();

    } catch (AssertionError | Exception error) {
      // Expected since first right batch is supposed to be empty
      assertTrue(error instanceof IllegalStateException);
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
    }
  }

  /**
   * Test for receiving unexpected EMIT outcome during build phase on left side.
   * @throws Exception
   */
  @Test
  public void testBuildSchemaWithEMITOutcome() throws Exception {
    // Get the left container with dummy data for Lateral Join
    leftContainer.add(emptyLeftRowSet.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.EMIT);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    rightContainer.add(nonEmptyRightRowSet.container());
    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, rightContainer.get(0).getSchema());

    final LateralJoinBatch ljBatch = new LateralJoinBatch(ljPopConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    try {
      ljBatch.next();
      fail();
    } catch (AssertionError | Exception error) {
      // Expected since first right batch is supposed to be empty
      assertTrue(error instanceof IllegalStateException);
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
    }
  }

  /**
   * Test to show correct IterOutcome produced by LATERAL when one record in left batch produces only 1 right batch
   * with EMIT outcome. Then output of LATERAL should be produced by OK outcome after the join. It verifies the number
   * of records in the output batch based on left and right input batches.
   *
   * @throws Exception
   */
  @Test
  public void test1RecordLeftBatchTo1RightRecordBatch() throws Exception {
    // Get the left container with dummy data for Lateral Join
    leftContainer.add(nonEmptyLeftRowSet.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet.container());

    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, rightContainer.get(0).getSchema());

    final LateralJoinBatch ljBatch = new LateralJoinBatch(ljPopConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    try {
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());
      assertTrue(RecordBatch.IterOutcome.OK == ljBatch.next());
      assertTrue(ljBatch.getRecordCount() == (nonEmptyLeftRowSet.rowCount() * nonEmptyRightRowSet.rowCount()));
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error) {
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
    }
  }

  /**
   * Test to show correct IterOutcome & output produced by LATERAL when one record in left batch produces 2 right
   * batches with OK and EMIT outcome respectively. Then output of LATERAL should be produced with OK outcome after the
   * join is done using both right batch with the left batch. It verifies the number of records in the output batch
   * based on left and right input batches.
   *
   * @throws Exception
   */
  @Test
  public void test1RecordLeftBatchTo2RightRecordBatch() throws Exception {
    // Get the left container with dummy data for Lateral Join
    leftContainer.add(nonEmptyLeftRowSet.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    final RowSet.SingleRowSet nonEmptyRightRowSet2 = fixture.rowSetBuilder(rightSchema)
      .addRow(4, 41, "item41")
      .addRow(5, 51, "item51")
      .build();

    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet2.container());

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
        (nonEmptyLeftRowSet.rowCount() * (nonEmptyRightRowSet.rowCount() + nonEmptyRightRowSet2.rowCount())));
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error) {
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      nonEmptyRightRowSet2.clear();
    }
  }

  /**
   * Test to show that if the result of cross join between left batch and empty right batch is an empty
   * batch and next batch on left side result in NONE outcome, then there is no output produced and result
   * returned to downstream is NONE.
   *
   * @throws Exception
   */
  @Test
  public void test1RecordLeftBatchToEmptyRightBatch() throws Exception {

    // Get the left container with dummy data for Lateral Join
    leftContainer.add(nonEmptyLeftRowSet.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(emptyRightRowSet.container());

    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, rightContainer.get(0).getSchema());

    final LateralJoinBatch ljBatch = new LateralJoinBatch(ljPopConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    try {
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error) {
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
    }
  }

  /**
   * Test to show LATERAL tries to pack the output batch until it's full or all the data is consumed from left and
   * right side. We have multiple left and right batches which fits after join inside the same output batch, hence
   * LATERAL only generates one output batch.
   *
   * @throws Exception
   */
  @Test
  public void testFillingUpOutputBatch() throws Exception {
    // Create data for left input
    final RowSet.SingleRowSet leftRowSet2 = fixture.rowSetBuilder(leftSchema)
      .addRow(2, 20, "item20")
      .build();

    // Create data for right input
    final RowSet.SingleRowSet nonEmptyRightRowSet2 = fixture.rowSetBuilder(rightSchema)
      .addRow(4, 41, "item41")
      .addRow(5, 51, "item51")
      .build();

    // Get the left container with dummy data for Lateral Join
    leftContainer.add(nonEmptyLeftRowSet.container());
    leftContainer.add(leftRowSet2.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    leftOutcomes.add(RecordBatch.IterOutcome.OK);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet2.container());

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
        (nonEmptyLeftRowSet.rowCount() * nonEmptyRightRowSet.rowCount() +
          leftRowSet2.rowCount() * nonEmptyRightRowSet2.rowCount()));
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error) {
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      leftRowSet2.clear();
      nonEmptyRightRowSet2.clear();
    }
  }

  /**
   * When multiple left batches are received with different schema, then LATERAL produces output for each schema type
   * separately (even though output batch is not filled completely) and handles the schema change in left batch.
   * Moreover in this case the schema change was only for columns which are not produced by the UNNEST or right branch.
   *
   * @throws Exception
   */
  @Test
  public void testHandlingSchemaChangeForNonUnnestField() throws Exception {

    // Create left input schema 2
    TupleMetadata leftSchema2 = new SchemaBuilder()
      .add("id_left", TypeProtos.MinorType.INT)
      .add("cost_left", TypeProtos.MinorType.VARCHAR)
      .add("name_left", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    final RowSet.SingleRowSet leftRowSet2 = fixture.rowSetBuilder(leftSchema2)
      .addRow(2, "20", "item20")
      .build();

    // Create data for right input
    final RowSet.SingleRowSet nonEmptyRightRowSet2 = fixture.rowSetBuilder(rightSchema)
      .addRow(4, 41, "item41")
      .addRow(5, 51, "item51")
      .build();

    // Get the left container with dummy data for Lateral Join
    leftContainer.add(nonEmptyLeftRowSet.container());
    leftContainer.add(leftRowSet2.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet2.container());

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
        (nonEmptyLeftRowSet.rowCount() * nonEmptyRightRowSet.rowCount() +
          leftRowSet2.rowCount() * nonEmptyRightRowSet2.rowCount()));
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error) {
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      leftRowSet2.clear();
      nonEmptyRightRowSet2.clear();
    }
  }

  /**
   * When multiple left and right batches are received with different schema, then LATERAL produces output for each
   * schema type separately (even though output batch is not filled completely) and handles the schema change correctly.
   * Moreover in this case the schema change was for both left and right branch (produced by UNNEST with empty batch).
   *
   * @throws Exception
   */
  @Test
  public void testHandlingSchemaChangeForUnnestField() throws Exception {
    // Create left input schema 2
    TupleMetadata leftSchema2 = new SchemaBuilder()
      .add("id_left", TypeProtos.MinorType.INT)
      .add("cost_left", TypeProtos.MinorType.VARCHAR)
      .add("name_left", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    // Create right input schema
    TupleMetadata rightSchema2 = new SchemaBuilder()
      .add("id_right", TypeProtos.MinorType.INT)
      .add("cost_right", TypeProtos.MinorType.VARCHAR)
      .add("name_right", TypeProtos.MinorType.VARCHAR)
      .buildSchema();


    // Create data for left input
    final RowSet.SingleRowSet leftRowSet2 = fixture.rowSetBuilder(leftSchema2)
      .addRow(2, "20", "item20")
      .build();

    // Create data for right input
    final RowSet.SingleRowSet emptyRightRowSet2 = fixture.rowSetBuilder(rightSchema2)
      .build();

    final RowSet.SingleRowSet nonEmptyRightRowSet2 = fixture.rowSetBuilder(rightSchema2)
      .addRow(4, "41", "item41")
      .addRow(5, "51", "item51")
      .build();

    // Get the left container with dummy data for Lateral Join
    leftContainer.add(nonEmptyLeftRowSet.container());
    leftContainer.add(leftRowSet2.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    // first OK_NEW_SCHEMA batch
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet.container());
    // second OK_NEW_SCHEMA batch. Right side batch for OK_New_Schema is always empty
    rightContainer.add(emptyRightRowSet2.container());
    rightContainer.add(nonEmptyRightRowSet2.container());

    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);
    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
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
      assertTrue(RecordBatch.IterOutcome.OK == ljBatch.next());
      totalRecordCount += ljBatch.getRecordCount();
      assertTrue(totalRecordCount ==
        (nonEmptyLeftRowSet.rowCount() * nonEmptyRightRowSet.rowCount() +
          leftRowSet2.rowCount() * nonEmptyRightRowSet2.rowCount()));
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error) {
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      leftRowSet2.clear();
      emptyRightRowSet2.clear();
      nonEmptyRightRowSet2.clear();
    }
  }

  /**
   * Verify if there is no schema change on left side and LATERAL still sees an unexpected schema change on right side
   * then it handles it correctly.
   * handle it corr
   * @throws Exception
   */
  @Test
  public void testHandlingUnexpectedSchemaChangeForUnnestField() throws Exception {
    // Create left input schema 2
    TupleMetadata leftSchema2 = new SchemaBuilder()
      .add("id_left", TypeProtos.MinorType.INT)
      .add("cost_left", TypeProtos.MinorType.VARCHAR)
      .add("name_left", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    // Create right input schema
    TupleMetadata rightSchema2 = new SchemaBuilder()
      .add("id_right", TypeProtos.MinorType.INT)
      .add("cost_right", TypeProtos.MinorType.VARCHAR)
      .add("name_right", TypeProtos.MinorType.VARCHAR)
      .buildSchema();


    // Create data for left input
    final RowSet.SingleRowSet leftRowSet2 = fixture.rowSetBuilder(leftSchema2)
      .addRow(2, "20", "item20")
      .build();

    // Create data for right input
    final RowSet.SingleRowSet emptyRightRowSet2 = fixture.rowSetBuilder(rightSchema2)
      .build();

    final RowSet.SingleRowSet nonEmptyRightRowSet2 = fixture.rowSetBuilder(rightSchema2)
      .addRow(4, "41", "item41")
      .addRow(5, "51", "item51")
      .build();

    // Get the left container with dummy data for Lateral Join
    leftContainer.add(nonEmptyLeftRowSet.container());
    leftContainer.add(leftRowSet2.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    leftOutcomes.add(RecordBatch.IterOutcome.OK);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    // first OK_NEW_SCHEMA batch
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet.container());
    // second OK_NEW_SCHEMA batch. Right side batch for OK_New_Schema is always empty
    rightContainer.add(emptyRightRowSet2.container());
    rightContainer.add(nonEmptyRightRowSet2.container());

    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    rightOutcomes.add(RecordBatch.IterOutcome.OK);
    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, rightContainer.get(0).getSchema());

    final LateralJoinBatch ljBatch = new LateralJoinBatch(ljPopConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    try {
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());
      ljBatch.next();
      fail();
    } catch (AssertionError | Exception error) {
      // Expected since first right batch is supposed to be empty
      assertTrue(error instanceof IllegalStateException);
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      leftRowSet2.clear();
      emptyRightRowSet2.clear();
      nonEmptyRightRowSet2.clear();
    }
  }

  /**
   * When multiple left batch is received with same schema but with OK_NEW_SCHEMA, then LATERAL detects that
   * correctly and suppresses schema change operation by producing output in same batch created with initial schema.
   * The schema change was only for columns which are not produced by the UNNEST or right branch.
   *
   * @throws Exception
   */
  @Test
  public void testOK_NEW_SCHEMA_WithNoActualSchemaChange_ForNonUnnestField() throws Exception {

    final RowSet.SingleRowSet leftRowSet2 = fixture.rowSetBuilder(leftSchema)
      .addRow(2, 20, "item20")
      .build();

    // Create data for right input
    final RowSet.SingleRowSet nonEmptyRightRowSet2 = fixture.rowSetBuilder(rightSchema)
      .addRow(4, 41, "item41")
      .addRow(5, 51, "item51")
      .build();

    // Get the left container with dummy data for Lateral Join
    leftContainer.add(nonEmptyLeftRowSet.container());
    leftContainer.add(leftRowSet2.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet2.container());

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
        (nonEmptyLeftRowSet.rowCount() * nonEmptyRightRowSet.rowCount() +
          leftRowSet2.rowCount() * nonEmptyRightRowSet2.rowCount()));
      // This means only 1 output record batch was received.
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error) {
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      leftRowSet2.clear();
      nonEmptyRightRowSet2.clear();
    }
  }

  /**
   * When multiple left batch is received with same schema but with OK_NEW_SCHEMA, then LATERAL detects that
   * correctly and suppresses false schema change indication from both left and right branch. It produces output in
   * same batch created with initial schema.
   * The schema change is for columns common on both left and right side.
   *
   * @throws Exception
   */
  @Test
  public void testOK_NEW_SCHEMA_WithNoActualSchemaChange_ForUnnestField() throws Exception {
    // Create data for left input
    final RowSet.SingleRowSet leftRowSet2 = fixture.rowSetBuilder(leftSchema)
      .addRow(2, 20, "item20")
      .build();

    // Create data for right input
    final RowSet.SingleRowSet nonEmptyRightRowSet2 = fixture.rowSetBuilder(rightSchema)
      .addRow(4, 41, "item41")
      .addRow(5, 51, "item51")
      .build();

    // Get the left container with dummy data for Lateral Join
    leftContainer.add(nonEmptyLeftRowSet.container());
    leftContainer.add(leftRowSet2.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet.container());
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet2.container());

    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);
    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
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
        (nonEmptyLeftRowSet.rowCount() * nonEmptyRightRowSet.rowCount() +
          leftRowSet2.rowCount() * nonEmptyRightRowSet2.rowCount()));
      // This means only 1 output record batch was received.
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error) {
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      leftRowSet2.clear();
      nonEmptyRightRowSet2.clear();
    }
  }


  /**
   * When multiple left batch is received with same schema but with OK_NEW_SCHEMA, then LATERAL detects that
   * correctly and suppresses schema change operation by producing output in same batch created with initial schema.
   * The schema change was only for columns which are not produced by the UNNEST or right branch.
   *
   * @throws Exception
   */
  @Test
  public void testHandlingEMITFromLeft() throws Exception {

    final RowSet.SingleRowSet leftRowSet2 = fixture.rowSetBuilder(leftSchema)
      .addRow(3, 30, "item30")
      .build();

    // Create data for right input
    final RowSet.SingleRowSet nonEmptyRightRowSet2 = fixture.rowSetBuilder(rightSchema)
      .addRow(4, 41, "item41")
      .addRow(5, 51, "item51")
      .build();

    // Get the left container with dummy data for Lateral Join
    leftContainer.add(emptyLeftRowSet.container());
    leftContainer.add(nonEmptyLeftRowSet.container());
    leftContainer.add(leftRowSet2.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    leftOutcomes.add(RecordBatch.IterOutcome.EMIT);
    leftOutcomes.add(RecordBatch.IterOutcome.EMIT);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet2.container());

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
        (nonEmptyLeftRowSet.rowCount() * nonEmptyRightRowSet.rowCount() +
          leftRowSet2.rowCount() * nonEmptyRightRowSet2.rowCount()));
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error) {
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      leftRowSet2.clear();
      nonEmptyRightRowSet2.clear();
    }
  }

  /**
   * Test for the case where LATERAL received a left batch with OK outcome and then populate the Join output in the
   * outgoing batch. There is still some space left in output batch so LATERAL call's next() on left side and receive
   * NONE outcome from left side. Then in this case LATERAL should produce the previous output batch with OK outcome
   * and then handle NONE outcome in future next() call.
   *
   * @throws Exception
   */
  @Test
  public void testHandlingNoneAfterOK() throws Exception {

    // Get the left container with dummy data for Lateral Join
    leftContainer.add(nonEmptyLeftRowSet.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet.container());

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
        (nonEmptyLeftRowSet.rowCount() * nonEmptyRightRowSet.rowCount()));
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error) {
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
    }
  }

  /**
   * Test for the case when LATERAL received a left batch with OK outcome and then populate the Join output in the
   * outgoing batch. There is still some space left in output batch so LATERAL call's next() on left side and receive
   * EMIT outcome from left side with empty batch. Then in this case LATERAL should produce the previous output batch
   * with EMIT outcome.
   *
   * @throws Exception
   */
  @Test
  public void testHandlingEmptyEMITAfterOK() throws Exception {

    // Get the left container with dummy data for Lateral Join
    leftContainer.add(nonEmptyLeftRowSet.container());
    leftContainer.add(emptyLeftRowSet.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    leftOutcomes.add(RecordBatch.IterOutcome.EMIT);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet.container());

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
        (nonEmptyLeftRowSet.rowCount() * nonEmptyRightRowSet.rowCount()));
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error) {
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
    }
  }

  /**
   * Test for the case when LATERAL received a left batch with OK outcome and then populate the Join output in the
   * outgoing batch. There is still some space left in output batch so LATERAL call's next() on left side and receive
   * EMIT outcome from left side with non-empty batch. Then in this case LATERAL should produce the output batch with
   * EMIT outcome if second left and right batches are also consumed entirely in same output batch.
   *
   * @throws Exception
   */
  @Test
  public void testHandlingNonEmptyEMITAfterOK() throws Exception {

    // Create data for left input
    final RowSet.SingleRowSet leftRowSet2 = fixture.rowSetBuilder(leftSchema)
      .addRow(2, 20, "item20")
      .build();

    // Create data for right input
    final RowSet.SingleRowSet nonEmptyRightRowSet2 = fixture.rowSetBuilder(rightSchema)
      .addRow(4, 41, "item41")
      .addRow(5, 51, "item51")
      .build();

    // Get the left container with dummy data for Lateral Join
    leftContainer.add(nonEmptyLeftRowSet.container());
    leftContainer.add(leftRowSet2.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    leftOutcomes.add(RecordBatch.IterOutcome.EMIT);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet2.container());

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
        (nonEmptyLeftRowSet.rowCount() * nonEmptyRightRowSet.rowCount()) +
          (leftRowSet2.rowCount() * nonEmptyRightRowSet2.rowCount()));
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error){
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      leftRowSet2.clear();
      nonEmptyRightRowSet2.clear();
    }
  }

  /**
   * Temporary test to validate LATERAL handling output batch getting filled without consuming full output from left
   * and right batch join.
   * <p>
   * For this test we are updating {@link LateralJoinBatch#MAX_BATCH_SIZE} by making it public, which might not expected
   * after including the BatchSizing logic
   * TODO: Update the test after incorporating the BatchSizing change.
   *
   * @throws Exception
   */
  @Test
  public void testHandlingNonEmpty_EMITAfterOK_WithMultipleOutput() throws Exception {

    // Create data for left input
    final RowSet.SingleRowSet leftRowSet2 = fixture.rowSetBuilder(leftSchema)
      .addRow(2, 20, "item20")
      .build();

    // Create data for right input
    final RowSet.SingleRowSet nonEmptyRightRowSet2 = fixture.rowSetBuilder(rightSchema)
      .addRow(4, 41, "item41")
      .addRow(5, 51, "item51")
      .build();

    // Get the left container with dummy data for Lateral Join
    leftContainer.add(nonEmptyLeftRowSet.container());
    leftContainer.add(leftRowSet2.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    leftOutcomes.add(RecordBatch.IterOutcome.EMIT);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet2.container());

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
        (nonEmptyLeftRowSet.rowCount() * nonEmptyRightRowSet.rowCount()) +
          (leftRowSet2.rowCount() * nonEmptyRightRowSet2.rowCount()));
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error) {
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      leftRowSet2.clear();
      nonEmptyRightRowSet2.clear();
      LateralJoinBatch.MAX_BATCH_SIZE = originalMaxBatchSize;
    }
  }

  @Test
  public void testHandlingOOMFromLeft() throws Exception {
    // Get the left container with dummy data for Lateral Join
    leftContainer.add(nonEmptyLeftRowSet.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    leftOutcomes.add(RecordBatch.IterOutcome.OUT_OF_MEMORY);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet.container());

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
        (nonEmptyLeftRowSet.rowCount() * nonEmptyRightRowSet.rowCount()));

      // TODO: We are not draining left or right batch anymore on receiving terminal outcome from either branch
      // TODO: since not sure if that's the right behavior
      //assertTrue(((MockRecordBatch) leftMockBatch).isCompleted());
      //assertTrue(((MockRecordBatch) rightMockBatch).isCompleted());

    } catch (AssertionError | Exception error) {
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
    }
  }

  @Test
  public void testHandlingOOMFromRight() throws Exception {
    // Get the left container with dummy data for Lateral Join
    leftContainer.add(nonEmptyLeftRowSet.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    leftOutcomes.add(RecordBatch.IterOutcome.OK);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet.container());

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

      // TODO: We are not draining left or right batch anymore on receiving terminal outcome from either branch
      // TODO: since not sure if that's the right behavior
      //assertTrue(((MockRecordBatch) leftMockBatch).isCompleted());
      //assertTrue(((MockRecordBatch) rightMockBatch).isCompleted());
    } catch (AssertionError | Exception error) {
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
    }
  }

  /**
   * Test to check basic left lateral join is working correctly or not. We create a left batch with one and
   * corresponding right batch with zero rows and check if output still get's populated with left side of data or not.
   * Expectation is since it's a left join and even though right batch is empty the left row will be pushed to output
   * batch.
   *
   * @throws Exception
   */
  @Test
  public void testBasicLeftLateralJoin() throws Exception {
    // Get the left container with dummy data for Lateral Join
    leftContainer.add(nonEmptyLeftRowSet.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(emptyRightRowSet.container());

    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, rightContainer.get(0).getSchema());

    final LateralJoinPOP popConfig = new LateralJoinPOP(null, null, JoinRelType.LEFT);

    final LateralJoinBatch ljBatch = new LateralJoinBatch(popConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    try {
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());
      assertTrue(RecordBatch.IterOutcome.OK == ljBatch.next());
      assertTrue(ljBatch.getRecordCount() == nonEmptyLeftRowSet.container().getRecordCount());
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error) {
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
    }
  }

  /**
   * Test to see if there are multiple rows in left batch and for some rows right side produces batch with records
   * and for other rows right side produces empty batches then based on left join type we are populating the output
   * batch correctly. Expectation is that for left rows if we find corresponding right rows then we will output both
   * using cross-join but for left rows for which there is empty right side we will produce only left row in output
   * batch. In this case all the output will be produces only in 1 record batch.
   *
   * @throws Exception
   */
  @Test
  public void testLeftLateralJoin_WithAndWithoutMatching() throws Exception {
    // Get the left container with dummy data for Lateral Join
    final RowSet.SingleRowSet leftRowSet2 = fixture.rowSetBuilder(leftSchema)
      .addRow(1, 10, "item10")
      .addRow(2, 20, "item20")
      .addRow(3, 30, "item30")
      .build();

    final RowSet.SingleRowSet nonEmptyRightRowSet2 = fixture.rowSetBuilder(rightSchema)
      .addRow(6, 60, "item61")
      .addRow(7, 70, "item71")
      .addRow(8, 80, "item81")
      .build();

    leftContainer.add(leftRowSet2.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet.container());
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet2.container());

    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, rightContainer.get(0).getSchema());

    final LateralJoinPOP popConfig = new LateralJoinPOP(null, null, JoinRelType.LEFT);

    final LateralJoinBatch ljBatch = new LateralJoinBatch(popConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    try {
      final int expectedOutputRecordCount = 7; // 3 for first left row and 1 for second left row
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());
      assertTrue(RecordBatch.IterOutcome.OK == ljBatch.next());
      assertTrue(ljBatch.getRecordCount() == expectedOutputRecordCount);
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error) {
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
    }
  }

  /**
   * Test to see if there are multiple rows in left batch and for some rows right side produces batch with records
   * and for other rows right side produces empty batches then based on left join type we are populating the output
   * batch correctly. Expectation is that for left rows if we find corresponding right rows then we will output both
   * using cross-join but for left rows for which there is empty right side we will produce only left row in output
   * batch. But in this test we have made the Batch size very small so that output will be returned in multiple
   * output batches. This test verifies even in this case indexes are manipulated correctly and outputs are produced
   * correctly.
   * TODO: Update the test case based on Batch Sizing project since then the variable might not be available.
   *
   * @throws Exception
   */
  @Test
  public void testLeftLateralJoin_WithAndWithoutMatching_MultipleBatch() throws Exception {
    // Get the left container with dummy data for Lateral Join
    final RowSet.SingleRowSet leftRowSet2 = fixture.rowSetBuilder(leftSchema)
      .addRow(1, 10, "item10")
      .addRow(2, 20, "item20")
      .addRow(3, 30, "item30")
      .build();

    final RowSet.SingleRowSet nonEmptyRightRowSet2 = fixture.rowSetBuilder(rightSchema)
      .addRow(6, 60, "item61")
      .addRow(7, 70, "item71")
      .addRow(8, 80, "item81")
      .build();

    leftContainer.add(leftRowSet2.container());

    // Get the left IterOutcomes for Lateral Join
    leftOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);

    // Create Left MockRecordBatch
    final CloseableRecordBatch leftMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      leftContainer, leftOutcomes, leftContainer.get(0).getSchema());

    // Get the right container with dummy data
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet.container());
    rightContainer.add(emptyRightRowSet.container());
    rightContainer.add(nonEmptyRightRowSet2.container());

    rightOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);
    rightOutcomes.add(RecordBatch.IterOutcome.EMIT);

    final CloseableRecordBatch rightMockBatch = new MockRecordBatch(fixture.getFragmentContext(), operatorContext,
      rightContainer, rightOutcomes, rightContainer.get(0).getSchema());

    final LateralJoinPOP popConfig = new LateralJoinPOP(null, null, JoinRelType.LEFT);

    final LateralJoinBatch ljBatch = new LateralJoinBatch(popConfig, fixture.getFragmentContext(),
      leftMockBatch, rightMockBatch);

    int originalMaxBatchSize = LateralJoinBatch.MAX_BATCH_SIZE;
    LateralJoinBatch.MAX_BATCH_SIZE = 2;

    try {
      final int expectedOutputRecordCount = 7; // 3 for first left row and 1 for second left row
      int actualOutputRecordCount = 0;
      assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == ljBatch.next());
      assertTrue(RecordBatch.IterOutcome.OK == ljBatch.next());
      actualOutputRecordCount += ljBatch.getRecordCount();
      assertTrue(RecordBatch.IterOutcome.OK == ljBatch.next());
      actualOutputRecordCount += ljBatch.getRecordCount();
      assertTrue(RecordBatch.IterOutcome.OK == ljBatch.next());
      actualOutputRecordCount += ljBatch.getRecordCount();
      assertTrue(RecordBatch.IterOutcome.OK == ljBatch.next());
      actualOutputRecordCount += ljBatch.getRecordCount();
      assertTrue(actualOutputRecordCount == expectedOutputRecordCount);
      assertTrue(RecordBatch.IterOutcome.NONE == ljBatch.next());
    } catch (AssertionError | Exception error) {
      fail();
    } finally {
      // Close all the resources for this test case
      ljBatch.close();
      leftMockBatch.close();
      rightMockBatch.close();
      LateralJoinBatch.MAX_BATCH_SIZE = originalMaxBatchSize;
    }
  }
}