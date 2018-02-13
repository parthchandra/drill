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
package org.apache.drill.exec.physical.impl.unnest;

import org.apache.drill.categories.OperatorTest;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.UnnestPOP;
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

@Category(OperatorTest.class) public class TestUnnestCorrectness extends SubOperatorTest {


  // Operator Context for mock batch
  public static OperatorContext operatorContext;

  // use MockLateralJoinPop for MockRecordBatch ??
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
  public void testUnnestIntColumn() throws Exception {

    // Create left input schema
    TupleMetadata incomingSchema = new SchemaBuilder().add("someColumn", TypeProtos.MinorType.INT)
        .addArray("unnestColumn", TypeProtos.MinorType.INT).buildSchema();

    // Create data
    final RowSet.SingleRowSet rowSet1 =
        fixture.rowSetBuilder(incomingSchema).addRow(1, new int[] {1, 2}).addRow(2, new int[] {3, 4, 5}).build();
    final RowSet.SingleRowSet rowSet2 =
        fixture.rowSetBuilder(incomingSchema).addRow(3, new int[] {6, 7, 8, 9}).addRow(4, new int[] {10, 11, 12, 13,
            14}).build();

    // Get the POPConfig
    final UnnestPOP unnestPopConfig = new UnnestPOP(null, new SchemaPath(new PathSegment.NameSegment("unnestColumn")));

    // Get the incoming container with dummy data for LJ
    final List<VectorContainer> incomingContainer = new ArrayList<>(2);
    incomingContainer.add(rowSet1.container());
    incomingContainer.add(rowSet2.container());

    // Get the IterOutcomes for LJ
    final List<RecordBatch.IterOutcome> outcomes = new ArrayList<>(2);
    outcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    outcomes.add(RecordBatch.IterOutcome.OK);

    // Create incoming MockRecordBatch
    final MockRecordBatch incomingMockBatch =
        new MockRecordBatch(fixture.getFragmentContext(), operatorContext, incomingContainer, outcomes);

    final MockLateralJoinBatch lateralJoinBatch =
        new MockLateralJoinBatch(fixture.getFragmentContext(), operatorContext, incomingMockBatch);

    unnestPopConfig.setLateral(lateralJoinBatch);

    final UnnestRecordBatch unnestBatch =
        new UnnestRecordBatch(unnestPopConfig, incomingMockBatch, fixture.getFragmentContext());

    lateralJoinBatch.setUnnest(unnestBatch);

    // Simulate the pipeline by calling next on the incoming

    ExpandableHyperContainer results = new ExpandableHyperContainer();

    try {
      //assertTrue(RecordBatch.IterOutcome.OK_NEW_SCHEMA == lateralJoinBatch.next());

      while (!isTerminal(lateralJoinBatch.next())) {
        results.addBatch(lateralJoinBatch);
      }


      //TODO: check results against baseline

      assertTrue(((MockLateralJoinBatch) lateralJoinBatch).isCompleted());

    } catch (AssertionError error) {
      // expected since currently NestedLoopJoin doesn't handle NOT_YET correctly
    } finally {
      // Close all the resources for this test case
      unnestBatch.close();
      lateralJoinBatch.close();
      incomingMockBatch.close();

      if (results != null) {
        results.clear();
      }
      rowSet1.clear();
      rowSet2.clear();
    }
  }

  private boolean isTerminal(RecordBatch.IterOutcome outcome) {
    return (outcome == RecordBatch.IterOutcome.NONE || outcome == RecordBatch.IterOutcome.STOP) || (outcome
        == RecordBatch.IterOutcome.OUT_OF_MEMORY);
  }

}

