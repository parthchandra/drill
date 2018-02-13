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
import org.apache.drill.exec.record.ExpandableHyperContainer;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.store.mock.MockStorePOP;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
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

  @BeforeClass public static void setUpBeforeClass() throws Exception {
    mockPopConfig = new MockStorePOP(null);
    operatorContext = fixture.newOperatorContext(mockPopConfig);
  }

  @AfterClass public static void tearDownAfterClass() throws Exception {
    operatorContext.close();
  }

  @Test
  public void testUnnestFixedWidthColumn() throws Exception {

    Object[][] data = {
        { (Object) new int[] {1, 2}, (Object) new int[] {3, 4, 5}},
        { (Object) new int[] {6, 7, 8, 9}, (Object) new int[] {10, 11, 12, 13}}
    };

    // Create input schema
    TupleMetadata incomingSchema = new SchemaBuilder().add("someColumn", TypeProtos.MinorType.INT)
        .addArray("unnestColumn", TypeProtos.MinorType.INT).buildSchema();

    // First batch in baseline is an empty batch corresponding to OK_NEW_SCHEMA
    Integer[][] baseline = {{}, {1, 2}, {3, 4, 5}, {6, 7, 8, 9}, {10, 11, 12, 13, 14}};

    RecordBatch.IterOutcome[] iterOutcomes = {RecordBatch.IterOutcome.OK_NEW_SCHEMA, RecordBatch.IterOutcome.OK};

    testUnnest(incomingSchema, iterOutcomes, data, baseline);

  }

  @Test
  public void testUnnestVarWidth() throws Exception {

    Object[][] data = {
        { (Object) new String[] {"", "zero"}, (Object) new String[] {"one", "two", "three"}},
        { (Object) new String[] {"four", "five", "six", "seven"}, (Object) new String[] {"eight", "nine", "ten",
            "eleven", "twelve"}}
    };

    // Create input schema
    TupleMetadata incomingSchema = new SchemaBuilder().add("someColumn", TypeProtos.MinorType.INT)
        .addArray("unnestColumn", TypeProtos.MinorType.VARCHAR).buildSchema();

    // First batch in baseline is an empty batch corresponding to OK_NEW_SCHEMA
    String[][] baseline = {{}, {null, ""}, {"one", "two", "three"}, {"four", "five", "six", "seven"}, {"eight", "nine",
        "ten", "eleven", "twelve"}};

    RecordBatch.IterOutcome[] iterOutcomes = {RecordBatch.IterOutcome.OK_NEW_SCHEMA, RecordBatch.IterOutcome.OK};

    testUnnest(incomingSchema, iterOutcomes, data, baseline);

  }

  private <T> void testUnnest( TupleMetadata incomingSchema, RecordBatch.IterOutcome[] iterOutcomes, T[][] data,
      T[][] baseline) throws
      Exception {

    // Get the incoming container with dummy data for LJ
    final List<VectorContainer> incomingContainer = new ArrayList<>(data.length);

    // Create data
    ArrayList<RowSet.SingleRowSet> rowSets = new ArrayList<>();

    for ( Object[] recordBatch : data) {
      RowSetBuilder rowSetBuilder = fixture.rowSetBuilder(incomingSchema);
      int rowNumber = 0;
      for ( Object rowData : recordBatch) {
        rowSetBuilder.addRow(++rowNumber, rowData);
      }
      RowSet.SingleRowSet rowSet = rowSetBuilder.build();
      rowSets.add(rowSet);
      incomingContainer.add(rowSet.container());
    }

    // Get the unnest POPConfig
    final UnnestPOP unnestPopConfig = new UnnestPOP(null, new SchemaPath(new PathSegment.NameSegment("unnestColumn")));

    // Get the IterOutcomes for LJ
    final List<RecordBatch.IterOutcome> outcomes = new ArrayList<>(iterOutcomes.length);
    for(RecordBatch.IterOutcome o : iterOutcomes) {
      outcomes.add(o);
    }

    // Create incoming MockRecordBatch
    final MockRecordBatch incomingMockBatch =
        new MockRecordBatch(fixture.getFragmentContext(), operatorContext, incomingContainer, outcomes,
            incomingContainer.get(0).getSchema());

    final MockLateralJoinBatch lateralJoinBatch =
        new MockLateralJoinBatch(fixture.getFragmentContext(), operatorContext, incomingMockBatch);

    // set pointer to Lateral in unnest pop config
    unnestPopConfig.setLateral(lateralJoinBatch);

    // setup Unnest record batch
    final UnnestRecordBatch unnestBatch =
        new UnnestRecordBatch(unnestPopConfig, incomingMockBatch, fixture.getFragmentContext());

    // set backpointer to lateral join in unnest
    lateralJoinBatch.setUnnest(unnestBatch);

    // Simulate the pipeline by calling next on the incoming
    ExpandableHyperContainer results = null;

    try {

      while (!isTerminal(lateralJoinBatch.next())) {
        // nothing to do
      }

      // Check results against baseline
      results = lateralJoinBatch.getResults();

      int i = 0;
      for (VectorWrapper vw : results) {
        for (ValueVector vv : vw.getValueVectors()) {
          int valueCount = vv.getAccessor().getValueCount();
          if (valueCount != baseline[i].length) {
            fail("Test failed in validating unnest (RepeatedInt) output. Value count mismatch.");
          }
          for (int j = 0; j < valueCount; j++) {
            if (baseline[i][j] != vv.getAccessor().getObject(j)) {
              fail("Test failed in validating unnest (RepeatedInt) output. Value mismatch");
            }
          }
          i++;
        }
      }

      assertTrue(((MockLateralJoinBatch) lateralJoinBatch).isCompleted());

    } catch (AssertionError error) {
    } finally {
      // Close all the resources for this test case
      unnestBatch.close();
      lateralJoinBatch.close();
      incomingMockBatch.close();

      if (results != null) {
        results.clear();
      }
      for(RowSet.SingleRowSet rowSet: rowSets) {
        rowSet.clear();
      }
    }

  }

  private boolean isTerminal(RecordBatch.IterOutcome outcome) {
    return (outcome == RecordBatch.IterOutcome.NONE || outcome == RecordBatch.IterOutcome.STOP) || (outcome
        == RecordBatch.IterOutcome.OUT_OF_MEMORY);
  }

}

