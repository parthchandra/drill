/*
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
package org.apache.drill.exec.physical.impl.join;

import com.google.common.base.Preconditions;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;

import javax.inject.Named;

/*
 * Template class that combined with the runtime generated source implements the LateralJoin interface. This
 * class contains the main lateral join logic.
 */
public abstract class LateralJoinTemplate implements LateralJoin {

  // Current left input batch being processed
  private RecordBatch left = null;

  // Current right input batch being processed
  private RecordBatch right = null;

  // Output batch
  private LateralJoinBatch outgoing = null;

  private int outputIndex = 0;

  /**
   * Method initializes necessary state and invokes the doSetup() to set the
   * input and output value vector references.
   *
   * @param context Fragment context
   * @param left Current left input batch being processed
   * @param right Current right input batch being processed
   * @param outgoing Output batch
   */
  public void setupLateralJoin(FragmentContext context,
                               RecordBatch left, RecordBatch right,
                               LateralJoinBatch outgoing) {
    this.left = left;
    this.right = right;
    this.outgoing = outgoing;

    doSetup(context, this.right, this.left, this.outgoing);
  }

  /**
   * Main entry point for producing the output records. Thin wrapper around populateOutgoingBatch(), this method
   * controls which left batch we are processing and fetches the next left input batch once we exhaust the current one.
   *
   * @param joinType join type (INNER ot LEFT)
   * @return the number of records produced in the output batch
   */
  public int outputRecords(JoinRelType joinType, int leftIndex, int rightIndex) {

    final int numOutputRecords = populateOutputBatch(joinType, leftIndex, rightIndex, outputIndex);

    // Check if output batch was exhausted
    if (numOutputRecords >= LateralJoinBatch.MAX_BATCH_SIZE) {
      // reset the output index for next new batch
      outputIndex = 0;
      return numOutputRecords;
    }

    outputIndex = numOutputRecords;
    return numOutputRecords;
  }

  public int populateOutputBatch(JoinRelType joinRelType, int leftIndex,
                                 int rightIndex, int outputBatchIndex) {

    int finalOutputIndex = outputBatchIndex;
    final int rightRecordCount = right.getRecordCount();

    // Check if right batch is empty
    if (rightRecordCount == 0) {
      Preconditions.checkState(rightIndex == -1, "Right batch record count is 0 but index is not -1");

      // Check if the join type is Left Join in which case we will populate outgoing container with only left side
      // columns
      if (joinRelType == JoinRelType.LEFT) {
        emitLeft(leftIndex, finalOutputIndex);
        ++finalOutputIndex;

        if (finalOutputIndex >= LateralJoinBatch.MAX_BATCH_SIZE) {
          return finalOutputIndex;
        }
      }
    } else {
      Preconditions.checkState(rightIndex != -1, "Right batch record count is >0 but index is -1");
      // For every record in right side just emit left and right records in output container
      for (; rightIndex < rightRecordCount; ++rightIndex) {
        emitLeft(leftIndex, finalOutputIndex);
        emitRight(rightIndex, finalOutputIndex);
        ++finalOutputIndex;

        if (finalOutputIndex >= LateralJoinBatch.MAX_BATCH_SIZE) {
          break;
        }
      }
    }
    return finalOutputIndex;
  }

  public abstract void doSetup(@Named("context") FragmentContext context,
                               @Named("rightBatch") RecordBatch rightBatch,
                               @Named("leftBatch") RecordBatch leftBatch,
                               @Named("outgoing") RecordBatch outgoing);

  public abstract void emitRight(@Named("rightIndex") int rightIndex,
                                 @Named("outIndex") int outIndex);

  public abstract void emitLeft(@Named("leftIndex") int leftIndex,
                                @Named("outIndex") int outIndex);
}
