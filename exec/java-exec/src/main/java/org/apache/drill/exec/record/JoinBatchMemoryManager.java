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
package org.apache.drill.exec.record;

public class JoinBatchMemoryManager extends RecordBatchMemoryManager {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinBatchMemoryManager.class);

  private int leftRowWidth;

  private int rightRowWidth;

  private RecordBatch leftIncoming;

  private RecordBatch rightIncoming;

  private static final int numInputs = 2;

  public static final int LEFT_INDEX = 0;

  public static final int RIGHT_INDEX = 1;

  public JoinBatchMemoryManager(int outputBatchSize, RecordBatch leftBatch, RecordBatch rightBatch) {
    super(numInputs, outputBatchSize);
    this.leftIncoming = leftBatch;
    this.rightIncoming = rightBatch;
  }

  @Override
  public void update(int inputIndex) {
    switch (inputIndex) {
      case LEFT_INDEX:
        setRecordBatchSizer(inputIndex, new RecordBatchSizer(leftIncoming));
        leftRowWidth = getRecordBatchSizer(inputIndex).netRowWidth();
        logger.debug("left incoming batch size : {}", getRecordBatchSizer(inputIndex));
        break;
      case RIGHT_INDEX:
        setRecordBatchSizer(inputIndex, new RecordBatchSizer(rightIncoming));
        rightRowWidth = getRecordBatchSizer(inputIndex).netRowWidth();
        logger.debug("right incoming batch size : {}", getRecordBatchSizer(inputIndex));
      default:
        break;
    }

    updateIncomingStats(inputIndex);
    final int newOutgoingRowWidth = leftRowWidth + rightRowWidth;

    // If outgoing row width is 0, just return. This is possible for empty batches or
    // when first set of batches come with OK_NEW_SCHEMA and no data.
    if (newOutgoingRowWidth == 0) {
      return;
    }

    final int configOutputBatchSize = getOutputBatchSize();

    // update the value to be used for next batch(es)
    setOutputRowCount(configOutputBatchSize, newOutgoingRowWidth);

    // set the new row width
    setOutgoingRowWidth(newOutgoingRowWidth);
  }

  @Override
  public RecordBatchSizer.ColumnSize getColumnSize(String name) {
    RecordBatchSizer leftSizer = getRecordBatchSizer(LEFT_INDEX);
    RecordBatchSizer rightSizer = getRecordBatchSizer(RIGHT_INDEX);

    if (leftSizer != null && leftSizer.getColumn(name) != null) {
      return leftSizer.getColumn(name);
    }
    return rightSizer == null ? null : rightSizer.getColumn(name);
  }

}
