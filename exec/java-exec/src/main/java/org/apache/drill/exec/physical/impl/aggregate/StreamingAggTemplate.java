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
package org.apache.drill.exec.physical.impl.aggregate;

import javax.inject.Named;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.VectorWrapper;

import static org.apache.drill.exec.record.RecordBatch.IterOutcome.EMIT;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.NONE;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.OK;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.OK_NEW_SCHEMA;

public abstract class StreamingAggTemplate implements StreamingAggregator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StreamingAggregator.class);
  private static final boolean EXTRA_DEBUG = false;
  private static final int OUTPUT_BATCH_SIZE = 32*1024;

  // lastOutcome is set if the lastOutcome was NONE or STOP. It could also be set if there was an
  // empty record batch with OK_NEW_SCHEMA
  private IterOutcome lastOutcome = null;

  // First batch after build schema phase
  private boolean first = true;
  private boolean firstBatchForSchema = true; // true if the current batch came in with an OK_NEW_SCHEMA.
  private boolean firstBatchForDataSet = true; // true if the current batch is the first batch in a data set

  private boolean newSchema = false;

  // End of all data
  private boolean done = false;

  private int previousIndex = -1;
  private int underlyingIndex = 0;
  private int currentIndex;
  /**
   * Number of records added to the current aggregation group.
   */
  private long addedRecordCount = 0;
  // There are two outcomes from the aggregator. One is the aggregator's outcome defined in
  // StreamingAggregator.AggOutcome. The other is the outcome from the last call to incoming.next
  private IterOutcome outcome;
  private int outputCount = 0;
  private RecordBatch incoming;
  // the Streaming Agg Batch that this aggregator belongs to
  private StreamingAggBatch outgoing;

  private OperatorContext context;


  @Override
  public void setup(OperatorContext context, RecordBatch incoming, StreamingAggBatch outgoing) throws SchemaChangeException {
    this.context = context;
    this.incoming = incoming;
    this.outgoing = outgoing;
    setupInterior(incoming, outgoing);
  }

  private void allocateOutgoing() {
    for (VectorWrapper<?> w : outgoing) {
      w.getValueVector().allocateNew();
    }
  }

  @Override
  public IterOutcome getOutcome() {
    return outcome;
  }

  @Override
  public int getOutputCount() {
    return outputCount;
  }

  @Override
  public AggOutcome doWork(IterOutcome outerOutcome) {
    if (done || outerOutcome == NONE) {
      outcome = IterOutcome.NONE;
      return AggOutcome.CLEANUP_AND_RETURN;
    }

    try { // outside block to ensure that first is set to false after the first run.
      outputCount = 0;
      // allocate outgoing since either this is the first time or if a subsequent time we would
      // have sent the previous outgoing batch to downstream operator
      allocateOutgoing();

      if (firstBatchForDataSet) {
        this.currentIndex = incoming.getRecordCount() == 0 ? 0 : this.getVectorIndex(underlyingIndex);

        // consume empty batches until we get one with data (unless we got an EMIT). If we got an emit
        // then this is the first batch, it was empty and we also got an emit.
        if (incoming.getRecordCount() == 0 ) {
          if (outerOutcome != EMIT) {
            outer:
            while (true) {
              IterOutcome out = outgoing.next(0, incoming);
              switch (out) {
                case OK_NEW_SCHEMA:
                  lastOutcome = out;
                case OK:
                  if (incoming.getRecordCount() == 0) {
                    continue;
                  } else {
                    currentIndex = this.getVectorIndex(underlyingIndex);
                    break outer;
                  }
                case OUT_OF_MEMORY:
                  outcome = out;
                  return AggOutcome.RETURN_OUTCOME;
                case EMIT:
                  if (incoming.getRecordCount() == 0) {
                    // When we see an EMIT we let the  agg record batch know that it should either
                    // send out an EMIT or an OK_NEW_SCHEMA, followed by an EMIT. To do that we simply return
                    // RETURN_AND_RESET with the outcome so the record batch can take care of it.
                    if(lastOutcome != null) {
                      outcome = lastOutcome == IterOutcome.OK_NEW_SCHEMA ? IterOutcome.OK_NEW_SCHEMA : EMIT;
                    } else {
                      outcome = outerOutcome == IterOutcome.OK_NEW_SCHEMA ? IterOutcome.OK_NEW_SCHEMA : EMIT;
                    }
                    if(firstBatchForSchema) {
                      firstBatchForSchema = false;
                    }
                    firstBatchForDataSet = true;
                    return AggOutcome.RETURN_AND_RESET;
                  } else {
                    break outer;
                  }

                case NONE:
                  out = IterOutcome.OK_NEW_SCHEMA;
                case STOP:
                default:
                  lastOutcome = out;
                  outcome = out;
                  done = true;
                  return AggOutcome.CLEANUP_AND_RETURN;
              } // switch (outcome)
            } // while empty batches are seen
          } else {
            //lastOutcome = EMIT;
            outcome = EMIT;
            firstBatchForDataSet = true;
            return AggOutcome.RETURN_AND_RESET;
          }
        }
      }


      if (newSchema) {
        return AggOutcome.UPDATE_AGGREGATOR;
      }

      // if the previous iteration has an outcome that was terminal, don't do anything.
      if (lastOutcome != null && lastOutcome != IterOutcome.OK_NEW_SCHEMA) {
        outcome = lastOutcome;
        return AggOutcome.CLEANUP_AND_RETURN;
      }

      outside: while(true) {
        // loop through existing records, adding as necessary.
        if(!processRemainingRecordsInBatch()) {
          // output batch is full. Return.
          return setOkAndReturn(OK);
        }
        // if the current batch came with an EMIT, we're done
        if(outerOutcome == EMIT) {
          // output the last record
          outputToBatch(previousIndex);
          outcome = outerOutcome;
          resetIndex();
          return setOkAndReturn(EMIT);
        }

        /**
         * Hold onto the previous incoming batch. When the incoming uses an
         * SV4, the InternalBatch DOES NOT clone or transfer the data. Instead,
         * it clones only the SV4, and assumes that the same hyper-list of
         * batches will be offered again after the next call to the incoming
         * next(). This is, in fact, how the SV4 works, so all is fine. The
         * trick to be aware of, however, is that this MUST BE TRUE even if
         * the incoming next() returns NONE: the incoming is obligated to continue
         * to offer the set of batches. That is, the incoming cannot try to be
         * tidy and release the batches one end-of-data or the following code
         * will fail, likely with an IndexOutOfBounds exception.
         */

        InternalBatch previous = new InternalBatch(incoming, context);

        try {
          while (true) {

            IterOutcome out = outgoing.next(0, incoming);
            if (EXTRA_DEBUG) {
              logger.debug("Received IterOutcome of {}", out);
            }
            switch (out) {
              case NONE:
                done = true;
                lastOutcome = out;
                if (firstBatchForDataSet && addedRecordCount == 0) {
                  return setOkAndReturn(out);
                } else if (addedRecordCount > 0) {
                  outputToBatchPrev(previous, previousIndex, outputCount); // No need to check the return value
                  // (output container full or not) as we are not going to insert any more records.
                  if (EXTRA_DEBUG) {
                    logger.debug("Received no more batches, returning.");
                  }
                  return setOkAndReturn(out);
                } else {
                  // not first batch and record Count == 0
                  outcome = out;
                  return AggOutcome.CLEANUP_AND_RETURN;
                }
                // EMIT is handled like OK, except that we do not loop back to process the
                // next incoming batch; we return instead
              case EMIT:
                if (incoming.getRecordCount() == 0) {
                  if (addedRecordCount > 0) {
                    outputToBatchPrev(previous, previousIndex, outputCount);
                  }
                  resetIndex();
                  previousIndex = -1;
                  return setOkAndReturn(out);
                } else {
                  resetIndex();
                  if (previousIndex != -1 && isSamePrev(previousIndex, previous, currentIndex)) {
                    if (EXTRA_DEBUG) {
                      logger.debug("New value was same as last value of previous batch, adding.");
                    }
                    addRecordInc(currentIndex);
                    previousIndex = currentIndex;
                    incIndex();
                    if (EXTRA_DEBUG) {
                      logger.debug("Continuing outside");
                    }
                    processRemainingRecordsInBatch();
                    // currentIndex has been reset to int_max so use previous index.
                    outputToBatch(previousIndex);
                    resetIndex();
                    previousIndex = -1;
                    return setOkAndReturn(out);
                  } else { // not the same
                    if (EXTRA_DEBUG) {
                      logger.debug("This is not the same as the previous, add record and continue outside.");
                    }
                    if (addedRecordCount > 0) {
                      if (outputToBatchPrev(previous, previousIndex, outputCount)) {
                        if (EXTRA_DEBUG) {
                          logger.debug("Output container is full. flushing it.");
                        }
                        previousIndex = -1;
                        return setOkAndReturn(out);
                      }
                    }
                    previousIndex = -1;
                    processRemainingRecordsInBatch();
                    outputToBatch(previousIndex); // currentIndex has been reset to int_max so use previous index.
                    resetIndex();
                    previousIndex = -1;
                    return setOkAndReturn(out);
                  }
                }

              case NOT_YET:
                this.outcome = out;
                return AggOutcome.RETURN_OUTCOME;

              case OK_NEW_SCHEMA:
                firstBatchForSchema = true;
                lastOutcome = out;
                if (EXTRA_DEBUG) {
                  logger.debug("Received new schema.  Batch has {} records.", incoming.getRecordCount());
                }
                if (addedRecordCount > 0) {
                  outputToBatchPrev(previous, previousIndex, outputCount); // No need to check the return value
                  // (output container full or not) as we are not going to insert anymore records.
                  if (EXTRA_DEBUG) {
                    logger.debug("Wrote out end of previous batch, returning.");
                  }
                  newSchema = true;
                  return setOkAndReturn(out);
                }
                cleanup();
                return AggOutcome.UPDATE_AGGREGATOR;
              case OK:
                resetIndex();
                if (incoming.getRecordCount() == 0) {
                  continue;
                } else {
                  if (previousIndex != -1 && isSamePrev(previousIndex, previous, currentIndex)) {
                    if (EXTRA_DEBUG) {
                      logger.debug("New value was same as last value of previous batch, adding.");
                    }
                    addRecordInc(currentIndex);
                    previousIndex = currentIndex;
                    incIndex();
                    if (EXTRA_DEBUG) {
                      logger.debug("Continuing outside");
                    }
                    continue outside;
                  } else { // not the same
                    if (EXTRA_DEBUG) {
                      logger.debug("This is not the same as the previous, add record and continue outside.");
                    }
                    if (addedRecordCount > 0) {
                      if (outputToBatchPrev(previous, previousIndex, outputCount)) {
                        if (EXTRA_DEBUG) {
                          logger.debug("Output container is full. flushing it.");
                        }
                        previousIndex = -1;
                        return setOkAndReturn(out);
                      }
                    }
                    previousIndex = -1;
                    continue outside;
                  }
                }
              case STOP:
              default:
                lastOutcome = out;
                outcome = out;
                return AggOutcome.CLEANUP_AND_RETURN;
            }
          }
        } finally {
          // make sure to clear previous
          if (previous != null) {
            previous.clear();
          }
        }
      }
    } finally {
      if (first) {
        first = false;
      }
    }

  }

  @Override
  public boolean isDone() {
    return done;
  }

  /**
   * Process the remaining records in the batch. Returns false if not all records are processed (if the output
   * container gets full), true otherwise.
   * @return  Boolean indicating all records were processed
   */
  private boolean processRemainingRecordsInBatch() {
    for (; underlyingIndex < incoming.getRecordCount(); incIndex()) {
      if (EXTRA_DEBUG) {
        logger.debug("Doing loop with values underlying {}, current {}", underlyingIndex, currentIndex);
      }
      if (previousIndex == -1) {
        if (EXTRA_DEBUG) {
          logger.debug("Adding the initial row's keys and values.");
        }
        addRecordInc(currentIndex);
      }
      else if (isSame( previousIndex, currentIndex )) {
        if (EXTRA_DEBUG) {
          logger.debug("Values were found the same, adding.");
        }
        addRecordInc(currentIndex);
      } else {
        if (EXTRA_DEBUG) {
          logger.debug("Values were different, outputting previous batch.");
        }
        if(!outputToBatch(previousIndex)) {
          // There is still space in outgoing container, so proceed to the next input.
          if (EXTRA_DEBUG) {
            logger.debug("Output successful.");
          }
          addRecordInc(currentIndex);
        } else {
          if (EXTRA_DEBUG) {
            logger.debug("Output container has reached its capacity. Flushing it.");
          }

          // Update the indices to set the state for processing next record in incoming batch in subsequent doWork calls.
          previousIndex = -1;
          return false;
        }
      }
      previousIndex = currentIndex;
    }
    return true;
  }

  private final void incIndex() {
    underlyingIndex++;
    if (underlyingIndex >= incoming.getRecordCount()) {
      currentIndex = Integer.MAX_VALUE;
      return;
    }
    currentIndex = getVectorIndex(underlyingIndex);
  }

  private final void resetIndex() {
    underlyingIndex = -1;
    incIndex();
  }

  /**
   * Set the outcome to OK (or OK_NEW_SCHEMA) and return the AggOutcome parameter
   * @param outcome
   * @return outcome
   */
  private final AggOutcome setOkAndReturn( IterOutcome outcome) {
    IterOutcome outcomeToReturn;
    if (outcome == EMIT) {
      firstBatchForDataSet = true;
    } else {
      firstBatchForDataSet = false;
    }
    if (firstBatchForSchema) {
      outcomeToReturn = OK_NEW_SCHEMA;
      firstBatchForSchema = false;
    } else if (outcome == EMIT) {
      firstBatchForDataSet = true;
      outcomeToReturn = EMIT;
    } else {
      outcomeToReturn = OK;
    }
    this.outcome = outcomeToReturn;

    for (VectorWrapper<?> v : outgoing) {
      v.getValueVector().getMutator().setValueCount(outputCount);
    }
    return (outcome == EMIT) ? AggOutcome.RETURN_AND_RESET : AggOutcome.RETURN_OUTCOME;
  }

  // Returns output container status after insertion of the given record. Caller must check the return value if it
  // plans to insert more records into outgoing container.
  private final boolean outputToBatch(int inIndex) {
    assert outputCount < OUTPUT_BATCH_SIZE:
        "Outgoing RecordBatch is not flushed. It reached its max capacity in the last update";

    outputRecordKeys(inIndex, outputCount);

    outputRecordValues(outputCount);

    if (EXTRA_DEBUG) {
      logger.debug("{} values output successfully", outputCount);
    }
    resetValues();
    outputCount++;
    addedRecordCount = 0;

    return outputCount == OUTPUT_BATCH_SIZE;
  }

  // Returns output container status after insertion of the given record. Caller must check the return value if it
  // plans to inserts more record into outgoing container.
  private final boolean outputToBatchPrev(InternalBatch b1, int inIndex, int outIndex) {
    assert outputCount < OUTPUT_BATCH_SIZE:
        "Outgoing RecordBatch is not flushed. It reached its max capacity in the last update";

    outputRecordKeysPrev(b1, inIndex, outIndex);
    outputRecordValues(outIndex);
    resetValues();
    outputCount++;
    addedRecordCount = 0;

    return outputCount == OUTPUT_BATCH_SIZE;
  }

  private void addRecordInc(int index) {
    addRecord(index);
    addedRecordCount++;
  }

  @Override
  public void cleanup() {
  }

  public abstract void setupInterior(@Named("incoming") RecordBatch incoming, @Named("outgoing") RecordBatch outgoing) throws SchemaChangeException;
  public abstract boolean isSame(@Named("index1") int index1, @Named("index2") int index2);
  public abstract boolean isSamePrev(@Named("b1Index") int b1Index, @Named("b1") InternalBatch b1, @Named("b2Index") int b2Index);
  public abstract void addRecord(@Named("index") int index);
  public abstract void outputRecordKeys(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);
  public abstract void outputRecordKeysPrev(@Named("previous") InternalBatch previous, @Named("previousIndex") int previousIndex, @Named("outIndex") int outIndex);
  public abstract void outputRecordValues(@Named("outIndex") int outIndex);
  public abstract int getVectorIndex(@Named("recordIndex") int recordIndex);
  public abstract boolean resetValues();
}
