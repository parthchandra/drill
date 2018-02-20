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
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JVar;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.LateralContract;
import org.apache.drill.exec.physical.config.LateralJoinPOP;
import org.apache.drill.exec.record.AbstractBinaryRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorAccessibleUtilities;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.AllocationHelper;

import java.io.IOException;

import static org.apache.drill.exec.record.RecordBatch.IterOutcome.EMIT;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.NONE;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.OK;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.OK_NEW_SCHEMA;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.OUT_OF_MEMORY;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.STOP;

/*
 * RecordBatch implementation for the lateral join operator
 */
public class LateralJoinBatch extends AbstractBinaryRecordBatch<LateralJoinPOP> implements LateralContract {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LateralJoinBatch.class);

  // Maximum number records in the outgoing batch
  protected static final int MAX_BATCH_SIZE = 4096;

  // Input indexes to correctly update the stats
  protected static final int LEFT_INPUT = 0;
  protected static final int RIGHT_INPUT = 1;

  // Schema on the left side
  private BatchSchema leftSchema = null;

  // Schema on the right side
  private BatchSchema rightSchema = null;

  // Runtime generated class implementing the LateralJoin interface
  private LateralJoin lateralJoiner = null;

  // Number of output records in the current outgoing batch
  private int outputRecords = 0;

  // Current index of record in left incoming which is being processed
  private int leftJoinIndex = -1;

  // Current index of record in right incoming which is being processed
  private int rightJoinIndex = -1;

  private boolean handleLeftBatchNextTime = false;

  private boolean enableLateralCGDebugging = true;

  // Generator mapping for the right side
  private static final GeneratorMapping EMIT_RIGHT =
    GeneratorMapping.create("doSetup"/* setup method */,"emitRight" /* eval method */,
      null /* reset */,null /* cleanup */);

  // Generator mapping for the right side : constant
  private static final GeneratorMapping EMIT_RIGHT_CONSTANT =
    GeneratorMapping.create("doSetup"/* setup method */,"doSetup" /* eval method */,
      null /* reset */, null /* cleanup */);

  // Generator mapping for the left side : scalar
  private static final GeneratorMapping EMIT_LEFT =
    GeneratorMapping.create("doSetup" /* setup method */, "emitLeft" /* eval method */,
      null /* reset */, null /* cleanup */);

  // Generator mapping for the left side : constant
  private static final GeneratorMapping EMIT_LEFT_CONSTANT =
    GeneratorMapping.create("doSetup" /* setup method */,"doSetup" /* eval method */,
      null /* reset */, null /* cleanup */);

  // Mapping set for the right side
  private static final MappingSet emitRightMapping =
    new MappingSet("rightIndex" /* read index */, "outIndex" /* write index */,
      "rightBatch" /* read container */,"outgoing" /* write container */,
      EMIT_RIGHT_CONSTANT, EMIT_RIGHT);

  // Mapping set for the left side
  private static final MappingSet emitLeftMapping =
    new MappingSet("leftIndex" /* read index */, "outIndex" /* write index */,
      "leftBatch" /* read container */,"outgoing" /* write container */,
      EMIT_LEFT_CONSTANT, EMIT_LEFT);

  protected LateralJoinBatch(LateralJoinPOP popConfig, FragmentContext context,
                             RecordBatch left, RecordBatch right) throws OutOfMemoryException {
    super(popConfig, context, left, right);
    Preconditions.checkNotNull(left);
    Preconditions.checkNotNull(right);
    enableLateralCGDebugging = true;//context.getConfig().getBoolean(ExecConstants.ENABLE_CODE_DUMP_DEBUG_LATERAL);
  }

  public boolean handleSchemaChange() {
    try {
      stats.startSetup();
      setupNewSchema();
      lateralJoiner = setupWorker();
      lateralJoiner.setupLateralJoin(context, left, right, this);
      return true;
    } catch (SchemaChangeException ex) {
      context.getExecutorState().fail(ex);
      kill(false);
      return false;
    } finally {
      stats.stopSetup();
    }
  }

  public boolean isTerminalOutcome(IterOutcome outcome) {
    return (outcome == STOP || outcome == OUT_OF_MEMORY || outcome == NONE);
  }

  public IterOutcome processLeftBatch(RecordBatch leftBatch) {

    // Call next() on left side until we get a non-empty RecordBatch with OK outcome.
    // OR we get either of OK_NEW_SCHEMA/EMIT/NONE/STOP/OOM/NOT_YET outcome with any type
    // of batch.
    boolean needLeftBatch = leftJoinIndex == -1;

    // If left batch is empty
    while (needLeftBatch && !handleLeftBatchNextTime) {
      leftUpstream = next(LEFT_INPUT, leftBatch);
      final boolean emptyLeftBatch = leftBatch.getRecordCount() <=0;

      switch (leftUpstream) {
        case OK_NEW_SCHEMA:
          // This means there is already some records from previous join inside left batch
          // So we need to pass that downstream and then handle the OK_NEW_SCHEMA in subsequent next call
          if (outputRecords > 0) {
            handleLeftBatchNextTime = true;
            return OK_NEW_SCHEMA;
          }
          // This OK_NEW_SCHEMA is received post build schema phase and from left side
          if (emptyLeftBatch) {
            if (!isSchemaChanged(left.getSchema(), leftSchema)) {
              logger.warn("New schema received from left side is same as previous known left schema. Ignoring this " +
                "schema change");
              continue;
            }
            if (handleSchemaChange()) {
              container.setRecordCount(0);
              leftJoinIndex = -1;
              return OK_NEW_SCHEMA;
            } else {
              return STOP;
            }
          } // else - setup the new schema information after getting it from right side too.
        case OK:
          // With OK outcome we will keep calling next until we get a batch with >0 records
          if (emptyLeftBatch) {
            leftJoinIndex = -1;
            continue;
          } else {
            leftJoinIndex = 0;
          }
          break;
        case EMIT:
          // don't call next on right batch
          if (emptyLeftBatch) {
            leftJoinIndex = -1;
            container.setRecordCount(0);
            return EMIT;
          } else {
            leftJoinIndex = 0;
          }
          break;
        case OUT_OF_MEMORY:
        case NONE:
        case STOP:
          // Not using =0 since if outgoing container is empty then no point returning anything
          if (outputRecords > 0) {
            handleLeftBatchNextTime = true;
          }
          //TODO we got a STOP, shouldn't we stop immediately ?
          // TODO: check what killAndDrain will do w.r.t UNNEST, we discussed about calling right side
          // of LATERAL with NONE outcome or calling stop explicitly when NONE is seen on left side
          killAndDrainIncoming(right, rightUpstream, RIGHT_INPUT);
          return leftUpstream;
        case NOT_YET:
          try {
            Thread.sleep(5);
          } catch (InterruptedException ex) {
            logger.debug("Thread interrupted while sleeping to call next on left branch of LATERAL since it " +
              "received NOT_YET");
          }
          break;
      }

      needLeftBatch = leftJoinIndex == -1;
    }

    // Just return the know left outcome. We can reach here in 2 cases:
    // 1) We have processed all the records in previous batch and now getting a new batch
    // 2) We have some left over records from previous batch
    return leftUpstream;
  }

  public IterOutcome processRightBatch(RecordBatch right) {
    // Check if we still have records left to process in left incoming from new batch or previously half processed
    // batch. We are making sure to update leftJoinIndex and rightJoinIndex correctly. Like for new
    // batch leftJoinIndex will always be set to zero and once leftSide batch is fully processed then
    // it will be set to -1.
    // Whereas rightJoinIndex is to keep track of record in right batch being joined with
    // record in left batch. So when there are cases such that all records in right batch is not consumed
    // by the output, then joinIndex will be a valid index. When all records are consumed it will be set to -1.
    boolean needNewRightBatch = (leftJoinIndex >= 0) && (rightJoinIndex == -1);
    while (needNewRightBatch) {
      rightUpstream = next(RIGHT_INPUT, right);
      switch (rightUpstream) {
        case OK_NEW_SCHEMA:
          // TODO: Need to set the container schema properly but ignore the batch since it's expected to be empty
          // We should not get OK_NEW_SCHEMA multiple times for the same left incoming batch. So there won't be a
          // case where we get OK_NEW_SCHEMA --> OK (with batch) ---> OK_NEW_SCHEMA --> OK/EMIT
          // fall through
          //
          // Right batch with OK_NEW_SCHEMA is always going to be an empty batch, so let's pass the new schema
          // downstream and later with subsequent next() call the join output will be produced
          Preconditions.checkState(right.getRecordCount() == 0,
            "Right side batch with OK_NEW_SCHEMA is not empty");

          if (!isSchemaChanged(right.getSchema(), rightSchema)) {
            logger.warn("New schema received from right side is same as previous known right schema. Ignoring this " + "schema change");
            continue;
          }
          if (handleSchemaChange()) {
            container.setRecordCount(0);
            rightJoinIndex = -1;
            return OK_NEW_SCHEMA;
          } else {
            return STOP;
          }
        case OK:
        case EMIT:
          // Even if there are no records we should not call next() again because in case of LEFT join
          // empty batch is of importance too
          rightJoinIndex = (right.getRecordCount() > 0) ? 0 : -1;
          needNewRightBatch = false;
          break;
        case OUT_OF_MEMORY:
        case NONE:
        case STOP:
          //TODO we got a STOP, shouldn't we stop immediately ?
          // TODO: Should we kill left side if right side fails ?
          killAndDrainIncoming(left, leftUpstream, LEFT_INPUT);
          needNewRightBatch = false;
          break;
        case NOT_YET:
          try {
            Thread.sleep(10);
          } catch (InterruptedException ex) {
            logger.debug("Thread interrupted while sleeping to call next on left branch of LATERAL since it " +
              "received NOT_YET");
          }
          break;
      }
    }

    return rightUpstream;
  }

  public void handlePreviousLeftBatch() {
    if (leftUpstream == OK_NEW_SCHEMA) {

      if (!isSchemaChanged(left.getSchema(), leftSchema)) {
        logger.warn("New schema received from left side is same as previous known left schema. Ignoring this " +
          "schema change");
      }

      if (left.getRecordCount() > 0) {
        leftJoinIndex = 0;
      } else {
        if (handleSchemaChange()) {
          container.setRecordCount(0);
          leftJoinIndex = -1;
        } else {
          leftUpstream = STOP;
        }
      }
    }
  }

  /**
   * Method that drains all the right side input until it see's EMIT outcome and accumulates
   * the data in a hyper container. Once it has all the data from right side, it processes
   * left side one row at a time and produce output batch.
   * @return IterOutcome state of the lateral join batch
   */
  @Override
  public IterOutcome innerNext() {

    if (handleLeftBatchNextTime) {
      handlePreviousLeftBatch();
    }

    // We don't do anything special on FIRST state. Process left batch first and then right batch if need be
    IterOutcome childOutcome = processLeftBatch(left);
    handleLeftBatchNextTime = false;

    // If the left batch doesn't have any record in the incoming batch or the state returned from
    // left side is terminal state then just return the IterOutcome and don't call next() on
    // right branch
    if (left.getRecordCount() == 0 || isTerminalOutcome(childOutcome)) {
      return childOutcome;
    }

    // Left side has some records in the batch so let's process right batch
    childOutcome = processRightBatch(right);

    // reset the left & right outcomes to OK here and send the empty batch downstream
    if (childOutcome == OK_NEW_SCHEMA) {
      leftUpstream = OK;
      rightUpstream = OK;
      return childOutcome;
    }

    if (isTerminalOutcome(childOutcome)) {
      return childOutcome;
    }

    // If OK_NEW_SCHEMA is seen only on non empty left batch but not on right batch
    if (leftUpstream == OK_NEW_SCHEMA) {
      if(!handleSchemaChange()) {
        return STOP;
      }
    }

    if (state == BatchState.FIRST) {
      lateralJoiner.setupLateralJoin(context, left, right, this);
      state = BatchState.NOT_FIRST;
    }

    // allocate space for the outgoing batch
    allocateVectors();

    while (outputRecords <= LateralJoinBatch.MAX_BATCH_SIZE) {
      // invoke the runtime generated method to emit records in the output batch for each leftJoinIndex
      outputRecords = lateralJoiner.outputRecords(popConfig.getJoinType(), leftJoinIndex, rightJoinIndex);

      if (right.getRecordCount() == 0) {
        rightJoinIndex = -1;
      } else { // One right batch might span across multiple output batch. So rightIndex will be moving sum of all the
        // output records for this record batch until it's fully consumed
        rightJoinIndex += outputRecords;
      }

      boolean isLeftEmpty = leftJoinIndex >= left.getRecordCount();
      final boolean isRightEmpty = rightJoinIndex == -1 || rightJoinIndex >= right.getRecordCount();

      // Check if above join to produce output was based on empty right batch or
      // it resulted in right side batch to be fully consumed. In this scenario only if rightUpstream
      // is EMIT then increase the leftJoinIndex.
      // Otherwise it means for the given right batch there is still some record left to be processed.
      if (isRightEmpty) {
        if (rightUpstream == EMIT) {
          ++leftJoinIndex;

          // Check if previous left record was last one, then set leftJoinIndex to -1
          isLeftEmpty = leftJoinIndex >= left.getRecordCount();
          if (isLeftEmpty) {
            leftJoinIndex = -1;
            clearBatchVectors(left);
          }
        }

        // Release vectors of right batch. This will happen for both rightUpstream = EMIT/OK
        clearBatchVectors(right);
        rightJoinIndex = -1;
      }

      // Check if output batch is full
      if (outputRecords >= MAX_BATCH_SIZE) {
        break;
      } else { // output batch still has some space
        // Check if left side still has records or not
        if (!isLeftEmpty) {
          // Then just get the right side of the batch and make sure rightJoinIndex is properly set
          rightUpstream = processRightBatch(right);

          Preconditions.checkState(rightUpstream != OK_NEW_SCHEMA, "Unexpected Schema change on right side for " +
            "the same left batch");

          if (isTerminalOutcome(rightUpstream)) {
            return rightUpstream;
          }
        } else {

          // The left batch was with EMIT outcome, then return output to downstream layer
          if (leftUpstream == EMIT) {
            finalizeOutputContainer();
            return EMIT;
          }

          if (leftUpstream == OK_NEW_SCHEMA) {
            // return output batch with OK_NEW_SCHEMA and reset the state to OK
            finalizeOutputContainer();
            leftUpstream = OK;
            return OK_NEW_SCHEMA;
          }

          // Can only reach here if previous left batch came with OK outcome
          //
          // Then get both left batch and the right batch and make sure indexes are properly set
          // TODO: Problem since if we see OK_NEW_SCHEMA then we just re-create the schema in the container
          // and pass it through
          leftUpstream = processLeftBatch(left);
          if (handleLeftBatchNextTime) {
            // We should return the current output batch with OK outcome and don't reset the leftUpstream
            finalizeOutputContainer();
            return OK;
          }

          // If left batch is not there then don't call next on right batch
          if (isTerminalOutcome(leftUpstream)) {
            return leftUpstream;
          }

          // Since we are here that means leftBatch came with OK/EMIT outcome only and now we can call
          // next on right batch too. It will not hit OK_NEW_SCHEMA since left side have not seen that outcome
          rightUpstream = processRightBatch(right);
          Preconditions.checkState(rightUpstream != OK_NEW_SCHEMA, "Unexpected schema change in right branch");

          if (isTerminalOutcome(rightUpstream)) {
            return rightUpstream;
          }

        }
      }
    }

    finalizeOutputContainer();

    // TODO: Need to change this condition based on recordCount and left and right outcome
    // and also left and right join index.
    // The left batch was with EMIT outcome, then return output to downstream layer
    if (leftUpstream == EMIT) {
      return EMIT;
    }

    if (leftUpstream == OK_NEW_SCHEMA) {
      // return output batch with OK_NEW_SCHEMA and reset the state to OK
      leftUpstream = OK;
      return OK_NEW_SCHEMA;
    }

    return OK;
  }

  private void finalizeOutputContainer() {

    VectorAccessibleUtilities.setValueCount(container, outputRecords);

    // Set the record count in the container
    container.setRecordCount(outputRecords);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);

    logger.debug("Number of records emitted: " + outputRecords);

    // We are about to send the output batch so reset the outputRecords for future next call
    outputRecords = 0;
  }

  private void killAndDrainIncoming(RecordBatch batch, IterOutcome outcome,
                                    int batchIndex) {
    if (!hasMore(outcome)) {
      return;
    }

    batch.kill(true);
    while (hasMore(outcome)) {
      for (final VectorWrapper<?> wrapper : batch) {
        wrapper.getValueVector().clear();
      }
      outcome = next(batchIndex, batch);
    }
    if (batchIndex == RIGHT_INPUT) {
      rightUpstream = outcome;
    } else {
      leftUpstream = outcome;
    }
  }

  private boolean hasMore(IterOutcome outcome) {
    return outcome == OK || outcome == OK_NEW_SCHEMA;
  }

  private boolean isSchemaChanged(BatchSchema newSchema, BatchSchema oldSchema) {
    return newSchema.isEquivalent(oldSchema);
  }

  private void setupNewSchema() throws SchemaChangeException {

    // Clear up the container
    container.clear();

    leftSchema = left.getSchema();
    rightSchema = right.getSchema();

    if (leftSchema == null || rightSchema == null) {
      throw new SchemaChangeException("Either of left or right schema was not set properly in the batches. Hence " +
        "failed to setupNewSchema");
    }

    // Setup LeftSchema

    // Get the left schema and set it in container
    leftSchema = left.getSchema();
    for (final VectorWrapper<?> vectorWrapper : left) {
      container.addOrGet(vectorWrapper.getField());
    }

    // Setup RightSchema


    // Get the right schema and set it in container
    for (final VectorWrapper<?> vectorWrapper : right) {
      MaterializedField rightField = vectorWrapper.getField();
      TypeProtos.MajorType rightFieldType = vectorWrapper.getField().getType();

      // make right input schema optional if we have LEFT join
      if (popConfig.getJoinType() == JoinRelType.LEFT &&
        rightFieldType.getMode() == TypeProtos.DataMode.REQUIRED) {
        final TypeProtos.MajorType outputType =
          Types.overrideMode(rightField.getType(), TypeProtos.DataMode.OPTIONAL);

        // Create the right field with optional type. This will also take care of creating
        // children fields in case of ValueVectors of map type
        rightField = rightField.withType(outputType);
      }
      container.addOrGet(rightField);
    }

    // Let's build schema for the container
    container.setRecordCount(0);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
  }

  /**
   * Method generates the runtime code needed for LateralJoin. Other than the setup method to set the input and output
   * value vector references we implement three more methods
   * 1. doEval() -> Evaluates if record from left side matches record from the right side
   * 2. emitLeft() -> Project record from the left side
   * 3. emitRight() -> Project record from the right side (which is a hyper container)
   * @return the runtime generated class that implements the LateralJoin interface
   */
  private LateralJoin setupWorker() throws SchemaChangeException {
    final CodeGenerator<LateralJoin> lateralCG = CodeGenerator.get(
      LateralJoin.TEMPLATE_DEFINITION, context.getOptions());
    lateralCG.plainJavaCapable(true);

    // To enabled code gen dump for lateral use the setting ExecConstants.ENABLE_CODE_DUMP_DEBUG_LATERAL
    lateralCG.saveCodeForDebugging(enableLateralCGDebugging);
    final ClassGenerator<LateralJoin> nLJClassGenerator = lateralCG.getRoot();

    // generate doEval
    //final ErrorCollector collector = new ErrorCollectorImpl();

    /*
        Logical expression may contain fields from left and right batches. During code generation (materialization)
        we need to indicate from which input field should be taken.

        Non-equality joins can belong to one of below categories. For example:
        1. Join on non-equality join predicates:
        select * from t1 inner join t2 on (t1.c1 between t2.c1 AND t2.c2) AND (...)
        2. Join with an OR predicate:
        select * from t1 inner join t2 on on t1.c1 = t2.c1 OR t1.c2 = t2.c2
     */
    /*
    Map<VectorAccessible, BatchReference> batches = ImmutableMap
        .<VectorAccessible, BatchReference>builder()
        .put(left, new BatchReference("leftBatch", "leftIndex"))
        .put(right, new BatchReference("rightBatch", "rightIndex"))
        .build();

    LogicalExpression materialize = ExpressionTreeMaterializer.materialize(
        popConfig.getCondition(),
        batches,
        collector,
        context.getFunctionRegistry(),
        false,
        false);

    if (collector.hasErrors()) {
      throw new SchemaChangeException(String.format("Failure while trying to materialize join condition. Errors:\n %s.",
          collector.toErrorString()));
    }

    nLJClassGenerator.addExpr(new ReturnValueExpression(materialize), ClassGenerator.BlkCreateMode.FALSE);
    */

    // generate emitLeft
    nLJClassGenerator.setMappingSet(emitLeftMapping);
    JExpression outIndex = JExpr.direct("outIndex");
    JExpression leftIndex = JExpr.direct("leftIndex");

    int fieldId = 0;
    int outputFieldId = 0;
    if (leftSchema != null) {
      // Set the input and output value vector references corresponding to the left batch
      for (MaterializedField field : leftSchema) {
        final TypeProtos.MajorType fieldType = field.getType();

        // Add the vector to the output container
        container.addOrGet(field);

        JVar inVV = nLJClassGenerator.declareVectorValueSetupAndMember("leftBatch",
            new TypedFieldId(fieldType, false, fieldId));
        JVar outVV = nLJClassGenerator.declareVectorValueSetupAndMember("outgoing",
            new TypedFieldId(fieldType, false, outputFieldId));

        nLJClassGenerator.getEvalBlock().add(outVV.invoke("copyFromSafe").arg(leftIndex).arg(outIndex).arg(inVV));
        nLJClassGenerator.rotateBlock();
        fieldId++;
        outputFieldId++;
      }
    }

    // generate emitRight
    fieldId = 0;
    nLJClassGenerator.setMappingSet(emitRightMapping);
    JExpression rightIndex = JExpr.direct("rightIndex");

    if (rightSchema != null) {
      // Set the input and output value vector references corresponding to the right batch
      for (MaterializedField field : rightSchema) {

        final TypeProtos.MajorType inputType = field.getType();
        TypeProtos.MajorType outputType;
        // if join type is LEFT, make sure right batch output fields data mode is optional
        if (popConfig.getJoinType() == JoinRelType.LEFT && inputType.getMode() == TypeProtos.DataMode.REQUIRED) {
          outputType = Types.overrideMode(inputType, TypeProtos.DataMode.OPTIONAL);
        } else {
          outputType = inputType;
        }

        MaterializedField newField = MaterializedField.create(field.getName(), outputType);
        container.addOrGet(newField);

        JVar inVV = nLJClassGenerator.declareVectorValueSetupAndMember("rightBatch",
            new TypedFieldId(inputType, false, fieldId));
        JVar outVV = nLJClassGenerator.declareVectorValueSetupAndMember("outgoing",
            new TypedFieldId(outputType, false, outputFieldId));
        nLJClassGenerator.getEvalBlock().add(outVV.invoke("copyFromSafe")
            .arg(rightIndex)
            .arg(outIndex)
            .arg(inVV));
        nLJClassGenerator.rotateBlock();
        fieldId++;
        outputFieldId++;
      }
    }

    try {
      return context.getImplementationClass(lateralCG);
    } catch (IOException | ClassTransformationException ex) {
      throw new SchemaChangeException("Failed while setting up generated class with new schema information", ex);
    }
  }

  /**
   * Simple method to allocate space for all the vectors in the container.
   */
  private void allocateVectors() {
    for (final VectorWrapper<?> vw : container) {
      AllocationHelper.allocateNew(vw.getValueVector(), MAX_BATCH_SIZE);
    }
  }

  /**
   * Builds the output container's schema. Goes over the left and the right
   * batch and adds the corresponding vectors to the output container.
   * @throws SchemaChangeException if batch schema was changed during execution
   *
   * TODO: We should evaluate that the columnToUnnest should still be considered while
   * building the left schema or not, since there is the overhead of copying that column.
   * Or we should leave it upto project to extract required columns/fields within a column
   * and remove columns which are not needed.
   */
  @Override
  protected void buildSchema() throws SchemaChangeException {
      if (!prefetchFirstBatchFromBothSides()) {
        return;
      }

      // Setup output container schema based on known left and right schema
      setupNewSchema();

      // TODO: Let's not add this right batch in HyperContainer as of now, since it's the first batch and is expected
      // to be empty
      Preconditions.checkState (right.getRecordCount() == 0, "Unexpected non-empty first right batch received");
      //addBatchToHyperContainer(right);
      // Release the vectors received from right side
      clearBatchVectors(right);


      // We should not allocate memory for all the value vectors inside output batch
      // since this is buildSchema phase and we are sending empty batches downstream
      //allocateVectors();
      lateralJoiner = setupWorker();

      // Set join index as invalid (-1) if the left side is empty, else set it to 0
      leftJoinIndex = (left.getRecordCount() <= 0) ? -1 : 0;

      // Reset the left side of the IterOutcome since for this call OK_NEW_SCHEMA will be returned correctly
      // by buildSchema caller.
      leftUpstream = OK;
      rightUpstream = OK;
  }

  @Override
  public void close() {
    super.close();
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    this.left.kill(sendUpstream);
    this.right.kill(sendUpstream);
  }

  @Override
  public int getRecordCount() {
    return container.getRecordCount();
  }

  /**
   * Helps to free the memory of value vectors inside a RecordBatch
   * @param batch
   */
  private void clearBatchVectors(RecordBatch batch) {
    // Clearing off ValueVector also make sure that future getValueCount will always return 0
    VectorAccessibleUtilities.clear(batch);
  }

  /**
   * Returns the left side incoming for the Lateral Join. Used by right branch leaf operator of Lateral
   * to process the records at joinIndex.
   *
   * @return - RecordBatch received as left side incoming
   */
  @Override
  public RecordBatch getIncoming() {
    Preconditions.checkState (left != null, "Retuning null left batch. It's unexpected since right side will only be " +
      "called iff there is any valid left batch");
    return left;
  }

  /**
   * Returns the current row index which the calling operator should process in current incoming left record batch
   *
   * @return - int - index of row to process.
   */
  @Override
  public int getRecordIndex() {
    Preconditions.checkState (leftJoinIndex < left.getRecordCount(),
      String.format("Left join index: %d is out of bounds: %d", leftJoinIndex, left.getRecordCount()));
    return leftJoinIndex;
  }
}
