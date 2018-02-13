package org.apache.drill.exec.physical.impl.unnest;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.base.LateralContract;
import org.apache.drill.exec.physical.impl.MockRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.ExpandableHyperContainer;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;

import java.util.Iterator;
import java.util.List;

public class MockLateralJoinBatch /*extends MockRecordBatch */ implements LateralContract, CloseableRecordBatch {

  private RecordBatch incoming;

  private TransferPair transfer;
  private VectorContainer container;

  private int recordIndex = 0;
  private RecordBatch unnest;

  private boolean isDone;

  // All the below resources are owned by caller
  private final FragmentContext context;
  private  final OperatorContext oContext;
  private  final BufferAllocator allocator;



  private ExpandableHyperContainer results = new ExpandableHyperContainer();

  public MockLateralJoinBatch(FragmentContext context, OperatorContext oContext, RecordBatch incoming) {
    this.context = context;
    this.oContext = oContext;
    this.allocator = oContext.getAllocator();

    this.incoming = incoming;
    this.container = new VectorContainer(oContext);

    this.isDone = false;
  }

  @Override public RecordBatch getIncoming() {
    return null; // don't need this
  }

  @Override public int getRecordIndex() {
    return recordIndex;
  }

  public void moveToNextRecord() {
    recordIndex++;
  }

  public void reset() {
    recordIndex = 0;
  }

  public void setUnnest(RecordBatch unnest){
    this.unnest = unnest;
  }

  public RecordBatch getUnnest() {
    return unnest;
  }

  public IterOutcome next() {

    IterOutcome currentOutcome = incoming.next();
    recordIndex = 0;

    container.zeroVectors();
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    final BatchSchema schema =  container.getSchema();

    switch (currentOutcome) {
      case OK_NEW_SCHEMA:
        // Hmm, we need the schema from unnest which doesn't have
        // it's outgoing container set up yet. So we need to call
        // setupTransfer every iteration of next().
      case OK:
        IterOutcome outcome;
        // consume all the outout from unnest until EMIT or end of
        // incoming data
        while (recordIndex < incoming.getRecordCount()) {
          outcome = unnest.next();
          if (outcome == IterOutcome.OK_NEW_SCHEMA) {
            setupTransfer();
            // unnest is expected to return an empty batch.
          }
          transfer.transfer();
          // We put each batch output from unnest into the output container.
          // This works only because we are in a unit test.
          //int count = container.hasRecordCount()?container.getRecordCount():0;
          //results.addBatch(container.getValueVector(count).getValueVector());
          if (outcome == IterOutcome.EMIT) {
            moveToNextRecord();
          }
        }
        return currentOutcome;
      case NONE:
      case STOP:
      case OUT_OF_MEMORY:
        isDone = true;
        container = new VectorContainer(allocator, schema);
        container.setRecordCount(0);
        return currentOutcome;
      case NOT_YET:
        container = new VectorContainer(allocator, schema);
        container.setRecordCount(0);
        return currentOutcome;
      default:
        throw new UnsupportedOperationException("This state is not supported");
    }
  }

  @Override public WritableBatch getWritableBatch() {
    return null;
  }

  public ExpandableHyperContainer getResults() {
    return results;
  }

  @Override
  public void close() throws Exception {
    container.clear();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    return null;
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    return null;
  }

  @Override
  public FragmentContext getContext() {
    return context;
  }

  @Override
  public BatchSchema getSchema() {
    return container.getSchema();
  }

  @Override
  public int getRecordCount() {
    return container.getRecordCount();
  }

  @Override
  public void kill(boolean sendUpstream) {
    container.clear();
  }

  @Override
  public VectorContainer getOutgoingContainer() {
    return container;
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    return container.getValueVectorId(path);
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids) {
    return container.getValueAccessorById(clazz, ids);
  }

  @Override public Iterator<VectorWrapper<?>> iterator() {
    return container.iterator();
  }

  private void setupTransfer(){
    ValueVector vv = unnest.getOutgoingContainer().getValueVector(0).getValueVector();
    transfer = vv.getTransferPair( vv.getField().getName(), this .oContext.getAllocator());
    container.add(transfer.getTo());
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
  }

  public boolean isCompleted() {
    return isDone;
  }


}
