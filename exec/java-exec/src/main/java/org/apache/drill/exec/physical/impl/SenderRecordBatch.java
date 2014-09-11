/**
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

package org.apache.drill.exec.physical.impl;

import io.netty.buffer.ByteBuf;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.Writer;
import org.apache.drill.exec.physical.impl.ScreenCreator.ScreenRoot.Metric;
import org.apache.drill.exec.physical.impl.materialize.QueryWritableBatch;
import org.apache.drill.exec.physical.impl.materialize.RecordMaterializer;
import org.apache.drill.exec.physical.impl.materialize.VectorRecordMaterializer;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryResult;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserBitShared.RecordBatchDef;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.BaseRpcOutcomeListener;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.work.ErrorHelper;
import org.apache.drill.exec.work.FragmentConnectionManager;

/**
 *  Send  RecordBatch to the given RPC connection 
 */
public class SenderRecordBatch extends AbstractRecordBatch<Writer> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SenderRecordBatch.class);

  private int counter = 0;
  private final RecordBatch incoming;
  private UserClientConnection connection;
  private RecordMaterializer materializer;
  private boolean first = true;
  private boolean processed = false;
  private String fragmentUniqueId;
  
  private final SendingAccountor sendCount = new SendingAccountor();
  volatile boolean ok = true;


  public SenderRecordBatch(Writer writer, RecordBatch incoming, FragmentContext context, RecordWriter recordWriter) throws OutOfMemoryException {
    super(writer, context);
    this.incoming = incoming;

    FragmentHandle handle = context.getHandle();
    fragmentUniqueId = String.format("%d_%d", handle.getMajorFragmentId(), handle.getMinorFragmentId());
  }

  @Override
  public int getRecordCount() {
    return container.getRecordCount();
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    incoming.kill(sendUpstream);
  }

  @Override
  public IterOutcome innerNext() {
    if(processed) {
      // if the upstream record batch is already processed and next() is called by
      // downstream then return NONE to indicate completion
      return IterOutcome.NONE;
    }

    if(!ok){
      cleanup();
      context.fail(this.listener.ex);
      // TODO - what to do in this case?
      return IterOutcome.STOP;
    }


    // process the complete upstream in one next() call
    IterOutcome upstream;
    do {
      upstream = next(incoming);
      if(first && upstream == IterOutcome.OK)
        upstream = IterOutcome.OK_NEW_SCHEMA;
      first = false;

      switch(upstream) {
        case NOT_YET:
        case NONE:
        case STOP:
          cleanup();
          QueryResult header = QueryResult.newBuilder()
              .setQueryId(context.getHandle().getQueryId())
              .setRowCount(0)
              .setQueryState(QueryState.COMPLETED)
              .setDef(RecordBatchDef.getDefaultInstance())
              .setIsLastChunk(true)
              .build();
          QueryWritableBatch lastBatch = new QueryWritableBatch(header);
          connection.sendResult(listener, lastBatch);
          if (upstream == IterOutcome.STOP)
            return upstream;
          break;
        case OK_NEW_SCHEMA:
          materializer = new VectorRecordMaterializer(context, incoming);
          // fall through.
        case OK:
          QueryWritableBatch batch = materializer.convertNext(false);
          updateStats(batch);
          stats.startWait();
          try {
            FragmentConnectionManager connManager = context.getConnectionManager();
            if ( connManager != null ) {
              // TODO getConnection is blocking
              // do we need to continue processing - accumulating RecordBatches
              // while waiting for connection?
              connection = connManager.getConnection(context.getHandle());
            }
            connection.sendResult(listener, batch);
            counter += incoming.getRecordCount();
          } finally {
            stats.stopWait();
          }
          sendCount.increment();

          for(VectorWrapper v : incoming)
            v.getValueVector().clear();

          break;
        
        default:
          throw new UnsupportedOperationException();
      }
    } while(upstream != IterOutcome.NONE);

    // Create two vectors for:
    //   1. Fragment unique id.
    //   2. Summary: currently contains number of records written.
    MaterializedField fragmentIdField = MaterializedField.create(SchemaPath.getSimplePath("Fragment"), Types.required(MinorType.VARCHAR));
    MaterializedField summaryField = MaterializedField.create(SchemaPath.getSimplePath("Number of records written"), Types.required(MinorType.BIGINT));

    VarCharVector fragmentIdVector = (VarCharVector) TypeHelper.getNewVector(fragmentIdField, context.getAllocator());
    AllocationHelper.allocate(fragmentIdVector, 1, TypeHelper.getSize(Types.required(MinorType.VARCHAR)));
    BigIntVector summaryVector = (BigIntVector) TypeHelper.getNewVector(summaryField, context.getAllocator());
    AllocationHelper.allocate(summaryVector, 1, TypeHelper.getSize(Types.required(MinorType.VARCHAR)));


    container.add(fragmentIdVector);
    container.add(summaryVector);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);

    fragmentIdVector.getMutator().setSafe(0, fragmentUniqueId.getBytes());
    fragmentIdVector.getMutator().setValueCount(1);
    summaryVector.getMutator().setSafe(0, counter);
    summaryVector.getMutator().setValueCount(1);

    container.setRecordCount(1);
    processed = true;

    return IterOutcome.OK_NEW_SCHEMA;
  }

  public void updateStats(QueryWritableBatch queryBatch) {
      stats.addLongStat(Metric.BYTES_SENT, queryBatch.getByteCount());
    }

  @Override
  public void cleanup() {
	if(!oContext.isClosed()){
      sendCount.waitForSendComplete();
      oContext.close();
	}
	// TODO - do we need to do this one?
    super.cleanup();
    incoming.cleanup();
   }

  private SendListener listener = new SendListener();

  //TODO refactor this class, so not to copy from ScreenRoot
  private class SendListener extends BaseRpcOutcomeListener<Ack>{
    volatile RpcException ex;


    @Override
    public void success(Ack value, ByteBuf buffer) {
      super.success(value, buffer);
      sendCount.decrement();
    }

    @Override
    public void failed(RpcException ex) {
      sendCount.decrement();
      logger.error("Failure while sending data to user.", ex);
      boolean verbose = context.getOptions().getOption(ExecConstants.ENABLE_VERBOSE_ERRORS_KEY).bool_val;
      ErrorHelper.logAndConvertError(context.getIdentity(), "Failure while sending fragment to client.", ex, logger,
        verbose);
      ok = false;
      this.ex = ex;
    }

  }

}
