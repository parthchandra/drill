package org.apache.drill.exec.inputformat;

import java.io.IOException;

import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.inputformat.DrillQueryInputFormat.DrillQueryInputSplit;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserProtos.QueryFragmentQuery;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.vector.complex.impl.CombinedMapVector;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;



public class DrillRecordReader extends RecordReader<Void, FieldReader>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillRecordReader.class);

  private DrillClient client;
  private StreamingBatchListener listener;
  private RecordBatchLoader loader;
  private CombinedMapVector mapReader;
  private DrillQueryInputSplit drillInputSplit;
  private boolean first = true;
  private boolean started = false;
  private boolean finished = false;
  private int currentRecord = 0;
  private long totalRecords = 0;
  private long len;
  private long recordBatchCount;

  private FieldReader value;
  private BatchSchema schema;


  public DrillRecordReader() {
    super();
  }

  @Override
  public void close() throws IOException {
    if ( listener != null ) {
      listener.close();
    }
    if ( loader != null ) {
      loader.clear();
    }
    if(client != null) client.close();
  }

  @Override
  public Void getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public FieldReader getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    // TODO need synchronization
    if ( len != 0 ) {
      // TODO fix the logic
      return Math.min(1.0f, (totalRecords + currentRecord) / ((float) len));
    }
    return 0f;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext arg1) throws IOException, InterruptedException {
    client = new DrillClient();
    DrillQueryInputSplit drillInputSplit = (DrillQueryInputSplit) split;
    // TODO Fix getLength() on DrillInputSplit
    len = drillInputSplit.getLength();
    DrillbitEndpoint assignedNode = drillInputSplit.getAssignedFragment().getAssignment();
    client.connect(assignedNode);

    loader = new RecordBatchLoader(client.getAllocator());
    mapReader = new CombinedMapVector(loader);
    QueryFragmentQuery.Builder reqBuilder = QueryFragmentQuery.newBuilder();
    reqBuilder.addAllFragments(drillInputSplit.getQueryPlanFragments().getFragmentsList());
    reqBuilder.setFragmentHandle(drillInputSplit.getAssignedFragment().getHandle());

    listener = new StreamingBatchListener();
    client.submitReadFragmentRequest(reqBuilder.build(), listener);

  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if(!started){
      started = true;
    }

    if(finished) {
      return false;
    }

    if(currentRecord+1 < loader.getRecordCount()){
      currentRecord++;
      // set next value
      value.setPosition(currentRecord);
      return true;
    } else{
      try {
        QueryResultBatch qrb = listener.getNext();
        recordBatchCount++;
        while(qrb != null && 
            qrb.getHeader().getRowCount() == 0 && 
            !first){
          qrb.release();
          qrb = listener.getNext();
          recordBatchCount++;
        }

        first = false;

        if(qrb == null){
          finished = true;
          return false;
        } else{
          totalRecords += currentRecord;
          currentRecord = 0;
          mapReader.clear();
          boolean changed = loader.load(qrb.getHeader().getDef(), qrb.getData());
          qrb.release();
          schema = loader.getSchema();
          // TODO deal with schema change
       //   if(changed) {
       //     updateColumns();
       //   }
        //  if (redoFirstNext && loader.getRecordCount() == 0) {
        //    redoFirstNext = false;
        //  }
          // set first value
          int rows = loader.getRecordCount();

          if ( rows != 0 ) {
            if ( changed ) {
              mapReader.load();
            }
            value = mapReader.getAccessor().getReader();
            value.setPosition(currentRecord);
          }
          return true;
        }
      } catch (RpcException | InterruptedException | SchemaChangeException e) {
        throw new IOException("Failure while trying to get next result batch.", e);
      }

    }
  }
}
