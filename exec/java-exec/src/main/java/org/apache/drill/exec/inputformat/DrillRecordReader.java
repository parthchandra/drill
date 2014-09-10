package org.apache.drill.exec.inputformat;

import java.io.IOException;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.client.PrintingResultsListener;
import org.apache.drill.exec.client.QuerySubmitter.Format;
import org.apache.drill.exec.inputformat.DrillQueryInputFormat.DrillQueryInputSplit;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserProtos.QueryFragmentQuery;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class DrillRecordReader extends RecordReader<Void, FieldReader>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillRecordReader.class);

  private DrillClient client;
  private PrintingResultsListener listener;
  private DrillQueryInputSplit drillInputSplit;

  public DrillRecordReader() {
    super();
  }

  @Override
  public void close() throws IOException {
    if(client != null) client.close();
  }

  @Override
  public Void getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public FieldReader getCurrentValue() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext arg1) throws IOException, InterruptedException {
    client = new DrillClient();
    DrillQueryInputSplit drillInputSplit = (DrillQueryInputSplit) split;
    DrillbitEndpoint assignedNode = drillInputSplit.getAssignedFragment().getAssignment();
    client.connect(assignedNode);

    QueryFragmentQuery.Builder reqBuilder = QueryFragmentQuery.newBuilder();
    reqBuilder.addAllFragments(drillInputSplit.getQueryPlanFragments().getFragmentsList());
    reqBuilder.setFragmentHandle(drillInputSplit.getAssignedFragment().getHandle());

    PrintingResultsListener listener = new PrintingResultsListener(DrillConfig.create(), Format.CSV, 10);
    client.submitReadFragmentRequest(reqBuilder.build(), listener);

  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    try {
      listener.await();
    } catch (Exception e) {
      throw new IOException(e);
    }
    return false;
  }



}
