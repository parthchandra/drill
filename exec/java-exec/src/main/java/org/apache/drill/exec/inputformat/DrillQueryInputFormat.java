/**
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
package org.apache.drill.exec.inputformat;

import com.google.common.collect.Lists;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.client.PrintingResultsListener;
import org.apache.drill.exec.client.QuerySubmitter.Format;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserProtos.QueryPlanFragments;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;
import org.apache.drill.exec.proto.UserProtos.QueryFragmentQuery;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class DrillQueryInputFormat implements InputFormat<Object, Object> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillQueryInputFormat.class);

  private final DrillClient client;

  public DrillQueryInputFormat(DrillClient client) throws RpcException {
    this.client = client;
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    String drillQuery = job.get("drill.query");
    logger.debug("Getting splits for query '{}'", drillQuery);

    QueryPlanFragments fragments;
    try {
      DrillRpcFuture<QueryPlanFragments> future = client.planQuery(drillQuery);
      fragments = future.get();
    } catch (InterruptedException | ExecutionException ex) {
      logger.error("Failed to get splits for query '{}'", drillQuery);
      throw new IOException("Failed to get splits", ex);
    }

    if (fragments.getStatus() != QueryState.COMPLETED) {
      throw new IOException(String.format("Query plan failed '[%s]'", fragments.getStatus()));
    }

    // TODO: this is not good, need a better way to find the root fragments
    List<DrillQueryInputSplit> splits = Lists.newArrayList();
    for(int i=0; i<fragments.getFragmentsCount(); i++) {
      PlanFragment fragment = fragments.getFragments(i);
      if (fragment.getFragmentJson().toLowerCase().contains("-writer")) {
        splits.add(new DrillQueryInputSplit(job, fragments, fragment));
      }
    }

    return splits.toArray(new DrillQueryInputSplit[0]);
  }

  @Override
  public RecordReader<Object, Object> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
    try {
      DrillQueryInputSplit drillInputSplit = (DrillQueryInputSplit) split;
      QueryFragmentQuery.Builder reqBuilder = QueryFragmentQuery.newBuilder();
      reqBuilder.addAllFragments(drillInputSplit.getQueryPlanFragments().getFragmentsList());
      reqBuilder.setFragmentHandle(drillInputSplit.getAssignedFragment().getHandle());

      PrintingResultsListener listener = new PrintingResultsListener(DrillConfig.create(), Format.CSV, 10);
      client.submitReadFragmentRequest(reqBuilder.build(), listener);
      listener.await();
    } catch (Exception e) {
      throw new IOException("Failed to make QueryFragmentQuery request", e);
    }

    return null;
  }

  public class DrillQueryInputSplit implements InputSplit {
    private final JobConf conf;
    private final QueryPlanFragments fragments;
    private final PlanFragment assignedFragment;


    public DrillQueryInputSplit(JobConf conf, QueryPlanFragments fragments, PlanFragment assignedFragment) {
      this.conf = conf;
      this.fragments = fragments;
      this.assignedFragment = assignedFragment;
    }

    @Override
    public long getLength() throws IOException {
      // TODO: return 256MB by default
      return 1 << 28;
    }

    @Override
    public String[] getLocations() throws IOException {
      return new String[] { assignedFragment.getAssignment().getAddress() };
    }

    @Override
    public void write(DataOutput out) throws IOException {
      throw new UnsupportedOperationException("TODO: DrillQueryInputSplit.write()");
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      throw new UnsupportedOperationException("TODO: DrillQueryInputSplit.readFields()");
    }

    public String getFragmentJson() {
      return assignedFragment.getFragmentJson();
    }

    public PlanFragment getAssignedFragment() {
      return assignedFragment;
    }

    public QueryPlanFragments getQueryPlanFragments() {
      return fragments;
    }
  }
}
