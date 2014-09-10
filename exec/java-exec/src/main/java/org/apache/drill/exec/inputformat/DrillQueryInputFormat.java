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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserProtos.QueryPlanFragments;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

public class DrillQueryInputFormat extends InputFormat<Void, FieldReader> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillQueryInputFormat.class);

  private DrillClient client;

  @VisibleForTesting
  public DrillQueryInputFormat(DrillClient clientForInitialPlanning){
    this.client = clientForInitialPlanning;
  }

  public DrillQueryInputFormat() throws RpcException {
  }

  @Override
  public DrillRecordReader createRecordReader(InputSplit split, TaskAttemptContext arg1)
      throws IOException, InterruptedException {
    return new DrillRecordReader();
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException, InterruptedException {
    DrillClient cl = this.client == null ? new DrillClient() : this.client;
    try (DrillClient client = cl) {
      if(this.client == null) client.connect(); // zookeeper work.

      String drillQuery = job.getConfiguration().get("drill.query");
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
      List<InputSplit> splits = Lists.newArrayList();
      for (int i = 0; i < fragments.getFragmentsCount(); i++) {
        PlanFragment fragment = fragments.getFragments(i);
        if (fragment.getFragmentJson().toLowerCase().contains("-writer")) {
          splits.add(new DrillQueryInputSplit(fragments, fragment));
        }
      }

      return splits;
    }
  }

  public class DrillQueryInputSplit extends InputSplit implements Writable {
    private QueryPlanFragments fragments;
    private PlanFragment assignedFragment;

    public DrillQueryInputSplit() {
    }

    public DrillQueryInputSplit(QueryPlanFragments fragments, PlanFragment assignedFragment) {
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
      byte[] assignment = assignedFragment.toByteArray();
      out.writeInt(assignment.length);
      out.write(assignment);

      byte[] frags = fragments.toByteArray();
      out.writeInt(frags.length);
      out.write(frags);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int assignment = in.readInt();
      byte[] aBytes = new byte[assignment];
      in.readFully(aBytes);
      this.assignedFragment = PlanFragment.parseFrom(aBytes);

      int frags = in.readInt();
      byte[] fBytes = new byte[frags];
      in.readFully(fBytes);
      this.fragments = QueryPlanFragments.parseFrom(fBytes);

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
