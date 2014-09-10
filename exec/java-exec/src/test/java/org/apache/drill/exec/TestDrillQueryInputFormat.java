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
package org.apache.drill.exec;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.inputformat.DrillQueryInputFormat;
import org.apache.drill.exec.inputformat.DrillQueryInputFormat.DrillQueryInputSplit;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestDrillQueryInputFormat {

  @Test
  public void test() throws Exception {
    String query = String.format("SELECT `N_REGIONKEY`, COUNT(*) FROM " +
        "dfs_test.`%s/../../sample-data/nation.parquet` GROUP BY `N_REGIONKEY`", TestTools.getWorkingPath());

    query = "SELECT `N_REGIONKEY`, COUNT(*) FROM " +
        "cp.`employee.json` GROUP BY `N_REGIONKEY`";

    JobConf job = new JobConf();
    job.set("drill.query", query);

    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
    DrillConfig drillConfig = DrillConfig.create();

    Drillbit bit1 = new Drillbit(drillConfig, serviceSet);
    Drillbit bit2 = new Drillbit(drillConfig, serviceSet);
    DrillClient client = new DrillClient(drillConfig, serviceSet.getCoordinator());

    bit1.run();
    bit2.run();
    client.connect();

    client.runQuery(QueryType.SQL, "alter session set `planner.slice_target`=1");

    DrillQueryInputFormat inputFormat = new DrillQueryInputFormat(client);
    InputSplit[] inputSplits = inputFormat.getSplits(job, 1);

    System.out.println("Number of splits: " + inputSplits.length);
    for(InputSplit split : inputSplits) {
      DrillQueryInputSplit drillSplit = (DrillQueryInputSplit) split;
      System.out.println(drillSplit.getFragmentJson());
      System.out.println("-------------");
    }

    for(InputSplit split : inputSplits) {
      inputFormat.getRecordReader(split, job, Reporter.NULL);
    }
  }
}
