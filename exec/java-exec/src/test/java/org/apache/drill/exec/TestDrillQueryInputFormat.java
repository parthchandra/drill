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

import java.io.ByteArrayOutputStream;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.inputformat.DrillQueryInputFormat;
import org.apache.drill.exec.inputformat.DrillQueryInputFormat.DrillQueryInputSplit;
import org.apache.drill.exec.inputformat.DrillRecordReader;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.vector.complex.fn.JsonWriter;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.junit.Test;

public class TestDrillQueryInputFormat {

  @Test
  public void test() throws Exception {
    String query = String.format("SELECT `N_REGIONKEY`, COUNT(*) FROM "
        + "dfs_test.`%s/../../sample-data/nation.parquet` GROUP BY `N_REGIONKEY`", TestTools.getWorkingPath());

    query = "SELECT `education_level`, count(*) FROM cp.`employee.json` GROUP BY `education_level`";
    //query = "SELECT * FROM cp.`employee.json`";

    JobConf job = new JobConf();
    job.set("drill.query", query);

    JobContext context = new JobContextImpl(job, new JobID("drill query", 0));

    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
    DrillConfig drillConfig = DrillConfig.create();

    try (Drillbit bit1 = new Drillbit(drillConfig, serviceSet);
        Drillbit bit2 = new Drillbit(drillConfig, serviceSet);
        DrillClient client = new DrillClient(drillConfig, serviceSet.getCoordinator());) {
      bit1.run();
      bit2.run();
      client.connect();

      List<QueryResultBatch> results = client.runQuery(QueryType.SQL, "alter session set `planner.slice_target`=1");
      for(QueryResultBatch batch : results) {
        batch.release();
      }

      DrillQueryInputFormat inputFormat = new DrillQueryInputFormat(client);
      List<InputSplit> inputSplits = inputFormat.getSplits(context);

      System.out.println("Number of splits: " + inputSplits.size());
      for (InputSplit split : inputSplits) {
        DrillQueryInputSplit drillSplit = (DrillQueryInputSplit) split;
        System.out.println(drillSplit.getFragmentJson());
        System.out.println("-------------");
      }

      List<DrillRecordReader> readers = Lists.newArrayList();

      for (InputSplit split : inputSplits) {
        DrillRecordReader reader = inputFormat.createRecordReader(split, null);
        reader.initialize(split, null);
        readers.add(reader);
      }

      for(DrillRecordReader reader : readers) {
        int i = 0;
        while ( reader.nextKeyValue() ) {
          FieldReader fr = reader.getCurrentValue();
          if ( i % 100 == 0 ) {
            System.out.println();
            for ( String name : fr ) {
              System.out.print(name + ", ");
            }            
            System.out.println();
          }
          System.out.print("Row#: " + ++i + ", ");
          for ( String name : fr ) {
            FieldReader frChild = fr.reader(name);
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            JsonWriter jsonWriter = new JsonWriter(stream, true);
            jsonWriter.write(frChild);
            System.out.print(new String(stream.toByteArray(), Charsets.UTF_8) + ", ");
            stream.close();
           }
          System.out.println();
        }
        reader.close();
      }

      Thread.sleep(3000);
    }
  }
}
