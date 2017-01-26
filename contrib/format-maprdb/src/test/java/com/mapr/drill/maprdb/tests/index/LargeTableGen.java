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
package com.mapr.drill.maprdb.tests.index;

import com.mapr.db.Admin;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import com.mapr.db.tests.utils.DBTests;
import com.mapr.fs.utils.ssh.TestCluster;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.json.Json;

import java.io.InputStream;
import java.io.StringBufferInputStream;

/**
 * This class is to generate a MapR json table of this schema:
 * {
 *   "address" : {
 *      "city":"wtj",
 *      "state":"ho"
 *   }
 *   "contact" : {
 *      "email":"VcFahjRfM@gmail.com",
 *      "phone":"6500005583"
 *   }
 *   "id" : {
 *      "ssn":"100005461"
 *   }
 *   "name" : {
 *      "fname":"VcFahj",
 *      "lname":"RfM"
 *   }
 * }
 *
 */
public class LargeTableGen extends LargeTableGenBase {

  private Admin admin;

  public LargeTableGen(Admin dbadmin) {
    admin = dbadmin;
  }

  Table createOrGetTable(String tableName) {
    if (admin.tableExists(tableName)) {
      return MapRDB.getTable(tableName);
      //admin.deleteTable(tableName);
    }
    else {
      return admin.createTable(tableName);
    }
  }

  private void createIndex(Table table, String[] indexDef) throws Exception {
    if(indexDef == null) {
      //don't create index here. indexes may have been created
      return;
    }
    for(int i=0; i<indexDef.length / 2; ++i) {
      String indexCmd = String.format("maprcli table index add"
          + " -path " + table.getPath()
          + " -index testindex_%d"
          + " -indexedfields '%s'"
          + " -nonindexedfields '%s'", i, indexDefInCommand(indexDef[2 * i]), indexDefInCommand(indexDef[2 * i + 1]));
      TestCluster.runCommand(indexCmd);
      DBTests.maprfs().getTableIndexes(table.getPath(), true);
    }
  }

  private String indexDefInCommand(String def) {
    String[] splitted = def.split(",");
    StringBuffer ret = new StringBuffer();
    for(String field: splitted) {
      if(ret.length() == 0) {
        ret.append("\"").append(field).append("\"");
      }
      else {
        ret.append(",").append("\"").append(field).append("\"");
      }
    }
    return ret.toString();
  }
  public void generateTableWithIndex(String tablePath, int recordNumber, String[] indexDef) throws Exception {
    // create index

    initRandVector(recordNumber);
    initDictionary();

    if (admin.tableExists(tablePath)) {
      //admin.deleteTable(tablePath);
    }

    //create Json String
    int batch, i;
    int BATCH_SIZE=2000;
    try (Table table = createOrGetTable(tablePath)) {
      //create index
      createIndex(table, indexDef);
      for (batch = 0; batch < recordNumber; batch += BATCH_SIZE) {
        int batchStop = Math.min(recordNumber, batch + BATCH_SIZE);
        StringBuffer strBuf = new StringBuffer();
        for (i = batch; i < batchStop; ++i) {
          strBuf.append(String.format("{\"rowid\": \"%d\", \"id\": {\"ssn\": \"%s\"}, \"contact\": {\"phone\": \"%s\", \"email\": \"%s\"}," +
                  "\"address\": {\"city\": \"%s\", \"state\": \"%s\"}, \"name\": { \"fname\": \"%s\", \"lname\": \"%s\" } }\n",
              i + 1, getSSN(i), getPhone(i), getEmail(i),
              getAddress(i)[2], getAddress(i)[1], getFirstName(i), getLastName(i)));
        }
        try (InputStream in = new StringBufferInputStream(strBuf.toString());
             DocumentStream stream = Json.newDocumentStream(in)) {
          //write by individual document
          //for (Document document : stream) {
          //  table.insert(document, "rowid");
          //}
          table.insert(stream, "rowid"); //insert a batch  of document in stream
        }
      }
      table.flush();
      DBTests.waitForIndexFlush(table.getPath());
    }
  }
}
