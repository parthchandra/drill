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
package com.mapr.drill.maprdb.tests.json;

import static com.mapr.drill.maprdb.tests.MaprDBTestsSuite.INDEX_FLUSH_TIMEOUT;

import org.apache.drill.PlanTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;

import com.mapr.db.Table;
import com.mapr.db.impl.MapRDBImpl;
import com.mapr.db.tests.utils.DBTests;
import com.mapr.tests.annotations.ClusterTest;

@Category(ClusterTest.class)
public class TestQueryWithIndex extends BaseJsonTest {

  private static final String TMP_TABLE_WITH_INDEX = "drill_test_table_with_index";

  private static final String TMP_TABLE_WITH_HASHED_INDEX = "drill_test_table_with_hashed_index";

  private static boolean tableCreated = false;
  private static boolean hashedIndexTableCreated = false;
  private static String tablePath;
  private static String hashedIndexTablePath;

  @BeforeClass
  public static void setup_TestQueryWithIndex() throws Exception {
    try (Table table = DBTests.createOrReplaceTable(TMP_TABLE_WITH_INDEX)) {
      tableCreated = true;
      tablePath = table.getPath().toUri().getPath();

      DBTests.createIndex(TMP_TABLE_WITH_INDEX, "testindex", new String[] {"name.last"}, new String[] {"age"});
      DBTests.admin().getTableIndexes(table.getPath(), true);

      // insert data
      table.insertOrReplace(MapRDBImpl.newDocument("{\"_id\":\"user001\", \"age\":43, \"name\": {\"first\":\"Sam\", \"last\":\"Harris\"}}"));
      table.insertOrReplace(MapRDBImpl.newDocument("{\"_id\":\"user002\", \"age\":12, \"name\": {\"first\":\"Leon\", \"last\":\"Russel\"}}"));
      table.insertOrReplace(MapRDBImpl.newDocument("{\"_id\":\"user003\", \"age\":87, \"name\": {\"first\":\"David\", \"last\":\"Bowie\"}}"));
      table.insertOrReplace(MapRDBImpl.newDocument("{\"_id\":\"user004\", \"age\":56, \"name\": {\"first\":\"Bob\", \"last\":\"Dylan\"}}"));
      table.insertOrReplace(MapRDBImpl.newDocument("{\"_id\":\"user005\", \"age\":54, \"name\": {\"first\":\"David\", \"last\":\"Ackert\"}}"));
      table.flush();

      DBTests.waitForRowCount(table.getPath(), 5, INDEX_FLUSH_TIMEOUT);
      DBTests.waitForIndexFlush(table.getPath(), INDEX_FLUSH_TIMEOUT);
    } finally {
      test("ALTER SESSION SET `planner.disable_full_table_scan` = true");
    }

    try (Table table = DBTests.createOrReplaceTable(TMP_TABLE_WITH_HASHED_INDEX)) {
      hashedIndexTableCreated = true;
      hashedIndexTablePath = table.getPath().toUri().getPath();
      DBTests.createIndex(TMP_TABLE_WITH_HASHED_INDEX, "testhashedindex", new String[] {"name.last"}, new String[] {"age"}, true, 5);
      DBTests.admin().getTableIndexes(table.getPath(), true);

      // insert data
      table.insertOrReplace(MapRDBImpl.newDocument("{\"_id\":\"user001\", \"age\":43, \"name\": {\"first\":\"Sam\", \"last\":\"Harris\"}}"));
      table.insertOrReplace(MapRDBImpl.newDocument("{\"_id\":\"user002\", \"age\":12, \"name\": {\"first\":\"Leon\", \"last\":\"Russel\"}}"));
      table.insertOrReplace(MapRDBImpl.newDocument("{\"_id\":\"user003\", \"age\":87, \"name\": {\"first\":\"David\", \"last\":\"Bowie\"}}"));
      table.insertOrReplace(MapRDBImpl.newDocument("{\"_id\":\"user004\", \"age\":56, \"name\": {\"first\":\"Bob\", \"last\":\"Dylan\"}}"));
      table.insertOrReplace(MapRDBImpl.newDocument("{\"_id\":\"user005\", \"age\":54, \"name\": {\"first\":\"David\", \"last\":\"Ackert\"}}"));
      table.flush();

      DBTests.waitForRowCount(table.getPath(), 5, INDEX_FLUSH_TIMEOUT);
      DBTests.waitForIndexFlush(table.getPath(), INDEX_FLUSH_TIMEOUT);
    } finally {
      test("ALTER SESSION SET `planner.disable_full_table_scan` = true");
    }
  }

  @AfterClass
  public static void cleanup_TestQueryWithIndex() throws Exception {
    test("ALTER SESSION SET `planner.disable_full_table_scan` = false");
    if (tableCreated) {
      DBTests.deleteTables(TMP_TABLE_WITH_INDEX);
    }

    if (hashedIndexTableCreated) {
      DBTests.deleteTables(TMP_TABLE_WITH_HASHED_INDEX);
    }
  }

  @Test
  public void testSelectWithIndex() throws Exception {
    final String sql = String.format(
          "SELECT\n"
        + "  _id, t.name.last\n"
        + "FROM\n"
        + "  hbase.root.`%s` t\n"
        + "WHERE t.name.last = 'Russel'",
        tablePath);

    runSQLAndVerifyCount(sql, 1);

    // plan test
    final String[] expectedPlan = {"indexName=testindex"};
    final String[] excludedPlan = {};

    PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);
  }

  @Test
  public void testSelectWithHashedIndex() throws Exception {
    final String sql = String.format(
          "SELECT\n"
        + "  _id, t.name.last\n"
        + "FROM\n"
        + "  hbase.root.`%s` t\n"
        + "WHERE t.name.last = 'Russel'",
        hashedIndexTablePath);

    runSQLAndVerifyCount(sql, 1);

    // plan test
    final String[] expectedPlan = {"indexName=testhashedindex"};
    final String[] excludedPlan = {};

    PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);
  }
}
