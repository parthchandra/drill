/*
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

import com.google.common.collect.Lists;
import com.mapr.db.Admin;
import com.mapr.drill.maprdb.tests.MaprDBTestsSuite;
import com.mapr.drill.maprdb.tests.json.BaseJsonTest;
import com.mapr.tests.annotations.ClusterTest;

import org.apache.drill.PlanTestBase;
import org.apache.hadoop.hbase.TableName;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

import java.util.List;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category(ClusterTest.class)
public class IndexPlanTest extends BaseJsonTest {

  final static String PRIMARY_TABLE_NAME = "/tmp/index_test_primary";
  final static List<TableName> indexes = Lists.newArrayList();
  final static int PRIMARY_TABLE_SIZE = 10000;
  private static final String sliceTargetSmall = "alter session set `planner.slice_target` = 1";
  private static final String sliceTargetDefault = "alter session reset `planner.slice_target`";
  private static final String noIndexPlan = "alter session set `planner.enable_index_planning` = false";
  private static final String defaultHavingIndexPlan = "alter session reset `planner.enable_index_planning`";

  /**
   *  A sample row of this 10K table:
   ------------------+-----------------------------+--------+
   | 1012  | {"city":"pfrrs","state":"pc"}  | {"email":"KfFzKUZwNk@gmail.com","phone":"6500005471"}  |
   {"ssn":"100007423"}  | {"fname":"KfFzK","lname":"UZwNk"}  | {"age":53.0,"income":45.0}  | 1012   |
   *
   * This test suite generate random content to fill all the rows, since the random function always start from
   * the same seed for different runs, when the row count is not changed, the data in table will always be the same,
   * thus the query result could be predicted and verified.
   */

  @BeforeClass
  public static void setupTableIndexes() throws Exception {
    System.out.print("setupTableIndexes begins");
    Admin admin = MaprDBTestsSuite.getAdmin();
    if (admin != null) {
      if (admin.tableExists(PRIMARY_TABLE_NAME)) {
        admin.deleteTable(PRIMARY_TABLE_NAME);
      }
    }

    LargeTableGen gen = new LargeTableGen(MaprDBTestsSuite.getAdmin());
    /**
     * indexDef is an array of string, LargeTableGen.generateTableWithIndex will take it as parameter to generate indexes
     * for primary table.
     * indexDef[2*i] defines i-th index's indexed field, index[2*i+1] defines i-th index's non-indexed fields
     */
    final String[] indexDef = //null;
        {"id.ssn", "contact.phone",
            "address.state,address.city", "name.fname,name.lname",//mainly for composite key test
            "personal.age", "",
            "personal.income", "",
            "driverlicense", ""
        };
    gen.generateTableWithIndex(PRIMARY_TABLE_NAME, PRIMARY_TABLE_SIZE, indexDef);
  }

  @AfterClass
  public static void cleanupTableIndexes() throws Exception {
    Admin admin = MaprDBTestsSuite.getAdmin();
    if (admin != null) {
      if (admin.tableExists(PRIMARY_TABLE_NAME)) {
   //     admin.deleteTable(PRIMARY_TABLE_NAME);
      }
    }
  }

  @Test
  public void CTASTestTable() throws Exception {
    String ctasQuery = "CREATE TABLE dfs_test.tmp.`backup_index_test_primary` " +
        "AS SELECT * FROM hbase.`index_test_primary` as t ";
    test(ctasQuery);
  }

  @Test
  public void CoveringPlanWithNonIndexedField() throws Exception {

    String query = "SELECT t.`contact`.`phone` AS `phone` FROM hbase.`index_test_primary` as t " +
        " where t.id.ssn = '100007423'";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {".*JsonTableGroupScan.*tableName=.*index_test_primary.*indexName="},
        new String[]{"HashJoin"}
    );

    System.out.println("Covering Plan Verified!");

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("phone").baselineValues("6500005471")
        .go();
    return;

  }

  @Test
  public void CoveringPlanWithOnlyIndexedField() throws Exception {
    String query = "SELECT t.`id`.`ssn` AS `ssn` FROM hbase.`index_test_primary` as t " +
        " where t.id.ssn = '100007423'";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {".*JsonTableGroupScan.*tableName=.*index_test_primary.*indexName="},
        new String[]{"HashJoin"}
    );

    System.out.println("Covering Plan Verified!");

    testBuilder()
        .optionSettingQueriesForTestQuery(defaultHavingIndexPlan)
        .sqlQuery(query)
        .ordered()
        .baselineColumns("ssn").baselineValues("100007423")
        .go();

    return;
  }

  @Test
  public void NoIndexPlanForNonIndexField() throws Exception {

    String query = "SELECT t.`id`.`ssn` AS `ssn` FROM hbase.`index_test_primary` as t " +
        " where t.contact.phone = '6500005471'";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {".*JsonTableGroupScan.*tableName=.*index_test_primary"},
        new String[]{"HashJoin", "indexName="}
    );

    System.out.println("No Index Plan Verified!");

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("ssn").baselineValues("100007423")
        .baselineColumns("ssn").baselineValues("100007632")
        .go();

    return;
  }

  @Test
  public void NonCoveringPlan() throws Exception {

    String query = "SELECT t.`name`.`fname` AS `fname` FROM hbase.`index_test_primary` as t " +
        " where t.id.ssn = '100007423'";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"HashJoin", ".*JsonTableGroupScan.*tableName=.*index_test_primary,", ".*JsonTableGroupScan.*tableName=.*index_test_primary,.*indexName="},
        new String[]{}
    );

    System.out.println("Non-Covering Plan Verified!");

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("fname").baselineValues("KfFzK")
        .go();

    return;
  }

  @Test
  public void RangeConditionIndexPlan() throws Exception {
    String query = "SELECT t.`name`.`lname` AS `lname` FROM hbase.`index_test_primary` as t " +
        " where t.personal.age > 52 AND t.name.fname='KfFzK'";
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"HashJoin", ".*JsonTableGroupScan.*tableName=.*index_test_primary,", ".*JsonTableGroupScan.*tableName=.*index_test_primary,.*indexName="},
        new String[]{}
    );
    testBuilder()
        .optionSettingQueriesForTestQuery(defaultHavingIndexPlan)
        .optionSettingQueriesForBaseline(noIndexPlan)
        .unOrdered()
        .sqlQuery(query)
        .sqlBaselineQuery(query)
        .build()
        .run();

    testBuilder()
        .optionSettingQueriesForTestQuery(sliceTargetSmall)
        .optionSettingQueriesForBaseline(sliceTargetDefault)
        .unOrdered()
        .sqlQuery(query)
        .sqlBaselineQuery(query)
        .build()
        .run();
  }

  @Test
  public void CoveringWithSimpleFieldsOnly() throws Exception {

    String query = "SELECT t._id AS `rowid` FROM hbase.`index_test_primary` as t " +
        " where t.driverlicense = '100007423'";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"JsonTableGroupScan.*tableName=.*index_test_primary,.*indexName="},
        new String[]{"HashJoin"}
    );

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("rowid").baselineValues("1012")
        .go();

    return;
  }

  @Test
  public void NonCoveringWithSimpleFieldsOnly() throws Exception {

    String query = "SELECT t.rowid AS `rowid` FROM hbase.`index_test_primary` as t " +
        " where t.driverlicense = '100007423'";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"HashJoin(.*[\n\r])+.*" +
            "JsonTableGroupScan.*tableName=.*index_test_primary(.*[\n\r])+.*" +
            "JsonTableGroupScan.*tableName=.*index_test_primary,.*indexName="},
        new String[]{}
    );

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("rowid").baselineValues("1012")
        .go();

    return;
  }

  @Test
  public void NonCoveringWithExtraConditonOnPrimary() throws Exception {

    String query = "SELECT t.`name`.`lname` AS `lname` FROM hbase.`index_test_primary` as t " +
        " where t.personal.age = 53 AND t.name.fname='KfFzK'";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"HashJoin", ".*RestrictedJsonTableGroupScan",
            ".*JsonTableGroupScan.*indexName=",},
        new String[]{}
    );

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("lname").baselineValues("UZwNk")
        .go();

    return;
  }

  @Test
  public void Intersect2indexesPlan() throws Exception {

    String query = "SELECT t.`name`.`lname` AS `lname` FROM hbase.`index_test_primary` as t " +
        " where t.personal.age = 53 AND t.personal.income=45";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"HashJoin(.*[\n\r])+.*RestrictedJsonTableGroupScan(.*[\n\r])+.*HashJoin(.*[\n\r])+.*JsonTableGroupScan.*indexName=(.*[\n\r])+.*JsonTableGroupScan.*indexName="},
        new String[]{}
    );

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("lname").baselineValues("UZwNk")
        .baselineColumns("lname").baselineValues("foNwtze")
        .baselineColumns("lname").baselineValues("qGZVfY")
        .go();
    testBuilder()
        .optionSettingQueriesForTestQuery(sliceTargetSmall)
        .optionSettingQueriesForBaseline(sliceTargetDefault)
        .unOrdered()
        .sqlQuery(query)
        .sqlBaselineQuery(query)
        .build()
        .run();
    return;
  }

  @Test
  public void CompositeIndexNonCoveringPlan() throws Exception {

    String query = "SELECT t.`id`.`ssn` AS `ssn` FROM hbase.`index_test_primary` as t " +
        " where t.address.state = 'pc' AND t.address.city='pfrrs'";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"HashJoin(.*[\n\r])+.*RestrictedJsonTableGroupScan(.*[\n\r])+.*JsonTableGroupScan.*indexName="},
        null
    );

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("ssn").baselineValues("100007423")
        .baselineColumns("ssn").baselineValues("100008861")
        .go();

    testBuilder()
        .optionSettingQueriesForTestQuery(sliceTargetSmall)
        .optionSettingQueriesForBaseline(sliceTargetDefault)
        .unOrdered()
        .sqlQuery(query)
        .sqlBaselineQuery(query)
        .build()
        .run();
    return;
  }

  @Test
  public void CompositeIndexCoveringPlan() throws Exception {

    String query = "SELECT t.`address`.`city` AS `city` FROM hbase.`index_test_primary` as t " +
        " where t.address.state = 'pc' AND t.address.city='pfrrs'";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {".*JsonTableGroupScan.*indexName="},
        new String[]{"HashJoin", "Filter"}
    );

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("city").baselineValues("pfrrs")
        .baselineColumns("city").baselineValues("pfrrs")
        .go();

    testBuilder()
        .optionSettingQueriesForTestQuery(sliceTargetSmall)
        .optionSettingQueriesForBaseline(sliceTargetDefault)
        .unOrdered()
        .sqlQuery(query)
        .sqlBaselineQuery(query)
        .build()
        .run();
    return;
  }

  @Test
  public void TestNonCoveringRangePartition_1() throws Exception {

    String query = "SELECT t.`name`.`lname` AS `lname` FROM hbase.`index_test_primary` as t " +
        " where t.personal.age = 53";
    String[] expectedPlan = new String[] {"HashJoin(.*[\n\r])+.*" +
        "RestrictedJsonTableGroupScan.*tableName=.*index_test_primary(.*[\n\r])+.*" +
        "RangePartitionExchange(.*[\n\r])+.*" +
    "JsonTableGroupScan.*tableName=.*index_test_primary,.*indexName="};
    test(defaultHavingIndexPlan);
    test(sliceTargetSmall);
    PlanTestBase.testPlanMatchingPatterns(query,
        expectedPlan, new String[]{});

    try {
      testBuilder()
          .optionSettingQueriesForTestQuery(defaultHavingIndexPlan)
          .optionSettingQueriesForBaseline(noIndexPlan)
          .unOrdered()
          .sqlQuery(query)
          .sqlBaselineQuery(query)
          .build()
          .run();
    } finally {
      test(defaultHavingIndexPlan);
      test(sliceTargetDefault);
    }
    return;
  }

}
