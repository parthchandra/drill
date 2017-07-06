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
  private static final String highFTSFactor = "alter session set `planner.fts_cost_factor` = 1500.0";
  private static final String defaultFTSFactor = "alter session reset `planner.fts_cost_factor`";
  private static final String lowRowKeyJoinBackIOFactor = "alter session set `planner.rowkey_joinback_io_cost_factor` = 0.1";
  private static final String defaultRowKeyJoinBackIOFactor = "alter session reset `planner.rowkey_joinback_io_cost_factor`";

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
            "driverlicense", "",
            "$CAST(id.ssn@INT)", "contact.phone",
            "$CAST(driverlicense@STRING)","contact.email",
            "address.state,personal.age,driverlicense", "name.fname",
            "personal.age", "name.fname"
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
    String ctasQuery = "CREATE TABLE hbase.tmp.`backup_index_test_primary` " +
        "AS SELECT * FROM hbase.`index_test_primary` as t ";
    test(ctasQuery);
    test("DROP TABLE IF EXISTS hbase.tmp.`backup_index_test_primary`");
  }

  @Test
  public void CoveringPlanWithNonIndexedField() throws Exception {

    String query = "SELECT t.`contact`.`phone` AS `phone` FROM hbase.`index_test_primary` as t " +
        " where t.id.ssn = '100007423'";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {".*JsonTableGroupScan.*tableName=.*index_test_primary.*indexName=testindex_0"},
        new String[]{"RowKeyJoin"}
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
        new String[] {".*JsonTableGroupScan.*tableName=.*index_test_primary.*indexName=testindex_0"},
        new String[]{"RowKeyJoin"}
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
        new String[]{"RowKeyJoin", "indexName="}
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
    test(defaultHavingIndexPlan+";"+ highFTSFactor +";");
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_primary,",
           ".*JsonTableGroupScan.*tableName=.*index_test_primary,.*indexName=testindex_0"},
        new String[]{}
    );

    System.out.println("Non-Covering Plan Verified!");

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("fname").baselineValues("KfFzK")
        .go();

    test(defaultFTSFactor);
    return;
  }

  @Test
  @Ignore("Currently picks full table scan; re-enable after stats and costing are done")
  public void RangeConditionIndexPlan() throws Exception {
    String query = "SELECT t.`name`.`lname` AS `lname` FROM hbase.`index_test_primary` as t " +
        " where t.personal.age > 52 AND t.name.fname='KfFzK'";
    test(defaultHavingIndexPlan+";"+ highFTSFactor +";");
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"RowKeyJoin", ".*RestrictedJsonTableGroupScan.*tableName=.*index_test_primary,",
           ".*JsonTableGroupScan.*tableName=.*index_test_primary,.*indexName=testindex_2"},
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

    test(defaultFTSFactor);
  }

  @Test
  public void CoveringWithSimpleFieldsOnly() throws Exception {

    String query = "SELECT t._id AS `rowid` FROM hbase.`index_test_primary` as t " +
        " where t.driverlicense = 100007423";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"JsonTableGroupScan.*tableName=.*index_test_primary,.*indexName=testindex_4"},
        new String[]{"RowKeyJoin"}
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
        " where t.driverlicense = 100007423";
    test(defaultHavingIndexPlan+";"+ highFTSFactor +";");
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"RowKeyJoin(.*[\n\r])+.*" +
            "RestrictedJsonTableGroupScan.*tableName=.*index_test_primary(.*[\n\r])+.*" +
            "JsonTableGroupScan.*tableName=.*index_test_primary,.*indexName=testindex_4"},
        new String[]{}
    );

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("rowid").baselineValues("1012")
        .go();

    test(defaultFTSFactor);
    return;
  }

  @Test
  public void NonCoveringWithExtraConditonOnPrimary() throws Exception {

    String query = "SELECT t.`name`.`lname` AS `lname` FROM hbase.`index_test_primary` as t " +
        " where t.personal.age = 53 AND t.name.fname='KfFzK'";
    test(defaultHavingIndexPlan+";"+ highFTSFactor +";");
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"RowKeyJoin", ".*RestrictedJsonTableGroupScan",
            ".*JsonTableGroupScan.*indexName=testindex_[28]",},
        new String[]{}
    );

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("lname").baselineValues("UZwNk")
        .go();

    test(defaultFTSFactor);
    return;
  }

  @Test
  public void Intersect2indexesPlan() throws Exception {

    String query = "SELECT t.`name`.`lname` AS `lname` FROM hbase.`index_test_primary` as t " +
        " where t.personal.age = 53 AND t.personal.income=45";
    test(defaultHavingIndexPlan+";"+ highFTSFactor +";");
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"RowKeyJoin(.*[\n\r])+.*RestrictedJsonTableGroupScan(.*[\n\r])+.*HashJoin(.*[\n\r])+.*JsonTableGroupScan.*indexName=testindex_(.*[\n\r])+.*JsonTableGroupScan.*indexName=testindex_"},
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
    test(defaultFTSFactor);
    return;
  }

  @Test
  public void CompositeIndexNonCoveringPlan() throws Exception {

    String query = "SELECT t.`id`.`ssn` AS `ssn` FROM hbase.`index_test_primary` as t " +
        " where t.address.state = 'pc' AND t.address.city='pfrrs'";
    test(defaultHavingIndexPlan+";"+ highFTSFactor +";");
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"RowKeyJoin(.*[\n\r])+.*RestrictedJsonTableGroupScan(.*[\n\r])+.*JsonTableGroupScan.*indexName=testindex_1"},
        new String[]{}
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

    test(defaultFTSFactor);
    return;
  }

  @Test
  public void CompositeIndexCoveringPlan() throws Exception {

    String query = "SELECT t.`address`.`city` AS `city` FROM hbase.`index_test_primary` as t " +
        " where t.address.state = 'pc' AND t.address.city='pfrrs'";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {".*JsonTableGroupScan.*indexName=testindex_1"},
        new String[]{"RowKeyJoin", "Filter"}
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
    String[] expectedPlan = new String[] {"RowKeyJoin(.*[\n\r])+.*" +
        "RestrictedJsonTableGroupScan.*tableName=.*index_test_primary(.*[\n\r])+.*" +
        "RangePartitionExchange(.*[\n\r])+.*" +
    "JsonTableGroupScan.*tableName=.*index_test_primary,.*indexName=testindex_[28]"};
    test(defaultHavingIndexPlan+";"+ highFTSFactor +";"+sliceTargetSmall+";");
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
      test(defaultFTSFactor);
    }
    return;
  }

  @Test
  public void TestCastVarCharCoveringPlan() throws Exception {
    String query = "SELECT t._id as tid, cast(t.driverlicense as varchar(128)) as driverlicense FROM hbase.`index_test_primary` as t " +
        " where cast(t.driverlicense as varchar(128))='100007423'";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {".*JsonTableGroupScan.*tableName=.*index_test_primary.*indexName="},
        new String[]{"RowKeyJoin"}
    );

    System.out.println("TestCastCoveringPlan Plan Verified!");

    testBuilder()
        .optionSettingQueriesForTestQuery(defaultHavingIndexPlan)
        .sqlQuery(query)
        .ordered()
        .baselineColumns("tid", "driverlicense").baselineValues("1012", "100007423")
        .go();

    return;
  }

  @Test
  public void TestCastINTCoveringPlan() throws Exception {
    String query = "SELECT t._id as tid, CAST(t.id.ssn as INT) as ssn, t.contact.phone AS `phone` FROM hbase.`index_test_primary` as t " +
        " where CAST(t.id.ssn as INT) = 100007423";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {".*JsonTableGroupScan.*tableName=.*index_test_primary.*indexName="},
        new String[]{"RowKeyJoin"}
    );

    System.out.println("TestCastCoveringPlan Plan Verified!");

    testBuilder()
        .optionSettingQueriesForTestQuery(defaultHavingIndexPlan)
        .sqlQuery(query)
        .ordered()
        .baselineColumns("tid", "ssn", "phone").baselineValues("1012", 100007423, "6500005471")
        .go();

    return;
  }

  @Test
  public void TestCastNonCoveringPlan() throws Exception {
    String query = "SELECT t.id.ssn AS `ssn` FROM hbase.`index_test_primary` as t " +
        " where CAST(t.id.ssn as INT) = 100007423";
    test(defaultHavingIndexPlan+";"+ highFTSFactor +";");
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"RowKeyJoin(.*[\n\r])+.*RestrictedJsonTableGroupScan(.*[\n\r])+.*JsonTableGroupScan.*indexName=testindex_5"},
        new String[]{}
    );

    System.out.println("TestCastNonCoveringPlan Plan Verified!");

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("ssn").baselineValues("100007423")
        .go();
    test(defaultFTSFactor);
    return;
  }

  @Test
  public void TestCastVarchar_ConvertToRangePlan() throws Exception {
    String query = "SELECT t.id.ssn AS `ssn` FROM hbase.`index_test_primary` as t " +
        " where CAST(driverlicense as VARCHAR(10)) = '100007423'";
    test(defaultHavingIndexPlan+";"+ highFTSFactor +";");
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"RowKeyJoin(.*[\n\r])+.*RestrictedJsonTableGroupScan(.*[\n\r])+.*JsonTableGroupScan.*MATCHES \"\\^.*100007423.*E.*\\$\".*indexName=testindex_6"},
        new String[]{}
    );

    System.out.println("TestCastVarchar_ConvertToRangePlan Verified!");

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("ssn").baselineValues("100007423")
        .go();

    test(defaultFTSFactor);
    return;
  }

  @Test
  public void TestCoveringPlanSortRemoved() throws Exception {
    String query = "SELECT t.`contact`.`phone` as phone FROM hbase.`index_test_primary` as t " +
        " where t.id.ssn <'100000003' order by t.id.ssn";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {".*JsonTableGroupScan.*tableName=.*index_test_primary.*indexName="},
        new String[]{"Sort"}
    );

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("phone").baselineValues("6500008069")
        .baselineColumns("phone").baselineValues("6500001411")
        .baselineColumns("phone").baselineValues("6500001595")
        .go();
  }

  @Test
  @Ignore("Using stats causes HashToRandomExchange to not be introduced for parallel plan")
  public void TestCoveringPlanSortNotRemoved() throws Exception {
    String query = "SELECT t.`contact`.`phone` as phone FROM hbase.`index_test_primary` as t " +
        " where t.id.ssn <'100000003' order by t.contact.phone";
    test(defaultHavingIndexPlan+";"+ highFTSFactor +";");
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"Sort", ".*JsonTableGroupScan.*tableName=.*index_test_primary.*indexName="},
        new String[]{"RowkeyJoin"}
    );

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("phone").baselineValues("6500001411")
        .baselineColumns("phone").baselineValues("6500001595")
        .baselineColumns("phone").baselineValues("6500008069")
        .go();
  }

  @Test
  public void TestCoveringPlanSortRemovedWithSimpleFields() throws Exception {
    String query = "SELECT t.driverlicense as l FROM hbase.`index_test_primary` as t " +
        " where t.driverlicense < 100000003 order by t.driverlicense";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {".*JsonTableGroupScan.*tableName=.*index_test_primary.*indexName="},
        new String[]{"Sort"}
    );

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("l").baselineValues(100000000l)
        .baselineColumns("l").baselineValues(100000001l)
        .baselineColumns("l").baselineValues(100000002l)
        .go();
  }

  @Test
  public void TestNonCoveringPlanSortRemoved() throws Exception {
    String query = "SELECT t.contact.phone as phone FROM hbase.`index_test_primary` as t " +
        " where t.driverlicense < 100000003 order by t.driverlicense";
    test(defaultHavingIndexPlan+";"+ highFTSFactor +";");
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"RowKeyJoin(.*[\n\r])+.*RestrictedJsonTableGroupScan(.*[\n\r])+.*JsonTableGroupScan.*indexName="},
        new String[]{"Sort"}
    );
    String query2 = "SELECT t.name.fname as fname FROM hbase.`index_test_primary` as t " +
        " where t.id.ssn < '100000003' order by t.id.ssn";
    PlanTestBase.testPlanMatchingPatterns(query2,
        new String[] {"RowKeyJoin(.*[\n\r])+.*RestrictedJsonTableGroupScan(.*[\n\r])+.*JsonTableGroupScan.*indexName="},
        new String[]{"Sort"}
    );

    //simple field, driverlicense
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("phone").baselineValues("6500008069")
        .baselineColumns("phone").baselineValues("6500001411")
        .baselineColumns("phone").baselineValues("6500001595")
        .go();

    //query on field of item expression(having capProject), non-simple field t.id.ssn
    testBuilder()
        .sqlQuery(query2)
        .ordered()
        .baselineColumns("fname").baselineValues("VcFahj")
        .baselineColumns("fname").baselineValues("WbKVK")
        .baselineColumns("fname").baselineValues("vSAEsyFN")
        .go();

    test(sliceTargetSmall);
    try {
      PlanTestBase.testPlanMatchingPatterns(query2,
          new String[]{"SingleMergeExchange(.*[\n\r])+.*"
              + "RowKeyJoin(.*[\n\r])+.*RestrictedJsonTableGroupScan(.*[\n\r])+.*JsonTableGroupScan.*indexName="},
          new String[]{"Sort"}
      );
    } finally {
      test(sliceTargetDefault);
      test(defaultFTSFactor);
    }
  }

  @Test  // 2 table join, each table has local predicate on top-level column
  public void TestCoveringPlanJoin_1() throws Exception {
    String query = "SELECT count(*) as cnt FROM hbase.`index_test_primary` as t1 " +
        " inner join hbase.`index_test_primary` as t2 on t1.driverlicense = t2.driverlicense " +
        " where t1.driverlicense < 100000003 and t2.driverlicense < 100000003" ;
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {".*JsonTableGroupScan.*tableName=.*index_test_primary.*indexName=",
                      ".*JsonTableGroupScan.*tableName=.*index_test_primary.*indexName="},
        new String[]{}
    );

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("cnt").baselineValues(3L)
        .go();
  }

  @Test  // 2 table join, each table has local predicate on nested column
  public void TestCoveringPlanJoin_2() throws Exception {
    String query = "SELECT count(*) as cnt FROM hbase.`index_test_primary` as t1 " +
        " inner join hbase.`index_test_primary` as t2 on t1.contact.phone = t2.contact.phone " +
        " where t1.id.ssn < '100000003' and t2.id.ssn < '100000003' ";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {".*JsonTableGroupScan.*tableName=.*index_test_primary.*indexName=",
                      ".*JsonTableGroupScan.*tableName=.*index_test_primary.*indexName="},
        new String[]{}
    );

    testBuilder()
       .sqlQuery(query)
       .ordered()
       .baselineColumns("cnt").baselineValues(3L)
       .go();
  }

  @Test  // leading prefix of index has Equality conditions and ORDER BY last column; Sort SHOULD be dropped
  public void TestCoveringPlanSortPrefix_1() throws Exception {
    String query = "SELECT t.driverlicense FROM hbase.`index_test_primary` as t " +
        " where t.address.state = 'wo' and t.personal.age = 35 and t.driverlicense < 100008000 order by t.driverlicense";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {".*JsonTableGroupScan.*tableName=.*index_test_primary.*indexName="},
        new String[]{"Sort"}
    );

    // compare the results of index plan with the no-index plan
    testBuilder()
      .optionSettingQueriesForTestQuery(defaultHavingIndexPlan)
      .optionSettingQueriesForBaseline(noIndexPlan)
      .unOrdered()
      .sqlQuery(query)
      .sqlBaselineQuery(query)
      .build()
      .run();
  }

  @Test  // leading prefix of index has Non-Equality conditions and ORDER BY last column; Sort SHOULD NOT be dropped
  public void TestCoveringPlanSortPrefix_2() throws Exception {
    String query = "SELECT t.driverlicense FROM hbase.`index_test_primary` as t " +
        " where t.address.state = 'wo' and t.personal.age < 35 and t.driverlicense < 100008000 order by t.driverlicense";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"Sort", ".*JsonTableGroupScan.*tableName=.*index_test_primary.*indexName="},
        new String[]{}
    );

    // compare the results of index plan with the no-index plan
    testBuilder()
      .optionSettingQueriesForTestQuery(defaultHavingIndexPlan)
      .optionSettingQueriesForBaseline(noIndexPlan)
      .unOrdered()
      .sqlQuery(query)
      .sqlBaselineQuery(query)
      .build()
      .run();
  }

  @Test  //ORDER BY last two columns not in the indexed order; Sort SHOULD NOT be dropped
  public void TestCoveringPlanSortPrefix_3() throws Exception {
    String query = "SELECT CAST(t.personal.age as VARCHAR) as age, t.driverlicense FROM hbase.`index_test_primary` as t " +
        " where t.address.state = 'wo' and t.personal.age < 35 and t.driverlicense < 100008000 order by t.driverlicense, t.personal.age";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"Sort", ".*JsonTableGroupScan.*tableName=.*index_test_primary.*indexName="},
        new String[]{}
    );

    // compare the results of index plan with the no-index plan
    testBuilder()
        .optionSettingQueriesForTestQuery(defaultHavingIndexPlan)
        .optionSettingQueriesForBaseline(noIndexPlan)
        .unOrdered()
        .sqlQuery(query)
        .sqlBaselineQuery(query)
        .build()
        .run();
  }

  @Test  // last two index fields in non-Equality conditions, ORDER BY last two fields; Sort SHOULD be dropped
  public void TestCoveringPlanSortPrefix_4() throws Exception {
    String query = "SELECT t._id as tid, t.driverlicense, CAST(t.personal.age as VARCHAR) as age FROM hbase.`index_test_primary` as t " +
        " where t.address.state = 'wo' and t.personal.age < 35 and t.driverlicense < 100008000 order by t.personal.age, t.driverlicense";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {".*JsonTableGroupScan.*tableName=.*index_test_primary.*indexName="},
        new String[]{"Sort"}
    );

    // compare the results of index plan with the no-index plan
    testBuilder()
        .optionSettingQueriesForTestQuery(defaultHavingIndexPlan)
        .optionSettingQueriesForBaseline(noIndexPlan)
        .unOrdered()
        .sqlQuery(query)
        .sqlBaselineQuery(query)
        .build()
        .run();
  }

  @Test
  public void orderByCastCoveringPlan() throws Exception {
    String query = "SELECT t.contact.phone as phone FROM hbase.`index_test_primary` as t " +
        " where CAST(t.id.ssn as INT) < 100000003 order by CAST(t.id.ssn as INT)";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {".*JsonTableGroupScan.*tableName=.*index_test_primary.*indexName="},
        new String[]{"Sort"}
    );

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("phone").baselineValues("6500008069")
        .baselineColumns("phone").baselineValues("6500001411")
        .baselineColumns("phone").baselineValues("6500001595")
        .go();
  }

  @Test // non-covering plan. sort by the only indexed field, sort SHOULD be removed
  public void orderByNonCoveringPlan() throws Exception {
    String query = "SELECT t.name.lname as lname FROM hbase.`index_test_primary` as t " +
        " where t.id.ssn < '100000003' order by t.id.ssn";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"RowKeyJoin(.*[\n\r])+.*RestrictedJsonTableGroupScan(.*[\n\r])+.*JsonTableGroupScan.*indexName="},
        new String[]{"Sort"}
    );

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("lname").baselineValues("iuMG")
        .baselineColumns("lname").baselineValues("KpFq")
        .baselineColumns("lname").baselineValues("bkkAvz")
        .go();
  }

  @Test //non-covering plan. order by cast indexed field, sort SHOULD be removed
  public void orderByCastNonCoveringPlan() throws Exception {
    String query = "SELECT t.name.lname as lname FROM hbase.`index_test_primary` as t " +
        " where CAST(t.id.ssn as INT) < 100000003 order by CAST(t.id.ssn as INT)";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"RowKeyJoin(.*[\n\r])+.*RestrictedJsonTableGroupScan(.*[\n\r])+.*JsonTableGroupScan.*indexName="},
        new String[]{"Sort"}
    );

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("lname").baselineValues("iuMG")
        .baselineColumns("lname").baselineValues("KpFq")
        .baselineColumns("lname").baselineValues("bkkAvz")
        .go();
  }


  @Test //non-covering, order by non leading field, and leading fields are not in equality condition, Sort SHOULD NOT be removed
  public void NonCoveringPlan_SortPrefix_1() throws Exception {

    String query = "SELECT t.`id`.`ssn` AS `ssn` FROM hbase.`index_test_primary` as t " +
        " where t.address.state > 'pc' AND t.address.city>'pfrrr' AND t.address.city<'pfrrt' order by t.adddress.city";
    test(defaultHavingIndexPlan+";"+ highFTSFactor +";");
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {"Sort",
            "RowKeyJoin(.*[\n\r])+.*RestrictedJsonTableGroupScan(.*[\n\r])+.*JsonTableGroupScan.*indexName="},
        new String[]{}
    );
    test(defaultFTSFactor);
    return;
  }

  @Test //non-covering, order by non leading field, and leading fields are in equality condition, Sort SHOULD be removed
  public void NonCoveringPlan_SortPrefix_2() throws Exception {

    String query = "SELECT t.`id`.`ssn` AS `ssn` FROM hbase.`index_test_primary` as t " +
        " where t.address.state = 'pc' AND t.address.city>'pfrrr' AND t.address.city<'pfrrt' order by t.address.city";
    test(defaultHavingIndexPlan+";"+ highFTSFactor +";");
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {
            "RowKeyJoin(.*[\n\r])+.*RestrictedJsonTableGroupScan(.*[\n\r])+.*JsonTableGroupScan.*indexName="},
        new String[]{"Sort"}
    );
    test(defaultFTSFactor);
    return;
  }

  @Test //Correct projection and results when filter on non-indexed column in covering plan.
  public void nonIndexedColumnFilterCoveringPlan() throws Exception {
    String query = "SELECT t.name.fname as fname FROM hbase.`index_test_primary` as t " +
        " where t.personal.age > 68 and t.name.fname IN ('CnGobfR', 'THOHP')";
    test(defaultHavingIndexPlan);
    PlanTestBase.testPlanMatchingPatterns(query,
        new String[] {".*Filter.*CnGobfR.*THOHP.*",
            ".*JsonTableGroupScan.*tableName=.*index_test_primary.*indexName="},
        new String[] {".*Filter.*ITEM*CnGobfR.*THOHP.*"});

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("fname").baselineValues("CnGobfR")
        .baselineColumns("fname").baselineValues("THOHP")
        .baselineColumns("fname").baselineValues("CnGobfR")
        .go();
  }
}
