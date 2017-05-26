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

import org.apache.drill.PlanTestBase;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.mapr.tests.annotations.ClusterTest;

@Category(ClusterTest.class)
public class TestQueryWithIndex extends BaseJsonTest {

  @Test
  public void testSelectWithIndex() throws Exception {
    final String sql = "SELECT\n"
        + "  _id, t.name.last\n"
        + "FROM\n"
        + "  hbase.`drill_test_table_with_index` t\n"
        + "WHERE t.name.last = 'Harris2345'";
    runSQLAndVerifyCount(sql, 1);

    // plan test
    final String[] expectedPlan = {"indexName=testindex"};
    final String[] excludedPlan = {};

    PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);
  }

}
