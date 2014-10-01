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
package org.apache.drill.rdd;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.drill.exec.store.spark.RDDTableSpec;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestSqlAnalyzer {

  @Test
  public void testNoExpansionQuery() throws Exception {
    SqlAnalyzer sqlAnalyzer = new SqlAnalyzer("select * from kv", ImmutableSet.<String>of());
    assertFalse(sqlAnalyzer.needsSqlExpansion());
  }

  @Test
  public void testSimpleExpansionQuery() throws Exception {
    SqlAnalyzer sqlAnalyzer = new SqlAnalyzer("select * from RddTable1", ImmutableSet.<String>of("RDDTABLE1"));
    assertTrue(sqlAnalyzer.needsSqlExpansion());
    Map<String, RDDTableSpec> mapTable2Spec = Maps.newHashMap();
    mapTable2Spec.put("RDDTABLE1", new RDDTableSpec("SC1", 1, new int[] {0, 1, 2}));

    String expectedExpandedQuery = "SELECT *\n" +
        "FROM `{\"scId\":\"SC1\",\"rddId\":1,\"partitionIds\":[0,1,2]}`";
    assertEquals("Expanded query is not valid", expectedExpandedQuery, sqlAnalyzer.getExpandedSql(mapTable2Spec));
  }


  @Test
  public void testMultipleRDDNamesExpansionQuery() throws Exception {
    Set<String> rddTableNameSet = ImmutableSet.<String>of("RDDTABLE1", "RDDTABLE2");
    Map<String, RDDTableSpec> mapTable2Spec = ImmutableMap.of(
        "RDDTABLE1", new RDDTableSpec("SC1", 1, new int[]{0, 1, 2}),
        "RDDTABLE2", new RDDTableSpec("SC1", 2, new int[]{0, 1, 2, 3, 4, 5, 6}));

    SqlAnalyzer sqlAnalyzer = new SqlAnalyzer("select * from RddTable1 join RddTable2 join drillTable1", rddTableNameSet);
    assertTrue(sqlAnalyzer.needsSqlExpansion());

    String expectedExpandedQuery = "SELECT *\n" +
        "FROM `{\"scId\":\"SC1\",\"rddId\":1,\"partitionIds\":[0,1,2]}`\n" +
        "INNER JOIN `{\"scId\":\"SC1\",\"rddId\":2,\"partitionIds\":[0,1,2,3,4,5,6]}`\n" +
        "INNER JOIN `drillTable1`";

    assertEquals("Expanded query is not valid", expectedExpandedQuery, sqlAnalyzer.getExpandedSql(mapTable2Spec));
  }
}