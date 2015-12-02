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
package org.apache.drill.exec.physical.impl.limit;

import com.google.common.collect.Maps;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.PlanTestBase;
import org.apache.drill.TestBuilder;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;

public class TestLimit0 extends BaseTestQuery {

  private static final String viewName = "limitZeroEmployeeView";

  private static String wrapLimit0(final String query) {
    return "SELECT * FROM (" + query + ") LZT LIMIT 0";
  }

  @BeforeClass
  public static void createView() throws Exception {
    test("USE dfs_test.tmp");
    test(String.format("CREATE OR REPLACE VIEW %s AS SELECT " +
        "CAST(employee_id AS INT) AS employee_id, " +
        "CAST(full_name AS VARCHAR(25)) AS full_name, " +
        "CAST(position_id AS INTEGER) AS position_id, " +
        "CAST(department_id AS BIGINT) AS department_id," +
        "CAST(birth_date AS DATE) AS birth_date, " +
        "CAST(hire_date AS TIMESTAMP) AS hire_date, " +
        "CAST(salary AS DOUBLE) AS salary, " +
        "CAST(salary AS FLOAT) AS fsalary, " +
        "CAST((CASE WHEN marital_status = 'S' THEN true ELSE false END) AS BOOLEAN) AS single, " +
        "CAST(education_level AS VARCHAR(60)) AS education_level," +
        "CAST(gender AS CHAR) AS gender " +
        "FROM cp.`employee.json` " +
        "ORDER BY employee_id " +
        "LIMIT 1;", viewName));
    // { "employee_id":1,"full_name":"Sheri Nowmer","first_name":"Sheri","last_name":"Nowmer","position_id":1,
    // "position_title":"President","store_id":0,"department_id":1,"birth_date":"1961-08-26",
    // "hire_date":"1994-12-01 00:00:00.0","end_date":null,"salary":80000.0000,"supervisor_id":0,
    // "education_level":"Graduate Degree","marital_status":"S","gender":"F","management_role":"Senior Management" }
  }

  @AfterClass
  public static void tearDownView() throws Exception {
    test("DROP VIEW " + viewName + ";");
  }

  // -------------------- SIMPLE QUERIES --------------------

  @Test
  public void infoSchema() throws Exception {
    testBuilder()
        .sqlQuery(String.format("DESCRIBE %s", viewName))
        .unOrdered()
        .baselineColumns("COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE")
        .baselineValues("employee_id", "INTEGER", "YES")
        .baselineValues("full_name", "CHARACTER VARYING", "YES")
        .baselineValues("position_id", "INTEGER", "YES")
        .baselineValues("department_id", "BIGINT", "YES")
        .baselineValues("birth_date", "DATE", "YES")
        .baselineValues("hire_date", "TIMESTAMP", "YES")
        .baselineValues("salary", "DOUBLE", "YES")
        .baselineValues("fsalary", "FLOAT", "YES")
        .baselineValues("single", "BOOLEAN", "NO")
        .baselineValues("education_level", "CHARACTER VARYING", "YES")
        .baselineValues("gender", "CHARACTER", "YES")
        .go();
  }

  @Test
  public void simpleSelect() throws Exception {
    testBuilder()
        .sqlQuery(String.format("SELECT * FROM %s", viewName))
        .ordered()
        .baselineColumns("employee_id", "full_name", "position_id", "department_id", "birth_date", "hire_date",
            "salary", "fsalary", "single", "education_level", "gender")
        .baselineValues(1, "Sheri Nowmer", 1, 1L, new DateTime("1961-08-26T00:00:00.000-07:00"),
            new DateTime("1994-12-01T00:00:00.000-08:00"), 80000.0D, 80000.0F, true, "Graduate Degree", "F")
        .go();
  }

  @Test
  public void simpleSelectLimit0() throws Exception {

    final TypeProtos.MajorType[] types = new TypeProtos.MajorType[] {
        Types.optional(TypeProtos.MinorType.INT), Types.optional(TypeProtos.MinorType.VARCHAR),
        Types.optional(TypeProtos.MinorType.INT), Types.optional(TypeProtos.MinorType.BIGINT),
        Types.optional(TypeProtos.MinorType.DATE), Types.optional(TypeProtos.MinorType.TIMESTAMP),
        Types.optional(TypeProtos.MinorType.FLOAT8), Types.optional(TypeProtos.MinorType.FLOAT4),
        Types.required(TypeProtos.MinorType.BIT), Types.optional(TypeProtos.MinorType.VARCHAR),
        Types.optional(TypeProtos.MinorType.VARCHAR)
    };

    testBuilder()
        .sqlQuery(wrapLimit0(String.format("SELECT * FROM %s", viewName)))
        .baselineColumns("employee_id", "full_name", "position_id", "department_id", "birth_date", "hire_date",
            "salary", "fsalary", "single", "education_level", "gender")
        .ordered()
        .csvBaselineFile("limit0/empty.tsv")
        .baselineTypes(types)
        .expectsEmptyResultSet()
        .go();
  }

  @Test
  public void simpleSelectLimit0Plan() throws Exception {
    checkThatQueryPlanIsOptimized("SELECT * FROM " + viewName);
  }

  private void checkThatQueryPlanIsOptimized(final String query) throws Exception {
    PlanTestBase.testPlanMatchingPatterns(
        wrapLimit0(query),
        new String[]{
            ".*Project.*\n" +
                ".*Scan.*RelDataTypeReader.*"
        },
        new String[]{});
  }

  private void checkThatQueryPlanIsNotOptimized(final String query) throws Exception {
    PlanTestBase.testPlanMatchingPatterns(
        wrapLimit0(query),
        new String[]{},
        new String[]{
            ".*Project.*\n" +
                ".*Scan.*RelDataTypeReader.*"
        });
  }

  // -------------------- AGGREGATE FUNC. QUERIES --------------------

  private static String getAggQuery(final String functionName) {
    return "SELECT " +
        functionName + "(employee_id) AS e, " +
        functionName + "(position_id) AS p, " +
        functionName + "(department_id) AS d, " +
        functionName + "(salary) AS s, " +
        functionName + "(fsalary) AS f " +
        "FROM " + viewName;
  }

  @Test
  public void sums() throws Exception {
    final String query = getAggQuery("SUM");

    final TypeProtos.MajorType[] types = new TypeProtos.MajorType[] {
        Types.optional(TypeProtos.MinorType.BIGINT), Types.optional(TypeProtos.MinorType.BIGINT),
        Types.optional(TypeProtos.MinorType.BIGINT), Types.optional(TypeProtos.MinorType.FLOAT8),
        Types.optional(TypeProtos.MinorType.FLOAT8) };

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .csvBaselineFile("limit0/sums.tsv")
        .baselineTypes(types)
        .baselineColumns("e", "p", "d", "s", "f")
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .expectsEmptyResultSet()
        .csvBaselineFile("limit0/empty.tsv")
        .baselineTypes(types)
        .baselineColumns("e", "p", "d", "s", "f")
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void counts() throws Exception {
    final String query = getAggQuery("COUNT");

    final Map<SchemaPath, TypeProtos.MajorType> types = Maps.newHashMap();
    types.put(TestBuilder.parsePath("e"), Types.required(TypeProtos.MinorType.BIGINT));
    types.put(TestBuilder.parsePath("p"), Types.required(TypeProtos.MinorType.BIGINT));
    types.put(TestBuilder.parsePath("d"), Types.required(TypeProtos.MinorType.BIGINT));
    types.put(TestBuilder.parsePath("s"), Types.required(TypeProtos.MinorType.BIGINT));
    types.put(TestBuilder.parsePath("f"), Types.required(TypeProtos.MinorType.BIGINT));

    testBuilder()
        .sqlQuery(query)
        .baselineColumns("e", "p", "d", "s", "f")
        .ordered()
        .baselineTypes(types)
        .baselineValues(1L, 1L, 1L, 1L, 1L)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .baselineColumns("e", "p", "d", "s", "f")
        .baselineTypes(types)
        .expectsEmptyResultSet()
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  private void minAndMaxTest(final String functionName) throws Exception {
    final String query = getAggQuery(functionName);

    final Map<SchemaPath, TypeProtos.MajorType> types = Maps.newHashMap();
    types.put(TestBuilder.parsePath("e"), Types.optional(TypeProtos.MinorType.INT));
    types.put(TestBuilder.parsePath("p"), Types.optional(TypeProtos.MinorType.INT));
    types.put(TestBuilder.parsePath("d"), Types.optional(TypeProtos.MinorType.BIGINT));
    types.put(TestBuilder.parsePath("s"), Types.optional(TypeProtos.MinorType.FLOAT4));
    types.put(TestBuilder.parsePath("f"), Types.optional(TypeProtos.MinorType.FLOAT8));

    testBuilder()
        .sqlQuery(query)
        .baselineColumns("e", "p", "d", "s", "f")
        .ordered()
        .baselineTypes(types)
        .baselineValues(1, 1, 1L, 80_000D, 80_000F)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .baselineColumns("e", "p", "d", "s", "f")
        .baselineTypes(types)
        .expectsEmptyResultSet()
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void mins() throws Exception {
    minAndMaxTest("MIN");
  }

  @Test
  public void maxs() throws Exception {
    minAndMaxTest("MAX");
  }

  @Test
  public void avgs() throws Exception {
    final String query = getAggQuery("AVG");

    final TypeProtos.MajorType[] types = new TypeProtos.MajorType[] {
        Types.optional(TypeProtos.MinorType.FLOAT8), Types.optional(TypeProtos.MinorType.FLOAT8),
        Types.optional(TypeProtos.MinorType.FLOAT8), Types.optional(TypeProtos.MinorType.FLOAT8),
        Types.optional(TypeProtos.MinorType.FLOAT8) };

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .csvBaselineFile("limit0/avgs.tsv")
        .baselineTypes(types)
        .baselineColumns("e", "p", "d", "s", "f")
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .expectsEmptyResultSet()
        .csvBaselineFile("limit0/empty.tsv")
        .baselineTypes(types)
        .baselineColumns("e", "p", "d", "s", "f")
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void measures() throws Exception {
    final String query = "SELECT " +
        "STDDEV_SAMP(employee_id) AS s, " +
        "STDDEV_POP(position_id) AS p, " +
        "AVG(position_id) AS a, " +
        "COUNT(position_id) AS c " +
        "FROM " + viewName;

    final Map<SchemaPath, TypeProtos.MajorType> types = Maps.newHashMap();
    types.put(TestBuilder.parsePath("s"), Types.optional(TypeProtos.MinorType.FLOAT8));
    types.put(TestBuilder.parsePath("p"), Types.optional(TypeProtos.MinorType.FLOAT8));
    types.put(TestBuilder.parsePath("a"), Types.optional(TypeProtos.MinorType.FLOAT8));
    types.put(TestBuilder.parsePath("c"), Types.required(TypeProtos.MinorType.BIGINT));

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("s", "p", "a", "c")
        .baselineTypes(types)
        .baselineValues(null, 0.0D, 1.0D, 1L)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .baselineColumns("s", "p", "a", "c")
        .baselineTypes(types)
        .expectsEmptyResultSet()
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void nullableCount() throws Exception {
    final String query = "SELECT " +
        "COUNT(CASE WHEN position_id = 1 THEN NULL ELSE position_id END) AS c FROM " + viewName;

    final Map<SchemaPath, TypeProtos.MajorType> types = Maps.newHashMap();
    types.put(TestBuilder.parsePath("c"), Types.required(TypeProtos.MinorType.BIGINT));
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("c")
        .baselineTypes(types)
        .baselineValues(0L)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .baselineColumns("c")
        .baselineTypes(types)
        .expectsEmptyResultSet()
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void nullableSumAndCount() throws Exception {
    final String query = "SELECT " +
        "COUNT(position_id) AS c, " +
        "SUM(CAST((CASE WHEN position_id = 1 THEN NULL ELSE position_id END) AS INT)) AS p " +
        "FROM " + viewName;

    final Map<SchemaPath, TypeProtos.MajorType> types = Maps.newHashMap();
    types.put(TestBuilder.parsePath("c"), Types.required(TypeProtos.MinorType.BIGINT));
    types.put(TestBuilder.parsePath("p"), Types.optional(TypeProtos.MinorType.BIGINT));
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("c", "p")
        .baselineTypes(types)
        .baselineValues(1L, null)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .baselineColumns("c", "p")
        .baselineTypes(types)
        .expectsEmptyResultSet()
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void castSum() throws Exception {
    final String query = "SELECT CAST(SUM(position_id) AS INT) AS s FROM cp.`employee.json`";

    final Map<SchemaPath, TypeProtos.MajorType> types = Maps.newHashMap();
    types.put(TestBuilder.parsePath("s"), Types.optional(TypeProtos.MinorType.INT));
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("s")
        .baselineTypes(types)
        .baselineValues(18422)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .baselineColumns("s")
        .baselineTypes(types)
        .expectsEmptyResultSet()
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void sumCast() throws Exception {
    final String query = "SELECT SUM(CAST(position_id AS INT)) AS s FROM cp.`employee.json`";

    final Map<SchemaPath, TypeProtos.MajorType> types = Maps.newHashMap();
    types.put(TestBuilder.parsePath("s"), Types.optional(TypeProtos.MinorType.BIGINT));
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("s")
        .baselineTypes(types)
        .baselineValues(18422L)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .baselineColumns("s")
        .baselineTypes(types)
        .expectsEmptyResultSet()
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test // negative aggregation test case
  public void rank() throws Exception {
    final String query = "SELECT RANK() OVER(PARTITION BY employee_id ORDER BY employee_id) AS r FROM " + viewName;

    final Map<SchemaPath, TypeProtos.MajorType> types = Maps.newHashMap();
    types.put(TestBuilder.parsePath("e"), Types.optional(TypeProtos.MinorType.BIGINT));
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("r")
        .baselineTypes(types)
        .baselineValues(1L)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .baselineColumns("r")
        .baselineTypes(types)
        .expectsEmptyResultSet()
        .go();

    checkThatQueryPlanIsNotOptimized(query);
  }

  // -------------------- SCALAR FUNC. QUERIES --------------------

  @Test
  public void cast() throws Exception {
    final String query = "SELECT CAST(fsalary AS DOUBLE) AS d," +
        "CAST(employee_id AS BIGINT) AS e FROM " + viewName;

    final Map<SchemaPath, TypeProtos.MajorType> types = Maps.newHashMap();
    types.put(TestBuilder.parsePath("d"), Types.optional(TypeProtos.MinorType.FLOAT8));
    types.put(TestBuilder.parsePath("e"), Types.optional(TypeProtos.MinorType.BIGINT));

    testBuilder()
        .sqlQuery(query)
        .baselineColumns("d", "e")
        .ordered()
        .baselineTypes(types)
        .baselineValues(80_000D, 1L)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .baselineColumns("d")
        .baselineTypes(types)
        .expectsEmptyResultSet()
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  public void concatTest(final String query) throws Exception {
    final Map<SchemaPath, TypeProtos.MajorType> types = Maps.newHashMap();
    types.put(TestBuilder.parsePath("c"), Types.optional(TypeProtos.MinorType.VARCHAR));

    testBuilder()
        .sqlQuery(query)
        .baselineColumns("c")
        .ordered()
        .baselineTypes(types)
        .baselineValues("Sheri NowmerGraduate Degree")
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .baselineColumns("c")
        .baselineTypes(types)
        .expectsEmptyResultSet()
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  @Ignore("Drill UDFs are currently not optimized.")
  public void concat() throws Exception {
    concatTest("SELECT CONCAT(full_name, education_level) AS c FROM " + viewName);
  }

  @Test
  public void concatOp() throws Exception {
    concatTest("SELECT full_name || education_level AS c FROM " + viewName);
  }

  @Test
  public void extract() throws Exception {
    final String query = "SELECT EXTRACT(YEAR FROM hire_date) AS e FROM " + viewName;

    final Map<SchemaPath, TypeProtos.MajorType> types = Maps.newHashMap();
    types.put(TestBuilder.parsePath("e"), Types.optional(TypeProtos.MinorType.BIGINT));

    testBuilder()
        .sqlQuery(query)
        .baselineColumns("e")
        .ordered()
        .baselineTypes(types)
        .baselineValues(1994L)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .baselineColumns("e")
        .baselineTypes(types)
        .expectsEmptyResultSet()
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void binary() throws Exception {
    final String query = "SELECT " +
        "single AND true AS b, " +
        "full_name || education_level AS c, " +
        "position_id / position_id AS d, " +
        "position_id = position_id AS e, " +
        "position_id > position_id AS g, " +
        "position_id >= position_id AS ge, " +
        "position_id IN (0, 1) AS i, +" +
        "position_id < position_id AS l, " +
        "position_id <= position_id AS le, " +
        "position_id - position_id AS m, " +
        "position_id * position_id AS mu, " +
        "position_id <> position_id AS n, " +
        "single OR false AS o, " +
        "position_id + position_id AS p FROM " + viewName;

    final Map<SchemaPath, TypeProtos.MajorType> types = Maps.newHashMap();
    types.put(TestBuilder.parsePath("b"), Types.required(TypeProtos.MinorType.BIT));
    types.put(TestBuilder.parsePath("c"), Types.optional(TypeProtos.MinorType.VARCHAR));
    types.put(TestBuilder.parsePath("d"), Types.optional(TypeProtos.MinorType.INT));
    types.put(TestBuilder.parsePath("e"), Types.optional(TypeProtos.MinorType.BIT));
    types.put(TestBuilder.parsePath("g"), Types.optional(TypeProtos.MinorType.BIT));
    types.put(TestBuilder.parsePath("ge"), Types.optional(TypeProtos.MinorType.BIT));
    types.put(TestBuilder.parsePath("i"), Types.optional(TypeProtos.MinorType.BIT));
    types.put(TestBuilder.parsePath("l"), Types.optional(TypeProtos.MinorType.BIT));
    types.put(TestBuilder.parsePath("le"), Types.optional(TypeProtos.MinorType.BIT));
    types.put(TestBuilder.parsePath("m"), Types.optional(TypeProtos.MinorType.INT));
    types.put(TestBuilder.parsePath("mu"), Types.optional(TypeProtos.MinorType.INT));
    types.put(TestBuilder.parsePath("n"), Types.optional(TypeProtos.MinorType.BIT));
    types.put(TestBuilder.parsePath("o"), Types.required(TypeProtos.MinorType.BIT));
    types.put(TestBuilder.parsePath("p"), Types.optional(TypeProtos.MinorType.INT));

    testBuilder()
        .sqlQuery(query)
        .baselineColumns("b", "c", "d", "e", "g", "ge", "i", "l", "le", "m", "mu", "n", "o", "p")
        .ordered()
        .baselineTypes(types)
        .baselineValues(true, "Sheri NowmerGraduate Degree", 1, true, false, true, true, false, true,
            0, 1, false, true, 2)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .baselineColumns("b", "c", "d", "e", "g", "ge", "i", "l", "le", "m", "mu", "n", "o", "p")
        .baselineTypes(types)
        .expectsEmptyResultSet()
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void substring() throws Exception {
    final String query = "SELECT SUBSTRING(full_name, 1, 5) AS s FROM " + viewName;
    final Map<SchemaPath, TypeProtos.MajorType> types = Maps.newHashMap();
    types.put(TestBuilder.parsePath("s"), Types.optional(TypeProtos.MinorType.VARCHAR));

    testBuilder()
        .sqlQuery(query)
        .baselineColumns("s")
        .ordered()
        .baselineTypes(types)
        .baselineValues("Sheri")
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .baselineColumns("s")
        .baselineTypes(types)
        .expectsEmptyResultSet()
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test // negative scalar func. test case
  public void substr() throws Exception {
    final String query = "SELECT SUBSTR(full_name, 1, 5) AS s FROM " + viewName;

    final Map<SchemaPath, TypeProtos.MajorType> types = Maps.newHashMap();
    types.put(TestBuilder.parsePath("s"), Types.optional(TypeProtos.MinorType.VARCHAR));

    testBuilder()
        .sqlQuery(query)
        .baselineColumns("s")
        .ordered()
        .baselineTypes(types)
        .baselineValues("Sheri")
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .baselineColumns("s")
        .baselineTypes(types)
        .expectsEmptyResultSet()
        .go();

    checkThatQueryPlanIsNotOptimized(query);
  }
}
