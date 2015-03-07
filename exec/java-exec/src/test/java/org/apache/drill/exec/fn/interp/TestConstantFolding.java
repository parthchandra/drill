/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.fn.interp;

import java.io.File;
import java.io.PrintWriter;

import org.apache.drill.PlanTestBase;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

@Ignore
public class TestConstantFolding extends PlanTestBase {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  // This should run as a @BeforeClass, but these methods must be defined static.
  // Unfortunately, the temporary folder with an @Rule annotation cannot be static, this issue
  // has been fixed in a newer version of JUnit
  // http://stackoverflow.com/questions/2722358/junit-rule-temporaryfolder
  public void createFiles() throws Exception{
    File bigFolder = folder.newFolder("bigfile");
    File bigFile = new File (bigFolder, "bigfile.csv");
    PrintWriter out = new PrintWriter(bigFile);
    out.println("1,2,3");
    out.println("1,2,3");
    out.println("1,2,3");
    out.println("1,2,3");
    out.println("1,2,3");
    out.println("1,2,3");
    out.close();

    File smallFolder = folder.newFolder("smallfile");
    File smallFile = new File (smallFolder, "smallfile.csv");
    out = new PrintWriter(smallFile);
    out.println("1,2,3");
    out.close();
  }

  @Ignore("WIP")
  @Test
  public void testConstantFolding_allTypes() throws Exception {

    test("alter system set `store.json.all_text_mode` = true;");

    String query2 = "SELECT *  " +
        "FROM   cp.`/parquet/alltypes.json`  " +
        "WHERE  cast( `int_col` AS             int) = castint('1')  " +
        "AND    cast( `bigint_col` AS          bigint) = castbigint('100000000000')  " +
        // TODO - fix, currently using approximate literals
//        "AND    cast( `decimal9_col` AS        decimal(9, 4)) = 1.0 + 0.0  " +
//        "AND    cast( `decimal18_col` AS       decimal(18,9)) = 123456789.000000000 + 0.0  " +
//        "AND    cast( `decimal28sparse_col` AS decimal(28, 14)) = 123456789.000000000 + 0.0 " +
//        "AND    cast( `decimal38sparse_col` AS decimal(38, 19)) = 123456789.000000000 + 0.0 " +

        // RETURNS 0 ROWS, folds that cast to: cast( 788947200000 as DATE). Interpreting the int as a date
        // gives some day in 1994
//        "AND    cast( `date_col` AS            date) = castdate('1995-01-01')  " +

        "AND    cast( `date_col` AS            date) = cast('1995-01-01' as date)  " +

        // THIS WORKS, RETURNS ONE RECORD
//        "AND    cast( `date_col` AS            date) = DATE '1995-01-01'  " +

//        "AND    cast( `time_col` AS            time) = casttime('01:00:00')  " +
//        "AND    cast( `timestamp_col` AS timestamp) = casttimestamp('1995-01-01 01:00:10.000')  " +
//        "AND    cast( `float4_col` AS float) = castfloat4('1')  " +
//        "AND    cast( `float8_col` AS DOUBLE) = castfloat8('1')  " +
        // TODO - fix, evaluation issues, looks like implicit casts are being added?
//        "AND    cast( `bit_col` AS       boolean) = castbit('false')  " +
//        "AND  `varchar_col` = concat('qwe','rty')  " +
//        "AND    cast( `varbinary_col` AS varbinary(65000)) = castvarbinary('qwerty', 0)  " +
        "AND    cast( `intervalyear_col` AS interval year) = castintervalyear('P1Y')  " +
        "AND    cast( `intervalday_col` AS interval day) = castintervalday('P1D')";

//        "SELECT " +
//        "       Cast( `int_col` AS             INT)             int_col,  " +
//        "       castBIGINT( `bigint_col`)          bigint_col  " +
//        "       Cast( `decimal9_col` AS        DECIMAL)         decimal9_col,  " +
//        "       Cast( `decimal18_col` AS       DECIMAL(18,9))   decimal18_col,  " +
//        "       Cast( `decimal28sparse_col` AS DECIMAL(28, 14)) decimal28sparse_col,  " +
//        "       Cast( `decimal38sparse_col` AS DECIMAL(38, 19)) decimal38sparse_col,  " +
//        "       Cast( `date_col` AS            DATE)            date_col,  " +
//        "       Cast( `time_col` AS            TIME)            time_col,  " +
//        "       Cast( `timestamp_col` AS TIMESTAMP)             timestamp_col,  " +
//        "       Cast( `float4_col` AS FLOAT)                    float4_col,  " +
//        "       Cast( `float8_col` AS DOUBLE)                   float8_col,  " +
//        "       Cast( `bit_col` AS       BOOLEAN)                     bit_col,  " +
//        "       Cast( `varchar_col` AS   VARCHAR(65000))              varchar_col,  " +
//        "       `varbinary_col`            varbinary_col,  " +
//        "       cast( `intervalyear_col` as INTERVAL YEAR)            intervalyear_col,  " +
//        "       cast( `intervalday_col` as INTERVAL DAY )              intervalday_col  " +
//        "FROM   cp.`/parquet/alltypes.json`  " +
//        "WHERE  `int_col` = 1 + 0 " +
//        "AND    cast(`bigint_col` as BIGINT) = 50000000000 + 50000000000  " +
//        "AND    cast(`decimal9_col` as decimal) = cast( '1.0' AS                        decimal)  " +
//        "AND     Cast( `decimal18_col` AS       DECIMAL(18,9))   = cast( '123456789.000000000' AS       decimal(18,9))  " +
//        "AND     Cast( `decimal28sparse_col` AS DECIMAL(28, 14)) = cast( '123456789.000000000' AS decimal(28, 14))  " +
//        "AND     Cast( `decimal38sparse_col` AS DECIMAL(38, 19)) = cast( '123456789.000000000' AS decimal(38, 19))  " +
//        "AND     Cast( `date_col` AS            DATE)            = cast( '1995-01-01' AS                     date)  " +
//        "AND     Cast( `time_col` AS            TIME)            = cast( '01:00:00' AS                       time)  " +
//        "AND     Cast( `timestamp_col` AS TIMESTAMP)             = cast( '1995-01-01 01:00:10.000' AS timestamp)  " +
//        "AND     Cast( `float4_col` AS FLOAT)                    = cast( '1' AS float)  " +
//        "AND     Cast( `float8_col` AS DOUBLE)                   = cast( '1' AS DOUBLE)  " +
//        "AND     Cast( `bit_col` AS       BOOLEAN)               = cast( 'false' AS        boolean)  " +
//        "AND     Cast( `varchar_col` AS   VARCHAR(65000))        = cast( 'qwerty' AS   varchar(65000))  " +
//        // TODO - the normal cast syntax does not work for this
//        "AND     castVARBINARY(`varbinary_col`, 0)     = castVARBINARY('qwerty', 0)  " +
//        "AND     cast( `intervalyear_col` as INTERVAL YEAR)      = converttonullableintervalyear( 'P1Y')  " +
//        "AND     cast( `intervalday_col` as INTERVAL DAY )       = converttonullableintervalday( 'P1D' )"
//        ;


    test(query2);
  }

  @Test
  public void testConstExprFolding_withPartitionPrune() throws Exception {
    createFiles();
    String path = folder.getRoot().toPath().toString();
    testPlanOneExpectedPatternOneExcluded(
        "select * from dfs.`" + path + "/*/*.csv` where dir0 = concat('small','file')",
        "smallfile",
        "bigfile");
  }

  @Test
  public void testConstExprFolding_nonDirFilter() throws Exception {
    testPlanOneExpectedPatternOneExcluded(
        "select * from cp.`functions/interp/test_input.csv` where columns[0] = 2+2",
        "Filter\\(condition=\\[=\\(ITEM\\(\\$[0-9]+, 0\\), 4\\)",
        "Filter\\(condition=\\[=\\(ITEM\\(\\$[0-9]+, 0\\), \\+\\(2, 2\\)\\)");
  }

  @Test
  public void testConstExprFolding_dontFoldRandom() throws Exception {
    testPlanOneExpectedPatternOneExcluded(
        "select * from cp.`functions/interp/test_input.csv` where columns[0] = random()",
        "Filter\\(condition=\\[=\\(ITEM\\(\\$[0-9]+, 0\\), RANDOM\\(\\)",
        "Filter\\(condition=\\[=\\(ITEM\\(\\$[0-9]+, 0\\), [0-9\\.]+");
  }

  @Test
  public void testConstExprFolding_ToLimit0() throws Exception {
    testPlanOneExpectedPatternOneExcluded(
        "select * from cp.`functions/interp/test_input.csv` where 1=0",
        "Limit\\(offset=\\[0\\], fetch=\\[0\\]\\)",
        "Filter\\(condition=\\[=\\(1, 0\\)\\]\\)");
  }

  // Despite a comment indicating that the plan generated by the ReduceExpressionRule
  // should be set to be always preferable to the input rel, I cannot get it to
  // produce a plan with the reduced result. I can trace through where the rule is fired
  // and I can see that the expression is being evaluated and the constant is being
  // added to a project, but this is not part of the final plan selected. May
  // need to open a calcite bug.
  // Tried to disable the calc and filter rules, only leave the project one, didn't help.
  @Ignore("DRILL-2218")
  @Test
  public void testConstExprFolding_InSelect() throws Exception {
    testPlanOneExcludedPattern("select columns[0], 3+5 from cp.`functions/interp/test_input.csv`",
        "EXPR\\$[0-9]+=\\[\\+\\(3, 5\\)\\]");
  }
}
