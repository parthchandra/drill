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

package org.apache.drill.exec.store.parquet;

import com.google.common.base.Stopwatch;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.parser.ExprLexer;
import org.apache.drill.common.expression.parser.ExprParser;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.BitControl;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.Assert;

import java.io.IOException;

public class MeasureParquetFilterEve {

  public static void main(String args[]) throws Exception {

    DrillConfig config = DrillConfig.create();
//    DrillClient client = null;

    RemoteServiceSet serviceSet = null;
    Drillbit[] drillbits = null;

    try {
        serviceSet = RemoteServiceSet.getLocalServiceSet();
        drillbits = new Drillbit[1];
        for (int i = 0; i < 1; i++) {
          drillbits[i] = new Drillbit(config, serviceSet);
          drillbits[i].run();
        }
//        client = new DrillClient(config, serviceSet.getCoordinator());

        Drillbit bit = drillbits[0];

        FragmentContext fragmentContext = new FragmentContext(bit.getContext(),
            BitControl.PlanFragment.getDefaultInstance(), null, bit.getContext().getFunctionImplementationRegistry());

        String TEST_RES_PATH = "/Users/jni/work/incubator-drill/exec/java-exec/src/test/resources";

        final String filePath = String.format("%s/parquetFilterPush/intTbl/intTbl.parquet", TEST_RES_PATH);
        ParquetMetadata footer = getParquetMetaData(filePath);

        final int numRun = 50000;

        for (int i = 0 ; i < numRun; i ++) {
          int value = (int) (Math.random() * 100) + 200;
          String filter = "intCol = " + value;
          testParquetRowGroupFilterEval(footer, filter, fragmentContext, true);
        }

      System.out.println("Complete " + numRun + " iterations!");

//      client.connect();


    } catch(Throwable th) {
      System.err.println("Query Failed due to : " + th.getMessage());
    } finally {
//      if (client != null) {
//        client.close();
//      }
      for (Drillbit b : drillbits) {
        b.close();
      }
      serviceSet.close();
    }
  }

  private static void testParquetRowGroupFilterEval(final ParquetMetadata footer, final String exprStr, FragmentContext fragmentContext,
      boolean canDropExpected) throws Exception{
    final LogicalExpression filterExpr = parseExpr(exprStr);
    testParquetRowGroupFilterEval(footer, 0, filterExpr, fragmentContext, canDropExpected);
  }

  private static void testParquetRowGroupFilterEval(final ParquetMetadata footer, final int rowGroupIndex,
      final LogicalExpression filterExpr, FragmentContext fragContext, boolean canDropExpected) throws Exception {
    final Stopwatch watch = Stopwatch.createStarted();

    boolean canDrop = ParquetRGFilterEvaluator.evalFilter(filterExpr, footer, rowGroupIndex,
        fragContext.getOptions(), fragContext);
//    logger.debug("Took {} ms to evaluate the filter", watch.elapsed(TimeUnit.MILLISECONDS));
    Assert.assertEquals(canDropExpected, canDrop);
  }

  private static ParquetMetadata getParquetMetaData(String filePathStr) throws IOException {
    Configuration fsConf = new Configuration();
    ParquetMetadata footer = ParquetFileReader.readFooter(fsConf, new Path(filePathStr));
    return footer;
  }

  protected static LogicalExpression parseExpr(String expr) throws RecognitionException {
    final ExprLexer lexer = new ExprLexer(new ANTLRStringStream(expr));
    final CommonTokenStream tokens = new CommonTokenStream(lexer);
    final ExprParser parser = new ExprParser(tokens);
    final ExprParser.parse_return ret = parser.parse();
    return ret.e;
  }

}
