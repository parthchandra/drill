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
package org.apache.drill.exec.planner.sql;

import com.google.common.collect.ImmutableList;
import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.ValidationException;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.parser.SqlCreateTable;
import org.apache.drill.exec.util.Pointer;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.sql.parser.SqlParserPos;

import java.io.IOException;

public class DrillParallelSqlWorker extends DrillSqlWorker {

  private static final SqlIdentifier RDD_TBL_NAME =
      new SqlIdentifier(ImmutableList.of("dfs", "tmp", "$RDD"), SqlParserPos.ZERO);

  public DrillParallelSqlWorker(QueryContext context) throws Exception {
    super(context);
  }

  @Override
  public PhysicalPlan getPlan(String sql, Pointer<String> textPlan)
      throws SqlParseException, ValidationException, RelConversionException, IOException {
    SqlNode querySqlNode = planner.parse(sql);

    SqlKind kind = querySqlNode.getKind();
    if (kind == SqlKind.EXPLAIN || kind == SqlKind.SET_OPTION || kind == SqlKind.OTHER) {
      throw new ValidationException("Only queries are supported.");
    }

    // Insert a parallel writer. Parallel writer is same writer that is used in CTAS except that it has
    // a special table name "$RDD". Based on this special table name we create RpcWriterRecordBatch.
    SqlCreateTable parallelQueryNode = new SqlCreateTable(SqlParserPos.ZERO, RDD_TBL_NAME, null, querySqlNode);

    return parallelQueryNode.getSqlHandler(planner, context).getPlan(parallelQueryNode);
  }
}
