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
package org.apache.drill.spark.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import net.hydromatic.avatica.Casing;
import net.hydromatic.avatica.Quoting;
import org.apache.drill.exec.planner.sql.parser.impl.DrillParserWithCompoundIdConverter;
import org.apache.drill.exec.store.spark.RDDTableSpec;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql.util.SqlShuttle;
import org.eigenbase.util.Util;

import java.lang.Exception;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SqlAnalyzer {
  private final String sql;
  private final Set<String> rddTableNames;
  private List<SqlIdentifier> ids;
  private SqlNode sqlTree;
  private boolean analyzed = false;
  private List<String> rddTablesNeedExpansion; // list of RDD tables that need expansion

  /**
   * Create a SqlAnalyzer instance.
   * @param sql
   * @param rddTableNames Set of RDD table names in <i>upperCase</i>.
   */
  public SqlAnalyzer(String sql, Set<String> rddTableNames) {
    this.sql = sql;
    this.rddTableNames = rddTableNames;
  }

  /**
   * Analyze the SQL query and check if any of the table names need expansion.
   * Table name expansion is needed when the table name refers to an RDD.
   * @return If expansion is needed, returns the list of "RDD" tables that need expansion.
   */
  public List<String> analyze() throws SqlParseException {
    if (analyzed) {
      return rddTablesNeedExpansion;
    }

    SqlParser parser = SqlParser.create(DrillParserWithCompoundIdConverter.FACTORY, sql,
        Quoting.BACK_TICK, Casing.UNCHANGED, Casing.UNCHANGED);
    this.sqlTree = parser.parseQuery();

    this.ids = Lists.newArrayList();
    this.rddTablesNeedExpansion = Lists.newArrayList();
    sqlTree.accept(new ListRDDTableIdentifiers(rddTableNames, this.ids, this.rddTablesNeedExpansion));

    this.analyzed = true;
    return rddTablesNeedExpansion;
  }

  /**
   * Expand RDD table names with augmented table info (such as number of partitions) and return the updated SQL query.
   * @param mapRDD2TableSpec Mapping of table names (in <i>upperCase</i>) to <i>RDDTableSpec</i>
   * @return
   */
  public String expand(Map<String, RDDTableSpec> mapRDD2TableSpec) throws Exception {
    if (analyze().isEmpty()) {
      return sql;
    }

    ObjectMapper objectMapper = new ObjectMapper();

    do {
      SqlIdentifier id = ids.remove(0);
      RDDTableSpec tableSpec = mapRDD2TableSpec.get(Util.last(id.names).toUpperCase());
      if (tableSpec == null) {
        throw new Exception(String.format("No RDDTableSpec found for RDD table name '%s'", Util.last(id.names)));
      }

      try {
        List<String> newNames = Lists.newArrayList();
        newNames.addAll(Util.skipLast(id.names));
        newNames.add(objectMapper.writeValueAsString(tableSpec));
        id.setNames(newNames, null);
      } catch(Exception ex) {
        throw new Exception(String.format("Failed to serialize RDDTableSpec '%s'", tableSpec.toString()), ex);
      }
    } while (ids.size() > 0);

    return sqlTree.toString();
  }

  private static class ListRDDTableIdentifiers extends SqlShuttle {

    private final Set<String> rddTableNames;
    private final List<SqlIdentifier> ids;
    private final List<String> rddTablesNeedExpansion;

    public ListRDDTableIdentifiers(Set<String> rddTableNames, List<SqlIdentifier> ids,
        List<String> rddTablesNeedExpansion) {
      this.rddTableNames = rddTableNames;
      this.ids = ids;
      this.rddTablesNeedExpansion = rddTablesNeedExpansion;
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
      String tblName = Util.last(id.names).toUpperCase();
      if (rddTableNames.contains(tblName)) {
        ids.add(id);
        rddTablesNeedExpansion.add(tblName);
      }

      return id;
    }
  }
}
