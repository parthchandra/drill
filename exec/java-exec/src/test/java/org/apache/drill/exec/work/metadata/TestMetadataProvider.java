/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.work.metadata;

import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.IS_CATALOG_CONNECT;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.IS_CATALOG_DESCR;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.IS_CATALOG_NAME;
import static org.junit.Assert.assertEquals;

import java.util.Iterator;
import java.util.List;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.proto.UserProtos.CatalogMetadata;
import org.apache.drill.exec.proto.UserProtos.ColumnMetadata;
import org.apache.drill.exec.proto.UserProtos.GetCatalogsResp;
import org.apache.drill.exec.proto.UserProtos.GetColumnsResp;
import org.apache.drill.exec.proto.UserProtos.GetSchemasResp;
import org.apache.drill.exec.proto.UserProtos.GetTablesResp;
import org.apache.drill.exec.proto.UserProtos.LikeFilter;
import org.apache.drill.exec.proto.UserProtos.RequestStatus;
import org.apache.drill.exec.proto.UserProtos.SchemaMetadata;
import org.apache.drill.exec.proto.UserProtos.TableMetadata;

import org.junit.Test;

/**
 * Tests for metadata provider APIs.
 */
public class TestMetadataProvider extends BaseTestQuery {

  @Test
  public void catalogs() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.CATALOGS"); // SQL equivalent

    GetCatalogsResp resp = client.getCatalogs(null).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<CatalogMetadata> catalogs = resp.getCatalogsList();
    assertEquals(1, catalogs.size());

    CatalogMetadata c = catalogs.get(0);
    assertEquals(IS_CATALOG_NAME, c.getCatalogName());
    assertEquals(IS_CATALOG_DESCR, c.getDescription());
    assertEquals(IS_CATALOG_CONNECT, c.getConnect());
  }

  @Test
  public void catalogsWithFilter() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.CATALOGS " +
    //    "WHERE CATALOG_NAME LIKE '%DRI%' ESCAPE '\\'"); // SQL equivalent
    GetCatalogsResp resp =
        client.getCatalogs(LikeFilter.newBuilder().setRegex("%DRI%").setEscape("\\").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<CatalogMetadata> catalogs = resp.getCatalogsList();
    assertEquals(1, catalogs.size());

    CatalogMetadata c = catalogs.get(0);
    assertEquals(IS_CATALOG_NAME, c.getCatalogName());
    assertEquals(IS_CATALOG_DESCR, c.getDescription());
    assertEquals(IS_CATALOG_CONNECT, c.getConnect());
  }

  @Test
  public void catalogsWithFilterNegative() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.CATALOGS
    //     WHERE CATALOG_NAME LIKE '%DRIj\\\\hgjh%' ESCAPE '\\'"); // SQL equivalent

    GetCatalogsResp resp =
        client.getCatalogs(LikeFilter.newBuilder().setRegex("%DRIj\\%hgjh%").setEscape("\\").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<CatalogMetadata> catalogs = resp.getCatalogsList();
    assertEquals(0, catalogs.size());
  }

  @Test
  public void schemas() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA"); // SQL equivalent

    GetSchemasResp resp = client.getSchemas(null, null).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<SchemaMetadata> schemas = resp.getSchemasList();
    assertEquals(9, schemas.size());

    Iterator<SchemaMetadata> iterator = schemas.iterator();
    verifySchema("INFORMATION_SCHEMA", iterator.next());
    verifySchema("cp.default", iterator.next());
    verifySchema("dfs.default", iterator.next());
    verifySchema("dfs.root", iterator.next());
    verifySchema("dfs.tmp", iterator.next());
    verifySchema("dfs_test.default", iterator.next());
    verifySchema("dfs_test.home", iterator.next());
    verifySchema("dfs_test.tmp", iterator.next());
    verifySchema("sys", iterator.next());
  }

  @Test
  public void schemasWithSchemaNameFilter() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE '%y%'"); // SQL equivalent

    GetSchemasResp resp = client.getSchemas(null, LikeFilter.newBuilder().setRegex("%y%").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<SchemaMetadata> schemas = resp.getSchemasList();
    assertEquals(1, schemas.size());

    verifySchema("sys", schemas.get(0));
  }

  @Test
  public void schemasWithCatalogNameFilterAndSchemaNameFilter() throws Exception {

    // test("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA " +
    //    "WHERE CATALOG_NAME LIKE '%RI%' AND SCHEMA_NAME LIKE '%y%'"); // SQL equivalent

    GetSchemasResp resp = client.getSchemas(
        LikeFilter.newBuilder().setRegex("%RI%").build(),
        LikeFilter.newBuilder().setRegex("%dfs_test%").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<SchemaMetadata> schemas = resp.getSchemasList();
    assertEquals(3, schemas.size());

    Iterator<SchemaMetadata> iterator = schemas.iterator();
    verifySchema("dfs_test.default", iterator.next());
    verifySchema("dfs_test.home", iterator.next());
    verifySchema("dfs_test.tmp", iterator.next());
  }

  @Test
  public void tables() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.`TABLES`"); // SQL equivalent

    GetTablesResp resp = client.getTables(null, null, null).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<TableMetadata> tables = resp.getTablesList();
    assertEquals(11, tables.size());

    Iterator<TableMetadata> iterator = tables.iterator();
    verifyTable("INFORMATION_SCHEMA", "CATALOGS", iterator.next());
    verifyTable("INFORMATION_SCHEMA", "COLUMNS", iterator.next());
    verifyTable("INFORMATION_SCHEMA", "SCHEMATA", iterator.next());
    verifyTable("INFORMATION_SCHEMA", "TABLES", iterator.next());
    verifyTable("INFORMATION_SCHEMA", "VIEWS", iterator.next());
    verifyTable("sys", "boot", iterator.next());
    verifyTable("sys", "drillbits", iterator.next());
    verifyTable("sys", "memory", iterator.next());
    verifyTable("sys", "options", iterator.next());
    verifyTable("sys", "threads", iterator.next());
    verifyTable("sys", "version", iterator.next());
  }

  @Test
  public void tablesWithTableNameFilter() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_NAME LIKE '%o%'"); // SQL equivalent

    GetTablesResp resp = client.getTables(null, null,
        LikeFilter.newBuilder().setRegex("%o%").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<TableMetadata> tables = resp.getTablesList();
    assertEquals(4, tables.size());

    Iterator<TableMetadata> iterator = tables.iterator();
    verifyTable("sys", "boot", iterator.next());
    verifyTable("sys", "memory", iterator.next());
    verifyTable("sys", "options", iterator.next());
    verifyTable("sys", "version", iterator.next());
  }

  @Test
  public void tablesWithTableNameFilterAndSchemaNameFilter() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.`TABLES` " +
    //    "WHERE TABLE_SCHEMA LIKE '%N\\_S%' ESCAPE '\\' AND TABLE_NAME LIKE '%o%'"); // SQL equivalent

    GetTablesResp resp = client.getTables(null,
        LikeFilter.newBuilder().setRegex("%N\\_S%").setEscape("\\").build(),
        LikeFilter.newBuilder().setRegex("%o%").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<TableMetadata> tables = resp.getTablesList();
    assertEquals(0, tables.size());
  }

  @Test
  public void columns() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.COLUMNS"); // SQL equivalent

    GetColumnsResp resp = client.getColumns(null, null, null, null).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<ColumnMetadata> columns = resp.getColumnsList();
    assertEquals(70, columns.size());
    // too many records to verify the output.
  }

  @Test
  public void columnsWithColumnNameFilter() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME LIKE '%\\_p%' ESCAPE '\\'"); // SQL equivalent

    GetColumnsResp resp = client.getColumns(null, null, null,
        LikeFilter.newBuilder().setRegex("%\\_p%").setEscape("\\").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<ColumnMetadata> columns = resp.getColumnsList();
    assertEquals(5, columns.size());

    Iterator<ColumnMetadata> iterator = columns.iterator();
    verifyColumn("sys", "drillbits", "user_port", iterator.next());
    verifyColumn("sys", "drillbits", "control_port", iterator.next());
    verifyColumn("sys", "drillbits", "data_port", iterator.next());
    verifyColumn("sys", "memory", "user_port", iterator.next());
    verifyColumn("sys", "threads", "user_port", iterator.next());
  }

  @Test
  public void columnsWithColumnNameFilterAndTableNameFilter() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.COLUMNS
    //     WHERE TABLE_NAME LIKE '%bits' AND COLUMN_NAME LIKE '%\\_p%' ESCAPE '\\'"); // SQL equivalent

    GetColumnsResp resp = client.getColumns(null, null,
        LikeFilter.newBuilder().setRegex("%bits").build(),
        LikeFilter.newBuilder().setRegex("%\\_p%").setEscape("\\").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<ColumnMetadata> columns = resp.getColumnsList();
    assertEquals(3, columns.size());

    Iterator<ColumnMetadata> iterator = columns.iterator();
    verifyColumn("sys", "drillbits", "user_port", iterator.next());
    verifyColumn("sys", "drillbits", "control_port", iterator.next());
    verifyColumn("sys", "drillbits", "data_port", iterator.next());
  }

  @Test
  public void columnsWithAllSupportedFilters() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE " +
    //    "TABLE_CATALOG LIKE '%ILL' AND TABLE_SCHEMA LIKE 'sys' AND " +
    //    "TABLE_NAME LIKE '%bits' AND COLUMN_NAME LIKE '%\\_p%' ESCAPE '\\'"); // SQL equivalent

    GetColumnsResp resp = client.getColumns(
        LikeFilter.newBuilder().setRegex("%ILL").build(),
        LikeFilter.newBuilder().setRegex("sys").build(),
        LikeFilter.newBuilder().setRegex("%bits").build(),
        LikeFilter.newBuilder().setRegex("%\\_p%").setEscape("\\").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<ColumnMetadata> columns = resp.getColumnsList();
    assertEquals(3, columns.size());

    Iterator<ColumnMetadata> iterator = columns.iterator();
    verifyColumn("sys", "drillbits", "user_port", iterator.next());
    verifyColumn("sys", "drillbits", "control_port", iterator.next());
    verifyColumn("sys", "drillbits", "data_port", iterator.next());
  }

  /** Helper method to verify schema contents */
  private static void verifySchema(String schemaName, SchemaMetadata schema) {
    assertEquals(IS_CATALOG_NAME, schema.getCatalogName());
    assertEquals(schemaName, schema.getSchemaName());
  }

  /** Helper method to verify table contents */
  private static void verifyTable(String schemaName, String tableName, TableMetadata table) {
    assertEquals(IS_CATALOG_NAME, table.getCatalogName());
    assertEquals(schemaName, table.getSchemaName());
    assertEquals(tableName, table.getTableName());
  }

  /** Helper method to verify column contents */
  private static void verifyColumn(String schemaName, String tableName, String columnName, ColumnMetadata column) {
    assertEquals(IS_CATALOG_NAME, column.getCatalogName());
    assertEquals(schemaName, column.getSchemaName());
    assertEquals(tableName, column.getTableName());
    assertEquals(columnName, column.getColumnName());
  }
}
