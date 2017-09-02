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
package org.apache.drill.exec.store.mapr.db;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FormatMatcher;
import org.apache.drill.exec.store.hbase.HBaseScanSpec;
import org.apache.drill.exec.store.mapr.PluginConstants;
import org.apache.drill.exec.store.mapr.TableFormatPlugin;
import org.apache.drill.exec.store.mapr.db.binary.BinaryTableGroupScan;
import org.apache.drill.exec.store.mapr.db.json.JsonScanSpec;
import org.apache.drill.exec.store.mapr.db.json.JsonTableGroupScan;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableSet;
import com.mapr.db.index.IndexDesc;
import com.mapr.fs.tables.TableBasicAttrs;

public class MapRDBFormatPlugin extends TableFormatPlugin {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MapRDBFormatPlugin.class);

  private final MapRDBFormatMatcher matcher;
  private final Configuration hbaseConf;
  private final Connection connection;
  private final MapRDBTableCache jsonTableCache;
  private final int scanRangeSizeMB;

  public MapRDBFormatPlugin(String name, DrillbitContext context, Configuration fsConf,
      StoragePluginConfig storageConfig, MapRDBFormatPluginConfig formatConfig) throws IOException {
    super(name, context, fsConf, storageConfig, formatConfig);
    matcher = new MapRDBFormatMatcher(this);
    hbaseConf = HBaseConfiguration.create(fsConf);
    hbaseConf.set(ConnectionFactory.DEFAULT_DB, ConnectionFactory.MAPR_ENGINE2);
    connection = ConnectionFactory.createConnection(hbaseConf);
    jsonTableCache = new MapRDBTableCache(context.getConfig());
    int scanRangeSizeMBConfig = context.getConfig().getInt(PluginConstants.JSON_TABLE_SCAN_SIZE_MB);
    if (scanRangeSizeMBConfig < 32 || scanRangeSizeMBConfig > 8192) {
      logger.warn("Invalid scan size {} for MapR-DB tables, using default", scanRangeSizeMBConfig);
      scanRangeSizeMBConfig = PluginConstants.JSON_TABLE_SCAN_SIZE_MB_DEFAULT;
    }
    scanRangeSizeMB = scanRangeSizeMBConfig;
  }

  @Override
  public FormatMatcher getMatcher() {
    return matcher;
  }

  @Override
  public MapRDBFormatPluginConfig getConfig() {
    return (MapRDBFormatPluginConfig)(super.getConfig());
  }

  public MapRDBTableCache getJsonTableCache() {
    return jsonTableCache;
  }

  @Override
  @JsonIgnore
  public Set<StoragePluginOptimizerRule> getOptimizerRules() {
    return ImmutableSet.of(MapRDBPushFilterIntoScan.FILTER_ON_SCAN, MapRDBPushFilterIntoScan.FILTER_ON_PROJECT);
  }

  public AbstractGroupScan getGroupScan(String userName, FileSelection selection,
      List<SchemaPath> columns, IndexDesc indexDesc) throws IOException {
    List<String> files = selection.getFiles();
    assert (files.size() == 1);
    String tableName = files.get(0);
    TableBasicAttrs attrs = getMaprFS().getTableBasicAttrs(new Path(tableName));

    if (attrs.getIsJson()) {
      JsonScanSpec scanSpec = new JsonScanSpec(tableName, indexDesc, null/*condition*/);
      return new JsonTableGroupScan(userName, getStoragePlugin(), this, scanSpec, columns);
    } else {
      HBaseScanSpec scanSpec = new HBaseScanSpec(tableName);
      return new BinaryTableGroupScan(userName, getStoragePlugin(), this, scanSpec, columns);
    }
  }

  @Override
  public AbstractGroupScan getGroupScan(String userName, FileSelection selection,
      List<SchemaPath> columns) throws IOException {
    return getGroupScan(userName, selection, columns, null /* indexDesc */);
  }

  @JsonIgnore
  public Configuration getHBaseConf() {
    return hbaseConf;
  }

  @JsonIgnore
  public Connection getConnection() {
    return connection;
  }

  public int getScanRangeSizeMB() {
    return scanRangeSizeMB;
  }

}
