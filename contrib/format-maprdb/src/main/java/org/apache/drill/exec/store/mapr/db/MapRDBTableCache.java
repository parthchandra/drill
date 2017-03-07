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
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillRuntimeException;

public class MapRDBTableCache {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MapRDBFormatPlugin.class);
  public static final String FORMAT_MAPRDB_JSON_TABLE_CACHE_ENABLED = "format-maprdb.json.tableCache.enabled";
  public static final String FORMAT_MAPRDB_JSON_TABLE_CACHE_SIZE = "format-maprdb.json.tableCache.size";
  public static final String FORMAT_MAPRDB_JSON_TABLE_CACHE_TIMEOUT = "format-maprdb.json.tableCache.expireTimeInMinutes";
  private static final int MIN_TABLE_CACHE_SIZE = 1;
  private static final int MIN_TABLE_CACHE_ENTRY_TIMEOUT = 10;
  LoadingCache<String, Table> tableLoadingCache;
  private final boolean tableCachingEnabled;

  public MapRDBTableCache(DrillConfig config) {
    tableCachingEnabled = config.getBoolean(FORMAT_MAPRDB_JSON_TABLE_CACHE_ENABLED);
    if (tableCachingEnabled) {
      final int tableCacheSize = Math.max((int) (config.getDouble(FORMAT_MAPRDB_JSON_TABLE_CACHE_SIZE)), MIN_TABLE_CACHE_SIZE);
      final int tableCacheExpiryTime = Math.max((int) (config.getDouble(FORMAT_MAPRDB_JSON_TABLE_CACHE_TIMEOUT)), MIN_TABLE_CACHE_ENTRY_TIMEOUT);

      RemovalListener<String, Table> removalListener = new RemovalListener<String, Table>() {
        public void onRemoval(RemovalNotification<String, Table> removal) {
          Table table = removal.getValue();
          table.close(); // close the table
        }
      };

      tableLoadingCache = CacheBuilder.newBuilder().
        expireAfterAccess(tableCacheExpiryTime, TimeUnit.MINUTES).
        maximumSize(tableCacheSize).
        removalListener(removalListener).build(new CacheLoader<String, Table>() {
        @Override
        public Table load(String tableName) throws Exception {
          return MapRDB.getTable(tableName);
        }});

      logger.debug("table cache created with size {} and expiryTimeInMin {} ", tableCacheSize, tableCacheExpiryTime);
    }
  }

  public Table getTable(String tableName) throws DrillRuntimeException {
    try {
      if (!tableCachingEnabled) {
        return MapRDB.getTable(tableName);
      } else {
        return tableLoadingCache.get(tableName);
      }
    } catch (Exception e) {
      throw new DrillRuntimeException("Error getting table: " + tableName, e);
    }
  }

  public void closeTable(Table table) {
    if (!tableCachingEnabled && table != null) {
      table.close();
    }
  }
}