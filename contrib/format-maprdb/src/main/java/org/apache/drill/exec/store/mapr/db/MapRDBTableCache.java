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

import java.util.concurrent.TimeUnit;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.mapr.db.Table;
import com.mapr.db.impl.MapRDBImpl;
import com.mapr.db.index.IndexDesc;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.hadoop.fs.Path;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.ImmutablePair;

public class MapRDBTableCache {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MapRDBFormatPlugin.class);
  public static final String FORMAT_MAPRDB_JSON_TABLE_CACHE_ENABLED = "format-maprdb.json.tableCache.enabled";
  public static final String FORMAT_MAPRDB_JSON_TABLE_CACHE_SIZE = "format-maprdb.json.tableCache.size";
  public static final String FORMAT_MAPRDB_JSON_TABLE_CACHE_TIMEOUT = "format-maprdb.json.tableCache.expireTimeInMinutes";
  private static final int MIN_TABLE_CACHE_SIZE = 1;
  private static final int MIN_TABLE_CACHE_ENTRY_TIMEOUT = 10;
  LoadingCache<Pair<Path, IndexDesc>, Table> tableCache;
  private final boolean tableCachingEnabled;

  public MapRDBTableCache(DrillConfig config) {
    tableCachingEnabled = config.getBoolean(FORMAT_MAPRDB_JSON_TABLE_CACHE_ENABLED);
    if (tableCachingEnabled) {
      final int tableCacheSize = Math.max((int) (config.getDouble(FORMAT_MAPRDB_JSON_TABLE_CACHE_SIZE)), MIN_TABLE_CACHE_SIZE);
      final int tableCacheExpiryTime = Math.max((int) (config.getDouble(FORMAT_MAPRDB_JSON_TABLE_CACHE_TIMEOUT)), MIN_TABLE_CACHE_ENTRY_TIMEOUT);

      RemovalListener<Pair<Path, IndexDesc>, Table> removalListener = new RemovalListener<Pair<Path, IndexDesc>, Table>() {
        public void onRemoval(RemovalNotification<Pair<Path, IndexDesc>, Table> removal) {
          Table table = removal.getValue();
          table.close(); // close the table
        }
      };

      // Common table cache for primary and index tables. Key is Pair<tablePath, indexDesc>
      // For primary table, indexDesc is null.
      tableCache =  CacheBuilder.newBuilder().
        expireAfterAccess(tableCacheExpiryTime, TimeUnit.MINUTES).
        maximumSize(tableCacheSize).
        removalListener(removalListener).build(new CacheLoader<Pair<Path, IndexDesc>, Table>() {

        @Override
        public Table load(Pair<Path, IndexDesc> key) throws Exception {
          // key.Left is Path. key.Right is indexDesc.
          return (key.getRight() == null) ? MapRDBImpl.getTable(key.getLeft()) : MapRDBImpl.getIndexTable(key.getRight());
        }});

      logger.debug("table cache created with size {} and expiryTimeInMin {} ", tableCacheSize, tableCacheExpiryTime);
    }
  }


  /**
   * getTable given primary table path and indexDesc.
   * returns Table for corresponding index table if indexDesc is not null.
   * returns Table for primary table if indexDesc is null.
   * @param tablePath primary table path
   * @param indexDesc  index table descriptor
   */
  public Table getTable(Path tablePath, IndexDesc indexDesc) throws DrillRuntimeException {
    try {
      if (tableCachingEnabled) {
        return tableCache.get(new ImmutablePair<Path, IndexDesc>(tablePath, indexDesc));
      } else {
        return indexDesc == null ? MapRDBImpl.getTable(tablePath): MapRDBImpl.getIndexTable(indexDesc);
      }
    } catch (Exception e) {
      throw new DrillRuntimeException("Error getting table: " +
        tablePath.toString()  + (indexDesc == null ? "" : (", indexDesc: " + indexDesc)), e);
    }
  }

  /**
   * getTable given primary table name.
   * returns Table for primary table with given name.
   * @param tableName primary table path
   */
  public Table getTable(String tableName) {
    return getTable(new Path(tableName), null);
  }

  /**
   * getTable given primary table path.
   * returns Table for primary table with given path.
   * @param tablePath primary table path
   */
  public Table getTable(Path tablePath) {
    return getTable(tablePath, null);
  }

  /**
   * getTable given primary table name and indexDesc.
   * returns Table for corresponding index table if indexDesc is not null.
   * returns Table for primary table if indexDesc is null.
   * @param tableName primary table name
   * @param indexDesc  index table Descriptor
   */
  public Table getTable(String tableName, IndexDesc indexDesc) {
    return getTable(new Path(tableName), indexDesc);
  }

  /**
   * closeTable
   * @param table table to be closed.
   */
  public void closeTable(Table table) {
    if (!tableCachingEnabled && table != null) {
      table.close();
    }
  }
}
