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

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.impl.join.RowKeyJoin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;

/**
 * A RestrictedMapRDBSubScan is intended for skip-scan (as opposed to sequential scan) operations
 * where the set of rowkeys is obtained from a corresponding HashJoinBatch instance
*/
@JsonTypeName("maprdb-restricted-subscan")
public class RestrictedMapRDBSubScan extends MapRDBSubScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RestrictedMapRDBSubScan.class);

  @JsonCreator
  public RestrictedMapRDBSubScan(@JacksonInject StoragePluginRegistry registry,
                       @JsonProperty("userName") String userName,
                       @JsonProperty("formatPluginConfig") MapRDBFormatPluginConfig formatPluginConfig,
                       @JsonProperty("storageConfig") StoragePluginConfig storage,
                       @JsonProperty("regionScanSpecList") List<RestrictedMapRDBSubScanSpec> regionScanSpecList,
                       @JsonProperty("columns") List<SchemaPath> columns,
                       @JsonProperty("maxRecordsToRead") long maxRecordsToRead,
                       @JsonProperty("tableType") String tableType) throws ExecutionSetupException {
    this(userName, formatPluginConfig,
        (FileSystemPlugin) registry.getPlugin(storage),
        storage, regionScanSpecList, columns, maxRecordsToRead, tableType);
  }

  public RestrictedMapRDBSubScan(String userName, MapRDBFormatPluginConfig formatPluginConfig,
      FileSystemPlugin storagePlugin, StoragePluginConfig storageConfig,
      List<RestrictedMapRDBSubScanSpec> maprDbSubScanSpecs, List<SchemaPath> columns, long maxRecordsToRead, String tableType) {
    super(userName, formatPluginConfig, storagePlugin, storageConfig,
        new ArrayList<MapRDBSubScanSpec>(), columns, maxRecordsToRead, tableType);

    for(RestrictedMapRDBSubScanSpec restrictedSpec : maprDbSubScanSpecs) {
      getRegionScanSpecList().add(restrictedSpec);
    }

  }

  @Override
  public void addJoinForRestrictedSubScan(RowKeyJoin rjbatch) {
    // currently, all subscan specs are sharing the same join batch instance
    for (MapRDBSubScanSpec s : getRegionScanSpecList()) {
      assert (s instanceof RestrictedMapRDBSubScanSpec);
      ((RestrictedMapRDBSubScanSpec)s).setJoinForSubScan(rjbatch);
    }
  }

  @Override
  public boolean isRestrictedSubScan() {
    return true;
  }

}
