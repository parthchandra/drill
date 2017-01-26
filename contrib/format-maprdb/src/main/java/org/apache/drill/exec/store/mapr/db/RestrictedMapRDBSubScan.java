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

import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.impl.join.HashJoinBatch;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;

/**
 * A RestrictedMapRDBSubScan is intended for skip-scan (as opposed to sequential scan) operations
 * where the set of rowkeys is obtained from a corresponding HashJoinBatch instance
*/
public class RestrictedMapRDBSubScan extends MapRDBSubScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RestrictedMapRDBSubScan.class);

  public RestrictedMapRDBSubScan(String userName, MapRDBFormatPluginConfig formatPluginConfig,
      FileSystemPlugin storagePlugin, StoragePluginConfig storageConfig,
      List<MapRDBSubScanSpec> maprDbSubScanSpecs, List<SchemaPath> columns, String tableType) {
    super(userName, formatPluginConfig, storagePlugin, storageConfig,
        maprDbSubScanSpecs, columns, tableType);
  }

  public void setJoinForSubScan(HashJoinBatch hjbatch) {
    // currently, all subscan specs are sharing the same join batch instance
    for (MapRDBSubScanSpec s : getRegionScanSpecList()) {
      assert (s instanceof RestrictedMapRDBSubScanSpec);
      ((RestrictedMapRDBSubScanSpec)s).setJoinForSubScan(hjbatch);
    }
  }

  @Override
  public boolean isRestrictedSubScan() {
    return true;
  }

}