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
package org.apache.drill.exec.store.mapr.db.json;

import java.util.List;
import java.util.Map;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.impl.join.HashJoinBatch;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.mapr.db.MapRDBFormatPlugin;
import org.apache.drill.exec.store.mapr.db.MapRDBSubScan;
import org.apache.drill.exec.store.mapr.db.RestrictedMapRDBSubScan;

import com.google.common.collect.Maps;

/**
 * A RestrictedJsonTableGroupScan encapsulates (along with a subscan) the functionality
 * for doing restricted (i.e skip) scan rather than sequential scan.  The skipping is based
 * on a supplied set of row keys (primary keys) from a join operator. 
 */
public class RestrictedJsonTableGroupScan extends JsonTableGroupScan {

  // A map of minor fragment id to the corresponding subscan instance
  private Map<Integer, RestrictedMapRDBSubScan> fidSubscanMap = Maps.newHashMap();
  
  protected RestrictedJsonTableGroupScan(String userName,
                            FileSystemPlugin storagePlugin,
                            MapRDBFormatPlugin formatPlugin,
                            JsonScanSpec scanSpec,
                            List<SchemaPath> columns) {
    super(userName, storagePlugin, formatPlugin, scanSpec, columns);
  }

  @Override
  public MapRDBSubScan getSpecificScan(int minorFragmentId) {
    assert minorFragmentId < endpointFragmentMapping.size() : String.format(
        "Mappings length [%d] should be greater than minor fragment id [%d] but it isn't.", endpointFragmentMapping.size(),
        minorFragmentId);
    RestrictedMapRDBSubScan subscan = 
        new RestrictedMapRDBSubScan(getUserName(), formatPluginConfig, getStoragePlugin(), getStoragePlugin().getConfig(),
        endpointFragmentMapping.get(minorFragmentId), columns, TABLE_JSON);
    
    // Remember that this minor fragment corresponds to a specific subscan. This will be used
    fidSubscanMap.put(minorFragmentId, subscan);
    return subscan;
  }

  /**
   * Private constructor, used for cloning.
   * @param that The RestrictedJsonTableGroupScan to clone
   */
  private RestrictedJsonTableGroupScan(RestrictedJsonTableGroupScan that) {
    super(that);
    this.fidSubscanMap = that.fidSubscanMap;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    RestrictedJsonTableGroupScan newScan = new RestrictedJsonTableGroupScan(this);
    newScan.columns = columns;
    return newScan;
  }
  
  @Override
  public ScanStats getScanStats() {
    //TODO: model the cost for restricted scan
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, 1, 1, 1);
  }

  @Override
  public void addJoinForRestrictedScan(HashJoinBatch joinBatch, int minorFragmentId) {
    RestrictedMapRDBSubScan subscan = fidSubscanMap.get(minorFragmentId);
    if (subscan != null) {
      subscan.setJoinForSubScan(joinBatch);
    }
  }

  @Override
  public boolean isRestrictedScan() { 
    return true;
  }
  
  @Override
  public String toString() {
    return "RestrictedJsonTableGroupScan [ScanSpec=" + scanSpec + ", columns=" + columns + "]";
  }


}
