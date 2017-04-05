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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.planner.index.MapRDBStatistics;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.mapr.db.MapRDBFormatPlugin;
import org.apache.drill.exec.store.mapr.db.MapRDBSubScan;
import org.apache.drill.exec.store.mapr.db.MapRDBSubScanSpec;
import org.apache.drill.exec.store.mapr.db.RestrictedMapRDBSubScan;
import org.apache.drill.exec.store.mapr.db.RestrictedMapRDBSubScanSpec;
import org.apache.drill.exec.store.mapr.db.TabletFragmentInfo;

/**
 * A RestrictedJsonTableGroupScan encapsulates (along with a subscan) the functionality
 * for doing restricted (i.e skip) scan rather than sequential scan.  The skipping is based
 * on a supplied set of row keys (primary keys) from a join operator.
 */
@JsonTypeName("restricted-json-scan")
public class RestrictedJsonTableGroupScan extends JsonTableGroupScan {

  @JsonCreator
  public RestrictedJsonTableGroupScan(@JsonProperty("userName") String userName,
                            @JsonProperty("storage") FileSystemPlugin storagePlugin,
                            @JsonProperty("format") MapRDBFormatPlugin formatPlugin,
                            @JsonProperty("scanSpec") JsonScanSpec scanSpec, /* scan spec of the original table */
                            @JsonProperty("columns") List<SchemaPath> columns,
                            @JsonProperty("")MapRDBStatistics statistics) {
    super(userName, storagePlugin, formatPlugin, scanSpec, columns, statistics);
  }

  // TODO:  this method needs to be fully implemented
  protected RestrictedMapRDBSubScanSpec getSubScanSpec(TabletFragmentInfo tfi) {
    JsonScanSpec spec = scanSpec;
    RestrictedMapRDBSubScanSpec subScanSpec =
        new RestrictedMapRDBSubScanSpec(
        spec.getTableName(),
        regionsToScan.get(tfi));
    return subScanSpec;
  }

  @Override
  public MapRDBSubScan getSpecificScan(int minorFragmentId) {
    assert minorFragmentId < endpointFragmentMapping.size() : String.format(
        "Mappings length [%d] should be greater than minor fragment id [%d] but it isn't.", endpointFragmentMapping.size(),
        minorFragmentId);
    RestrictedMapRDBSubScan subscan =
        new RestrictedMapRDBSubScan(getUserName(), formatPluginConfig, getStoragePlugin(), getStoragePlugin().getConfig(),
        getEndPointFragmentMapping(minorFragmentId), columns, TABLE_JSON);

    return subscan;
  }

  private List<RestrictedMapRDBSubScanSpec> getEndPointFragmentMapping(int minorFragmentId) {
    List<RestrictedMapRDBSubScanSpec> restrictedSubScanSpecList = Lists.newArrayList();
    List<MapRDBSubScanSpec> subScanSpecList = endpointFragmentMapping.get(minorFragmentId);
    for(MapRDBSubScanSpec s : subScanSpecList) {
      restrictedSubScanSpecList.add((RestrictedMapRDBSubScanSpec) s);
    }
    return restrictedSubScanSpecList;
  }

  /**
   * Private constructor, used for cloning.
   * @param that The RestrictedJsonTableGroupScan to clone
   */
  private RestrictedJsonTableGroupScan(RestrictedJsonTableGroupScan that) {
    super(that);
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    RestrictedJsonTableGroupScan newScan = new RestrictedJsonTableGroupScan(this);
    newScan.columns = columns;
    return newScan;
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new RestrictedJsonTableGroupScan(this);
  }

  @Override
  public ScanStats getScanStats() {
    //TODO: ideally here we should use the rowcount from index scan, and multiply a factor of restricted scan
    int totalColNum = 10;
    int numColumns = (columns == null || columns.isEmpty()) ?  totalColNum: columns.size();
    long rowCount = (long)(0.001f * tableStats.getNumRows());
    final int avgColumnSize = 10;
    float diskCost = avgColumnSize * numColumns * rowCount;
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, rowCount, 1, diskCost);
    //return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, 1, 1, 1);
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
