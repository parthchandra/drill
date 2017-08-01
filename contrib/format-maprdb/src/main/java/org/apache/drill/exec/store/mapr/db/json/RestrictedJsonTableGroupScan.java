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
import org.apache.drill.exec.planner.index.Statistics;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.mapr.db.MapRDBCost;
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
        regionsToScan.get(tfi), getUserName());
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
    final int avgColumnSize = MapRDBCost.AVG_COLUMN_SIZE;
    double rowCount;
    int numColumns = (columns == null || columns.isEmpty()) ?  STAR_COLS: columns.size();
    // Get the restricted group scan row count - same as the right side index rows
    rowCount = computeRestrictedScanRowcount();
    // Get the average row size of the primary table
    double avgRowSize = stats.getAvgRowSize(null, null, true);
    if (avgRowSize == Statistics.AVG_ROWSIZE_UNKNOWN || avgRowSize == 0) {
      avgRowSize = avgColumnSize * numColumns;
    }
    // restricted scan does random lookups and each row may belong to a different block, with the number
    // of blocks upper bounded by the total num blocks in the primary table
    double totalBlocksPrimary = Math.ceil((avgRowSize * rowCount)/MapRDBCost.DB_BLOCK_SIZE);
    double numBlocks = Math.min(totalBlocksPrimary, rowCount);
    double diskCost = numBlocks * MapRDBCost.SSD_BLOCK_RANDOM_READ_COST;
    // For non-covering plans, the dominating cost would be of the join back. Reduce it using the factor
    // for biasing towards non-covering plans.
    diskCost *= stats.getRowKeyJoinBackIOFactor();
    logger.debug("RestrictedJsonGroupScan:{} rowCount:{}, avgRowSize:{}, blocks:{}, totalBlocks:{}, diskCost:{}",
        this, rowCount, avgRowSize, numBlocks, totalBlocksPrimary, diskCost);
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, rowCount, 1, diskCost);
  }

  private double computeRestrictedScanRowcount() {
    double rowCount = Statistics.ROWCOUNT_UNKNOWN;
    // The rowcount should be the same as the build side which was FORCED by putting it in forcedRowCountMap
    if (forcedRowCountMap.get(scanSpec.getCondition()) != null) {
      rowCount = forcedRowCountMap.get(scanSpec.getCondition());
    }
    if (rowCount == Statistics.ROWCOUNT_UNKNOWN || rowCount == 0) {
      rowCount = (0.001f * fullTableRowCount);
    }
    return rowCount;
  }

  @Override
  public boolean isRestrictedScan() {
    return true;
  }

  @Override
  public String toString() {
    return "RestrictedJsonTableGroupScan [ScanSpec=" + scanSpec + ", columns=" + columns
        + ", rowcount=" + computeRestrictedScanRowcount() + "]";
  }


}
