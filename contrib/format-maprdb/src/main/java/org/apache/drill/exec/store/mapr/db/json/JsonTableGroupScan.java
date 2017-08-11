/*
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

import static org.apache.drill.exec.planner.index.Statistics.ROWCOUNT_UNKNOWN;
import static org.apache.drill.exec.store.mapr.db.util.CommonFns.isNullOrEmpty;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.mapr.db.impl.ConditionImpl;
import com.mapr.db.impl.ConditionNode.RowkeyRange;

import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.planner.index.Statistics;
import org.apache.drill.exec.planner.index.IndexDescriptor;
import org.apache.drill.exec.planner.index.MapRDBIndexDescriptor;
import org.apache.drill.exec.planner.index.MapRDBStatistics;
import org.apache.drill.exec.planner.index.MapRDBStatisticsPayload;

import org.apache.drill.exec.planner.physical.PartitionFunction;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.mapr.db.MapRDBCost;
import org.apache.drill.exec.store.mapr.db.MapRDBFormatPlugin;
import org.apache.drill.exec.store.mapr.db.MapRDBFormatPluginConfig;
import org.apache.drill.exec.store.mapr.db.MapRDBGroupScan;
import org.apache.drill.exec.store.mapr.db.MapRDBSubScan;
import org.apache.drill.exec.store.mapr.db.MapRDBSubScanSpec;
import org.apache.drill.exec.store.mapr.db.MapRDBTableStats;
import org.apache.drill.exec.store.mapr.db.TabletFragmentInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.codehaus.jackson.annotate.JsonCreator;
import org.ojai.store.QueryCondition;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.mapr.db.MetaTable;
import com.mapr.db.Table;
import com.mapr.db.impl.TabletInfoImpl;
import com.mapr.db.index.IndexDesc;
import com.mapr.db.scan.ScanRange;

@JsonTypeName("maprdb-json-scan")
public class JsonTableGroupScan extends MapRDBGroupScan implements IndexGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonTableGroupScan.class);

  public static final int STAR_COLS = 100;
  public static final String TABLE_JSON = "json";
  /*
   * The <forcedRowCountMap> maintains a mapping of <RexNode, Rowcount>. These RowCounts take precedence over
   * anything computed using <MapRDBStatistics> stats. Currently, it is used for picking index plans with the
   * index_selectivity_factor. We forcibly set the full table rows as HUGE <Statistics.ROWCOUNT_HUGE> in this
   * map when the selectivity of the index is lower than index_selectivity_factor. During costing, the table
   * rowCount is returned as HUGE instead of the correct <stats> rowcount. This results in the planner choosing
   * the cheaper index plans!
   * NOTE: Full table rowCounts are specified with the NULL condition. e.g. forcedRowCountMap<NULL, 1000>
   */
  protected Map<RexNode, Double> forcedRowCountMap;
  /*
   * This stores the statistics associated with this GroupScan. Please note that the stats must be initialized
   * before using it to compute filter row counts based on query conditions.
   */
  protected MapRDBStatistics stats;
  protected JsonScanSpec scanSpec;
  protected long rowCount;
  protected double fullTableRowCount;

  @JsonCreator
  public JsonTableGroupScan(@JsonProperty("userName") final String userName,
                            @JsonProperty("scanSpec") JsonScanSpec scanSpec,
                            @JsonProperty("storage") FileSystemConfig storagePluginConfig,
                            @JsonProperty("format") MapRDBFormatPluginConfig formatPluginConfig,
                            @JsonProperty("columns") List<SchemaPath> columns,
                            @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
    this (userName,
          (FileSystemPlugin) pluginRegistry.getPlugin(storagePluginConfig),
          (MapRDBFormatPlugin) pluginRegistry.getFormatPlugin(storagePluginConfig, formatPluginConfig),
          scanSpec, columns);
  }

  public JsonTableGroupScan(String userName, FileSystemPlugin storagePlugin,
                            MapRDBFormatPlugin formatPlugin, JsonScanSpec scanSpec, List<SchemaPath> columns) {
    super(storagePlugin, formatPlugin, columns, userName);
    this.scanSpec = scanSpec;
    this.stats = new MapRDBStatistics();
    this.forcedRowCountMap = new HashMap<>();
    init();
  }

  public JsonTableGroupScan(String userName, FileSystemPlugin storagePlugin,
                            MapRDBFormatPlugin formatPlugin, JsonScanSpec scanSpec, List<SchemaPath> columns,
                            MapRDBStatistics stats) {
    super(storagePlugin, formatPlugin, columns, userName);
    this.scanSpec = scanSpec;
    this.stats = stats;
    this.forcedRowCountMap = new HashMap<>();
    init();
  }

  /**
   * Private constructor, used for cloning.
   * @param that The HBaseGroupScan to clone
   */
  protected JsonTableGroupScan(JsonTableGroupScan that) {
    super(that);
    this.scanSpec = that.scanSpec;
    this.endpointFragmentMapping = that.endpointFragmentMapping;
    this.stats = that.stats;
    this.fullTableRowCount = that.fullTableRowCount;
    this.forcedRowCountMap = that.forcedRowCountMap;
    init();
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    JsonTableGroupScan newScan = new JsonTableGroupScan(this);
    newScan.columns = columns;
    return newScan;
  }


  /**
   * Compute regions to scan based on the scanSpec
   */
  private void computeRegionsToScan() {
    boolean foundStartRegion = false;

    regionsToScan = new TreeMap<TabletFragmentInfo, String>();
    for (TabletInfo tabletInfo : tabletInfos) {
      TabletInfoImpl tabletInfoImpl = (TabletInfoImpl) tabletInfo;
      if (!foundStartRegion && !isNullOrEmpty(scanSpec.getStartRow()) && !tabletInfoImpl.containsRow(scanSpec.getStartRow())) {
        continue;
      }
      foundStartRegion = true;
      regionsToScan.put(new TabletFragmentInfo(tabletInfoImpl), tabletInfo.getLocations()[0]);
      if (!isNullOrEmpty(scanSpec.getStopRow()) && tabletInfoImpl.containsRow(scanSpec.getStopRow())) {
        break;
      }
    }
  }

  public GroupScan clone(JsonScanSpec scanSpec) {
    JsonTableGroupScan newScan = new JsonTableGroupScan(this);
    newScan.scanSpec = scanSpec;
    return newScan;
  }

  private void init() {
    logger.debug("Getting tablet locations");
    try {
      Configuration conf = new Configuration();
      final Table t = this.formatPlugin.getJsonTableCache().getTable(
          scanSpec.getTableName(), scanSpec.getIndexDesc(), getUserName());
      final MetaTable metaTable = t.getMetaTable();
      QueryCondition scanSpecCondition = scanSpec.getCondition();
      List<ScanRange> scanRanges = (scanSpecCondition == null) 
          ? metaTable.getScanRanges() : metaTable.getScanRanges(scanSpecCondition);

      // set the start-row of the scanspec as the start-row of the first scan range
      ScanRange firstRange = scanRanges.get(0);
      QueryCondition firstCondition = firstRange.getCondition();
      byte[] firstStartRow = ((ConditionImpl)firstCondition).getRowkeyRanges().get(0).getStartRow();
      scanSpec.setStartRow(firstStartRow);

      // set the stop-row of ScanSpec as the stop-row of the last scan range
      ScanRange lastRange = scanRanges.get(scanRanges.size() - 1);
      QueryCondition lastCondition = lastRange.getCondition();
      List<RowkeyRange> rowkeyRanges =  ((ConditionImpl)lastCondition).getRowkeyRanges();
      byte[] lastStopRow = rowkeyRanges.get(rowkeyRanges.size() - 1).getStopRow();
      scanSpec.setStopRow(lastStopRow);

      // Get the fullTableRowCount only once i.e. if not already obtained before.
      if (fullTableRowCount == 0) {
        // For condition null, we get full table stats.
        com.mapr.db.scan.ScanStats stats = metaTable.getScanStats();
        fullTableRowCount = stats.getEstimatedNumRows();
        // MapRDB client can return invalid rowCount i.e. 0, especially right after table
        // creation. It takes 15 minutes before table stats are obtained and cached in client.
        // If we get 0 rowCount, fallback to getting rowCount using old admin API.
        if (fullTableRowCount == 0) {
          MapRDBTableStats tableStats = new MapRDBTableStats(conf, scanSpec.getTableName());
          fullTableRowCount = tableStats.getNumRows();
        }
      }

      regionsToScan = new TreeMap<TabletFragmentInfo, String>();
      for (ScanRange range : scanRanges) {
        TabletInfoImpl tabletInfoImpl = (TabletInfoImpl) range;
        regionsToScan.put(new TabletFragmentInfo(tabletInfoImpl), range.getLocations()[0]);
      }

      computeRegionsToScan();

    } catch (Exception e) {
      throw new DrillRuntimeException("Error getting region info for table: " +
        scanSpec.getTableName() + (scanSpec.getIndexDesc() == null ? "" : (", index: " + scanSpec.getIndexName())), e);
    }
  }

  protected MapRDBSubScanSpec getSubScanSpec(TabletFragmentInfo tfi) {
    // XXX/TODO check filter/Condition
    JsonScanSpec spec = scanSpec;
    JsonSubScanSpec subScanSpec = new JsonSubScanSpec(
        spec.getTableName(),
        spec.getIndexDesc(),
        regionsToScan.get(tfi),
        (!isNullOrEmpty(spec.getStartRow()) && tfi.containsRow(spec.getStartRow())) ? spec.getStartRow() : tfi.getStartKey(),
        (!isNullOrEmpty(spec.getStopRow()) && tfi.containsRow(spec.getStopRow())) ? spec.getStopRow() : tfi.getEndKey(),
        spec.getCondition(),
        getUserName());
    return subScanSpec;
  }

  @Override
  public MapRDBSubScan getSpecificScan(int minorFragmentId) {
    assert minorFragmentId < endpointFragmentMapping.size() : String.format(
        "Mappings length [%d] should be greater than minor fragment id [%d] but it isn't.",
        endpointFragmentMapping.size(), minorFragmentId);
    return new MapRDBSubScan(getUserName(), formatPluginConfig, getStoragePlugin(),
        getStoragePlugin().getConfig(), endpointFragmentMapping.get(minorFragmentId), columns, TABLE_JSON);
  }

  @Override
  public ScanStats getScanStats() {
    //TODO: look at stats for this.
    if (isIndexScan()) {
      return indexScanStats();
    }

    final int avgColumnSize = MapRDBCost.AVG_COLUMN_SIZE;
    final int numColumns = (columns == null || columns.isEmpty()) ? STAR_COLS : columns.size();
    // index will be NULL for FTS
    double rowCount = stats.getRowCount(scanSpec.getCondition(), null);
    double avgRowSize = stats.getAvgRowSize(null, null, true);
    double totalRowCount = stats.getRowCount(null, null);
    // If UNKNOWN, or DB stats sync issues(manifests as 0 rows) use defaults.
    if (rowCount == ROWCOUNT_UNKNOWN || rowCount == 0) {
      rowCount = (scanSpec.getSerializedFilter() != null ? .5 : 1) * fullTableRowCount;
    }
    if (totalRowCount == ROWCOUNT_UNKNOWN || rowCount == 0) {
      totalRowCount = fullTableRowCount;
    }
    if (avgRowSize == Statistics.AVG_ROWSIZE_UNKNOWN || avgRowSize == 0) {
      avgRowSize = avgColumnSize * numColumns;
    }
    double rowsFromDisk = rowCount;
    if (Arrays.equals(scanSpec.getStartRow(), HConstants.EMPTY_START_ROW) &&
        Arrays.equals(scanSpec.getStopRow(), HConstants.EMPTY_END_ROW)) {
      // both start and stop rows are empty, indicating this is a full scan so
      // use the total rows for calculating disk i/o
      rowsFromDisk = totalRowCount;
    }

    double totalBlocks = Math.ceil((avgRowSize * totalRowCount)/MapRDBCost.DB_BLOCK_SIZE);
    double numBlocks = Math.ceil((avgRowSize * rowsFromDisk)/MapRDBCost.DB_BLOCK_SIZE);
    numBlocks = Math.min(totalBlocks, numBlocks);
    double diskCost = numBlocks * MapRDBCost.SSD_BLOCK_SEQ_READ_COST;
    /*
     * Table scan cost made INFINITE in order to pick index plans. Use the MAX possible rowCount for
     * costing purposes.
     * NOTE: Full table rowCounts are specified with the NULL condition.
     * e.g. forcedRowCountMap<NULL, 1000>
     */
    if (forcedRowCountMap.get(null) != null && //Forced full table rowcount and it is HUGE
        forcedRowCountMap.get(null) == Statistics.ROWCOUNT_HUGE) {
      rowCount = Statistics.ROWCOUNT_HUGE;
      diskCost = Statistics.ROWCOUNT_HUGE;
    }
    logger.debug("JsonGroupScan:{} rowCount:{}, avgRowSize:{}, blocks:{}, totalBlocks:{}, diskCost:{}",
        this, rowCount, avgRowSize, numBlocks, totalBlocks, diskCost);
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, rowCount, 1, diskCost);
  }

  private ScanStats indexScanStats() {
    int totalColNum = STAR_COLS;
    final int avgColumnSize = MapRDBCost.AVG_COLUMN_SIZE;
    boolean filterPushed = (scanSpec.getSerializedFilter() != null);
    if(scanSpec != null && scanSpec.getIndexDesc() != null) {
      totalColNum = scanSpec.getIndexDesc().getIncludedFields().size()
          + scanSpec.getIndexDesc().getIndexedFields().size() + 1;
    }
    int numColumns = (columns == null || columns.isEmpty()) ?  totalColNum: columns.size();
    String idxIdentifier = stats.buildUniqueIndexIdentifier(scanSpec.getIndexDesc().getPrimaryTablePath(),
        scanSpec.getIndexDesc().getIndexName());
    double rowCount = stats.getRowCount(scanSpec.getCondition(), idxIdentifier);
    double avgRowSize = stats.getAvgRowSize(scanSpec.getCondition(), idxIdentifier, false);
    // If UNKNOWN, use defaults
    if (rowCount == ROWCOUNT_UNKNOWN || rowCount == 0) {
      rowCount = (filterPushed ? 0.0005f : 0.001f) * fullTableRowCount / scanSpec.getIndexDesc().getIndexedFields().size() ;
    }
    if (avgRowSize == Statistics.AVG_ROWSIZE_UNKNOWN || avgRowSize == 0) {
      avgRowSize = avgColumnSize * numColumns;
    }
    double rowsFromDisk = rowCount;
    if (Arrays.equals(scanSpec.getStartRow(), HConstants.EMPTY_START_ROW) &&
        Arrays.equals(scanSpec.getStopRow(), HConstants.EMPTY_END_ROW)) {
      // both start and stop rows are empty, indicating this is a full scan so
      // use the total rows for calculating disk i/o
      rowsFromDisk = fullTableRowCount;
    }
    double totalBlocks = Math.ceil((avgRowSize * fullTableRowCount)/MapRDBCost.DB_BLOCK_SIZE);
    double numBlocks = Math.ceil(((avgRowSize * rowsFromDisk)/MapRDBCost.DB_BLOCK_SIZE));
    numBlocks = Math.min(totalBlocks, numBlocks);
    double diskCost = numBlocks * MapRDBCost.SSD_BLOCK_SEQ_READ_COST;
    logger.debug("JsonIndexGroupScan:{} rowCount:{}, avgRowSize:{}, blocks:{}, totalBlocks:{}, diskCost:{}",
        this, rowCount, avgRowSize, numBlocks, totalBlocks, diskCost);
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, rowCount, 1, diskCost);
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new JsonTableGroupScan(this);
  }

  @Override
  @JsonIgnore
  public String getTableName() {
    return scanSpec.getTableName();
  }

  public IndexDesc getIndexDesc() {
    return scanSpec.getIndexDesc();
  }

  public boolean isDisablePushdown() {
    return !formatPluginConfig.isEnablePushdown();
  }

  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return formatPluginConfig.isEnablePushdown();
  }

  @Override
  public String toString() {
    return "JsonTableGroupScan [ScanSpec=" + scanSpec + ", columns=" + columns + "]";
  }

  public JsonScanSpec getScanSpec() {
    return scanSpec;
  }

  @Override
  public boolean supportsSecondaryIndex() {
    return true;
  }

  @Override
  @JsonIgnore
  public boolean isIndexScan() {
    return scanSpec != null && scanSpec.isSecondaryIndex();
  }

  @Override
  public boolean supportsRestrictedScan() {
    return true;
  }

  @Override
  public RestrictedJsonTableGroupScan getRestrictedScan(List<SchemaPath> columns) {
    RestrictedJsonTableGroupScan newScan =
        new RestrictedJsonTableGroupScan(this.getUserName(),
            this.getStoragePlugin(),
            this.getFormatPlugin(),
            this.getScanSpec(),
            this.getColumns(),
            this.getStatistics());
    newScan.columns = columns;
    return newScan;
  }

  /**
   * Get the estimated statistics after applying the {@link RexNode} condition. DO NOT call this API directly.
   * Call the stats API instead which modifies the counts based on preference options.
   * @param condition, filter to apply
   * @param index, to use for generating the estimate
   * @return row count post filtering
   */
  public MapRDBStatisticsPayload getEstimatedStats(QueryCondition condition, IndexDescriptor index, DrillScanRel scanRel) {
    IndexDesc indexDesc = null;
    if (index != null) {
      indexDesc = (IndexDesc)((MapRDBIndexDescriptor)index).getOriginalDesc();
    }
    return getEstimatedRowCountInternal(condition, indexDesc, scanRel);

  }

  /**
   * Get the estimated row count after applying the {@link QueryCondition} condition
   * @param condition, filter to apply
   * @param index, to use for generating the estimate
   * @return row count post filtering
   */
  private MapRDBStatisticsPayload getEstimatedRowCountInternal(QueryCondition condition, IndexDesc index, DrillScanRel scanRel) {
    // double totalRows = getRowCount(null, scanPrel);

    // If no index is specified, get it from the primary table
    if(index == null && scanSpec.isSecondaryIndex()) {
      // If stats not cached get it from the table.
      //table = MapRDB.getTable(scanSpec.getPrimaryTablePath());
      throw new UnsupportedOperationException("getEstimatedRowCount should be invoked on primary table");
    }

    // Get the index table or primary table and use the DB API to get the estimated number of rows. For size estimates,
    // we assume that all the columns would be read from the disk.
    final Table table = this.formatPlugin.getJsonTableCache().getTable(scanSpec.getTableName(), index, getUserName());

    if (table != null) {
      final MetaTable metaTable = table.getMetaTable();
      final com.mapr.db.scan.ScanStats stats = (condition == null) 
          ? metaTable.getScanStats() : metaTable.getScanStats(condition);
      // Factor reflecting confidence in the DB estimates. If a table has few tablets, the tablet-level stats
      // might be off. The decay factor will reduce estimates when one tablet represents a significant percentage
      // of the entire table.
      double factor = 1.0;
      // Use the factor only when a condition filters out rows from the table. If no condition is present, all rows
      // should be selected. So the factor should not reduce the returned rows
      if (condition != null) {
        factor = Math.min(1.0, 1.0 / Math.sqrt(100.0 / metaTable.getScanStats().getPartitionCount()));
      }
      return new MapRDBStatisticsPayload(factor * stats.getEstimatedNumRows(),
          ((double)stats.getEstimatedSize()/stats.getEstimatedNumRows()));
    } else {
      return new MapRDBStatisticsPayload(ROWCOUNT_UNKNOWN, Statistics.AVG_ROWSIZE_UNKNOWN);
    }
  }

  /**
   * Set the row count resulting from applying the {@link RexNode} condition. Forced row counts will take
   * precedence over stats row counts
   * @param condition
   * @param count
   * @param capRowCount
   */
  @Override
  @JsonIgnore
  public void setRowCount(RexNode condition, double count, double capRowCount) {
    forcedRowCountMap.put(condition, count);
  }

  @Override
  public void setStatistics(Statistics statistics) {
    assert statistics instanceof MapRDBStatistics : String.format(
        "Passed unexpected statistics instance. Expects MAPR-DB Statistics instance");
    this.stats = ((MapRDBStatistics) statistics);
  }

  /**
   * Get the row count after applying the {@link RexNode} condition
   * @param condition, filter to apply
   * @return row count post filtering
   */
  @Override
  @JsonIgnore
  public double getRowCount(RexNode condition, DrillScanRel scanRel) {
    // Do not use statistics if row count is forced. Forced rowcounts take precedence over stats
    if (forcedRowCountMap.get(condition) != null) {
      return forcedRowCountMap.get(condition);
    }
    if (scanSpec.getIndexDesc() != null) {
      String idxIdentifier = stats.buildUniqueIndexIdentifier(scanSpec.getIndexDesc().getPrimaryTablePath(),
          scanSpec.getIndexName());
      return stats.getRowCount(condition, idxIdentifier, scanRel);
    } else {
      return stats.getRowCount(condition, null, scanRel);
    }
  }

  @Override
  public MapRDBStatistics getStatistics() {
    return stats;
  }

  @Override
  @JsonIgnore
  public void setColumns(List<SchemaPath> columns) {
    this.columns = columns;
  }

  @Override
  @JsonIgnore
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @Override
  @JsonIgnore
  public PartitionFunction getRangePartitionFunction(List<FieldReference> refList) {
    return new JsonTableRangePartitionFunction(refList, scanSpec.getTableName());
  }

  /**
   * Convert a given {@link LogicalExpression} condition into a {@link QueryCondition} condition
   * @param condition expressed as a {@link LogicalExpression}
   * @return {@link QueryCondition} condition equivalent to the given expression
   */
  @JsonIgnore
  public QueryCondition convertToQueryCondition(LogicalExpression condition) {
    final JsonConditionBuilder jsonConditionBuilder = new JsonConditionBuilder(this, condition);
    final JsonScanSpec newScanSpec = jsonConditionBuilder.parseTree();
    if (newScanSpec != null) {
      return newScanSpec.getCondition();
    } else {
      return null;
    }
  }
}
