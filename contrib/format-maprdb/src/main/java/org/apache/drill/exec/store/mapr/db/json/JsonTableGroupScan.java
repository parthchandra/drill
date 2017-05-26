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

import static org.apache.drill.exec.store.mapr.db.util.CommonFns.isNullOrEmpty;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.calcite.rex.RexBuilder;
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
import org.apache.drill.exec.planner.physical.PartitionFunction;
import org.apache.drill.exec.planner.index.IndexDescriptor;
import org.apache.drill.exec.planner.index.MapRDBIndexDescriptor;
import org.apache.drill.exec.planner.index.MapRDBStatistics;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.physical.PlannerSettings;
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
import com.mapr.db.TabletInfo;
import com.mapr.db.impl.TabletInfoImpl;
import com.mapr.db.index.IndexDesc;
import com.mapr.db.index.IndexFieldDesc;
import com.mapr.db.scan.ScanRange;

@JsonTypeName("maprdb-json-scan")
public class JsonTableGroupScan extends MapRDBGroupScan implements IndexGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonTableGroupScan.class);

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
  protected MapRDBTableStats tableStats;
  protected JsonScanSpec scanSpec;
  protected long rowCount;

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
    this.tableStats = that.tableStats;
    this.forcedRowCountMap = that.forcedRowCountMap;
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

  @SuppressWarnings("deprecation")
  private void init() {
    logger.debug("Getting tablet locations");
    try {
      Configuration conf = new Configuration();

      Table t;
      t = this.formatPlugin.getJsonTableCache().getTable(scanSpec.getTableName(), scanSpec.getIndexFid());
      MetaTable metaTable = t.getMetaTable();
      QueryCondition scanSpecCondition = scanSpec.getCondition();
      //if (scanSpecCondition != null) {
      //  scanSpecCondition.build();
      //}
      List<? extends ScanRange> scanRanges = metaTable.getScanRanges(scanSpecCondition);
      
      tableStats = new MapRDBTableStats(conf, scanSpec.getTableName());

      regionsToScan = new TreeMap<TabletFragmentInfo, String>();
      for (ScanRange range : scanRanges) {
        TabletInfoImpl tabletInfoImpl = (TabletInfoImpl) range;
        regionsToScan.put(new TabletFragmentInfo(tabletInfoImpl), range.getLocations()[0]);
        //System.err.println("Scan Range: startkey = " + tabletInfoImpl.getStartRow() + " stoprow = " + tabletInfoImpl.getStopRow());
      }

      computeRegionsToScan();

    } catch (Exception e) {
      throw new DrillRuntimeException("Error getting region info for table: " +
        scanSpec.getTableName() + (scanSpec.getIndexFid() == null ? "" : (", index: " + scanSpec.getIndexName())), e);
    }
  }

  protected MapRDBSubScanSpec getSubScanSpec(TabletFragmentInfo tfi) {
    // XXX/TODO check filter/Condition
    JsonScanSpec spec = scanSpec;
    JsonSubScanSpec subScanSpec = new JsonSubScanSpec(
        spec.getTableName(),
        spec.getIndexFid(),
        regionsToScan.get(tfi),
        (!isNullOrEmpty(spec.getStartRow()) && tfi.containsRow(spec.getStartRow())) ? spec.getStartRow() : tfi.getStartKey(),
        (!isNullOrEmpty(spec.getStopRow()) && tfi.containsRow(spec.getStopRow())) ? spec.getStopRow() : tfi.getEndKey(),
        spec.getCondition());
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

    double rowCount = stats.getRowCount(scanSpec.getCondition());
    // If UNKNOWN, use defaults
    if (rowCount == Statistics.ROWCOUNT_UNKNOWN) {
      rowCount = (scanSpec.getSerializedFilter() != null ? .5 : 1) * tableStats.getNumRows();
    }
    final int avgColumnSize = MapRDBCost.AVG_COLUMN_SIZE;
    final int numColumns = (columns == null || columns.isEmpty()) ? 100 : columns.size();

    double rowsFromDisk = rowCount;
    if (scanSpec.getStartRow() == HConstants.EMPTY_START_ROW &&
        scanSpec.getStopRow() == HConstants.EMPTY_END_ROW) {
      // both start and stop rows are empty, indicating this is a full scan so
      // use the total rows for calculating disk i/o
      rowsFromDisk = tableStats.getNumRows();
    }
    double totalBlocks = Math.ceil((avgColumnSize * numColumns * tableStats.getNumRows())/MapRDBCost.DB_BLOCK_SIZE);
    double numBlocks = Math.ceil(((avgColumnSize * numColumns * rowsFromDisk)/MapRDBCost.DB_BLOCK_SIZE));
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
    logger.debug("JsonGroupScan:{} rowCount:{}, diskCost:{}", this, rowCount, diskCost);
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, rowCount, 1, diskCost);
  }

  private ScanStats indexScanStats() {
    int totalColNum = 100;
    boolean filterPushed = (scanSpec.getSerializedFilter() != null);
    if(scanSpec != null && scanSpec.getIndexDesc() != null) {
      totalColNum = scanSpec.getIndexDesc().getCoveredFields().size()
          + scanSpec.getIndexDesc().getIndexedFields().size() + 1;
    }
    int numColumns = (columns == null || columns.isEmpty()) ?  totalColNum: columns.size();
    double rowCount = stats.getRowCount(scanSpec.getCondition());
    // If UNKNOWN, use defaults
    if (rowCount == Statistics.ROWCOUNT_UNKNOWN) {
      rowCount = (filterPushed ? 0.0001f : 0.001f) * tableStats.getNumRows();
    }
    final int avgColumnSize = MapRDBCost.AVG_COLUMN_SIZE;

    double rowsFromDisk = rowCount;
    if (scanSpec.getStartRow() == HConstants.EMPTY_START_ROW &&
        scanSpec.getStopRow() == HConstants.EMPTY_END_ROW) {
      // both start and stop rows are empty, indicating this is a full scan so
      // use the total rows for calculating disk i/o
      rowsFromDisk = tableStats.getNumRows();
    }
    double totalBlocks = Math.ceil((avgColumnSize * numColumns * tableStats.getNumRows())/MapRDBCost.DB_BLOCK_SIZE);
    double numBlocks = Math.ceil(((avgColumnSize * numColumns * rowsFromDisk)/MapRDBCost.DB_BLOCK_SIZE));
    numBlocks = Math.min(totalBlocks, numBlocks);
    double diskCost = numBlocks * MapRDBCost.SSD_BLOCK_SEQ_READ_COST;
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

  public IndexFieldDesc[] getIndexedFields() {
    return scanSpec.getIndexedFields();
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
   * Get the estimated row count after applying the {@link RexNode} condition
   * @param condition, filter to apply
   * @param index, to use for generating the estimate
   * @return row count post filtering
   */
  public double getEstimatedRowCount(QueryCondition condition, IndexDescriptor index, DrillScanRel scanRel) {
    if (condition == null) {
      return Statistics.ROWCOUNT_UNKNOWN;
    }
    return getEstimatedRowCountInternal(condition,
        (IndexDesc)((MapRDBIndexDescriptor)index).getOriginalDesc(), scanRel);
  }

  /**
   * Get the estimated row count after applying the {@link QueryCondition} condition
   * @param condition, filter to apply
   * @param index, to use for generating the estimate
   * @return row count post filtering
   */
  private double getEstimatedRowCountInternal(QueryCondition condition, IndexDesc index, DrillScanRel scanRel) {
    // double totalRows = getRowCount(null, scanPrel);
    // Get the index table and use the DB API to get the estimated number of rows
    Table primaryTable;
    if (scanSpec.isSecondaryIndex()) {
      // If stats not cached get it from the table.
      primaryTable = MapRDB.getTable(scanSpec.getPrimaryTablePath());
    } else {
      primaryTable = this.formatPlugin.getJsonTableCache().getTable(scanSpec.getTableName());
    }
    Table idxTable = MapRDB.getIndexTable(primaryTable.getPath(), index.getIndexFid(), index.getIndexName());
    if (idxTable != null) {
      return idxTable.getMetaTable().getScanStats(condition).getEstimatedNumRows();
    } else {
      return Statistics.ROWCOUNT_UNKNOWN;
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
    if (condition != null) {
      return stats.getRowCount(condition, scanRel);
    } else {
      return tableStats.getNumRows();
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
