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
package org.apache.drill.exec.planner.index;


import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.expr.CloneVisitor;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.cost.DrillCostBase.DrillCostFactory;
import org.apache.drill.exec.planner.index.IndexSelector.IndexProperties;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.store.mapr.PluginConstants;
import org.apache.drill.exec.store.mapr.db.MapRDBCost;
import org.apache.drill.exec.util.EncodedSchemaPathSet;
import org.apache.drill.common.expression.LogicalExpression;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

public class MapRDBIndexDescriptor extends DrillIndexDescriptor {

  protected final Object desc;
  protected final Set<LogicalExpression> allFields;
  protected final Set<LogicalExpression> indexedFields;
  protected MapRDBFunctionalIndexInfo functionalInfo;

  public MapRDBIndexDescriptor(List<LogicalExpression> indexCols,
                               CollationContext indexCollationContext,
                               List<LogicalExpression> nonIndexCols,
                               List<LogicalExpression> rowKeyColumns,
                               String indexName,
                               String tableName,
                               IndexDescriptor.IndexType type,
                               Object desc) {
    super(indexCols, indexCollationContext, nonIndexCols, rowKeyColumns, indexName, tableName, type);
    this.desc = desc;
    this.indexedFields = ImmutableSet.copyOf(indexColumns);
    this.allFields = new ImmutableSet.Builder<LogicalExpression>()
        .add(PluginConstants.DOCUMENT_SCHEMA_PATH)
        .addAll(indexColumns)
        .addAll(nonIndexColumns)
        .build();
  }

  public Object getOriginalDesc(){
    return desc;
  }

  @Override
  public boolean isCoveringIndex(List<LogicalExpression> columns) {
    DecodePathinExpr decodeExpr = new DecodePathinExpr();
    List<LogicalExpression> decodedCols = Lists.newArrayList();
    for(LogicalExpression expr : columns) {
      decodedCols.add(expr.accept(decodeExpr, null));
    }
    return columnsInIndexFields(decodedCols, allFields);
  }

  @Override
  public boolean allColumnsIndexed(Collection<LogicalExpression> columns) {
    DecodePathinExpr decodeExpr = new DecodePathinExpr();
    List<LogicalExpression> decodedCols = Lists.newArrayList();
    for(LogicalExpression expr : columns) {
      decodedCols.add(expr.accept(decodeExpr, null));
    }
    return columnsInIndexFields(decodedCols, indexedFields);
  }

  public FunctionalIndexInfo getFunctionalInfo() {
    if (this.functionalInfo == null) {
      this.functionalInfo = new MapRDBFunctionalIndexInfo(this);
    }
    return this.functionalInfo;
  }
  /**
   * Search through a LogicalExpression, finding all internal schema path references and returning a decoded path.
   */
  private class DecodePathinExpr extends CloneVisitor {

    @Override
    public LogicalExpression visitSchemaPath(SchemaPath path, Void value) {
      List<SchemaPath> paths = Lists.newArrayList();
      paths.add(path);
      return EncodedSchemaPathSet.decode(paths).iterator().next();
    }
  }
  @Override
  public RelOptCost getCost(IndexProperties indexProps, RelOptPlanner planner,
      int numProjectedFields, GroupScan primaryTableGroupScan) {
    DrillCostFactory costFactory = (DrillCostFactory)planner.getCostFactory();
    double totalRows = indexProps.getTotalRows();
    double leadRowCount = indexProps.getLeadingSelectivity() * totalRows;
    Preconditions.checkArgument(primaryTableGroupScan instanceof DbGroupScan);
    DbGroupScan dbGroupScan = (DbGroupScan)primaryTableGroupScan;
    if (indexProps.isCovering()) { // covering index
      int numIndexCols = allFields.size();
      // for disk i/o, all index columns are going to be read into memory
      double numBlocks = Math.ceil((leadRowCount * numIndexCols * MapRDBCost.AVG_COLUMN_SIZE)/MapRDBCost.DB_BLOCK_SIZE);
      double diskCost = numBlocks * MapRDBCost.SSD_BLOCK_SEQ_READ_COST;
      // cpu cost is cost of filter evaluation for the remainder condition
      double cpuCost = 0.0;
      if (indexProps.getRemainderFilter() != null) {
        cpuCost = leadRowCount * DrillCostBase.COMPARE_CPU_COST;
      }
      double networkCost = 0.0; // TODO: add network cost once full table scan also considers network cost
      diskCost *= PrelUtil.getPlannerSettings(planner).getIndexIOCostFactor();
      return costFactory.makeCost(leadRowCount, cpuCost, diskCost, networkCost);

    } else { // non-covering index
      int numIndexCols = allFields.size();
      double numBlocksIndex = Math.ceil((leadRowCount * numIndexCols * MapRDBCost.AVG_COLUMN_SIZE)/MapRDBCost.DB_BLOCK_SIZE);
      double diskCostIndex = numBlocksIndex * MapRDBCost.SSD_BLOCK_SEQ_READ_COST;
      // for the primary table join-back each row may belong to a different block, so in general num_blocks = num_rows;
      // however, num_blocks cannot exceed the total number of blocks of the table
      double totalBlocksPrimary = Math.ceil((dbGroupScan.getColumns().size() * MapRDBCost.AVG_COLUMN_SIZE * totalRows)/MapRDBCost.DB_BLOCK_SIZE);
      double diskBlocksPrimary = Math.min(totalBlocksPrimary, leadRowCount);
      double diskCostPrimary = diskBlocksPrimary * MapRDBCost.SSD_BLOCK_RANDOM_READ_COST;
      double diskCostTotal = diskCostIndex + diskCostPrimary;

      // cpu cost of remainder condition evaluation over the selected rows
      double cpuCost = 0.0;
      if (indexProps.getRemainderFilter() != null) {
        cpuCost = leadRowCount * DrillCostBase.COMPARE_CPU_COST;
      }
      double networkCost = 0.0; // TODO: add network cost once full table scan also considers network cost
      diskCostTotal *= PrelUtil.getPlannerSettings(planner).getIndexIOCostFactor();
      return costFactory.makeCost(leadRowCount, cpuCost, diskCostTotal, networkCost);
    }

  }

  // Future use once full table scan also includes network cost
  private double getNetworkCost(double leadRowCount, int numProjectedFields, boolean isCovering) {
    if (isCovering) {
      // db server will send only the projected columns to the db client for the selected
      // number of rows, so network cost is based on the number of actual projected columns
      double networkCost = leadRowCount * numProjectedFields * MapRDBCost.AVG_COLUMN_SIZE;
      return networkCost;
    } else {
      // only the rowkey column is projected from the index and sent over the network
      double networkCostIndex = leadRowCount * 1 * MapRDBCost.AVG_COLUMN_SIZE;

      // after join-back to primary table, all projected columns are sent over the network
      double networkCostPrimary = leadRowCount * numProjectedFields * MapRDBCost.AVG_COLUMN_SIZE;

      return networkCostIndex + networkCostPrimary;
    }

  }

}
