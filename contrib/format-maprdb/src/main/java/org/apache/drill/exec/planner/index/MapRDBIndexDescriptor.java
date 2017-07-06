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
import com.google.common.collect.Sets;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.drill.common.expression.CastExpression;
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
import com.google.common.collect.ImmutableList;
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
  public boolean isCoveringIndex(List<LogicalExpression> expressions) {
    List<LogicalExpression> decodedCols = new DecodePathinExpr().parseExpressions(expressions);
    return columnsInIndexFields(decodedCols, allFields);
  }

  @Override
  public boolean allColumnsIndexed(Collection<LogicalExpression> expressions) {
    List<LogicalExpression> decodedCols = new DecodePathinExpr().parseExpressions(expressions);
    return columnsInIndexFields(decodedCols, indexedFields);
  }

  @Override
  public boolean someColumnsIndexed(Collection<LogicalExpression> columns) {
    return columnsIndexed(columns, false);
  }

  private boolean columnsIndexed(Collection<LogicalExpression> expressions, boolean allColsIndexed) {
    List<LogicalExpression> decodedCols = new DecodePathinExpr().parseExpressions(expressions);
    if (allColsIndexed) {
      return columnsInIndexFields(decodedCols, indexedFields);
    } else {
      return someColumnsInIndexFields(decodedCols, indexedFields);
    }
  }

  public FunctionalIndexInfo getFunctionalInfo() {
    if (this.functionalInfo == null) {
      this.functionalInfo = new MapRDBFunctionalIndexInfo(this);
    }
    return this.functionalInfo;
  }

  /**
   * Search through a LogicalExpression, finding all referenced schema paths
   * and replace them with decoded paths.
   * If one encoded path could be decoded to multiple paths, add these decoded paths to
   * the end of returned list of expressions from parseExpressions.
   */
  private class DecodePathinExpr extends CloneVisitor {
    Set<SchemaPath> schemaPathSet = Sets.newHashSet();

    public List<LogicalExpression> parseExpressions(Collection<LogicalExpression> expressions) {
      List<LogicalExpression> decodedCols = Lists.newArrayList();
      for(LogicalExpression expr : expressions) {
        LogicalExpression decoded = expr.accept(this, null);

        //encoded multiple path will return null
        if(decoded != null) {
          decodedCols.add(decoded);
        }
      }

      decodedCols.addAll(schemaPathSet);
      return decodedCols;
    }

    @Override
    public LogicalExpression visitSchemaPath(SchemaPath path, Void value) {
      List<SchemaPath> paths = Lists.newArrayList();
      paths.add(path);
      Collection<SchemaPath> decoded = EncodedSchemaPathSet.decode(paths);

      if ( decoded.size() == 1) {
        return decoded.iterator().next();
      }
      else {
        schemaPathSet.addAll(decoded);
      }

      // if decoded size is not one, incoming path is encoded path thus there is no cast or other function applied on it,
      // since users won't pass in encoded fields, so it is safe to return null,
      return null;
    }

  }

  @Override
  public RelOptCost getCost(IndexProperties indexProps, RelOptPlanner planner,
      int numProjectedFields, GroupScan primaryTableGroupScan) {
    DrillCostFactory costFactory = (DrillCostFactory)planner.getCostFactory();
    double totalRows = indexProps.getTotalRows();
    double leadRowCount = indexProps.getLeadingSelectivity() * totalRows;
    double avgRowSize = indexProps.getAvgRowSize();
    Preconditions.checkArgument(primaryTableGroupScan instanceof DbGroupScan);
    DbGroupScan dbGroupScan = (DbGroupScan)primaryTableGroupScan;
    if (indexProps.isCovering()) { // covering index
      // int numIndexCols = allFields.size();
      // for disk i/o, all index columns are going to be read into memory
      double numBlocks = Math.ceil((leadRowCount * avgRowSize)/MapRDBCost.DB_BLOCK_SIZE);
      double diskCost = numBlocks * MapRDBCost.SSD_BLOCK_SEQ_READ_COST;
      // cpu cost is cost of filter evaluation for the remainder condition
      double cpuCost = 0.0;
      if (indexProps.getRemainderFilter() != null) {
        cpuCost = leadRowCount * DrillCostBase.COMPARE_CPU_COST;
      }
      double networkCost = 0.0; // TODO: add network cost once full table scan also considers network cost
      return costFactory.makeCost(leadRowCount, cpuCost, diskCost, networkCost);

    } else { // non-covering index
      // int numIndexCols = allFields.size();
      double numBlocksIndex = Math.ceil((leadRowCount * avgRowSize)/MapRDBCost.DB_BLOCK_SIZE);
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
