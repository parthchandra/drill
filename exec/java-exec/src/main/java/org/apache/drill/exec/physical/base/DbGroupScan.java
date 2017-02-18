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
package org.apache.drill.exec.physical.base;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.planner.index.IndexCollection;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;
import org.apache.drill.exec.planner.physical.PartitionFunction;
import org.apache.drill.exec.planner.physical.ScanPrel;

import java.util.List;

/**
 * A DbGroupScan operator represents the scan associated with a database. The underlying
 * database may support secondary indexes, so there are interface methods for indexes.
 */
public interface DbGroupScan extends GroupScan {


  @JsonIgnore
  public boolean supportsSecondaryIndex();

  /**
   * Get the index collection associated with this table if any
   */
  @JsonIgnore
  public IndexCollection getSecondaryIndexCollection(ScanPrel scan);

  public List<SchemaPath> getColumns();

  public void setCostFactor(double sel);

  @JsonIgnore
  boolean isIndexScan();

  /**
   * Whether this DbGroupScan supports creating a restricted (skip) scan
   * @return true if restricted scan is supported, false otherwise
   */
  @JsonIgnore
  boolean supportsRestrictedScan();

  /**
   * Whether this DbGroupScan is itself a restricted scan
   * @return true if this DbGroupScan is itself a restricted scan, false otherwise
   */
  @JsonIgnore
  boolean isRestrictedScan();

  /**
   * If this DbGroupScan supports restricted scan, create a restricted scan from this DbGroupScan.
   * @param columns
   * @return a non-null DbGroupScan if restricted scan is supported, null otherwise
   */
  @JsonIgnore
  DbGroupScan getRestrictedScan(List<SchemaPath> columns);

  @JsonIgnore
  String getRowKeyName();

  @JsonIgnore
  SchemaPath getRowKeyPath();

/**
 * Get a partition function instance for range based partitioning
 * @param refList a list of FieldReference exprs that are participating in the range partitioning
 * @return instance of a partitioning function
 */
  @JsonIgnore
  PartitionFunction getRangePartitionFunction(List<FieldReference> refList);

}
