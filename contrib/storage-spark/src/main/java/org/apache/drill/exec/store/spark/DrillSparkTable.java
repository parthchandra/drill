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
package org.apache.drill.exec.store.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.spark.SparkGroupScan.SparkGroupScanSpec;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.type.SqlTypeName;

public class DrillSparkTable extends DrillTable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSparkTable.class);

  public DrillSparkTable(String storageEngineName, SparkStoragePlugin plugin, SparkGroupScanSpec scanSpec) {
    super(storageEngineName, plugin, scanSpec);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    // TODO: Depends on how we interpret the type of records in RDD
    return null;
  }

}
