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

import org.apache.drill.exec.planner.cost.DrillCostBase;

public class MapRDBCost {
  public static final int AVG_COLUMN_SIZE = 10;
  public static final int DB_BLOCK_SIZE = 8192;  // bytes per block

  // TODO: Currently, DrillCostBase has a byte read cost, but not a block read cost. The block
  // read cost is dependent on block size and the type of the storage, so it makes more sense to
  // define one here. However, the appropriate factor needs to be decided.
  public static final int SSD_BLOCK_SEQ_READ_COST = 32 * DrillCostBase.BASE_CPU_COST;

  // for SSD random and sequential costs are the same
  public static final int SSD_BLOCK_RANDOM_READ_COST = SSD_BLOCK_SEQ_READ_COST;

}
