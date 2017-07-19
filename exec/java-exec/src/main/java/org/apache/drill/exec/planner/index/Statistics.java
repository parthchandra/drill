/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.index;

import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.planner.logical.DrillScanRel;

public interface Statistics {

  double ROWCOUNT_UNKNOWN = -1;
  //HUGE is same as DrillCostBase.HUGE
  double ROWCOUNT_HUGE = Double.MAX_VALUE;
  double AVG_ROWSIZE_UNKNOWN = -1;
  long AVG_COLUMN_SIZE = 10;

  boolean isStatsAvailable();

  String buildUniqueIndexIdentifier(IndexDescriptor idx);
  /** Returns the statistics given the specified filter condition
   * @param condition - Filter specified as a {@link RexNode}
   *  @param scanRel - The current scan rel
   */
  double getRowCount(RexNode condition, String tabIdxName, DrillScanRel scanRel);

  double getAvgRowSize(RexNode condition, String tabIdxName, DrillScanRel scanRel, boolean isIndexScan);

  boolean initialize(RexNode condition, DrillScanRel scanRel, IndexPlanCallContext context);
}
