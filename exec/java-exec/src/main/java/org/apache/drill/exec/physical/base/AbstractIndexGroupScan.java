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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.sun.tools.javac.util.Pair;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.planner.logical.DrillTable;

import java.util.Collection;


public abstract class AbstractIndexGroupScan extends AbstractGroupScan implements IndexGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractIndexGroupScan.class);

  protected Pair<RexNode, Long> conditionRowCountPair;
  protected long maxRowCount = ScanStats.TRIVIAL_TABLE.getRecordCount();
  public AbstractIndexGroupScan(String userName) {
    super(userName);
    conditionRowCountPair = new Pair<>((RexNode)null, (long)0);
  }

  public AbstractIndexGroupScan(AbstractIndexGroupScan that) {
    super(that);
    this.maxRowCount = that.maxRowCount;
    this.conditionRowCountPair = new Pair<>(that.conditionRowCountPair.fst, that.conditionRowCountPair.snd);

  }

  @Override
  public int getRowKeyOrdinal() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setRowCount(RexNode condition, long count, long capCount) {
    this.maxRowCount = capCount;
    this.conditionRowCountPair = new Pair<>(condition, count);
  }

  @Override
  public long getRowCount(RexNode condition) {
    if(this.conditionRowCountPair.fst != null &&
        (this.conditionRowCountPair.fst == condition)) {
      return this.conditionRowCountPair.snd;
    }
    return this.maxRowCount;
  }

  protected static boolean isStarQuery(Collection<SchemaPath> paths) {
    return Iterables.tryFind(Preconditions.checkNotNull(paths, "No Null schema path expected"), new Predicate<SchemaPath>() {
      @Override
      public boolean apply(SchemaPath path) {
        return Preconditions.checkNotNull(path).equals(GroupScan.ALL_COLUMNS.get(0));
      }
    }).isPresent();
  }

  public abstract void convertColumns(DrillTable table);

}
