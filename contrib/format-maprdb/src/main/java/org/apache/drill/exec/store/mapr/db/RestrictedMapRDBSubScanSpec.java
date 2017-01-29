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

import java.util.NoSuchElementException;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.physical.impl.join.HashJoinBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Iterables;

/**
 * A RestrictedMapRDBSubScanSpec encapsulates a join instance which contains the ValueVectors of row keys and
 * is associated with this sub-scan and also exposes an iterator type interface over the row key vectors.
 */
public class RestrictedMapRDBSubScanSpec extends MapRDBSubScanSpec {

  /**
   * The HashJoin instance (specific to one minor fragment) which will supply this
   * subscan with the set of rowkeys. For efficiency, we keep a reference to this
   * join rather than making another copy of the rowkeys.
   */
  private HashJoinBatch hjbatch = null;

  /**
   * The following are needed to maintain internal state of iteration over the set
   * of row keys
   */
  private ValueVector rowKeyVector = null; // the current row key value vector
  private int currentIndex = 0;  // the current index within the row key vector
  private int maxOccupiedIndex = -1; // max occupied index within a row key vector

  public RestrictedMapRDBSubScanSpec(String tableName, String regionServer) {
    super(tableName, null, regionServer, null, null, null, null);
  }
  /* package */ RestrictedMapRDBSubScanSpec() {
    // empty constructor, to be used with builder pattern;
  }

  public void setJoinForSubScan(HashJoinBatch hjbatch) {
    this.hjbatch = hjbatch;
  }

  @JsonIgnore
  public HashJoinBatch getJoinForSubScan() {
    return hjbatch;
  }

  @JsonIgnore
  private void init(Pair<VectorContainer, Integer> b) {
    VectorContainer vc = b.getLeft();
    this.maxOccupiedIndex = b.getRight();

    // get the value vector corresponding to the given column index (0 in this case since there
    // should only be 1 column)
    VectorWrapper<?> vw = Iterables.get(vc, 0);
    this.rowKeyVector = vw.getValueVector();
    this.currentIndex = 0;
  }

  /**
   *
   */
  @JsonIgnore
  public boolean readyToGetRowKey() {
    return hjbatch != null && hjbatch.hashTableBuilt();
  }
  /**
   * Returns {@code true} if the iteration has more row keys.
   * (In other words, returns {@code true} if {@link #nextRowKey} would
   * return a non-null row key)
   * @return {@code true} if the iteration has more row keys
   */
  @JsonIgnore
  public boolean hasRowKey() {
    if (rowKeyVector != null && currentIndex <= maxOccupiedIndex) {
      return true;
    }

    if (hjbatch != null) {
      Pair<VectorContainer, Integer> currentBatch = hjbatch.nextBuildBatch();

      // note that the hash table could be null initially during the BUILD_SCHEMA phase
      if (currentBatch != null) {
        init(currentBatch);
        return true;
      }
    }

    return false;
  }

  /**
   * Returns the next row key in the iteration.
   * @return the next row key in the iteration or null if no more row keys
   */
  @JsonIgnore
  public String nextRowKey() {
    if (hasRowKey()) {
      // get the entry at the current index within this batch
      Object o = rowKeyVector.getAccessor().getObject(currentIndex++);
      if (o == null) {
        throw new DrillRuntimeException("Encountered a null row key during restricted subscan !");
      }

      // this is specific to the way the hash join maintains its entries. once we have reached the max
      // occupied index within a batch, move to the next one and reset the current index to 0
      // TODO: we should try to abstract this out
      if (currentIndex > maxOccupiedIndex) {
        Pair<VectorContainer, Integer> currentBatch = hjbatch.nextBuildBatch();
        if (currentBatch != null) {
          init(currentBatch);
        }
      }

      return o.toString();
    }
    return null;
  }

}
