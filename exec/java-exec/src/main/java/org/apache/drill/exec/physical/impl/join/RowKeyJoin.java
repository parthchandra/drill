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
package org.apache.drill.exec.physical.impl.join;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.exec.record.AbstractRecordBatch.BatchState;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Interface for a row key join
 */
public interface RowKeyJoin {

  /**
   * Is the next batch of row keys ready to be returned
   * @return True if ready, false if not
   */
  public boolean hasRowKeyBatch();

  /**
   * Get the next batch of row keys
   * @return a Pair whose left element is the ValueVector containing the row keys, right
   *    element is the number of row keys in this batch
   */
  public Pair<ValueVector, Integer> nextRowKeyBatch();


  /**
   * Get the current BatchState (this is useful when performing row key join)
   */
  public BatchState getBatchState();

  /**
   * Set the BatchState (this is useful when performing row key join)
   * @param newState
   */
  public void setBatchState(BatchState newState);

}
