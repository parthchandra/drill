/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.spark;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;

import java.util.Arrays;

public class RDDTableSpec {

  private final String scId; // SparkContext unique id
  private final int rddId; // Unique RDD id within the SparkContext
  private final int[] partitionIds; // Ids of partitions within RDD

  @JsonCreator
  public RDDTableSpec(@JsonProperty("scId") String scId,
                      @JsonProperty("rddId") int rddId,
                      @JsonProperty("partitionIds") int[] partitionIds) {
    this.scId = scId;
    this.rddId = rddId;
    this.partitionIds = Arrays.copyOf(partitionIds, partitionIds.length);
  }

  @JsonProperty
  public String getScId() {
    return scId;
  }

  @JsonProperty
  public int getRddId() {
    return rddId;
  }

  @JsonProperty
  public int[] getPartitionIds() {
    return partitionIds;
  }

  @JsonIgnore
  public int getPartitionId(int i) {
    return partitionIds[i];
  }

  @JsonIgnore
  public int getNumPartitions() {
    return partitionIds.length;
  }

  @Override
  public String toString() {
    return String.format("RDDTableSpec[ scId=%s, rddId=%d, partitionIds=[%s] ]", scId, rddId,
        Joiner.on(",").join(Arrays.asList(partitionIds)));
  }
}
