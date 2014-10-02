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
import com.google.common.base.Preconditions;

import java.util.Arrays;

public class RDDTableSpec {
  private final String name;
  private final int numPartitions;

  @JsonCreator
  public RDDTableSpec(@JsonProperty("name") String name, @JsonProperty("numPartitions") int numPartitions) {
    Preconditions.checkArgument(numPartitions >= 0);
    this.name = name;
    this.numPartitions = numPartitions;
  }

  @JsonProperty
  public String getName() {
    return name;
  }

  @JsonProperty
  public int getNumPartitions() {
    return numPartitions;
  }

  @Override
  public String toString() {
    return String.format("RDDTableSpec[ name = '%s', numPartitions=%d ]", name, numPartitions);
  }
}
