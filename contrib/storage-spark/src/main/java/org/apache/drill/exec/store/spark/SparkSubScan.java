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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.graph.GraphVisitor;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;

import java.util.Iterator;
import java.util.List;

@JsonTypeName("spark-sub-scan")
public class SparkSubScan extends AbstractBase implements SubScan {
  private SparkSubScanSpec subScanSpec;

  @JsonCreator
  public SparkSubScan(@JsonProperty("subScanSpec") SparkSubScanSpec subScanSpec) {
    this.subScanSpec = subScanSpec;
  }

  @JsonProperty("subScanSpec")
  public SparkSubScanSpec getSubScanSpec() {
    return subScanSpec;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    // TODO:
    return null;
  }

  @Override
  public void accept(GraphVisitor<PhysicalOperator> visitor) {
    // TODO:
    super.accept(visitor);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    // TODO:
    return null;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    // TODO:
    return null;
  }

  @Override
  public int getOperatorType() {
    // TODO:
    return 0;
  }

  public static class SparkSubScanSpec {
    private int partitionId;

    public SparkSubScanSpec(int partitionId) {
      this.partitionId = partitionId;
    }
  }
}
