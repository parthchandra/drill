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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import java.util.List;
import java.util.Map;

@JsonTypeName("spark-scan")
public class SparkGroupScan extends AbstractGroupScan {

  private SparkGroupScanSpec scanSpec;

  @JsonCreator
  public SparkGroupScan(@JsonProperty("scanSpec") SparkGroupScanSpec scanSpec) {
    this.scanSpec = scanSpec;
  }

  @JsonProperty("scanSpec")
  public SparkGroupScanSpec getScanSpec() {
    return scanSpec;
  }

  @Override
  public ScanStats getScanStats() {
    // TODO:
    return null;
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    // TODO:
    return null;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    // TODO:
    return null;
  }

  @Override
  public int getMaxParallelizationWidth() {
    // TODO:
    return 0;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
    // TODO:
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    // TODO:
    return null;
  }

  @Override
  public String getDigest() {
    // TODO:
    return null;
  }

  public static class SparkGroupScanSpec {
    private String tableName;
    private Map<String, String> sparkConfOverride;

    @JsonCreator
    public SparkGroupScanSpec(@JsonProperty("tableName") String tableName,
                         @JsonProperty("config") Map<String, String> sparkConfOverride) {
      this.tableName = tableName;
      this.sparkConfOverride = sparkConfOverride;
    }

    @JsonProperty
    public String getTableName() {
      return tableName;
    }

    @JsonProperty
    public Map<String, String> getConfig() {
      return sparkConfOverride;
    }

    // Need to add methods to find the partitions from tableName
  }
}