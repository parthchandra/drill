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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.PhysicalOperatorSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.spark.SparkSubScan.SparkSubScanSpec;
import org.apache.hadoop.mapred.InputSplit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@JsonTypeName("spark-scan")
public class SparkGroupScan extends AbstractGroupScan {

  private SparkGroupScanSpec scanSpec;

  @JsonIgnore
  private List<List<Integer>> mappings;

  @JsonIgnore
  private final Collection<DrillbitEndpoint> endpoints;

  private String storagePluginName;

  @JsonCreator
  public SparkGroupScan(@JsonProperty("scanSpec") SparkGroupScanSpec scanSpec,
                        @JsonProperty("storage-plugin") String storagePluginName,
                        @JacksonInject StoragePluginRegistry pluginRegistry) throws ExecutionSetupException {
    this.scanSpec = scanSpec;
    this.storagePluginName = storagePluginName;
    this.endpoints = ((SparkStoragePlugin)pluginRegistry.getPlugin(storagePluginName)).getContext().getBits();
  }

  public SparkGroupScan(SparkGroupScan that) {
    this.scanSpec = that.scanSpec;
    this.endpoints = that.endpoints;
    this.mappings = that.mappings;
  }

  @JsonProperty("scanSpec")
  public SparkGroupScanSpec getScanSpec() {
    return scanSpec;
  }

  @JsonProperty("storage-plugin")
  public String getStoragePluginName() {
    return storagePluginName;
  }

  @Override
  public ScanStats getScanStats() {
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, 1024, 1, 0);
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
    mappings = Lists.newArrayList();
    for (int i = 0; i < endpoints.size(); i++) {
      mappings.add(new ArrayList<Integer>());
    }
    int count = endpoints.size();
    RDDTableSpec table = scanSpec.getTable();
    for (int i = 0; i < table.getNumPartitions(); i++) {
      mappings.get(i % count).add(table.getPartitionId(i));
    }
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    return new SparkSubScan(new SparkSubScanSpec(scanSpec.getTable(), mappings.get(minorFragmentId)));
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    return new SparkGroupScan(this);
  }

  @Override
  public int getMaxParallelizationWidth() {
    return scanSpec.getTable().getNumPartitions();
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    // All Drillbits have equal affinity. Will change later when we try to use RDD partition affinity.
    List<EndpointAffinity> affinities = Lists.newArrayList();
    for(DrillbitEndpoint bit : endpoints) {
      affinities.add(new EndpointAffinity(bit, 1.0f/scanSpec.getTable().getNumPartitions()));
    }

    return affinities;
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public String toString() {
    return "SparkGroupScan [RDD table name=" + scanSpec.getTable() + "]";
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    return new SparkGroupScan(this);
  }

  public static class SparkGroupScanSpec {
    private RDDTableSpec table;
    private Map<String, String> sparkConfOverride;

    @JsonCreator
    public SparkGroupScanSpec(@JsonProperty("table") RDDTableSpec table,
                         @JsonProperty("config") Map<String, String> sparkConfOverride) {
      this.table = table;
      this.sparkConfOverride = sparkConfOverride;
    }

    @JsonProperty
    public RDDTableSpec getTable() {
      return table;
    }

    @JsonProperty
    public Map<String, String> getConfig() {
      return sparkConfOverride;
    }
  }
}