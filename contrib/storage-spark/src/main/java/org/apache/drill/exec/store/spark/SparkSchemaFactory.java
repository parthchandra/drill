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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Table;

import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaFactory;

import com.google.common.collect.ImmutableList;
import org.apache.drill.exec.store.spark.SparkGroupScan.SparkGroupScanSpec;

public class SparkSchemaFactory implements SchemaFactory {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SparkSchemaFactory.class);

  final String schemaName;
  final SparkStoragePlugin plugin;
  final Map<String, String> sparkConfOveride;

  public SparkSchemaFactory(SparkStoragePlugin plugin, String name, Map<String, String> sparkConfOveride) {
    this.plugin = plugin;
    this.schemaName = name;
    this.sparkConfOveride = sparkConfOveride;
  }

  @Override
  public void registerSchemas(UserSession session, SchemaPlus parent) {
    SparkSchema schema = new SparkSchema(schemaName);
    SchemaPlus hPlus = parent.add(schemaName, schema);
    schema.setHolder(hPlus);
  }

  class SparkSchema extends AbstractSchema {

    public SparkSchema(String name) {
      super(ImmutableList.<String>of(), name);
    }

    public void setHolder(SchemaPlus plusOfThis) {
    }

    @Override
    public Schema getSubSchema(String name) {
      return null;
    }

    @Override
    public Set<String> getSubSchemaNames() {
      return Collections.emptySet();
    }

    @Override
    public Table getTable(String name) {
      return new DrillSparkTable(schemaName, plugin, new SparkGroupScanSpec(name, sparkConfOveride));
    }

    @Override
    public Set<String> getTableNames() {
      return Collections.emptySet();
    }

    @Override
    public String getTypeName() {
      return SparkStoragePluginConfig.NAME;
    }
  }
}
