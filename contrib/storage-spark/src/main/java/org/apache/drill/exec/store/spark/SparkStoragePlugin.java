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

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.hydromatic.optiq.SchemaPlus;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.spark.SparkGroupScan.SparkGroupScanSpec;

public class SparkStoragePlugin extends AbstractStoragePlugin {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SparkStoragePlugin.class);

  private final SparkStoragePluginConfig config;
  private final SparkSchemaFactory schemaFactory;
  private final DrillbitContext context;
  private final String name;

  public SparkStoragePlugin(SparkStoragePluginConfig config, DrillbitContext context, String name)
      throws ExecutionSetupException {
    this.config = config;
    this.context = context;
    this.schemaFactory = new SparkSchemaFactory(this, name, config.getConfig());
    this.name = name;
  }

  public SparkStoragePluginConfig getConfig() {
    return config;
  }

  public DrillbitContext getContext() {
    return context;
  }

  @Override
  public SparkGroupScan getPhysicalScan(JSONOptions selection, List<SchemaPath> columns) throws IOException {
    try {
      SparkGroupScanSpec scanSpec = selection.getListWith(new ObjectMapper(), new TypeReference<SparkGroupScanSpec>() {
      });
      return new SparkGroupScan(scanSpec, name, context.getStorage());
    } catch (ExecutionSetupException ex) {
      throw new IOException("Failed to create SparkGroupScan.", ex);
    }
  }


  @Override
  public void registerSchemas(UserSession session, SchemaPlus parent) {
    schemaFactory.registerSchemas(session, parent);
  }
}
