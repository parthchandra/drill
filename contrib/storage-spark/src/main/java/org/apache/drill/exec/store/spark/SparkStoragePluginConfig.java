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

import java.util.Map;

import org.apache.drill.common.logical.StoragePluginConfigBase;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

@JsonTypeName(SparkStoragePluginConfig.NAME)
public class SparkStoragePluginConfig extends StoragePluginConfigBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SparkStoragePluginConfig.class);

  public static final String NAME = "spark";

  @JsonProperty
  public Map<String, String> config;

//  @JsonIgnore
//  private SparkConf sparkConf;

  @JsonCreator
  public SparkStoragePluginConfig(@JsonProperty("config") Map<String, String> config) {
    this.config = config;
    if (this.config == null) {
      this.config = Maps.newHashMap();
    }
  }

  @JsonProperty
  public Map<String, String> getConfig() {
    return ImmutableMap.copyOf(config);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SparkStoragePluginConfig that = (SparkStoragePluginConfig) o;
    return config.equals(that.config);
  }

  @Override
  public int hashCode() {
    return this.config != null ? this.config.hashCode() : 0;
  }

//  @JsonIgnore
//  public SparkConf getSparkConf() {
//    if (sparkConf == null) {
//      sparkConf = new SparkConf();
//      for (Map.Entry<String, String> entry : config.entrySet()) {
//        sparkConf.set(entry.getKey(), entry.getValue());
//      }
//    }
//
//    return sparkConf;
//  }
}
