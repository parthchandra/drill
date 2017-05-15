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

package org.apache.drill.exec.planner.index;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractDbGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ConfiguredIndexDiscover is to provide a way for users to explicitly define indexes for tables.
 * It can be used for testing purposes.
 * Index configure file will be stored as a json file on
 * the same file system (if the incoming table is format of file system), if incoming table
 * is not on FileSystem StoragePlugin, using 'dfs' default file system to access table index info.
 * {
 *   "storage" : "hbase",
 *   "indexes" : [
 *      "/tmp/table1":
 *        [
 *          {"location": "/tmp/table1_idx1",
 *           "indexedCols": ["col1", "col2"],
 *           "coveringCols": ["col3", "col4"]
 *          },
 *          {"location": "/tmp/table1_idx2",
 *           "indexedCols": ["col2"],
 *           "coveringCols": ["col4", "col5"]
 *          }
 *        ],
 *      "/tmp/table2":
 *        [
 *          {"location": "/tmp/table2_idx2",
 *           "indexedCols": ["col1"],
 *           "coveringCols": ["col2"]
 *          }
 *        ]
 *      }
 *   ]
 * }
 */

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property="type")
@JsonTypeName(IndexConfig.NAME)
class IndexConfig {
  final static String NAME = "index-meta-config";

  @JsonProperty
  public String storage;

  @JsonProperty
  public Map<String, List<DrillIndexDefinition>> indexes;

  @JsonCreator
  public IndexConfig(@JsonProperty("indexes") Map<String, List<DrillIndexDefinition>> idxs) {
    this.indexes = idxs;
  }

  public void addTableIndex(String tableName, List<DrillIndexDefinition> idx) {
    this.indexes.put(tableName, idx);
  }

  public  List<DrillIndexDefinition> getTableIndex(String tableName) {
    return this.indexes.get(tableName);
  }

}

public class ConfiguredIndexDiscover extends IndexDiscoverBase {

  protected ConcurrentHashMap<String, String> registeredIndexes;

  public ConfiguredIndexDiscover(GroupScan inScan, DrillScanRelBase inScanPrel) {
    super((AbstractDbGroupScan) inScan, inScanPrel);
  }

  @Override
  public DrillTable getNativeDrillTable(IndexDescriptor idxDesc) {
    //XXX to implement for maprdb
    return null;
  }

  @Override
  public IndexCollection getTableIndex(String tableName) {

    StoragePluginConfig conf = getOriginalScan().getStorageConfig();
    StoragePlugin storage = getOriginalScan().getStoragePlugin();

    String idxConf = conf.getValue(IndexDiscoverFactory.INDEX_DISCOVER_CONFIG_KEY);
    try {
      IndexConfig idxConfig = null;
      if (storage instanceof FileSystemPlugin) {
        idxConfig = readIndexConfigFromFile(idxConf);

      }
      else {
        //original scan is not on FileSystemPlugin. Need to find a default or configured FileSystemPlugin to use

      }
      if(idxConfig == null) {
        return null;
      }

      List<DrillIndexDefinition> indexes = idxConfig.getTableIndex(tableName);
      return buildIndexCollectionFromConfig(idxConfig.storage, indexes);
    } catch(Exception e) {
      logger.error("Not able to get IndexCollection. {}", e);
    }
    return null;
  }

  private IndexConfig readIndexConfigFromFile(String idxConf) throws IOException {
    final FileSystemPlugin fss = (FileSystemPlugin) getOriginalScan().getStoragePlugin();
    FileSystem fs = new DrillFileSystem(fss.getFsConf());
    Path p = new Path(idxConf);

    FSDataInputStream idxConfStream = fs.open(p);
    FileStatus fileStatus = fs.getFileStatus(p);
    fileStatus.getModificationTime();
    ObjectMapper mapper = new ObjectMapper();
    IndexConfig idxConfig = mapper.readValue(idxConfStream, IndexConfig.class);
    return idxConfig;
  }

  private IndexCollection buildIndexCollectionFromConfig(String storage,
                                                         List<DrillIndexDefinition> indexes) {
    Set<DrillIndexDescriptor> idxSet = new HashSet<>();
    for (DrillIndexDefinition idxDef : indexes) {
      DrillIndexDescriptor index = new DrillIndexDescriptor(idxDef);
      materializeIndex(storage, index);
      idxSet.add(index);
    }
    return new DrillIndexCollection(getOriginalScanRel(), idxSet);
  }

}
