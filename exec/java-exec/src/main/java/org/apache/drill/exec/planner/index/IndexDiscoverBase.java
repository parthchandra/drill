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

import org.apache.drill.exec.physical.base.AbstractDbGroupScan;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * IndexDiscoverBase is the layer to read index configurations of tables on storage plugins,
 * then based on the properties it collected, get the StoragePlugin from StoragePluginRegistry,
 * together with indexes information, build an IndexCollection
 */
public abstract class IndexDiscoverBase implements IndexDiscover {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IndexDiscoverBase.class);

  //from ElasticSearchConstant
  static final String ES_CONFIG_KEY_CLUSTER = "elasticsearch.config.cluster";

  private AbstractDbGroupScan scan; // group scan corresponding to the primary table
  private ScanPrel scanPrel;   // physical scan rel corresponding to the primary table

  public IndexDiscoverBase(AbstractDbGroupScan inScan, ScanPrel inScanPrel) {
    scan = inScan;
    scanPrel = inScanPrel;
  }

  public AbstractDbGroupScan getOriginalScan() {
    return scan;
  }


  public ScanPrel getOriginalScanPrel() {
    return scanPrel;
  }


  public IndexCollection getTableIndex(String tableName, String storageName, Collection<DrillIndexDefinition>  indexDefs ) {
    Set<DrillIndexDescriptor> idxSet = new HashSet<>();
    for (DrillIndexDefinition def : indexDefs) {
      DrillIndexDescriptor indexDescriptor = new DrillIndexDescriptor(def);
      materializeIndex(storageName, indexDescriptor);
    }
    return new DrillIndexCollection(getOriginalScanPrel(), idxSet);
  }

  public void materializeIndex(String storageName, DrillIndexDescriptor index) {
    index.setStorageName(storageName);
    index.setDrillTable(buildDrillTable(index));
  }

  /**
   * When there is storageName in IndexDescriptor, get a DrillTable instance based on the
   * StorageName and other informaiton in idxDesc that helps identifying the table.
   * @param idxDesc
   * @return
   */
  public DrillTable externalGetDrillTable(IndexDescriptor idxDesc) {
    DrillIndexDescriptor hbaseIdx = (DrillIndexDescriptor)idxDesc;

    StoragePlugin plugin = null;
    final String storageName = hbaseIdx.getStorageName();
    try {
      plugin = getStorageRegistry().getPlugin(storageName);
    }catch (Exception e) {
      logger.error("No storage found {}. Exception thrown {}", storageName, e);
    }

    if(plugin == null) {
      logger.warn("No correspondent storage found {}", storageName);
      return null;
    }

    //get table object for this index

    SchemaFactory schemaFactory =
        ((AbstractStoragePlugin) plugin).getSchemaFactory();
    if (! ( schemaFactory instanceof IndexDiscoverable ) ) {
      logger.warn("This Storage plugin does not support IndexDiscoverable interface: {}",
          plugin.toString());
      return null;
    }
    DrillTable foundTable = ((IndexDiscoverable) schemaFactory).findTable(this, hbaseIdx);
    return foundTable;

  }

  /**
   *Implement in derived classes that have the details of storage and
   */
  public StoragePluginRegistry getStorageRegistry() {
    return getOriginalScan().getStoragePlugin().getContext().getStorage();
  }

  /**
   * Abstract function getDrillTable will be implemented the IndexDiscover within storage plugin(e.g. HBase, MaprDB)
   * since the implementations of AbstractStoragePlugin, IndexDescriptor and DrillTable in that storage plugin may have
   * the implement details.
   * @param idxDesc

   * @return
   */
  public DrillTable buildDrillTable(IndexDescriptor idxDesc) {
    if(idxDesc.getIndexType() == IndexDescriptor.IndexType.EXTERNAL_SECONDARY_INDEX) {
      return externalGetDrillTable(idxDesc);
    }
    else {
      return nativeGetDrillTable(idxDesc);
    }
  }

  /**
   * When it is native index(index provided by native storage plugin),
   * the actual IndexDiscover should provide the implementation to get the DrillTable object of index,
   * Otherwise, we call IndexDiscoverable interface exposed from external storage plugin's SchemaFactory
   * to get the desired DrillTable.
   * @param idxDesc
   * @return
   */
  public abstract DrillTable nativeGetDrillTable(IndexDescriptor idxDesc);

}
