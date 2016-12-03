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


import com.mapr.fs.MapRFileSystem;
import com.mapr.fs.tables.IndexDesc;
import com.mapr.fs.tables.IndexFieldDesc;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractDbGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.FormatMatcher;
import org.apache.drill.exec.store.mapr.db.MapRDBFormatMatcher;
import org.apache.drill.exec.store.mapr.db.MapRDBFormatPlugin;
import org.apache.drill.exec.store.mapr.db.MapRDBGroupScan;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.List;
import java.util.Set;

public class MapRDBIndexDiscover extends IndexDiscoverBase implements IndexDiscover {

  public MapRDBIndexDiscover(GroupScan inScan, ScanPrel prel) {
    super((AbstractDbGroupScan) inScan, prel);
  }


  @Override
  public IndexCollection getTableIndex(String tableName) {
    //return getTableIndexFromCommandLine(tableName);
    return getTableIndexFromMFS(tableName);
  }

  /**
   *
   * @param tableName
   * @return
   */
  private IndexCollection getTableIndexFromMFS(String tableName) {
    MapRFileSystem mfs = getMaprFS();
    try {
      Set<DrillIndexDescriptor> idxSet = new HashSet<>();
      Collection<IndexDesc> indexes = mfs.getTableIndexes(new Path(tableName));
      for (IndexDesc idx : indexes) {
        DrillIndexDescriptor hbaseIdx = buildIndexDescriptor(tableName, idx);
        if ( hbaseIdx == null ) {
          //not able to build a valid index based on the index info from MFS
          logger.error("Not able to build index for {}", idx.toString());
          continue;
        }
        idxSet.add(hbaseIdx);
      }
      if(idxSet.size() == 0) {
        logger.error("No index found for table {}.", tableName);
        return null;
      }
      return new DrillIndexCollection(getOriginalScanPrel(), idxSet);
    }
    catch(IOException ex) {
      logger.error("Could not get table index from File system. {}", ex.getMessage());
    }
    return null;
  }

  private Map.Entry<String, StoragePlugin>
  getExternalIdxStorage(String cluster) {
    return null;
  }


  FileSelection deriveFSSelection(DrillFileSystem fs, IndexDescriptor idxDesc) throws IOException {
    //assume indexName = '_idx1', tableName is '/tmp/maprdb_index_test',
    // and the index we are looking for is '/tmp/maprdb_index_test_idx1'

    String idxName = idxDesc.getIndexName();
    String tableName = idxDesc.getTableName();
    if(tableName.startsWith("/")) {
      tableName = tableName.substring(1).concat(idxName);
    }
    String[] tablePath = tableName.split("/");

    return FileSelection.create(fs, tableName, idxName);
  }

  @Override
  public DrillTable getNativeDrillTable(IndexDescriptor idxDescriptor) {

    try {
      final AbstractDbGroupScan origScan = getOriginalScan();
      if (!(origScan instanceof MapRDBGroupScan)) {
        return null;
      }
      MapRDBFormatPlugin maprFormatPlugin = ((MapRDBGroupScan) origScan).getFormatPlugin();
      FileSystemPlugin fsPlugin = ((MapRDBGroupScan) origScan).getStoragePlugin();

      DrillFileSystem fs = new DrillFileSystem(fsPlugin.getFsConf());
      MapRDBFormatMatcher matcher = (MapRDBFormatMatcher)(maprFormatPlugin.getMatcher());
      FileSelection fsSelection = deriveFSSelection(fs, idxDescriptor);
      return matcher.isReadableIndex(fs, fsSelection, fsPlugin, fsPlugin.getName(),
              UserGroupInformation.getCurrentUser().getUserName(), idxDescriptor);

    }
    catch(Exception e) {
      logger.error("Failed to get native DrillTable.", e);
    }
    return null;
  }

  private SchemaPath fieldName2SchemaPath(String fieldName) {
    if(fieldName.contains(":")) {
      return SchemaPath.getCompoundPath(fieldName.split(":"));
    }
    else if(fieldName.contains(".")) {
      return SchemaPath.getCompoundPath(fieldName.split("\\."));
    }
    return SchemaPath.getSimplePath(fieldName);
  }

  private List<SchemaPath> field2SchemaPath(Collection<IndexFieldDesc> descCollection) {
    List<SchemaPath> listSchema = new ArrayList<>();
    for (IndexFieldDesc field: descCollection) {
      listSchema.add(fieldName2SchemaPath(field.getFieldName()));
    }
    return listSchema;
  }

  private DrillIndexDescriptor buildIndexDescriptor(String tableName, IndexDesc desc) {
    if( desc.isExternal() ) {
      //XX: not support external index
      return null;
    }

    IndexDescriptor.IndexType idxType = IndexDescriptor.IndexType.NATIVE_SECONDARY_INDEX;

    DrillIndexDescriptor idx = new MapRDBIndexDescriptor (
        field2SchemaPath(desc.getIndexedFields()),
        field2SchemaPath(desc.getCoveredFields()),
        null,
        desc.getIndexName(),
        tableName,
        idxType,
        desc.getIndexFid());

    String storageName = this.getOriginalScan().getStoragePlugin().getName();
    materializeIndex(storageName, idx);
    return idx;
  }

  private MapRFileSystem getMaprFS() {
    try {
      Configuration conf;
      if (getOriginalScan() instanceof MapRDBGroupScan) {
        //if we can get FsConf from storage plugin(MapRDBPlugin case), use it.
        conf = ((MapRDBGroupScan) getOriginalScan()).getStoragePlugin().getFsConf();
      }
      else {
        conf = new Configuration();
        conf.set("fs.defaultFS", "maprfs:///");
        conf.set("fs.mapr.disable.namecache", "true");
      }

      MapRFileSystem fs = new MapRFileSystem();
      URI fsuri = new URI(conf.get("fs.defaultFS"));
      fs.initialize(fsuri, conf);
      return fs;
    } catch (Exception e) {
      logger.error("No MaprFS loaded {}", e);
      return null;
    }
  }
}
