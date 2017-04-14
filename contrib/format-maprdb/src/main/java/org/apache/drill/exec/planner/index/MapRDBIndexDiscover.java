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

import static org.apache.drill.exec.store.mapr.db.json.FieldPathHelper.fieldPath2SchemaPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractDbGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.mapr.db.MapRDBFormatMatcher;
import org.apache.drill.exec.store.mapr.db.MapRDBFormatPlugin;
import org.apache.drill.exec.store.mapr.db.MapRDBGroupScan;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import com.mapr.db.Admin;
import com.mapr.db.MapRDB;
import com.mapr.db.exceptions.DBException;
import com.mapr.db.index.IndexDesc;
import com.mapr.db.index.IndexFieldDesc;
import com.mapr.fs.MapRFileSystem;

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
    try {
      Set<DrillIndexDescriptor> idxSet = new HashSet<>();
      Collection<IndexDesc> indexes = admin().getTableIndexes(new Path(tableName));
      for (IndexDesc idx : indexes) {
        DrillIndexDescriptor hbaseIdx = buildIndexDescriptor(tableName, idx);
        if (hbaseIdx == null) {
          //not able to build a valid index based on the index info from MFS
          logger.error("Not able to build index for {}", idx.toString());
          continue;
        }
        idxSet.add(hbaseIdx);
      }
      if (idxSet.size() == 0) {
        logger.error("No index found for table {}.", tableName);
        return null;
      }
      return new DrillIndexCollection(getOriginalScanPrel(), idxSet);
    } catch (DBException ex) {
      logger.error("Could not get table index from File system. {}", ex);
    }
    return null;
  }

  FileSelection deriveFSSelection(DrillFileSystem fs, IndexDescriptor idxDesc) throws IOException {
    String tableName = idxDesc.getTableName();
    String[] tablePath = tableName.split("/");
    String tableParent = tableName.substring(0, tableName.lastIndexOf("/"));

    return FileSelection.create(fs, tableParent, tablePath[tablePath.length - 1]);
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
      MapRDBFormatMatcher matcher = (MapRDBFormatMatcher) (maprFormatPlugin.getMatcher());
      FileSelection fsSelection = deriveFSSelection(fs, idxDescriptor);
      return matcher.isReadableIndex(fs, fsSelection, fsPlugin, fsPlugin.getName(),
          UserGroupInformation.getCurrentUser().getUserName(), idxDescriptor);

    } catch (Exception e) {
      logger.error("Failed to get native DrillTable.", e);
    }
    return null;
  }

  private List<SchemaPath> field2SchemaPath(Collection<IndexFieldDesc> descCollection) {
    List<SchemaPath> listSchema = new ArrayList<>();
    for (IndexFieldDesc field : descCollection) {
      listSchema.add(fieldPath2SchemaPath(field.getFieldPath()));
    }
    return listSchema;
  }

  private DrillIndexDescriptor buildIndexDescriptor(String tableName, IndexDesc desc) {
    if (desc.isExternal()) {
      //XX: not support external index
      return null;
    }

    IndexDescriptor.IndexType idxType = IndexDescriptor.IndexType.NATIVE_SECONDARY_INDEX;
    List<SchemaPath> indexFields = field2SchemaPath(desc.getIndexedFields());
    List<SchemaPath> coveringFields = field2SchemaPath(desc.getCoveredFields());
    coveringFields.add(SchemaPath.getSimplePath("_id"));
    DrillIndexDescriptor idx = new MapRDBIndexDescriptor (
        indexFields,
        coveringFields,
        null,
        desc.getIndexName(),
        tableName,
        idxType,
        desc);

    String storageName = this.getOriginalScan().getStoragePlugin().getName();
    materializeIndex(storageName, idx);
    return idx;
  }

  @SuppressWarnings("deprecation")
  private Admin admin() {
    assert getOriginalScan() instanceof MapRDBGroupScan;
    Configuration conf = ((MapRDBGroupScan) getOriginalScan()).getFormatPlugin().getFsConf();
    return MapRDB.getAdmin(conf);
  }

}
