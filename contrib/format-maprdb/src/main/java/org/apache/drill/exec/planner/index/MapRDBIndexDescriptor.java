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

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.store.mapr.PluginConstants;
import org.apache.drill.exec.util.EncodedSchemaPathSet;

import com.google.common.collect.ImmutableSet;

public class MapRDBIndexDescriptor extends DrillIndexDescriptor {

  protected final Object desc;
  protected final Set<SchemaPath> allFields;
  protected final Set<SchemaPath> indexedFields;

  public MapRDBIndexDescriptor(List<SchemaPath> indexCols,
                               List<SchemaPath> nonIndexCols,
                               List<SchemaPath> rowKeyColumns,
                               String indexName,
                               String tableName,
                               IndexDescriptor.IndexType type,
                               Object desc) {
    super(indexCols, nonIndexCols, rowKeyColumns, indexName, tableName, type);
    this.desc = desc;
    this.indexedFields = ImmutableSet.copyOf(indexColumns);
    this.allFields = new ImmutableSet.Builder<SchemaPath>()
        .add(PluginConstants.DOCUMENT_SCHEMA_PATH)
        .addAll(indexColumns)
        .addAll(nonIndexColumns)
        .build();
  }

  public Object getOriginalDesc(){
    return desc;
  }

  @Override
  public boolean isCoveringIndex(List<SchemaPath> columns) {
    final Collection<SchemaPath> decodedColumns = EncodedSchemaPathSet.decode(columns);
    return allFields.containsAll(decodedColumns);
  }

  @Override
  public boolean allColumnsIndexed(Collection<SchemaPath> columns) {
    final Collection<SchemaPath> decodedColumns = EncodedSchemaPathSet.decode(columns);
    return indexedFields.containsAll(decodedColumns);
  }

}
