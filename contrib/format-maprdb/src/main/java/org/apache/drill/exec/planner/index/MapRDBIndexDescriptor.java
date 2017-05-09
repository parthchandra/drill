/*
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

import com.google.common.collect.Lists;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.expr.CloneVisitor;
import org.apache.drill.exec.store.mapr.PluginConstants;
import org.apache.drill.exec.util.EncodedSchemaPathSet;
import org.apache.drill.common.expression.LogicalExpression;

import com.google.common.collect.ImmutableSet;

public class MapRDBIndexDescriptor extends DrillIndexDescriptor {

  protected final Object desc;
  protected final Set<LogicalExpression> allFields;
  protected final Set<LogicalExpression> indexedFields;

  public MapRDBIndexDescriptor(List<LogicalExpression> indexCols,
                               List<LogicalExpression> nonIndexCols,
                               List<LogicalExpression> rowKeyColumns,
                               String indexName,
                               String tableName,
                               IndexDescriptor.IndexType type,
                               Object desc) {
    super(indexCols, nonIndexCols, rowKeyColumns, indexName, tableName, type);
    this.desc = desc;
    this.indexedFields = ImmutableSet.copyOf(indexColumns);
    this.allFields = new ImmutableSet.Builder<LogicalExpression>()
        .add(PluginConstants.DOCUMENT_SCHEMA_PATH)
        .addAll(indexColumns)
        .addAll(nonIndexColumns)
        .build();
  }

  public Object getOriginalDesc(){
    return desc;
  }

  @Override
  public boolean isCoveringIndex(List<LogicalExpression> columns) {
    DecodePathinExpr decodeExpr = new DecodePathinExpr();
    List<LogicalExpression> decodedCols = Lists.newArrayList();
    for(LogicalExpression expr : columns) {
      decodedCols.add(expr.accept(decodeExpr, null));
    }
    return columnsInIndexFields(decodedCols, allFields);
  }

  @Override
  public boolean allColumnsIndexed(Collection<LogicalExpression> columns) {
    DecodePathinExpr decodeExpr = new DecodePathinExpr();
    List<LogicalExpression> decodedCols = Lists.newArrayList();
    for(LogicalExpression expr : columns) {
      decodedCols.add(expr.accept(decodeExpr, null));
    }
    return columnsInIndexFields(decodedCols, indexedFields);
  }

  public FunctionalIndexInfo getFunctionalInfo() {
    return new MapRDBFunctionalIndexInfo(this);
  }
  /**
   * Search through a LogicalExpression, finding all internal schema path references and returning a decoded path.
   */
  private class DecodePathinExpr extends CloneVisitor {

    @Override
    public LogicalExpression visitSchemaPath(SchemaPath path, Void value) {
      List<SchemaPath> paths = Lists.newArrayList();
      paths.add(path);
      return EncodedSchemaPathSet.decode(paths).iterator().next();
    }
  }
}
