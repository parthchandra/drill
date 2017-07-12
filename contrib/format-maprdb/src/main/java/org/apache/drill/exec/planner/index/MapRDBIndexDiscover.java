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

import com.google.common.collect.Maps;
import com.mapr.db.Admin;
import com.mapr.db.MapRDB;
import com.mapr.db.exceptions.DBException;
import com.mapr.db.index.IndexDesc;
import com.mapr.db.index.IndexFieldDesc;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionCallFactory;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.parser.ExprLexer;
import org.apache.drill.common.expression.parser.ExprParser;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.base.AbstractDbGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.mapr.db.MapRDBFormatMatcher;
import org.apache.drill.exec.store.mapr.db.MapRDBFormatPlugin;
import org.apache.drill.exec.store.mapr.db.MapRDBGroupScan;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MapRDBIndexDiscover extends IndexDiscoverBase implements IndexDiscover {

  public MapRDBIndexDiscover(GroupScan inScan, DrillScanRelBase scanRel) {
    super((AbstractDbGroupScan) inScan, scanRel);
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
      return new DrillIndexCollection(getOriginalScanRel(), idxSet);
    } catch (DBException ex) {
      logger.error("Could not get table index from File system.", ex);
    }
    catch(InvalidIndexDefinitionException ex) {
      logger.error("Invalid index definition detected.", ex);
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

      DrillFileSystem fs = ImpersonationUtil.createFileSystem(origScan.getUserName(), fsPlugin.getFsConf());
      MapRDBFormatMatcher matcher = (MapRDBFormatMatcher) (maprFormatPlugin.getMatcher());
      FileSelection fsSelection = deriveFSSelection(fs, idxDescriptor);
      return matcher.isReadableIndex(fs, fsSelection, fsPlugin, fsPlugin.getName(),
          origScan.getUserName(), idxDescriptor);

    } catch (Exception e) {
      logger.error("Failed to get native DrillTable.", e);
    }
    return null;
  }

  private SchemaPath fieldName2SchemaPath(String fieldName) {
    if (fieldName.contains(":")) {
      fieldName = fieldName.split(":")[1];
    }
    if (fieldName.contains(".")) {
      return SchemaPath.getCompoundPath(fieldName.split("\\."));
    }
    return SchemaPath.getSimplePath(fieldName);
  }

  String getDrillTypeStr(String maprdbTypeStr) {
    String typeStr = maprdbTypeStr.toUpperCase();
    switch(typeStr){
      case "STRING":
        // set default width since it is not specified
        return "VARCHAR(128)";
      case "LONG":
        return "BIGINT";
      case "INT":
      case "INTEGER":
        return "INT";
      case "FLOAT":
        return "FLOAT4";
      case "DOUBLE":
        return "FLOAT8";
      case "INTERVAL_YEAR_MONTH":
        return "INTERVALYEAR";
      case "INTERVAL_DAY_TIME":
        return "INTERVALDAY";
      case "BOOLEAN":
        return "BIT";
      case "BINARY":
        return "VARBINARY";
      case "ANY":
      case "DECIMAL":
        return null;
      default: return typeStr;
    }

  }

  TypeProtos.MajorType getDrillType(String typeStr) {
    switch(typeStr){
      case "VARCHAR":
      case "CHAR":
      case "STRING":
        // set default width since it is not specified
        return
            Types.required(TypeProtos.MinorType.VARCHAR).toBuilder().setWidth(
                getOriginalScanRel().getCluster().getTypeFactory().createSqlType(SqlTypeName.VARCHAR).getPrecision()).build();
      case "LONG":
      case "BIGINT":
        return Types.required(TypeProtos.MinorType.BIGINT);
      case "INT":
      case "INTEGER":
        return Types.required(TypeProtos.MinorType.INT);
      case "FLOAT":
        return Types.required(TypeProtos.MinorType.FLOAT4);
      case "DOUBLE":
        return Types.required(TypeProtos.MinorType.FLOAT8);
      case "INTERVAL_YEAR_MONTH":
        return Types.required(TypeProtos.MinorType.INTERVALYEAR);
      case "INTERVAL_DAY_TIME":
        return Types.required(TypeProtos.MinorType.INTERVALDAY);
      case "BOOLEAN":
        return Types.required(TypeProtos.MinorType.BIT);
      case "BINARY":
        return Types.required(TypeProtos.MinorType.VARBINARY).toBuilder().build();
      case "ANY":
      case "DECIMAL":
        return null;
      default: return Types.required(TypeProtos.MinorType.valueOf(typeStr));
    }
  }

  /**
   * build castExpression withe the type and field defined in indexDesc
   * @param field
   * @param type
   * @return
   */
  private LogicalExpression castFunctionCall(String field, String type) {
    TypeProtos.MajorType castType = getDrillType(type);
    if(castType == null) {//no cast
      return fieldName2SchemaPath(field);
    }
    return FunctionCallFactory.createCast(castType, ExpressionPosition.UNKNOWN, fieldName2SchemaPath(field));
  }

  private LogicalExpression castFunctionSQLSyntax(String field, String type) throws InvalidIndexDefinitionException {
    //get castTypeStr so we can construct SQL syntax string before MapRDB could provide such syntax
    String castTypeStr = getDrillTypeStr(type);
    if(castTypeStr == null) {//no cast
      throw new InvalidIndexDefinitionException("cast function type not recognized: " + type + "for field " + field);
    }
    try {
      String castFunc = String.format("cast( %s as %s)", field, castTypeStr);
      final ExprLexer lexer = new ExprLexer(new ANTLRStringStream(castFunc));
      final CommonTokenStream tokens = new CommonTokenStream(lexer);
      final ExprParser parser = new ExprParser(tokens);
      final ExprParser.parse_return ret = parser.parse();
      logger.trace("{}, {}", tokens, ret);
      return ret.e;
    }catch(Exception ex) {
      logger.error("parse failed{}", ex);
    }
    return null;
  }

  private LogicalExpression getIndexExpression(IndexFieldDesc desc) throws InvalidIndexDefinitionException {
    final String fieldName = desc.getFieldPath().asPathString();
    final String functionDef = desc.getFunctionName();
    if ((functionDef != null)) {//this is a function
      String[] tokens = functionDef.split("\\s+");
      if (tokens[0].equalsIgnoreCase("cast")) {
        if (tokens.length != 3) {
          throw new InvalidIndexDefinitionException("cast function definition not recognized: " + functionDef);
        }
        return castFunctionSQLSyntax(fieldName, tokens[2]);
      } else {
        throw new InvalidIndexDefinitionException("function {} is not supported for indexing" + functionDef);
      }
    }
    //else it is a schemaPath
    return fieldName2SchemaPath(fieldName);
  }

  private List<LogicalExpression> field2SchemaPath(Collection<IndexFieldDesc> descCollection)
      throws InvalidIndexDefinitionException {
    List<LogicalExpression> listSchema = new ArrayList<>();
    for (IndexFieldDesc field : descCollection) {
        listSchema.add(getIndexExpression(field));
    }
    return listSchema;
  }

  private List<RelFieldCollation> getFieldCollations(Collection<IndexFieldDesc> descCollection) {
    List<RelFieldCollation> fieldCollations = new ArrayList<>();
    int i=0;
    for (IndexFieldDesc field : descCollection) {
      RelFieldCollation.Direction direction = (field.getSortOrder() == IndexFieldDesc.Order.Asc) ?
          RelFieldCollation.Direction.ASCENDING : (field.getSortOrder() == IndexFieldDesc.Order.Desc ?
              RelFieldCollation.Direction.DESCENDING : null);
      if (direction != null) {
        // assume null direction of NULLS UNSPECIFIED for now until MapR-DB adds that to the APIs
        RelFieldCollation.NullDirection nulldir = RelFieldCollation.NullDirection.UNSPECIFIED;
        RelFieldCollation c = new RelFieldCollation(i++, direction, nulldir);
        fieldCollations.add(c);
      } else {
        // if the direction is not present for a field, no need to examine remaining fields
        break;
      }
    }
    return fieldCollations;
  }

  private CollationContext buildCollationContext(List<LogicalExpression> indexFields,
      List<RelFieldCollation> indexFieldCollations) {
    assert indexFieldCollations.size() <= indexFields.size();
    Map<LogicalExpression, RelFieldCollation> collationMap = Maps.newHashMap();
    for (int i = 0; i < indexFieldCollations.size(); i++) {
      collationMap.put(indexFields.get(i), indexFieldCollations.get(i));
    }
    CollationContext collationContext = new CollationContext(collationMap, indexFieldCollations);
    return collationContext;
  }

  private DrillIndexDescriptor buildIndexDescriptor(String tableName, IndexDesc desc)
      throws InvalidIndexDefinitionException {
    if (desc.isExternal()) {
      //XX: not support external index
      return null;
    }

    IndexDescriptor.IndexType idxType = IndexDescriptor.IndexType.NATIVE_SECONDARY_INDEX;
    List<LogicalExpression> indexFields = field2SchemaPath(desc.getIndexedFields());
    List<LogicalExpression> coveringFields = field2SchemaPath(desc.getCoveredFields());
    coveringFields.add(SchemaPath.getSimplePath("_id"));
    CollationContext collationContext = null;
    if (!desc.isHashed()) { // hash index has no collation property
      List<RelFieldCollation> indexFieldCollations = getFieldCollations(desc.getIndexedFields());
      collationContext = buildCollationContext(indexFields, indexFieldCollations);
    }

    DrillIndexDescriptor idx = new MapRDBIndexDescriptor (
        indexFields,
        collationContext,
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

    final MapRDBGroupScan dbGroupScan = (MapRDBGroupScan) getOriginalScan();
    final UserGroupInformation currentUser = ImpersonationUtil.createProxyUgi(dbGroupScan.getUserName());
    final Configuration conf = dbGroupScan.getFormatPlugin().getFsConf();

    final Admin admin;
    try {
      admin = currentUser.doAs(new PrivilegedExceptionAction<Admin>() {
        public Admin run() throws Exception {
          return MapRDB.getAdmin(conf);
        }
      });
    } catch (Exception e) {
      throw new DrillRuntimeException("Failed to get Admin instance for user: " + currentUser.getUserName(), e);
    }
    return admin;
  }
}
