/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.parquet;

import com.google.common.collect.Sets;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.exec.compile.sig.ConstantExpressionIdentifier;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.stat.ParquetFilterPredicate;
import org.apache.drill.exec.expr.stat.RangeExprEvaluator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.parquet.stat.ColumnStatCollector;
import org.apache.drill.exec.store.parquet.stat.ColumnStatistics;
import org.apache.drill.exec.store.parquet.stat.ParquetFooterStatCollector;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.statisticslevel.StatisticsFilter;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ParquetRGFilterEvaluator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetRGFilterEvaluator.class);

  public static boolean evalFilter(LogicalExpression expr, ParquetMetadata footer, int rowGroupIndex,
      OptionManager options, FragmentContext fragmentContext) {
    final HashMap<String, String> emptyMap = new HashMap<String, String>();
    return evalFilter(expr, footer, rowGroupIndex, options, fragmentContext, emptyMap);
  }

  public static boolean evalFilter(LogicalExpression expr, ParquetMetadata footer, int rowGroupIndex,
      OptionManager options, FragmentContext fragmentContext, Map<String, String> implicitColValues) {
    // figure out the set of columns referenced in expression.
    final Set<SchemaPath> schemaPathsInExpr = expr.accept(new FieldReferenceFinder(), null);
    final ColumnStatCollector columnStatCollector = new ParquetFooterStatCollector(footer, rowGroupIndex, implicitColValues,true, options);

    Map<SchemaPath, ColumnStatistics> columnStatisticsMap = columnStatCollector.collectColStat(schemaPathsInExpr);

    boolean canDrop = canDrop(expr, columnStatisticsMap, footer.getBlocks().get(rowGroupIndex).getRowCount(), fragmentContext, fragmentContext.getFunctionRegistry());
    return canDrop;
  }


  public static boolean canDrop(ParquetFilterPredicate parquetPredicate, Map<SchemaPath,
      ColumnStatistics> columnStatisticsMap, long rowCount) {
    boolean canDrop = false;
    if (parquetPredicate != null) {
      RangeExprEvaluator rangeExprEvaluator = new RangeExprEvaluator(columnStatisticsMap, rowCount);
      canDrop = parquetPredicate.canDrop(rangeExprEvaluator);
    }
    return canDrop;
  }


  public static boolean canDrop(LogicalExpression expr, Map<SchemaPath, ColumnStatistics> columnStatisticsMap,
      long rowCount, UdfUtilities udfUtilities, FunctionImplementationRegistry functionImplementationRegistry) {
    ErrorCollector errorCollector = new ErrorCollectorImpl();
    LogicalExpression materializedFilter = ExpressionTreeMaterializer.materializeFilterExpr(
        expr, columnStatisticsMap, errorCollector, functionImplementationRegistry);

    if (errorCollector.hasErrors()) {
      logger.error("{} error(s) encountered when materialize filter expression : {}",
          errorCollector.getErrorCount(), errorCollector.toErrorString());
      return false;
    }

    Set<LogicalExpression> constantBoundaries = ConstantExpressionIdentifier.getConstantExpressionSet(materializedFilter);
    ParquetFilterPredicate parquetPredicate = (ParquetFilterPredicate) ParquetFilterBuilder.buildParquetFilterPredicate(
        materializedFilter, constantBoundaries, udfUtilities);

    return canDrop(parquetPredicate, columnStatisticsMap, rowCount);
  }

//  public static boolean evalFilter2(LogicalExpression expr, ParquetMetadata footer, int rowGroupIndex, OptionManager options, FragmentContext fragmentContext, Map<String, String> implicitColValues) {
//    // map from column name to SchemaPath
//    final CaseInsensitiveMap<SchemaPath> columnInExprMap = CaseInsensitiveMap.newHashMap();
//    // map from column name to ColumnDescriptor
//    CaseInsensitiveMap<ColumnDescriptor> columnDescMap = CaseInsensitiveMap.newHashMap();
//    // map from column name to ColumnChunkMetaData
//    final CaseInsensitiveMap<ColumnChunkMetaData> columnChkMetaMap = CaseInsensitiveMap.newHashMap();
//    // map from column name to MajorType
//    final CaseInsensitiveMap<TypeProtos.MajorType> columnTypeMap = CaseInsensitiveMap.newHashMap();
//    // map from column name to SchemaElement
//    final CaseInsensitiveMap<SchemaElement> schemaElementMap = CaseInsensitiveMap.newHashMap();
//    // map from column name to column statistics.
//    CaseInsensitiveMap<Statistics> statMap = CaseInsensitiveMap.newHashMap();
//
//
//    final FileMetaData fileMetaData = new ParquetMetadataConverter().toParquetMetadata(ParquetFileWriter.CURRENT_VERSION, footer);
//
//    // figure out the set of columns referenced in expression.
//    final Set<SchemaPath> schemaPathsInExpr = expr.accept(new FieldReferenceFinder(), null);
//    for (final SchemaPath path : schemaPathsInExpr) {
//      columnInExprMap.put(path.getRootSegment().getPath(), path);
//    }
//
//    for (final ColumnDescriptor column : footer.getFileMetaData().getSchema().getColumns()) {
//      if (columnInExprMap.containsKey(column.getPath()[0])) {
//        columnDescMap.put(column.getPath()[0], column);
//      }
//    }
//
//    for (final SchemaElement se : fileMetaData.getSchema()) {
//      if (columnInExprMap.containsKey(se.getName())) {
//        schemaElementMap.put(se.getName(), se);
//      }
//    }
//
//    final long rowCount = footer.getBlocks().get(rowGroupIndex).getRowCount();
//    for (final ColumnChunkMetaData colMetaData: footer.getBlocks().get(rowGroupIndex).getColumns()) {
//
//      if (rowCount != colMetaData.getValueCount()) {
//        logger.warn("rowCount : {} for rowGroup {} is different from column {}'s valueCount : {}",
//            rowCount, rowGroupIndex, colMetaData.getPath().toDotString(), colMetaData.getValueCount());
//        return false;
//      }
//
//      if (columnInExprMap.containsKey(colMetaData.getPath().toDotString())) {
//        columnChkMetaMap.put(colMetaData.getPath().toDotString(), colMetaData);
//      }
//    }
//
//    for (final String path : columnInExprMap.keySet()) {
//      if (columnDescMap.containsKey(path) && schemaElementMap.containsKey(path) && columnChkMetaMap.containsKey(path)) {
//        ColumnDescriptor columnDesc =  columnDescMap.get(path);
//        SchemaElement se = schemaElementMap.get(path);
//        ColumnChunkMetaData metaData = columnChkMetaMap.get(path);
//
//        TypeProtos.MajorType type = ParquetToDrillTypeConverter.toMajorType(columnDesc.getType(), se.getType_length(),
//            getDataMode(columnDesc), se, options);
//
//        columnTypeMap.put(path, type);
//
//        if (metaData != null) {
//          Statistics stat = convertStatIfNecessary(metaData.getStatistics(), type.getMinorType());
//          statMap.put(path, stat);
//        }
//      } else if (implicitColValues.containsKey(path)) {
//        columnTypeMap.put(path, Types.required(TypeProtos.MinorType.VARCHAR)); // implicit columns "dir0", "filename", etc.
//        Statistics stat = new BinaryStatistics();
//        stat.setNumNulls(0);
//        byte[] val = implicitColValues.get(path).getBytes();
//        stat.setMinMaxFromBytes(val, val);
//        statMap.put(path, stat);
//      }
//    }
//
//    ErrorCollector errorCollector = new ErrorCollectorImpl();
//    LogicalExpression materializedFilter = ExpressionTreeMaterializer.materializeFilterExpr(
//        expr, columnTypeMap, errorCollector, fragmentContext.getFunctionRegistry());
//
//    if (errorCollector.hasErrors()) {
//      logger.error("{} error(s) encountered when materialize filter expression : {}",
//          errorCollector.getErrorCount(), errorCollector.toErrorString());
//      return false;
//    }
//    logger.debug("materializedFilter : {}", ExpressionStringBuilder.toString(materializedFilter));
//
//    Set<LogicalExpression> constantBoundaries = ConstantExpressionIdentifier.getConstantExpressionSet(materializedFilter);
//    ParquetFilterPredicate parquetPredicate = (ParquetFilterPredicate) ParquetFilterBuilder.buildParquetFilterPredicate(
//        materializedFilter, constantBoundaries, fragmentContext);
//
//    boolean canDrop = false;
//    if (parquetPredicate != null) {
//      RangeExprEvaluator rangeExprEvaluator = new RangeExprEvaluator(statMap, rowCount);
//      canDrop = parquetPredicate.canDrop(rangeExprEvaluator);
//    }
//    logger.debug(" canDrop {} ", canDrop);
//    return canDrop;
//  }
//
//  private static TypeProtos.DataMode getDataMode(ColumnDescriptor column) {
//    if (column.getMaxRepetitionLevel() > 0 ) {
//      return TypeProtos.DataMode.REPEATED;
//    } else if (column.getMaxDefinitionLevel() == 0) {
//      return TypeProtos.DataMode.REQUIRED;
//    } else {
//      return TypeProtos.DataMode.OPTIONAL;
//    }
//  }
//
//  private static Statistics convertStatIfNecessary(Statistics stat, TypeProtos.MinorType type) {
//    if (type != TypeProtos.MinorType.DATE) {
//      return stat;
//    } else {
//      IntStatistics dateStat = (IntStatistics) stat;
//      LongStatistics dateMLS = new LongStatistics();
//      if (!dateStat.isEmpty()) {
//        dateMLS.setMinMax(convertToDrillDateValue(dateStat.getMin()), convertToDrillDateValue(dateStat.getMax()));
//        dateMLS.setNumNulls(dateStat.getNumNulls());
//      }
//
//      return dateMLS;
//    }
//  }
//
//  private static long convertToDrillDateValue(int dateValue) {
//    long  dateInMillis = DateTimeUtils.fromJulianDay(dateValue - ParquetOutputRecordWriter.JULIAN_DAY_EPOC - 0.5);
////    // Specific for date column created by Drill CTAS prior fix for DRILL-4203.
////    // Apply the same shift as in ParquetOutputRecordWriter.java for data value.
////    final int intValue = (int) (DateTimeUtils.toJulianDayNumber(dateInMillis) + JULIAN_DAY_EPOC);
////    return intValue;
//    return dateInMillis;
//
//  }

  /**
   * Search through a LogicalExpression, finding all internal schema path references and returning them in a set.
   */
  public static class FieldReferenceFinder extends AbstractExprVisitor<Set<SchemaPath>, Void, RuntimeException> {
    @Override
    public Set<SchemaPath> visitSchemaPath(SchemaPath path, Void value) {
      Set<SchemaPath> set = Sets.newHashSet();
      set.add(path);
      return set;
    }

    @Override
    public Set<SchemaPath> visitUnknown(LogicalExpression e, Void value) {
      Set<SchemaPath> paths = Sets.newHashSet();
      for (LogicalExpression ex : e) {
        paths.addAll(ex.accept(this, null));
      }
      return paths;
    }
  }
}
