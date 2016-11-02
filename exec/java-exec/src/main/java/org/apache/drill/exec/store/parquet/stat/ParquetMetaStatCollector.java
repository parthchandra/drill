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
package org.apache.drill.exec.store.parquet.stat;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.store.parquet.Metadata;
import org.apache.drill.exec.store.parquet.ParquetGroupScan;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.joda.time.DateTimeConstants;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ParquetMetaStatCollector implements  ColumnStatCollector{
  private  final Metadata.ParquetTableMetadataBase parquetTableMetadata;
  private  final List<? extends Metadata.ColumnMetadata> columnMetadataList;
  final Map<String, String> implicitColValues;

  public ParquetMetaStatCollector(Metadata.ParquetTableMetadataBase parquetTableMetadata, List<? extends Metadata.ColumnMetadata> columnMetadataList, Map<String, String> implicitColValues) {
    this.parquetTableMetadata = parquetTableMetadata;
    this.columnMetadataList = columnMetadataList;
    this.implicitColValues = implicitColValues;
  }

  @Override
  public Map<SchemaPath, ColumnStatistics> collectColStat(Set<SchemaPath> fields) {
    // map from column to ColumnMetadata
    final Map<SchemaPath, Metadata.ColumnMetadata> columnMetadataMap = new HashMap<>();

    // map from column name to column statistics.
    final Map<SchemaPath, ColumnStatistics> statMap = new HashMap<>();

    for (final Metadata.ColumnMetadata columnMetadata : columnMetadataList) {
      SchemaPath schemaPath = SchemaPath.getCompoundPath(columnMetadata.getName());
      columnMetadataMap.put(schemaPath, columnMetadata);
    }

    for (final SchemaPath schemaPath : fields) {
      final PrimitiveType.PrimitiveTypeName primitiveType;
      final OriginalType originalType;

      final Metadata.ColumnMetadata columnMetadata = columnMetadataMap.get(schemaPath);

      if (columnMetadata != null) {
        final Object min = columnMetadata.getMinValue();
        final Object max = columnMetadata.getMaxValue();
        final Long numNull = columnMetadata.getNulls();

        primitiveType = this.parquetTableMetadata.getPrimitiveType(columnMetadata.getName());
        originalType = this.parquetTableMetadata.getOriginalType(columnMetadata.getName());
        final Integer repetitionLevel = this.parquetTableMetadata.getRepetitionLevel(columnMetadata.getName());

        statMap.put(schemaPath, getStat(min, max, numNull, primitiveType, originalType, repetitionLevel));
      } else {
        final String columnName = schemaPath.getRootSegment().getPath();
        if (implicitColValues.containsKey(columnName)) {
          TypeProtos.MajorType type = Types.required(TypeProtos.MinorType.VARCHAR);
          Statistics stat = new BinaryStatistics();
          stat.setNumNulls(0);
          byte[] val = implicitColValues.get(columnName).getBytes();
          stat.setMinMaxFromBytes(val, val);
          statMap.put(schemaPath, new ColumnStatistics(stat, type));
        }
      }
    }

    return statMap;
  }

  private ColumnStatistics getStat(Object min, Object max, Long numNull,
      PrimitiveType.PrimitiveTypeName primitiveType, OriginalType originalType, Integer repetitionLevel) {
    Statistics stat = Statistics.getStatsBasedOnType(primitiveType);
    Statistics convertedStat = stat;

    TypeProtos.MajorType type = ParquetGroupScan.getType(primitiveType, originalType);

    // Change to repeated if repetitionLevel > 0
    if (repetitionLevel != null && repetitionLevel > 0) {
      type = TypeProtos.MajorType.newBuilder().setMinorType(type.getMinorType()).setMode(TypeProtos.DataMode.REPEATED).build();
    }

    if (numNull != null) {
      stat.setNumNulls(numNull.longValue());
    }

    if (min != null && max != null ) {
      switch (type.getMinorType()) {
      case INT :
      case TIME:
        ((IntStatistics) stat).setMinMax(((Integer) min).intValue(), ((Integer) max).intValue());
        break;
      case BIGINT:
      case TIMESTAMP:
        ((LongStatistics) stat).setMinMax(((Long) min).longValue(), ((Long) max).longValue());
        break;
      case FLOAT4:
        ((FloatStatistics) stat).setMinMax(((Float) min).floatValue(), ((Float) max).floatValue());
        break;
      case FLOAT8:
        ((DoubleStatistics) stat).setMinMax(((Double) min).doubleValue(), ((Double) max).doubleValue());
        break;
      case DATE:
        convertedStat = new LongStatistics();
        convertedStat.setNumNulls(stat.getNumNulls());
        final long minMS = convertToDrillDateValue(((Integer) min).intValue());
        final long maxMS = convertToDrillDateValue(((Integer) max).intValue());
        ((LongStatistics) convertedStat ).setMinMax(minMS, maxMS);
        break;
      default:
      }
    }

    return new ColumnStatistics(convertedStat, type);
  }

  private static long convertToDrillDateValue(int dateValue) {
      return dateValue * (long) DateTimeConstants.MILLIS_PER_DAY;
  }

}
