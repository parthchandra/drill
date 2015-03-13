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
package org.apache.drill.exec.store.parquet;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.exec.store.parquet.columnreaders.ParquetRecordReader;
import org.apache.drill.exec.store.parquet2.DrillParquetReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import parquet.column.ColumnDescriptor;
import parquet.hadoop.Footer;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.Type;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;


public class ParquetScanBatchCreator implements BatchCreator<ParquetRowGroupScan>{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetScanBatchCreator.class);

  private static final String ENABLE_BYTES_READ_COUNTER = "parquet.benchmark.bytes.read";
  private static final String ENABLE_BYTES_TOTAL_COUNTER = "parquet.benchmark.bytes.total";
  private static final String ENABLE_TIME_READ_COUNTER = "parquet.benchmark.time.read";

  @Override
  public RecordBatch getBatch(FragmentContext context, ParquetRowGroupScan rowGroupScan, List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    String partitionDesignator = context.getOptions()
      .getOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL).string_val;
    List<SchemaPath> columns = rowGroupScan.getColumns();
    List<RecordReader> readers = Lists.newArrayList();
    OperatorContext oContext = new OperatorContext(rowGroupScan, context,
        false /* ScanBatch is not subject to fragment memory limit */);

    Map<Object,String[]> partitionColumns = Maps.newHashMap();
    List<Integer> selectedPartitionColumns = Lists.newArrayList();
    boolean selectAllColumns = AbstractRecordReader.isStarQuery(columns);

    List<SchemaPath> newColumns = columns;
    if (!selectAllColumns) {
      newColumns = Lists.newArrayList();
      Pattern pattern = Pattern.compile(String.format("%s[0-9]+", partitionDesignator));
      for (SchemaPath column : columns) {
        Matcher m = pattern.matcher(column.getAsUnescapedPath());
        if (m.matches()) {
          selectedPartitionColumns.add(Integer.parseInt(column.getAsUnescapedPath().toString().substring(partitionDesignator.length())));
        } else {
          newColumns.add(column);
        }
      }
      if (newColumns.isEmpty()) {
        newColumns = GroupScan.ALL_COLUMNS;
      }
      final int id = rowGroupScan.getOperatorId();
      // Create the new row group scan with the new columns
      rowGroupScan = new ParquetRowGroupScan(rowGroupScan.getStorageEngine(), rowGroupScan.getRowGroupReadEntries(), newColumns, rowGroupScan.getSelectionRoot());
      rowGroupScan.setOperatorId(id);
    }

    DrillFileSystem fs = new DrillFileSystem(rowGroupScan.getStorageEngine().getFileSystem(), oContext.getStats());
    Configuration conf = fs.getConf();
    conf.setBoolean(ENABLE_BYTES_READ_COUNTER, false);
    conf.setBoolean(ENABLE_BYTES_TOTAL_COUNTER, false);
    conf.setBoolean(ENABLE_TIME_READ_COUNTER, false);

    int numParts = 0;
    for(ReadEntryWithPath e : rowGroupScan.getRowGroupReadEntries()){
      if (rowGroupScan.getSelectionRoot() != null) {
        String[] r = rowGroupScan.getSelectionRoot().split("/");
        String[] p = e.getPath().split("/");
        if (p.length > r.length) {
          String[] q = ArrayUtils.subarray(p, r.length, p.length - 1);
          partitionColumns.put(e.getPath(), q);
          numParts = Math.max(numParts, q.length);
        }
      }
    }

    if (selectAllColumns) {
      for (int i = 0; i < numParts; i++) {
        selectedPartitionColumns.add(i);
      }
    }

    Iterator<RecordReader> readerIterator = new RecordReaderIterator(context, rowGroupScan, fs);

    ScanBatch s =
        new ScanBatch(rowGroupScan, context, oContext, readerIterator, partitionColumns, selectedPartitionColumns);

    for(RecordReader r  : readers){
      r.setOperatorContext(s.getOperatorContext());
    }

    return s;
  }

  private static boolean isComplex(ParquetMetadata footer) {
    MessageType schema = footer.getFileMetaData().getSchema();

    for (Type type : schema.getFields()) {
      if (!type.isPrimitive()) {
        return true;
      }
    }
    for (ColumnDescriptor col : schema.getColumns()) {
      if (col.getMaxRepetitionLevel() > 0) {
        return true;
      }
    }
    return false;
  }

  static class RecordReaderIterator implements Iterator<RecordReader> {

    private ParquetRowGroupScan scan;
    private Iterator<ReadEntryWithPath> entries;
    private DrillFileSystem fs;
    private ReadEntryWithPath currentEntry;
    private ParquetMetadata currentFooter;
    private int currentRowGroupIndex = 0;
    private RecordReader next;
    private FragmentContext context;
    private boolean useNewReader;

    RecordReaderIterator(FragmentContext context, ParquetRowGroupScan scan, DrillFileSystem fs) {
      this.context = context;
      this.scan = scan;
      this.entries = scan.getRowGroupReadEntries().iterator();
      this.fs = fs;
      this.useNewReader = context.getOptions().getOption(ExecConstants.PARQUET_NEW_RECORD_READER).bool_val;
    }

    @Override
    public boolean hasNext() {
      if (next != null) {
        return true;
      }
      return getNext();
    }

    @Override
    public RecordReader next() {
      if (next != null) {
        RecordReader toReturn = next;
        next = null;
        return toReturn;
      } else {
        if (!getNext()) {
          throw new NoSuchElementException();
        }
        RecordReader toReturn = next;
        next = null;
        return toReturn;
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    private boolean getNext() {
      if (currentEntry == null || currentRowGroupIndex >= currentFooter.getBlocks().size()) {
        if (!entries.hasNext()) {
          return false;
        }
        currentEntry = entries.next();
        try {
          currentFooter = ParquetFileReader.readFooter(fs.getConf(), new Path(currentEntry.getPath()));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        currentRowGroupIndex = 0;
      }
      try {
        RecordReader reader;
        if (useNewReader || isComplex(currentFooter)) {
          reader = new DrillParquetReader(context, currentFooter, currentEntry, currentRowGroupIndex, scan.getColumns(), fs);
          reader.setKey(currentEntry.getPath());
        } else {
          reader = new ParquetRecordReader(context, currentEntry.getPath(), currentRowGroupIndex, fs,
            scan.getStorageEngine().getCodecFactoryExposer(), currentFooter, scan.getColumns());
          reader.setKey(currentEntry.getPath());
        }
        next = reader;
      } catch (ExecutionSetupException e) {
        throw new RuntimeException(e);
      }
      currentRowGroupIndex++;
      return true;
    }
  }

}
