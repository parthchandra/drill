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
package org.apache.drill.exec.store.parquet.columnreaders;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.parquet.ParquetReaderStats;
import org.apache.drill.exec.store.parquet.ParquetReaderUtility;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.PrimitiveType;

import com.google.common.collect.Lists;

public class ParquetRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetRecordReader.class);

  // this value has been inflated to read in multiple value vectors at once, and then break them up into smaller vectors
  private static final int NUMBER_OF_VECTORS = 1;
  private static final long DEFAULT_BATCH_LENGTH = 256 * 1024 * NUMBER_OF_VECTORS; // 256kb
  private static final long DEFAULT_BATCH_LENGTH_IN_BITS = DEFAULT_BATCH_LENGTH * 8; // 256kb
  private static final char DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH = 32*1024;
  private static final int NUM_RECORDS_TO_READ_NOT_SPECIFIED = -1;

  // When no column is required by the downstrea operator, ask SCAN to return a DEFAULT column. If such column does not exist,
  // it will return as a nullable-int column. If that column happens to exist, return that column.
  protected static final List<SchemaPath> DEFAULT_COLS_TO_READ = ImmutableList.of(SchemaPath.getSimplePath("_DEFAULT_COL_TO_READ_"));

  // TODO - should probably find a smarter way to set this, currently 1 megabyte
  public static final int PARQUET_PAGE_MAX_SIZE = 1024 * 1024 * 1;

  // used for clearing the last n bits of a byte
  public static final byte[] endBitMasks = {-2, -4, -8, -16, -32, -64, -128};
  // used for clearing the first n bits of a byte
  public static final byte[] startBitMasks = {127, 63, 31, 15, 7, 3, 1};

  private int bitWidthAllFixedFields;
  private boolean allFieldsFixedLength;
  private int recordsPerBatch;
  private OperatorContext operatorContext;
//  private long totalRecords;
//  private long rowGroupOffset;

  private List<ColumnReader<?>> columnStatuses;
  private FileSystem fileSystem;
  private long batchSize;
  private long numRecordsToRead; // number of records to read

  Path hadoopPath;
  private VarLenBinaryReader varLengthReader;
  private ParquetMetadata footer;
  // This is a parallel list to the columns list above, it is used to determine the subset of the project
  // pushdown columns that do not appear in this file
  private boolean[] columnsFound;
  // For columns not found in the file, we need to return a schema element with the correct number of values
  // at that position in the schema. Currently this requires a vector be present. Here is a list of all of these vectors
  // that need only have their value count set at the end of each call to next(), as the values default to null.
  private List<NullableIntVector> nullFilledVectors;
  // Keeps track of the number of records returned in the case where only columns outside of the file were selected.
  // No actual data needs to be read out of the file, we only need to return batches until we have 'read' the number of
  // records specified in the row group metadata
  long mockRecordsRead;

  private final CodecFactory codecFactory;
  int rowGroupIndex;
  long totalRecordsRead;
  private final FragmentContext fragmentContext;
  ParquetReaderUtility.DateCorruptionStatus dateCorruptionStatus;

  public boolean useAsyncColReader;
  public boolean useAsyncPageReader;
  public boolean useBufferedReader;
  public int bufferedReadSize;
  public boolean useFadvise;
  public boolean enforceTotalSize;
  public long readQueueSize;

  private String name;


  public ParquetReaderStats parquetReaderStats = new ParquetReaderStats();

  public enum Metric implements MetricDef {
    NUM_DICT_PAGE_LOADS,         // Number of dictionary pages read
    NUM_DATA_PAGE_lOADS,         // Number of data pages read
    NUM_DATA_PAGES_DECODED,      // Number of data pages decoded
    NUM_DICT_PAGES_DECOMPRESSED, // Number of dictionary pages decompressed
    NUM_DATA_PAGES_DECOMPRESSED, // Number of data pages decompressed
    TOTAL_DICT_PAGE_READ_BYTES,  // Total bytes read from disk for dictionary pages
    TOTAL_DATA_PAGE_READ_BYTES,  // Total bytes read from disk for data pages
    TOTAL_DICT_DECOMPRESSED_BYTES, // Total bytes decompressed for dictionary pages (same as compressed bytes on disk)
    TOTAL_DATA_DECOMPRESSED_BYTES, // Total bytes decompressed for data pages (same as compressed bytes on disk)
    TIME_DICT_PAGE_LOADS,          // Time in nanos in reading dictionary pages from disk
    TIME_DATA_PAGE_LOADS,          // Time in nanos in reading data pages from disk
    TIME_DATA_PAGE_DECODE,         // Time in nanos in decoding data pages
    TIME_DICT_PAGE_DECODE,         // Time in nanos in decoding dictionary pages
    TIME_DICT_PAGES_DECOMPRESSED,  // Time in nanos in decompressing dictionary pages
    TIME_DATA_PAGES_DECOMPRESSED,  // Time in nanos in decompressing data pages
    TIME_DISK_SCAN_WAIT,           // Time in nanos spent in waiting for an async disk read to complete
    TIME_DISK_SCAN,                // Time in nanos spent in reading data from disk.
    TIME_FIXEDCOLUMN_READ,         // Time in nanos spent in converting fixed width data to value vectors
    TIME_VARCOLUMN_READ,           // Time in nanos spent in converting varwidth data to value vectors
    TIME_PROCESS,                  // Time in nanos spent in processing
    TIME_STATS,                    // Time in nanos spent in processing stats
    TIME_INIT,                     // Time in nanos spent in initializing
    CPU_TIME_DICT_PAGE_LOADS,          // CPU time in nanos in reading dictionary pages from disk
    CPU_TIME_DATA_PAGE_LOADS,          // CPU time in nanos in reading data pages from disk
    CPU_TIME_DATA_PAGE_DECODE,         // CPU time in nanos in decoding data pages
    CPU_TIME_DICT_PAGE_DECODE,         // CPU time in nanos in decoding dictionary pages
    CPU_TIME_DICT_PAGES_DECOMPRESSED,  // CPU time in nanos in decompressing dictionary pages
    CPU_TIME_DATA_PAGES_DECOMPRESSED,  // CPU time in nanos in decompressing data pages
    CPU_TIME_DISK_SCAN,                // CPU time in nanos spent in reading data from disk.
    CPU_TIME_FIXEDCOLUMN_READ,         // CPU time in nanos spent in converting fixed width data to value vectors
    CPU_TIME_VARCOLUMN_READ,           // CPU time in nanos spent in converting varwidth data to value vectors
    CPU_TIME_PROCESS,                  // CPU time in nanos spent in processing
    CPU_TIME_STATS,                    // CPU time in nanos spent in processing stats
    CPU_TIME_INIT;                     // CPU time in nanos spent in initializing


    @Override public int metricId() {
      return ordinal();
    }
  }

  public ParquetRecordReader(FragmentContext fragmentContext,
      String path,
      int rowGroupIndex,
      long numRecordsToRead,
      FileSystem fs,
      CodecFactory codecFactory,
      ParquetMetadata footer,
      List<SchemaPath> columns,
      ParquetReaderUtility.DateCorruptionStatus dateCorruptionStatus) throws ExecutionSetupException {
    this(fragmentContext, DEFAULT_BATCH_LENGTH_IN_BITS, numRecordsToRead,
         path, rowGroupIndex, fs, codecFactory, footer, columns, dateCorruptionStatus);
  }

  public ParquetRecordReader(FragmentContext fragmentContext,
      String path,
      int rowGroupIndex,
      FileSystem fs,
      CodecFactory codecFactory,
      ParquetMetadata footer,
      List<SchemaPath> columns,
      ParquetReaderUtility.DateCorruptionStatus dateCorruptionStatus)
      throws ExecutionSetupException {
      this(fragmentContext, DEFAULT_BATCH_LENGTH_IN_BITS, footer.getBlocks().get(rowGroupIndex).getRowCount(),
           path, rowGroupIndex, fs, codecFactory, footer, columns, dateCorruptionStatus);
  }

  public ParquetRecordReader(
      FragmentContext fragmentContext,
      long batchSize,
      long numRecordsToRead,
      String path,
      int rowGroupIndex,
      FileSystem fs,
      CodecFactory codecFactory,
      ParquetMetadata footer,
      List<SchemaPath> columns,
      ParquetReaderUtility.DateCorruptionStatus dateCorruptionStatus) throws ExecutionSetupException {
    this.name = path;
    this.hadoopPath = new Path(path);
    this.fileSystem = fs;
    this.codecFactory = codecFactory;
    this.rowGroupIndex = rowGroupIndex;
    this.batchSize = batchSize;
    this.footer = footer;
    this.dateCorruptionStatus = dateCorruptionStatus;
    this.fragmentContext = fragmentContext;
    // Callers can pass -1 if they want to read all rows.
    if (numRecordsToRead == NUM_RECORDS_TO_READ_NOT_SPECIFIED) {
      this.numRecordsToRead = footer.getBlocks().get(rowGroupIndex).getRowCount();
    } else {
      assert (numRecordsToRead >= 0);
      this.numRecordsToRead = Math.min(numRecordsToRead, footer.getBlocks().get(rowGroupIndex).getRowCount());
    }
    useAsyncColReader =
        fragmentContext.getOptions().getOption(ExecConstants.PARQUET_COLUMNREADER_ASYNC).bool_val;
    useAsyncPageReader =
        fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_ASYNC).bool_val;
    useBufferedReader =
        fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_USE_BUFFERED_READ).bool_val;
    bufferedReadSize =
        fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_BUFFER_SIZE).num_val.intValue();
    useFadvise =
        fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_USE_FADVISE).bool_val;
    readQueueSize =
        fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_QUEUE_SIZE).num_val;
    enforceTotalSize =
        fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_ENFORCETOTALSIZE).bool_val;

    setColumns(columns);
  }

  /**
   * Flag indicating if the old non-standard data format appears
   * in this file, see DRILL-4203.
   *
   * @return true if the dates are corrupted and need to be corrected
   */
  public ParquetReaderUtility.DateCorruptionStatus getDateCorruptionStatus() {
    return dateCorruptionStatus;
  }

  public CodecFactory getCodecFactory() {
    return codecFactory;
  }

  public Path getHadoopPath() {
    return hadoopPath;
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }

  public int getRowGroupIndex() {
    return rowGroupIndex;
  }

  public int getBitWidthAllFixedFields() {
    return bitWidthAllFixedFields;
  }

  public long getBatchSize() {
    return batchSize;
  }

  /**
   * @param type a fixed length type from the parquet library enum
   * @return the length in pageDataByteArray of the type
   */
  public static int getTypeLengthInBits(PrimitiveType.PrimitiveTypeName type) {
    switch (type) {
      case INT64:   return 64;
      case INT32:   return 32;
      case BOOLEAN: return 1;
      case FLOAT:   return 32;
      case DOUBLE:  return 64;
      case INT96:   return 96;
      // binary and fixed length byte array
      default:
        throw new IllegalStateException("Length cannot be determined for type " + type);
    }
  }

  private boolean fieldSelected(MaterializedField field) {
    // TODO - not sure if this is how we want to represent this
    // for now it makes the existing tests pass, simply selecting
    // all available data if no columns are provided
    if (isStarQuery()) {
      return true;
    }

    int i = 0;
    for (SchemaPath expr : getColumns()) {
      if ( field.getPath().equalsIgnoreCase(expr.getAsUnescapedPath())) {
        columnsFound[i] = true;
        return true;
      }
      i++;
    }
    return false;
  }

  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  public FragmentContext getFragmentContext() {
    return fragmentContext;
  }

  /**
   * Returns data type length for a given {@see ColumnDescriptor} and it's corresponding
   * {@see SchemaElement}. Neither is enough information alone as the max
   * repetition level (indicating if it is an array type) is in the ColumnDescriptor and
   * the length of a fixed width field is stored at the schema level.
   *
   * @return the length if fixed width, else -1
   */
  private int getDataTypeLength(ColumnDescriptor column, SchemaElement se) {
    if (column.getType() != PrimitiveType.PrimitiveTypeName.BINARY) {
      if (column.getMaxRepetitionLevel() > 0) {
        return -1;
      }
      if (column.getType() == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
        return se.getType_length() * 8;
      } else {
        return getTypeLengthInBits(column.getType());
      }
    } else {
      return -1;
    }
  }

  @Override
  public void setup(OperatorContext operatorContext, OutputMutator output) throws ExecutionSetupException {
    Stopwatch timer = Stopwatch.createStarted();
    long cpuStart = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
    this.operatorContext = operatorContext;
    if (!isStarQuery()) {
      columnsFound = new boolean[getColumns().size()];
      nullFilledVectors = new ArrayList<>();
    }
    columnStatuses = new ArrayList<>();
//    totalRecords = footer.getBlocks().get(rowGroupIndex).getRowCount();
    List<ColumnDescriptor> columns = footer.getFileMetaData().getSchema().getColumns();
    allFieldsFixedLength = true;
    ColumnDescriptor column;
    ColumnChunkMetaData columnChunkMetaData;
    int columnsToScan = 0;
    mockRecordsRead = 0;

    logger.debug("SETUP: 1: {} : {}", name, timer.elapsed(TimeUnit.NANOSECONDS));

    MaterializedField field;
//    ParquetMetadataConverter metaConverter = new ParquetMetadataConverter();
    FileMetaData fileMetaData;

    logger.debug("Reading row group({}) with {} records in file {}.", rowGroupIndex, footer.getBlocks().get(rowGroupIndex).getRowCount(),
        hadoopPath.toUri().getPath());
    totalRecordsRead = 0;

    // TODO - figure out how to deal with this better once we add nested reading, note also look where this map is used below
    // store a map from column name to converted types if they are non-null
    Map<String, SchemaElement> schemaElements = ParquetReaderUtility.getColNameToSchemaElementMapping(footer);

    // loop to add up the length of the fixed width columns and build the schema
    logger.debug("SETUP: 2: {} : {}", name, timer.elapsed(TimeUnit.NANOSECONDS));
    for (int i = 0; i < columns.size(); ++i) {
      column = columns.get(i);
      SchemaElement se = schemaElements.get(column.getPath()[0]);
      MajorType mt = ParquetToDrillTypeConverter.toMajorType(column.getType(), se.getType_length(),
          getDataMode(column), se, fragmentContext.getOptions());
      field = MaterializedField.create(toFieldName(column.getPath()), mt);
      if ( ! fieldSelected(field)) {
        continue;
      }
      columnsToScan++;
      int dataTypeLength = getDataTypeLength(column, se);
      if (dataTypeLength == -1) {
          allFieldsFixedLength = false;
        } else {
        bitWidthAllFixedFields += dataTypeLength;
        }
      }
//    rowGroupOffset = footer.getBlocks().get(rowGroupIndex).getColumns().get(0).getFirstDataPageOffset();

    logger.debug("SETUP: 3: {} : {}", name, timer.elapsed(TimeUnit.NANOSECONDS));
    if (columnsToScan != 0  && allFieldsFixedLength) {
      recordsPerBatch = (int) Math.min(Math.min(batchSize / bitWidthAllFixedFields,
          footer.getBlocks().get(0).getColumns().get(0).getValueCount()), 65535);
    }
    else {
      recordsPerBatch = DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH;
    }

    logger.debug("SETUP: 4: {} : {}", name, timer.elapsed(TimeUnit.NANOSECONDS));
    try {
      ValueVector vector;
      SchemaElement schemaElement;
      final ArrayList<VarLengthColumn<? extends ValueVector>> varLengthColumns = new ArrayList<>();
      // initialize all of the column read status objects
      boolean fieldFixedLength;
      // the column chunk meta-data is not guaranteed to be in the same order as the columns in the schema
      // a map is constructed for fast access to the correct columnChunkMetadata to correspond
      // to an element in the schema
      Map<String, Integer> columnChunkMetadataPositionsInList = new HashMap<>();
      BlockMetaData rowGroupMetadata = footer.getBlocks().get(rowGroupIndex);

      int colChunkIndex = 0;
      for (ColumnChunkMetaData colChunk : rowGroupMetadata.getColumns()) {
        columnChunkMetadataPositionsInList.put(Arrays.toString(colChunk.getPath().toArray()), colChunkIndex);
        colChunkIndex++;
      }
      logger.debug("SETUP: 5: {} : {}", name, timer.elapsed(TimeUnit.NANOSECONDS));
      for (int i = 0; i < columns.size(); ++i) {
        logger.debug("SETUP: 5.1: {} : {}", name, timer.elapsed(TimeUnit.NANOSECONDS));
        column = columns.get(i);
        columnChunkMetaData = rowGroupMetadata.getColumns().get(columnChunkMetadataPositionsInList.get(Arrays.toString(column.getPath())));
        schemaElement = schemaElements.get(column.getPath()[0]);
        MajorType type = ParquetToDrillTypeConverter.toMajorType(column.getType(), schemaElement.getType_length(),
            getDataMode(column), schemaElement, fragmentContext.getOptions());
        field = MaterializedField.create(toFieldName(column.getPath()), type);
        // the field was not requested to be read
        if ( ! fieldSelected(field)) {
          continue;
        }

        logger.debug("SETUP: 5.2: {} : {}", name, timer.elapsed(TimeUnit.NANOSECONDS));
        fieldFixedLength = column.getType() != PrimitiveType.PrimitiveTypeName.BINARY;
        vector = output.addField(field, (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(type.getMinorType(), type.getMode()));
        if (column.getType() != PrimitiveType.PrimitiveTypeName.BINARY) {
          if (column.getMaxRepetitionLevel() > 0) {
            final RepeatedValueVector repeatedVector = RepeatedValueVector.class.cast(vector);
            ColumnReader<?> dataReader = ColumnReaderFactory.createFixedColumnReader(this, fieldFixedLength,
                column, columnChunkMetaData, recordsPerBatch,
                repeatedVector.getDataVector(), schemaElement);
            varLengthColumns.add(new FixedWidthRepeatedReader(this, dataReader,
                getTypeLengthInBits(column.getType()), -1, column, columnChunkMetaData, false, repeatedVector, schemaElement));
          }
          else {

            logger.debug("SETUP, 5.2.1, {},{},{}", name, schemaElement, timer.elapsed(TimeUnit.NANOSECONDS));
           ColumnReader<?> cr = ColumnReaderFactory.createFixedColumnReader(this, fieldFixedLength,
                column, columnChunkMetaData, recordsPerBatch, vector,
                schemaElement) ;
            logger.debug("SETUP, 5.2.3, {},{},{}", name, schemaElement, timer.elapsed(TimeUnit.NANOSECONDS));
            columnStatuses.add(cr);
          }
        } else {
          // create a reader and add it to the appropriate list
          varLengthColumns.add(ColumnReaderFactory.getReader(this, -1, column, columnChunkMetaData, false, vector, schemaElement));
        }
        logger.debug("SETUP: 5.3: {} : {}", name, timer.elapsed(TimeUnit.NANOSECONDS));
      }
      logger.debug("SETUP: 6: {} : {}", name, timer.elapsed(TimeUnit.NANOSECONDS));
      varLengthReader = new VarLenBinaryReader(this, varLengthColumns);

      if (!isStarQuery()) {
        List<SchemaPath> projectedColumns = Lists.newArrayList(getColumns());
        SchemaPath col;
        for (int i = 0; i < columnsFound.length; i++) {
          col = projectedColumns.get(i);
          assert col!=null;
          if ( ! columnsFound[i] && !col.equals(STAR_COLUMN)) {
            nullFilledVectors.add((NullableIntVector)output.addField(MaterializedField.create(col.getAsUnescapedPath(),
                    Types.optional(TypeProtos.MinorType.INT)),
                (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(TypeProtos.MinorType.INT, DataMode.OPTIONAL)));

          }
        }
      }
      logger.debug("SETUP: 7: {} : {}", name, timer.elapsed(TimeUnit.NANOSECONDS));
    } catch (Exception e) {
      handleAndRaise("Failure in setting up reader", e);
    } finally {
      long cpuStop = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
      parquetReaderStats.timeInit.addAndGet(timer.elapsed(TimeUnit.NANOSECONDS));
      parquetReaderStats.cpuTimeInit.addAndGet(cpuStop - cpuStart);
    }
  }

  protected void handleAndRaise(String s, Exception e) {
    String message = "Error in parquet record reader.\nMessage: " + s +
      "\nParquet Metadata: " + footer;
    throw new DrillRuntimeException(message, e);
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    try {
      for (final ValueVector v : vectorMap.values()) {
        AllocationHelper.allocate(v, recordsPerBatch, 50, 10);
      }
    } catch (NullPointerException e) {
      throw new OutOfMemoryException();
    }
  }


  private String toFieldName(String[] paths) {
    return SchemaPath.getCompoundPath(paths).getAsUnescapedPath();
  }

  private TypeProtos.DataMode getDataMode(ColumnDescriptor column) {
    if (column.getMaxRepetitionLevel() > 0 ) {
      return DataMode.REPEATED;
    } else if (column.getMaxDefinitionLevel() == 0) {
      return TypeProtos.DataMode.REQUIRED;
    } else {
      return TypeProtos.DataMode.OPTIONAL;
    }
  }

  private void resetBatch() {
    for (final ColumnReader<?> column : columnStatuses) {
      column.valuesReadInCurrentPass = 0;
    }
    for (final VarLengthColumn<?> r : varLengthReader.columns) {
      r.valuesReadInCurrentPass = 0;
    }
  }

 public void readAllFixedFields(long recordsToRead) throws IOException {
   Stopwatch timer = Stopwatch.createStarted();
   long cpuStart = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
   if(useAsyncColReader){
     readAllFixedFieldsParallel(recordsToRead) ;
   } else {
     readAllFixedFieldsSerial(recordsToRead);
   }
   long cpuStop = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
   parquetReaderStats.timeFixedColumnRead.addAndGet(timer.elapsed(TimeUnit.NANOSECONDS));
   parquetReaderStats.cpuTimeFixedColumnRead.addAndGet(cpuStop-cpuStart);
 }

  public void readAllFixedFieldsSerial(long recordsToRead) throws IOException {
    for (ColumnReader<?> crs : columnStatuses) {
      crs.processPages(recordsToRead);
    }
  }

  public void readAllFixedFieldsParallel(long recordsToRead) throws IOException {
    ArrayList<Future<Long>> futures = Lists.newArrayList();
    for (ColumnReader<?> crs : columnStatuses) {
      Future<Long> f = crs.processPagesAsync(recordsToRead);
      futures.add(f);
    }
    Exception exception = null;
    for(Future f: futures){
      if(exception != null) {
        f.cancel(true);
      } else {
        try {
          f.get();
        } catch (Exception e) {
          f.cancel(true);
          exception = e;
        }
      }
    }
    if(exception != null){
      handleAndRaise(null, exception);
    }
  }

  @Override
  public int next() {
    resetBatch();
    long recordsToRead = 0;
    Stopwatch timer = Stopwatch.createStarted();
    long cpuStart = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
    try {
      ColumnReader<?> firstColumnStatus;
      if (columnStatuses.size() > 0) {
        firstColumnStatus = columnStatuses.iterator().next();
      }
      else{
        if (varLengthReader.columns.size() > 0) {
          firstColumnStatus = varLengthReader.columns.iterator().next();
        }
        else{
          firstColumnStatus = null;
        }
      }
      // No columns found in the file were selected, simply return a full batch of null records for each column requested
      if (firstColumnStatus == null) {
        if (mockRecordsRead == footer.getBlocks().get(rowGroupIndex).getRowCount()) {
          long cpuStop = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
          parquetReaderStats.timeProcess.addAndGet(timer.elapsed(TimeUnit.NANOSECONDS));
          parquetReaderStats.cpuTimeProcess.addAndGet(cpuStop-cpuStart);
          return 0;
        }
        recordsToRead = Math.min(DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH, footer.getBlocks().get(rowGroupIndex).getRowCount() - mockRecordsRead);

        // Pick the minimum of recordsToRead calculated above and numRecordsToRead (based on rowCount and limit).
        recordsToRead = Math.min(recordsToRead, numRecordsToRead);

        for (final ValueVector vv : nullFilledVectors ) {
          vv.getMutator().setValueCount( (int) recordsToRead);
        }
        mockRecordsRead += recordsToRead;
        totalRecordsRead += recordsToRead;
        numRecordsToRead -= recordsToRead;
        long cpuStop = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
        parquetReaderStats.timeProcess.addAndGet(timer.elapsed(TimeUnit.NANOSECONDS));
        parquetReaderStats.cpuTimeProcess.addAndGet(cpuStop-cpuStart);
        return (int) recordsToRead;
      }

      if (allFieldsFixedLength) {
        recordsToRead = Math.min(recordsPerBatch, firstColumnStatus.columnChunkMetaData.getValueCount() - firstColumnStatus.totalValuesRead);
      } else {
        recordsToRead = DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH;

      }

      // Pick the minimum of recordsToRead calculated above and numRecordsToRead (based on rowCount and limit)
      recordsToRead = Math.min(recordsToRead, numRecordsToRead);

      if (allFieldsFixedLength) {
        readAllFixedFields(recordsToRead);
      } else { // variable length columns
        long fixedRecordsToRead = varLengthReader.readFields(recordsToRead, firstColumnStatus);
        readAllFixedFields(fixedRecordsToRead);
      }

      // if we have requested columns that were not found in the file fill their vectors with null
      // (by simply setting the value counts inside of them, as they start null filled)
      if (nullFilledVectors != null) {
        for (final ValueVector vv : nullFilledVectors ) {
          vv.getMutator().setValueCount(firstColumnStatus.getRecordsReadInCurrentPass());
        }
      }

//      logger.debug("So far read {} records out of row group({}) in file '{}'", totalRecordsRead, rowGroupIndex, hadoopPath.toUri().getPath());
      totalRecordsRead += firstColumnStatus.getRecordsReadInCurrentPass();
      numRecordsToRead -= firstColumnStatus.getRecordsReadInCurrentPass();
      long cpuStop = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
      parquetReaderStats.timeProcess.addAndGet(timer.elapsed(TimeUnit.NANOSECONDS));
      parquetReaderStats.cpuTimeProcess.addAndGet(cpuStop-cpuStart);

      return firstColumnStatus.getRecordsReadInCurrentPass();
    } catch (Exception e) {
      handleAndRaise("\nHadoop path: " + hadoopPath.toUri().getPath() +
        "\nTotal records read: " + totalRecordsRead +
        "\nMock records read: " + mockRecordsRead +
        "\nRecords to read: " + recordsToRead +
        "\nRow group index: " + rowGroupIndex +
        "\nRecords in row group: " + footer.getBlocks().get(rowGroupIndex).getRowCount(), e);
    }

    // this is never reached
    return 0;
  }

  @Override
  public void close() {
    logger.debug("Read {} records out of row group({}) in file '{}'", totalRecordsRead, rowGroupIndex,
        hadoopPath.toUri().getPath());
    // enable this for debugging when it is know that a whole file will be read
    // limit kills upstream operators once it has enough records, so this assert will fail
//    assert totalRecordsRead == footer.getBlocks().get(rowGroupIndex).getRowCount();
    if (columnStatuses != null) {
      for (final ColumnReader<?> column : columnStatuses) {
        column.clear();
      }
      columnStatuses.clear();
      columnStatuses = null;
    }

    codecFactory.release();

    if (varLengthReader != null) {
      for (final VarLengthColumn r : varLengthReader.columns) {
        r.clear();
      }
      varLengthReader.columns.clear();
      varLengthReader = null;
    }


    if(parquetReaderStats != null) {
      updateStats();
      logger.trace(
          "ParquetTrace,Summary,{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
          hadoopPath,
          parquetReaderStats.numDictPageLoads,
          parquetReaderStats.numDataPageLoads,
          parquetReaderStats.numDataPagesDecoded,
          parquetReaderStats.numDictPagesDecompressed,
          parquetReaderStats.numDataPagesDecompressed,
          parquetReaderStats.totalDictPageReadBytes,
          parquetReaderStats.totalDataPageReadBytes,
          parquetReaderStats.totalDictDecompressedBytes,
          parquetReaderStats.totalDataDecompressedBytes,
          parquetReaderStats.timeDictPageLoads,
          parquetReaderStats.timeDataPageLoads,
          parquetReaderStats.timeDataPageDecode,
          parquetReaderStats.timeDictPageDecode,
          parquetReaderStats.timeDictPagesDecompressed,
          parquetReaderStats.timeDataPagesDecompressed,
          parquetReaderStats.timeDiskScanWait,
          parquetReaderStats.timeDiskScan,
          parquetReaderStats.timeFixedColumnRead,
          parquetReaderStats.timeVarColumnRead,
          parquetReaderStats.cpuTimeDictPageLoads,
          parquetReaderStats.cpuTimeDataPageLoads,
          parquetReaderStats.cpuTimeDataPageDecode,
          parquetReaderStats.cpuTimeDictPageDecode,
          parquetReaderStats.cpuTimeDictPagesDecompressed,
          parquetReaderStats.cpuTimeDataPagesDecompressed,
          parquetReaderStats.cpuTimeDiskScan,
          parquetReaderStats.cpuTimeFixedColumnRead,
          parquetReaderStats.cpuTimeVarColumnRead
      );
      parquetReaderStats=null;
    }

  }

  private void updateStats(){

    Stopwatch timer = Stopwatch.createStarted();
    long cpuStart = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
    operatorContext.getStats().addLongStat(Metric.NUM_DICT_PAGE_LOADS,
        parquetReaderStats.numDictPageLoads.longValue());
    operatorContext.getStats().addLongStat(Metric.NUM_DATA_PAGE_lOADS, parquetReaderStats.numDataPageLoads.longValue());
    operatorContext.getStats().addLongStat(Metric.NUM_DATA_PAGES_DECODED, parquetReaderStats.numDataPagesDecoded.longValue());
    operatorContext.getStats().addLongStat(Metric.NUM_DICT_PAGES_DECOMPRESSED,
        parquetReaderStats.numDictPagesDecompressed.longValue());
    operatorContext.getStats().addLongStat(Metric.NUM_DATA_PAGES_DECOMPRESSED,
        parquetReaderStats.numDataPagesDecompressed.longValue());
    operatorContext.getStats().addLongStat(Metric.TOTAL_DICT_PAGE_READ_BYTES,
        parquetReaderStats.totalDictPageReadBytes.longValue());
    operatorContext.getStats().addLongStat(Metric.TOTAL_DATA_PAGE_READ_BYTES,
        parquetReaderStats.totalDataPageReadBytes.longValue());
    operatorContext.getStats().addLongStat(Metric.TOTAL_DICT_DECOMPRESSED_BYTES,
        parquetReaderStats.totalDictDecompressedBytes.longValue());
    operatorContext.getStats().addLongStat(Metric.TOTAL_DATA_DECOMPRESSED_BYTES,
        parquetReaderStats.totalDataDecompressedBytes.longValue());
    operatorContext.getStats().addLongStat(Metric.TIME_DICT_PAGE_LOADS,
        parquetReaderStats.timeDictPageLoads.longValue());
    operatorContext.getStats().addLongStat(Metric.TIME_DATA_PAGE_LOADS,
        parquetReaderStats.timeDataPageLoads.longValue());
    operatorContext.getStats().addLongStat(Metric.TIME_DATA_PAGE_DECODE,
        parquetReaderStats.timeDataPageDecode.longValue());
    operatorContext.getStats().addLongStat(Metric.TIME_DICT_PAGE_DECODE,
        parquetReaderStats.timeDictPageDecode.longValue());
    operatorContext.getStats().addLongStat(Metric.TIME_DICT_PAGES_DECOMPRESSED,
        parquetReaderStats.timeDictPagesDecompressed.longValue());
    operatorContext.getStats().addLongStat(Metric.TIME_DATA_PAGES_DECOMPRESSED,
        parquetReaderStats.timeDataPagesDecompressed.longValue());
    operatorContext.getStats().addLongStat(Metric.TIME_DISK_SCAN_WAIT,
        parquetReaderStats.timeDiskScanWait.longValue());
    operatorContext.getStats().addLongStat(Metric.TIME_DISK_SCAN, parquetReaderStats.timeDiskScan.longValue());
    operatorContext.getStats().addLongStat(Metric.TIME_FIXEDCOLUMN_READ, parquetReaderStats.timeFixedColumnRead.longValue());
    operatorContext.getStats().addLongStat(Metric.TIME_VARCOLUMN_READ, parquetReaderStats.timeVarColumnRead.longValue());

    operatorContext.getStats().addLongStat(Metric.CPU_TIME_DICT_PAGE_LOADS,
        parquetReaderStats.cpuTimeDictPageLoads.longValue());
    operatorContext.getStats().addLongStat(Metric.CPU_TIME_DATA_PAGE_LOADS,
        parquetReaderStats.cpuTimeDataPageLoads.longValue());
    operatorContext.getStats().addLongStat(Metric.CPU_TIME_DATA_PAGE_DECODE,
        parquetReaderStats.cpuTimeDataPageDecode.longValue());
    operatorContext.getStats().addLongStat(Metric.CPU_TIME_DICT_PAGE_DECODE,
        parquetReaderStats.cpuTimeDictPageDecode.longValue());
    operatorContext.getStats().addLongStat(Metric.CPU_TIME_DICT_PAGES_DECOMPRESSED,
        parquetReaderStats.cpuTimeDictPagesDecompressed.longValue());
    operatorContext.getStats().addLongStat(Metric.CPU_TIME_DATA_PAGES_DECOMPRESSED,
        parquetReaderStats.cpuTimeDataPagesDecompressed.longValue());
    operatorContext.getStats().addLongStat(Metric.CPU_TIME_DISK_SCAN, parquetReaderStats.cpuTimeDiskScan.longValue());
    operatorContext.getStats().addLongStat(Metric.CPU_TIME_FIXEDCOLUMN_READ, parquetReaderStats.cpuTimeFixedColumnRead.longValue());
    operatorContext.getStats().addLongStat(Metric.CPU_TIME_VARCOLUMN_READ, parquetReaderStats.cpuTimeVarColumnRead.longValue());

    operatorContext.getStats().addLongStat(Metric.TIME_PROCESS, parquetReaderStats.timeProcess.longValue());
    operatorContext.getStats().addLongStat(Metric.CPU_TIME_PROCESS, parquetReaderStats.cpuTimeProcess.longValue());

    operatorContext.getStats().addLongStat(Metric.TIME_INIT, parquetReaderStats.timeInit.longValue());
    operatorContext.getStats().addLongStat(Metric.CPU_TIME_INIT, parquetReaderStats.cpuTimeInit.longValue());

    long cpuStop = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
    parquetReaderStats.timeStats.addAndGet(timer.elapsed(TimeUnit.NANOSECONDS));
    parquetReaderStats.cpuTimeStats.addAndGet(cpuStop-cpuStart);
    operatorContext.getStats().addLongStat(Metric.TIME_STATS, parquetReaderStats.cpuTimeStats.longValue());
    operatorContext.getStats().addLongStat(Metric.CPU_TIME_STATS, parquetReaderStats.timeStats.longValue());
  }

  @Override
  protected List<SchemaPath> getDefaultColumnsToRead() {
    return DEFAULT_COLS_TO_READ;
  }

}
