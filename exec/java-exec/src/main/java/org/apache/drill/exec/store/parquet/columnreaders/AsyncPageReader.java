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

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.drill.exec.store.parquet.ParquetFormatPlugin;
import org.apache.drill.exec.store.parquet.ParquetReaderStats;
import org.apache.drill.exec.util.filereader.BufferedDirectBufInputStream;
import org.apache.drill.exec.util.filereader.DirectBufInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.dictionary.DictionaryValuesReader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.parquet.column.Encoding.valueOf;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics;

// class to keep track of the read position of variable length columns
final class AsyncPageReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(
      AsyncPageReader.class);

  public static final ParquetMetadataConverter METADATA_CONVERTER = ParquetFormatPlugin.parquetMetadataConverter;

  private final ColumnReader<?> parentColumnReader;
  //private final ColumnDataReader dataReader;
  private final DirectBufInputStream dataReader;
  //der; buffer to store bytes of current page
  DrillBuf pageData;

  // for variable length data we need to keep track of our current position in the page data
  // as the values and lengths are intermixed, making random access to the length data impossible
  long readyToReadPosInBytes;
  // read position in the current page, stored in the ByteBuf in ParquetRecordReader called bufferWithAllData
  long readPosInBytes;
  // bit shift needed for the next page if the last one did not line up with a byte boundary
  int bitShift;
  // storage space for extra bits at the end of a page if they did not line up with a byte boundary
  // prevents the need to keep the entire last page, as these pageDataByteArray need to be added to the next batch
  //byte extraBits;

  // used for columns where the number of values that will fit in a vector is unknown
  // currently used for variable length
  // TODO - reuse this when compressed vectors are added, where fixed length values will take up a
  // variable amount of space
  // For example: if nulls are stored without extra space left in the data vector
  // (this is currently simplifying random access to the data during processing, but increases the size of the vectors)
  int valuesReadyToRead;

  // the number of values read out of the last page
  int valuesRead;
  int byteLength;
  //int rowGroupIndex;
  ValuesReader definitionLevels;
  ValuesReader repetitionLevels;
  ValuesReader valueReader;
  ValuesReader dictionaryLengthDeterminingReader;
  ValuesReader dictionaryValueReader;
  Dictionary dictionary;
  PageHeader pageHeader = null;

  int currentPageCount = -1;

  private FSDataInputStream inputStream;

  // These need to be held throughout reading of the entire column chunk
  List<ByteBuf> allocatedDictionaryBuffers;

  private final CodecFactory codecFactory;

  private final ParquetReaderStats stats;

  private final String fileName;
  private final ExecutorService threadPool;
  private Future<ReadStatus> asyncPageRead;

  AsyncPageReader(ColumnReader<?> parentStatus, FileSystem fs, Path path,
      ColumnChunkMetaData columnChunkMetaData)
    throws ExecutionSetupException {
    this.parentColumnReader = parentStatus;
    allocatedDictionaryBuffers = new ArrayList<ByteBuf>();
    codecFactory = parentColumnReader.parentReader.getCodecFactory();
    this.stats = parentColumnReader.parentReader.parquetReaderStats;
    this.fileName = path.toString();
    threadPool =  parentColumnReader.parentReader.getOperatorContext().getScanExecutor();

    long start = columnChunkMetaData.getFirstDataPageOffset();
    try {
      inputStream  = fs.open(path);
      BufferAllocator allocator =  parentColumnReader.parentReader.getOperatorContext().getAllocator();

      // Instead of a the input stream parameter, we will use the wrapped stream that it contains. This is
      // because the input stream passed in tracks operator stats for each operator using the stream.
      // However, if (as in this case) the operator decides to have many parallel input streams, the
      // operator stats get clobbered as each stream will in parallel start and stop the wait time
      // for the operator.
      // These stats are best tracked by the operator and not the input stream.

      boolean useBufferedReader  = parentColumnReader.parentReader.getFragmentContext().getOptions()
          .getOption(ExecConstants.PARQUET_PAGEREADER_USE_BUFFERED_READ).bool_val;
      if (useBufferedReader) {
      this.dataReader = new BufferedDirectBufInputStream(inputStream, allocator, path.getName(),
            columnChunkMetaData.getStartingPos(), columnChunkMetaData.getTotalSize(), 8 * 1024 * 1024,
            true);
      } else {
        this.dataReader = new DirectBufInputStream(inputStream, allocator, path.getName(),
            columnChunkMetaData.getStartingPos(), columnChunkMetaData.getTotalSize(), true);
      }
      dataReader.init();

      loadDictionaryIfExists(parentStatus, columnChunkMetaData, dataReader);
      // start reading the next page
      asyncPageRead = threadPool.submit(new AsyncPageReaderTask());
    } catch (IOException e) {
      throw new ExecutionSetupException("Error opening or reading metadata for parquet file at location: "
          + path.getName(), e);
    }

  }

  private void loadDictionaryIfExists(final ColumnReader<?> parentStatus,
      final ColumnChunkMetaData columnChunkMetaData, final DirectBufInputStream f) throws UserException {
    Stopwatch timer = Stopwatch.createUnstarted();
    if (columnChunkMetaData.getDictionaryPageOffset() > 0) {
      try {
        dataReader.skip(columnChunkMetaData.getDictionaryPageOffset() - dataReader.getPos());
      } catch (IOException e) {
        handleAndThrowException(e, "Error Reading dictionary page.");
      }
      asyncPageRead = threadPool.submit(new AsyncPageReaderTask());
      readDictionaryPage(asyncPageRead, parentStatus);
    }
  }

  private DrillBuf getDecompressedPageData(ReadStatus readStatus){
    DrillBuf data;
    synchronized(this) {
      data = readStatus.getPageData();
      readStatus.setPageData(null);
    }
    if (parentColumnReader.columnChunkMetaData.getCodec() != CompressionCodecName.UNCOMPRESSED) {
      DrillBuf uncompressedData = data;
      data = decompress(readStatus.getPageHeader(), uncompressedData);
      synchronized(this) {
        readStatus.setPageData(null);
      }
      uncompressedData.release();
    }
    return data;
  }

  // Read and decode the dictionary and the header
  private void readDictionaryPage(final Future<ReadStatus> asyncPageRead,
      final ColumnReader<?> parentStatus) throws UserException {
    try {
      ReadStatus readStatus = asyncPageRead.get();
      readDictionaryPageData(readStatus, parentStatus);
    } catch (Exception e) {
      handleAndThrowException(e, "Error Reading dictionary page.");
    }
  }

  // Read and decode the dictionary data
  private void readDictionaryPageData(final ReadStatus readStatus,
    final ColumnReader<?> parentStatus) throws UserException {
      try {
        pageHeader = readStatus.getPageHeader();
        int uncompressedSize = pageHeader.getUncompressed_page_size();

        final DrillBuf dictionaryData = getDecompressedPageData(readStatus);
        allocatedDictionaryBuffers.add(dictionaryData);
        DictionaryPage page = new DictionaryPage(
            asBytesInput(dictionaryData, 0, uncompressedSize),
            pageHeader.uncompressed_page_size,
            pageHeader.dictionary_page_header.num_values,
            valueOf(pageHeader.dictionary_page_header.encoding.name()) );
        this.dictionary = page.getEncoding().initDictionary(parentStatus.columnDescriptor, page);
      } catch (Exception e) {
        handleAndThrowException(e, "Error Reading dictionary page.");
      }
    }

  private void handleAndThrowException(Exception e, String msg) throws UserException{
    UserException ex = UserException.dataReadError(e)
        .message(msg)
        .pushContext("Row Group Start: ", this.parentColumnReader.columnChunkMetaData.getStartingPos())
        .pushContext("Column: ", this.parentColumnReader.schemaElement.getName())
        .pushContext("File: ", this.fileName )
        .build(logger);
    throw ex;
  }

  private DrillBuf decompress(PageHeader pageHeader, DrillBuf compressedData){
    DrillBuf pageDataBuf = null;
    Stopwatch timer = Stopwatch.createUnstarted();
    long timeToRead;
    int compressedSize = pageHeader.getCompressed_page_size();
    int uncompressedSize = pageHeader.getUncompressed_page_size();
    pageDataBuf=allocateTemporaryBuffer(uncompressedSize);
    try {
      timer.start();
      codecFactory.getDecompressor(parentColumnReader.columnChunkMetaData.getCodec())
          .decompress(compressedData.nioBuffer(0, compressedSize), compressedSize,
              pageDataBuf.nioBuffer(0, uncompressedSize), uncompressedSize);
      timeToRead = timer.elapsed(TimeUnit.MICROSECONDS);
      this.updateStats(pageHeader, "Decompress", 0, timeToRead, compressedSize, uncompressedSize);
    } catch (IOException e) {
      handleAndThrowException(e, "Error decompressing data.");
    } finally {
      // caller owns the compressed data and should release it.
      //if(compressedData != null) {
      //  compressedData.release();
      //}
    }
    return pageDataBuf;
  }

  public DrillBuf readPage(PageHeader pageHeader, int compressedSize, int uncompressedSize) throws IOException {
    DrillBuf pageDataBuf = null;
    Stopwatch timer = Stopwatch.createUnstarted();
    long timeToRead;
    long start=dataReader.getPos();
    if (parentColumnReader.columnChunkMetaData.getCodec() == CompressionCodecName.UNCOMPRESSED) {
      timer.start();
      pageDataBuf = dataReader.getNext(compressedSize);
      timeToRead = timer.elapsed(TimeUnit.MICROSECONDS);
      this.updateStats(pageHeader, "Page Read", start, timeToRead, compressedSize, uncompressedSize);
    } else {
      DrillBuf compressedData = null;
      pageDataBuf=allocateTemporaryBuffer(uncompressedSize);

      try {
      timer.start();
      compressedData = dataReader.getNext(compressedSize);
       // dataReader.loadPage(compressedData, compressedSize);
      timeToRead = timer.elapsed(TimeUnit.MICROSECONDS);
      timer.reset();
      this.updateStats(pageHeader, "Page Read", start, timeToRead, compressedSize, compressedSize);
      start=dataReader.getPos();
      timer.start();
      codecFactory.getDecompressor(parentColumnReader.columnChunkMetaData
          .getCodec()).decompress(compressedData.nioBuffer(0, compressedSize), compressedSize,
          pageDataBuf.nioBuffer(0, uncompressedSize), uncompressedSize);
        timeToRead = timer.elapsed(TimeUnit.MICROSECONDS);
        this.updateStats(pageHeader, "Decompress", start, timeToRead, compressedSize, uncompressedSize);
      } finally {
        if(compressedData != null) {
          compressedData.release();
        }
      }
    }
    return pageDataBuf;
  }

  public static BytesInput asBytesInput(DrillBuf buf, int offset, int length) throws IOException {
    return BytesInput.from(buf.nioBuffer(offset, length), 0, length);
  }

  /**
   * Grab the next page.
   *
   * @return - if another page was present
   * @throws IOException
   */
  public boolean next() throws IOException {
    Stopwatch timer = Stopwatch.createUnstarted();
    currentPageCount = -1;
    valuesRead = 0;
    valuesReadyToRead = 0;

    // TODO - the metatdata for total size appears to be incorrect for impala generated files, need to find cause
    // and submit a bug report
    if(!dataReader.hasRemainder() || parentColumnReader.totalValuesRead == parentColumnReader.columnChunkMetaData.getValueCount()) {
      return false;
    }
    clearBuffers();

    ReadStatus readStatus = null;
    try {
      readStatus = asyncPageRead.get();
      pageHeader = readStatus.getPageHeader();
    } catch (Exception e) {
      handleAndThrowException(e, "Error reading page data.");
    }

    // next, we need to decompress the bytes
    // TODO - figure out if we need multiple dictionary pages, I believe it may be limited to one
    // I think we are clobbering parts of the dictionary if there can be multiple pages of dictionary

    do {
      //long start=dataReader.getPos();
      //timer.start();
      //pageHeader = Util.readPageHeader(dataReader);
      //long timeToRead = timer.elapsed(TimeUnit.MICROSECONDS);
      //long pageHeaderBytes=dataReader.getPos()-start;
      //this.updateStats(pageHeader, "Page Header", start, timeToRead, pageHeaderBytes, pageHeaderBytes);
      //logger.trace("ParquetTrace,{},{},{},{},{},{},{},{}","Page Header Read","",
      //    this.parentColumnReader.parentReader.hadoopPath,
      //    this.parentColumnReader.columnDescriptor.toString(), start, 0, 0, timeToRead);
      //timer.reset();
      if (pageHeader.getType() == PageType.DICTIONARY_PAGE) {
        readDictionaryPageData(readStatus, parentColumnReader);
        // Ugly. Use the Async task to make a synchronous read call.
        readStatus = new AsyncPageReaderTask().call();
        pageHeader = readStatus.getPageHeader();
      }
    } while (pageHeader.getType() == PageType.DICTIONARY_PAGE);


    //TODO: Handle buffer allocation exception
    //allocatePageData(pageHeader.getUncompressed_page_size());
    // start the next read if necessary
    if (dataReader.hasRemainder() &&
        parentColumnReader.totalValuesRead + readStatus.getValuesRead()
            < parentColumnReader.columnChunkMetaData.getValueCount()) {
      asyncPageRead = threadPool.submit(new AsyncPageReaderTask());
    }

    pageHeader = readStatus.getPageHeader();
    pageData = getDecompressedPageData(readStatus);
    //pageData = readPage(pageHeader, compressedSize, uncompressedSize);
    int compressedSize = pageHeader.getCompressed_page_size();
    int uncompressedSize = pageHeader.getUncompressed_page_size();

    currentPageCount = pageHeader.data_page_header.num_values;
    final int uncompressedPageSize = pageHeader.uncompressed_page_size;
    //final Statistics<?> stats = fromParquetStatistics(pageHeader.data_page_header.getStatistics(), parentColumnReader
    //    .getColumnDescriptor().getType());


    final Encoding rlEncoding = METADATA_CONVERTER.getEncoding(pageHeader.data_page_header.repetition_level_encoding);

    final Encoding dlEncoding = METADATA_CONVERTER.getEncoding(pageHeader.data_page_header.definition_level_encoding);
    final Encoding valueEncoding = METADATA_CONVERTER.getEncoding(pageHeader.data_page_header.encoding);

    byteLength = pageHeader.uncompressed_page_size;

    final ByteBuffer pageDataBuffer = pageData.nioBuffer(0, pageData.capacity());

    readPosInBytes = 0;
    if (parentColumnReader.getColumnDescriptor().getMaxRepetitionLevel() > 0) {
      repetitionLevels = rlEncoding.getValuesReader(parentColumnReader.columnDescriptor, ValuesType.REPETITION_LEVEL);
      repetitionLevels.initFromPage(currentPageCount, pageDataBuffer, (int) readPosInBytes);
      // we know that the first value will be a 0, at the end of each list of repeated values we will hit another 0 indicating
      // a new record, although we don't know the length until we hit it (and this is a one way stream of integers) so we
      // read the first zero here to simplify the reading processes, and start reading the first value the same as all
      // of the rest. Effectively we are 'reading' the non-existent value in front of the first allowing direct access to
      // the first list of repetition levels
      readPosInBytes = repetitionLevels.getNextOffset();
      repetitionLevels.readInteger();
    }
    if (parentColumnReader.columnDescriptor.getMaxDefinitionLevel() != 0){
      parentColumnReader.currDefLevel = -1;
      definitionLevels = dlEncoding.getValuesReader(parentColumnReader.columnDescriptor, ValuesType.DEFINITION_LEVEL);
      definitionLevels.initFromPage(currentPageCount, pageDataBuffer, (int) readPosInBytes);
      readPosInBytes = definitionLevels.getNextOffset();
      if (!valueEncoding.usesDictionary()) {
        valueReader = valueEncoding.getValuesReader(parentColumnReader.columnDescriptor, ValuesType.VALUES);
        valueReader.initFromPage(currentPageCount, pageDataBuffer, (int) readPosInBytes);
      }
    }
    if (parentColumnReader.columnDescriptor.getType() == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
      valueReader = valueEncoding.getValuesReader(parentColumnReader.columnDescriptor, ValuesType.VALUES);
      valueReader.initFromPage(currentPageCount, pageDataBuffer, (int) readPosInBytes);
    }
    if (valueEncoding.usesDictionary()) {
      // initialize two of the dictionary readers, one is for determining the lengths of each value, the second is for
      // actually copying the values out into the vectors
      dictionaryLengthDeterminingReader = new DictionaryValuesReader(dictionary);
      dictionaryLengthDeterminingReader.initFromPage(currentPageCount, pageDataBuffer, (int) readPosInBytes);
      dictionaryValueReader = new DictionaryValuesReader(dictionary);
      dictionaryValueReader.initFromPage(currentPageCount, pageDataBuffer, (int) readPosInBytes);
      parentColumnReader.usingDictionary = true;
    } else {
      parentColumnReader.usingDictionary = false;
    }
    // readPosInBytes is used for actually reading the values after we determine how many will fit in the vector
    // readyToReadPosInBytes serves a similar purpose for the vector types where we must count up the values that will
    // fit one record at a time, such as for variable length data. Both operations must start in the same location after the
    // definition and repetition level data which is stored alongside the page data itself
    readyToReadPosInBytes = readPosInBytes;
    return true;
  }

  /**
   * Allocate a page data buffer. Note that only one page data buffer should be active at a time. The reader will ensure
   * that the page data is released after the reader is completed.
   */
  private void allocatePageData(int size) {
    Preconditions.checkArgument(pageData == null);
    pageData = parentColumnReader.parentReader.getOperatorContext().getAllocator().buffer(size);
  }

  /**
   * Allocate a buffer which the user should release immediately. The reader does not manage release of these buffers.
   */
  private DrillBuf allocateTemporaryBuffer(int size) {
    return parentColumnReader.parentReader.getOperatorContext().getAllocator().buffer(size);
  }

  /**
   * Allocate and return a dictionary buffer. These are maintained for the life of the reader and then released when the
   * reader is cleared.
   */
  private DrillBuf allocateDictionaryBuffer(int size) {
    DrillBuf buf = parentColumnReader.parentReader.getOperatorContext().getAllocator().buffer(size);
    allocatedDictionaryBuffers.add(buf);
    return buf;
  }

  protected boolean hasPage() {
    return currentPageCount != -1;
  }

  private void updateStats(PageHeader pageHeader, String op, long start, long time, long bytesin, long bytesout) {
    String pageType = "Data Page";
    if (pageHeader.type == PageType.DICTIONARY_PAGE) {
      pageType = "Dictionary Page";
    }
    logger.trace("ParquetTrace,{},{},{},{},{},{},{},{}", op, pageType.toString(),
        this.parentColumnReader.parentReader.hadoopPath,
        this.parentColumnReader.columnDescriptor.toString(), start, bytesin, bytesout, time);
    if (pageHeader.type != PageType.DICTIONARY_PAGE) {
      if (bytesin == bytesout) {
        this.stats.timePageLoads += time;
        this.stats.numPageLoads++;
        this.stats.totalPageReadBytes += bytesin;
      } else {
        this.stats.timePagesDecompressed += time;
        this.stats.numPagesDecompressed++;
        this.stats.totalDecompressedBytes += bytesin;
      }
    } else {
      if (bytesin == bytesout) {
        this.stats.timeDictPageLoads += time;
        this.stats.numDictPageLoads++;
        this.stats.totalDictPageReadBytes += bytesin;
      } else {
        this.stats.timeDictPagesDecompressed += time;
        this.stats.numDictPagesDecompressed++;
        this.stats.totalDictDecompressedBytes += bytesin;
      }
    }
  }

  public void clearBuffers() {
    if (pageData != null) {
      pageData.release();
      pageData = null;
    }
  }

  public void clearDictionaryBuffers() {
    for (ByteBuf b : allocatedDictionaryBuffers) {
      b.release();
    }
    allocatedDictionaryBuffers.clear();
  }

  public void clear(){
    try {
      this.inputStream.close();
      this.dataReader.close();
    } catch (IOException e) {
      //Swallow the exception which is OK for input streams
    }
    // Free all memory, including fixed length types. (Data is being copied for all types not just var length types)
    //if(!this.parentColumnReader.isFixedLength) {
    clearBuffers();
    clearDictionaryBuffers();
    //}
  }

  public static class ReadStatus {
    private PageHeader pageHeader;
    private DrillBuf pageData;
    private boolean isDictionaryPage = false;
    private long bytesRead = 0;
    private long valuesRead = 0;

    public synchronized PageHeader getPageHeader() {
      return pageHeader;
    }

    public synchronized void setPageHeader(PageHeader pageHeader) {
      this.pageHeader = pageHeader;
    }

    public synchronized DrillBuf getPageData() {
      return pageData;
    }

    public synchronized void setPageData(DrillBuf pageData) {
      this.pageData = pageData;
    }

    public synchronized boolean isDictionaryPage() {
      return isDictionaryPage;
    }

    public synchronized void setIsDictionaryPage(boolean isDictionaryPage) {
      this.isDictionaryPage = isDictionaryPage;
    }

    public synchronized long getBytesRead() {
      return bytesRead;
    }

    public synchronized void setBytesRead(long bytesRead) {
      this.bytesRead = bytesRead;
    }

    public synchronized long getValuesRead() {
      return valuesRead;
    }

    public synchronized void setValuesRead(long valuesRead) {
      this.valuesRead = valuesRead;
    }
  }

  private class AsyncPageReaderTask implements Callable<ReadStatus> {

    private final AsyncPageReader parent = AsyncPageReader.this;

    public AsyncPageReaderTask(){
    }

    @Override public ReadStatus call() throws IOException{
      ReadStatus readStatus = new ReadStatus();

      String oldname = Thread.currentThread().getName();
      Thread.currentThread().setName(parent.parentColumnReader.columnChunkMetaData.toString());

      long bytesRead = 0;
      long valuesRead = 0;

      DrillBuf pageData = null;
      long elapsedTime = 0;
      Stopwatch stopwatch = Stopwatch.createStarted();
      try {
        // Use a stupid way to find how many bytes the page header was
        //long pageHeaderStart = parent.dataReader.getPos();
        PageHeader pageHeader = Util.readPageHeader(parent.dataReader);
        //long pageHeaderSize = parent.dataReader.getPos() - pageHeaderStart;
        //bytesRead += pageHeaderSize;
        int compressedSize = pageHeader.getCompressed_page_size();
        // get the data from the input stream
        pageData = parent.dataReader.getNext(compressedSize);
        //bytesRead += compressedSize;
        synchronized (parent) {
          if (pageHeader.getType() == PageType.DICTIONARY_PAGE) {
            readStatus.setIsDictionaryPage(true);
            valuesRead += pageHeader.getDictionary_page_header().getNum_values();
          } else {
            valuesRead += pageHeader.getData_page_header().getNum_values();
          }

          readStatus.setPageHeader(pageHeader);
          readStatus.setPageData(pageData);
          readStatus.setBytesRead(bytesRead);
          readStatus.setValuesRead(valuesRead);
        }

        elapsedTime = stopwatch.elapsed(TimeUnit.MICROSECONDS);
        //updateStats(pageHeader, " ASYNC PAGE READ", pageHeaderStart, elapsedTime,
        //    bytesRead + pageHeaderSize, bytesRead + pageHeaderSize);

      } catch (Exception e) {
        if(pageData != null){
          pageData.release();
        }
        throw e;
      }
      Thread.currentThread().setName(oldname);
      return readStatus;
    }

  }

}
