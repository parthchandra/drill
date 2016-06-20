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
package org.apache.drill.exec.util.filereader;

import io.netty.buffer.DrillBuf;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.CompatibilityUtil;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by pchandra on 12/22/15.
 * <p/>
 * Reads a ColumnChunk from a Parquet file given the columnChunkMetaData.
 * Data is read from disk in chunks of configurable size and reading is asynchronous.
 * No more than two chunks worth of data is kept in memory at a time.
 */
public class ChunkedBufferedDirectBufInputStream extends BufferedDirectBufInputStream implements Closeable{

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ChunkedBufferedDirectBufInputStream.class);

  private final FSDataInputStream fileInputStream;
  private final BufferAllocator allocator;
  private final String streamId;
  private final long startOffset;
  private final long totalByteSize;
  private final int chunkSize;

  // read offset in current column chunk
  private long currentOffset = 0;
  // read offset in the current chunk
  private int currentChunkOffset = 0;
  // number of chunks read or skipped
  private int numChunksRead = 0;
  // bytes remaining to be read from the current chunk
  private long bytesRemaining = 0;

  private DrillBuf currentChunk;

  public ChunkedBufferedDirectBufInputStream(FSDataInputStream fileInputStream, BufferAllocator allocator,
      String streamId, long startOffset, long totalByteSize, int chunkSize, boolean enableHints) {
    super(fileInputStream, enableHints);
    this.fileInputStream = fileInputStream;
    this.allocator = allocator;
    this.streamId = streamId;
    //this.startOffset = columnChunkMetadata.getStartingPos();
    //this.totalByteSize = columnChunkMetadata.getTotalSize();
    this.startOffset = startOffset;
    this.totalByteSize = totalByteSize;

    // We make the buffer size the smaller of the chunk Size parameter or the total Byte Size
    // rounded to next highest pwoer of two
    int bSize = chunkSize < (int) totalByteSize ? chunkSize : (int) totalByteSize;
    // round up to next power of 2
    bSize--;
    bSize |= bSize >>> 1;
    bSize |= bSize >>> 2;
    bSize |= bSize >>> 4;
    bSize |= bSize >>> 8;
    bSize |= bSize >>> 16;
    bSize++;
    this.chunkSize = bSize;

    this.currentChunk = null;
  }

  public void init() {
    try {
      if(enableHints) {
        fadviseIfAvailable(fileInputStream, startOffset, totalByteSize);
      }
      fileInputStream.seek(startOffset);
      readChunk();
    } catch (IOException e) {
      //TODO: Throw UserException here
    }
  }

  public boolean hasRemainder() throws IOException{
    return currentOffset < totalByteSize;
  }

  /*
  * Gets the next bytes bytes starting at offset offset in the column chunk
  */
  private DrillBuf getNext(long offset, int bytes) {
    if (offset < currentOffset) {
      //TODO: Throw internal exception. Cannot be reading backwards
    }

    if(offset == totalByteSize){
      return null;
    }

    int offsetToRead = (int) ( offset - (numChunksRead == 0 ? 0 : (numChunksRead - 1) * (chunkSize)) );
    int bytesToRead = offset + bytes > totalByteSize ? (int) (totalByteSize - offset) : bytes;

    while ((bytesToRead) > bytesRemaining) {
      readChunk(); // bytesRemaining increases by the number of bytes read
      offsetToRead = 0;
    }

    DrillBuf newBuf = currentChunk.slice(offsetToRead, bytesToRead);
    newBuf.retain();
    currentChunkOffset = offsetToRead + bytesToRead;
    currentOffset = offset + bytesToRead;
    bytesRemaining -= bytesToRead;
    logger.trace("Read {} bytes at current chunk offset {}. There are {} bytes remaining.", bytesToRead,
        currentChunkOffset, bytesRemaining);
    logger.trace("Current offset is {}. Number of Chunks read is {}", currentOffset, numChunksRead);
    return newBuf;
  }

  /*
   *  Get the next bytes bytes
   */
  public DrillBuf getNext(int bytes) {
    return getNext(currentOffset, bytes);
  }

  /**
   * Implements the read method for InputStream. This is used in the Parquet method to
   * read the Page Header.
   *
   * @return
   * @throws IOException
   */
  @Override
  public int read() throws IOException {
    DrillBuf buf = getNext(1);
    if (buf == null || buf.nioBuffer().remaining() <= 0 ) {
      return -1;
    }
    return buf.nioBuffer().get() & 0xFF;
  }

  @Override public int read(byte[] b) throws IOException {
    return read(b, (int)0, b.length);
  }


  @Override
  public int read(byte[] bytes, int off, int len)
      throws IOException {
    DrillBuf buf = getNext(currentOffset+off, len);
    if (buf == null || buf.nioBuffer().remaining() <= 0 ) {
      return -1;
    }

    len = Math.min(len, buf.nioBuffer().remaining());
    buf.nioBuffer().get(bytes, off, len);
    buf.release();
    return len;
  }

  /*
    Returns the current position from the beginning of the underlying input stream
   */
   public long getPos(){
     return startOffset+currentOffset;
   }

  /*
     * Skips reading the next bytes bytes. Moves current read pointer forward
     * by bytes bytes in the file. Releases the currentChunk and reads a new chunk.
     *
     */
  public long skip(long bytes) throws IOException {
    long bytesToSkip = bytes;
    if (bytes > (totalByteSize - currentOffset)) {
      bytesToSkip = totalByteSize - currentOffset;
    }
    if (bytesToSkip > 0) {
      fileInputStream.seek(currentOffset + bytesToSkip);
      if (currentChunk != null) {
        currentChunk.release();
        currentChunk = null;
        currentOffset += bytesToSkip;
        bytesRemaining = 0;
        numChunksRead = (int) currentOffset / chunkSize;
      }
    }
    return bytesToSkip;
  }

  @Override public int available() throws IOException {
    return super.available();
  }

  /*
   * Reads the next chunk.
   */
  private DrillBuf readChunk() {
    try {
      int bytesToRead = chunkSize <= (totalByteSize - currentOffset - bytesRemaining) ?
          chunkSize :
          (int) (totalByteSize - currentOffset - bytesRemaining);
      DrillBuf chunk = allocator.buffer(bytesToRead + (int) bytesRemaining);
      chunk.clear();
      if (bytesRemaining > 0) {
        chunk.setBytes(0, currentChunk, currentChunkOffset, (int) bytesRemaining);
        chunk.writerIndex((int) bytesRemaining);
      }
      ByteBuffer directBuffer = chunk.nioBuffer((int)bytesRemaining, bytesToRead);
      int lengthLeftToRead = bytesToRead;
      while (lengthLeftToRead > 0) {
        lengthLeftToRead -= CompatibilityUtil.getBuf(fileInputStream, directBuffer, lengthLeftToRead);
      }
      chunk.writerIndex((int)bytesRemaining + bytesToRead);

      numChunksRead++;
      bytesRemaining += bytesToRead;

      if(currentChunk != null){
        currentChunk.release();
      }
      currentChunk = chunk;
      currentChunkOffset = 0;
      //TODO: set state variables correctly
      logger.trace("Column {}, Read Chunk # {}, {} bytes", streamId, numChunksRead,
          bytesToRead);
    } catch (IOException e) {
      //TODO: throw UserException here
    }
    return currentChunk;
  }

  public void close() throws IOException {
    currentChunk.release();
    fileInputStream.close();
    return;
  }

  @Override public synchronized void mark(int readlimit) {
    throw new UnsupportedOperationException("Mark/reset is not supported.");
  }

  @Override public synchronized void reset() throws IOException {
    throw new UnsupportedOperationException("Mark/reset is not supported.");
  }

  @Override public boolean markSupported() {
    return false;
  }

  public static void main(String[] args) {
    final DrillConfig config = DrillConfig.create();
    final BufferAllocator allocator = RootAllocatorFactory.newRoot(config);
    final Configuration dfsConfig = new Configuration();
    String fileName = args[0];
    Path filePath = new Path(fileName);
    final int BUFSZ = 8*1024*1024;
    try {
      List<Footer> footers = ParquetFileReader.readFooters(dfsConfig, filePath);
      Footer footer = (Footer) footers.iterator().next();
      FileSystem fs = FileSystem.get(dfsConfig);
      int rowGroupIndex = 0;
      List<BlockMetaData> blocks = footer.getParquetMetadata().getBlocks();
      for (BlockMetaData block : blocks) {
        List<ColumnChunkMetaData> columns = block.getColumns();
        for (ColumnChunkMetaData columnMetadata : columns) {
          FSDataInputStream inputStream = fs.open(filePath);
          long startOffset = columnMetadata.getStartingPos();
          long totalByteSize = columnMetadata.getTotalSize();
          String streamId = fileName + ":" + columnMetadata.toString();
          ChunkedBufferedDirectBufInputStream reader =
              new ChunkedBufferedDirectBufInputStream(inputStream, allocator, streamId, startOffset, totalByteSize,
                  BUFSZ, true);
          reader.init();
          while (true) {
            try {
              DrillBuf buf = reader.getNext(BUFSZ - 1);
              if (buf == null) {
                break;
              }
              buf.release();
            }catch (Exception e){
              e.printStackTrace();
              break;
            }
          }
          reader.close();
        }
      } // for each Block
    } catch (Exception e) {
      e.printStackTrace();
    }
    allocator.close();
    return;
  }

}
