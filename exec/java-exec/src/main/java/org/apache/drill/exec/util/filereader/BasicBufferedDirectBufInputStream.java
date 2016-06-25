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

import com.google.common.base.Preconditions;
import io.netty.buffer.DrillBuf;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ByteBufferReadable;
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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * <code>BasicBufferedDirectBufInputStream</code>  reads from the
 * underlying <code>InputStream</code> in blocks of data, into an
 * internal buffer. The internal buffer is a direct memory backed
 * buffer. The implementation is similar to the <code>BufferedInputStream</code>
 * class except that the internal buffer is a Drillbuf and
 * not a byte array. The mark and reset methods of the underlying
 * <code>InputStream</code>are not supported.
 */
public
class BasicBufferedDirectBufInputStream extends BufferedDirectBufInputStream implements Closeable {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicBufferedDirectBufInputStream.class);

    private static int defaultBufferSize = 8192*1024; // 8 MB
    /**
     * The internal buffer to keep data read from the underlying inputStream.
     * <code>internalBuffer[0]</code>  through <code>internalBuffer[count-1] </code>
     * contains data read from the underlying  input stream.
     */
    protected volatile DrillBuf internalBuffer; // the internal buffer

    /**
     * The number of valid bytes in <code>internalBuffer</code>.
     * <code> count </code> is always in the range <code>[0,internalBuffer.capacity]</code>
     * <code>internalBuffer[count-1]</code> is the last valid byte in the buffer.
     */
    protected int count;

    /**
     * The current read position in the buffer; the index of the next
     * character to be read from the <code>internalBuffer</code> array.
     * <p>
     * This value is always in the range <code>[0,count]</code>.
     * If <code>curPosInBuffer</code> is equal to <code>count></code> then we have read
     * all the buffered data and the next read (or skip) will require more data to be read
     * from the underlying input stream.
     */
    protected int curPosInBuffer;

    protected long curPosInStream; // current offset in the input stream

    protected String streamId; // a name for logging purposes only

    /**
     * The offset in the underlying stream to start reading from
     */
    private final long startOffset;
    /**
     * The length of the data we expect to read. The caller may, in fact,
     * ask for more or less bytes. However this is useful for providing hints where
     * the underlying InputStream supports hints (e.g. fadvise)
     */
    private final long totalByteSize;

    protected BufferAllocator allocator;
    private final int bufSize;

    private FSDataInputStream getInputStream() throws IOException {
        // Make sure stream is open
        checkInputStreamState();
        return (FSDataInputStream) in;
    }

    private void checkInputStreamState() throws IOException {
        if (in == null) {
            throw new IOException("Input stream is closed.");
        }
    }

    private void checkStreamSupportsByteBuffer() throws UnsupportedOperationException {
        // iCheck input stream supports ByteBuffer
        if (!(in instanceof ByteBufferReadable)) {
            throw new UnsupportedOperationException("The input stream is not ByteBuffer readable.");
        }
    }

    private DrillBuf getBuf() throws IOException {
        checkInputStreamState();
        if (internalBuffer == null) {
            throw new IOException("Input stream is closed.");
        }
        return this.internalBuffer;
    }

    /**
     * Creates a <code>BasicBufferedDirectBufInputStream</code>
     * with the default (8 MiB) buffer size.
     */
    public BasicBufferedDirectBufInputStream(InputStream in, BufferAllocator allocator, String id,
        long startOffset, long totalByteSize, boolean enableHints) {
        this(in, allocator, id, startOffset, totalByteSize, defaultBufferSize, enableHints);
    }

    /**
     * Creates a <code>BasicBufferedDirectBufInputStream</code>
     * with the specified buffer size.
     */
    public BasicBufferedDirectBufInputStream(InputStream in, BufferAllocator allocator, String id,
        long startOffset, long totalByteSize, int bufSize, boolean enableHints) {
        super(in, enableHints);
        Preconditions.checkArgument(startOffset >= 0);
        Preconditions.checkArgument(totalByteSize >= 0);
        Preconditions.checkArgument(bufSize >= 0);
        this.streamId = id;
        this.allocator = allocator;

        // We make the buffer size the smaller of the buffer Size parameter or the total Byte Size
        // rounded to next highest pwoer of two

        int bSize = bufSize < (int) totalByteSize ? bufSize : (int) totalByteSize;
        // round up to next power of 2
        bSize--;
        bSize |= bSize >>> 1;
        bSize |= bSize >>> 2;
        bSize |= bSize >>> 4;
        bSize |= bSize >>> 8;
        bSize |= bSize >>> 16;
        bSize++;
        this.bufSize = bSize;

        this.startOffset = startOffset;
        this.totalByteSize = totalByteSize;
    }

    public void init() throws UnsupportedOperationException, IOException {
        checkStreamSupportsByteBuffer();
        //do a late allocation of the buffer
        this.internalBuffer = this.allocator.buffer(this.bufSize);
        if (enableHints) {
            fadviseIfAvailable(getInputStream(), this.startOffset, this.totalByteSize);
        }
        getInputStream().seek(this.startOffset);
        //TODO: int bytesRead = getNextBlock();
        getNextBlock();
        if (this.count - this.curPosInBuffer <= 0) {
            throw new IOException("End of stream reached while initializing buffered reader.");
        }
    }

    /**
     * Read one more block from the underlying stream.
     * Assumes we have reached the end of buffered data
     * Assumes it is being called from a synchronized block.
     * returns number of bytes read or -1 if EOF
     */
    private int getNextBlock() throws IOException {
        Preconditions.checkState(this.curPosInBuffer >= this.count,
            "Internal error: Buffered stream has not been consumed and trying to read more from underlying stream");
        checkInputStreamState();
        DrillBuf buffer = getBuf();
        this.count = this.curPosInBuffer = 0;

        // We *cannot* rely on the totalByteSize being correct because
        // metadata for Parquet files is incorrect. So we read as
        // musch as we can up to the size of the buffer
        //int bytesToRead = buffer.capacity() <= (totalByteSize + startOffset - curPosInStream ) ?
        //    buffer.Capacity() :
        //    (int) (totalByteSize + startOffset - curPosInStream );
        int bytesToRead = buffer.capacity();

        ByteBuffer directBuffer = buffer.nioBuffer(curPosInBuffer, bytesToRead);
        // The DFS can return *more* bytes than requested if the capacity of the buffer is greater.
        // i.e 'n' can be greater than bytes requested which is pretty stupid and violates
        // the API contract; but we still have to deal with it. So we make sure the size of the
        // buffer is exactly the same as the number of bytes requested
        int bytesRead = -1;
        if (bytesToRead > 0) {
            int n = CompatibilityUtil.getBuf(getInputStream(), directBuffer, bytesToRead);
            if (n > 0) {
                buffer.writerIndex(n);
                this.count = n + this.curPosInBuffer;
                this.curPosInStream = getInputStream().getPos();
                bytesRead = n;
            }
        }
        return this.count - this.curPosInBuffer;
    }

    /**
     * Implements the  <code>read</code> method of <code>InputStream</code>.
     * returns one more byte or -1 if end of stream is reached.
     */
    public synchronized int read() throws IOException {
        if (this.count - this.curPosInBuffer <= 0) {
            getNextBlock();
        }
        // reached end of stream
        if (this.count - this.curPosInBuffer <= 0) {
            return -1;
        }
        this.curPosInBuffer++;
        return getBuf().nioBuffer().get() & 0xff;
    }

    // reads from the internal Buffer into the output buffer
    // May read less than the requested size if the remaining data in the buffer
    // is less than the requested amount
    private int readInternal(DrillBuf buf, int off, int len) throws IOException {
        // check how many bytes are available in the buffer.
        int bytesAvailable = this.count - this.curPosInBuffer;
        if (bytesAvailable <= 0) {
            // read more
            int bytesRead = getNextBlock();
            if (this.count - this.curPosInBuffer <= 0) { // End of stream
                return -1;
            }
        }
        bytesAvailable = this.count - this.curPosInBuffer;
        //copy into output buffer
        int copyBytes = bytesAvailable < len ? bytesAvailable : len;
        getBuf().getBytes(curPosInBuffer, buf, off, copyBytes);
        buf.writerIndex(off + copyBytes);
        this.curPosInBuffer += copyBytes;

        return copyBytes;
    }

    // reads from the internal Buffer into the output buffer
    // May read less than the requested size if the remaining data in the buffer
    // is less than the requested amount
    // Does not make a copy but returns a slice of the internal buffer.
    // Returns null if end of stream is reached
    private DrillBuf readInternal(int off, int len) throws IOException {
        // check how many bytes are available in the buffer.
        int bytesAvailable = this.count - this.curPosInBuffer;
        if (bytesAvailable <= 0) {
            // read more
            int bytesRead = getNextBlock();
            if (this.count - this.curPosInBuffer <= 0) { // End of stream
                return null;
            }
        }
        bytesAvailable = this.count - this.curPosInBuffer;
        // return a slice as the  output
        int bytesToRead = bytesAvailable < len ? bytesAvailable : len;
        DrillBuf newBuf = this.getBuf().slice(off, bytesToRead);
        newBuf.retain();
        return newBuf;

    }

    /**
     Has the same contract as {@link java.io.InputStream#read(byte[], int, int)}
     Except with DrillBuf
     */
    public synchronized int read(DrillBuf buf, int off, int len) throws IOException {
        checkInputStreamState();
        Preconditions.checkArgument((off >= 0) && (len >= 0) && (buf.capacity()) >= (off + len));
        int bytesRead = 0;
        do {
            int readStart = off + bytesRead;
            int lenToRead = len - bytesRead;
            int nRead = readInternal(buf, readStart, lenToRead);
            if (nRead <= 0) {// if End of stream
                if (bytesRead == 0) { // no bytes read at all
                    return -1;
                } else {
                    return bytesRead;
                }
            } else {
                bytesRead += nRead;
                // If the last read caused us to reach the end of stream
                // we are done
                InputStream input = in;
                if (input != null && input.available() <= 0) {
                    return bytesRead;
                }
            }
        } while (bytesRead < len);
        return bytesRead;
    }


    public synchronized DrillBuf getNext(int bytes) throws IOException {
        //TODO: this is an unnecessary copy being made here
        DrillBuf b = allocator.buffer(bytes);
        int bytesRead = read(b, 0, bytes);
        if (bytesRead <= -1) {
            b.release();
            return null;
        }
        return b;
    }

    /**
     Has the same contract as {@link java.io.InputStream#skip(long)}
     * Skips upto the next n bytes.
     * Skip may return with less than n bytes skipped
     */
    public synchronized long skip(long n) throws IOException {
        checkInputStreamState();
        long bytesAvailable = this.count - this.curPosInBuffer;
        long bytesSkipped = 0;
        if (n <= 0) {
            return 0;
        }
        if(bytesAvailable <= 0){
            checkInputStreamState();
            getNextBlock();
            bytesAvailable = this.count - this.curPosInBuffer;
            if (bytesAvailable <= 0) { // End of stream
                return 0;
            }
        }
        bytesSkipped = bytesAvailable < n ? bytesAvailable : n;
        this.curPosInBuffer += bytesSkipped;

        return bytesSkipped;
    }


    public synchronized int available() throws IOException {
        checkInputStreamState();
        int bytesAvailable = this.count - this.curPosInBuffer;
        int underlyingAvailable = getInputStream().available();
        int available = bytesAvailable + underlyingAvailable;
        if (available < 0) { // overflow
            return Integer.MAX_VALUE;
        }
        return available;
    }

    public synchronized void mark(int readlimit) {
        throw new UnsupportedOperationException("Mark/reset is not supported.");
    }

    public synchronized void reset() throws IOException {
        throw new UnsupportedOperationException("Mark/reset is not supported.");
    }

    public boolean markSupported() {
        return false;
    }


    /*
      Returns the current position from the beginning of the underlying input stream
     */
    public long getPos() throws IOException {
        return getInputStream().getPos();
    }

    public boolean hasRemainder() throws IOException{
        return available() > 0;
    }

    @Override public int read(byte[] b) throws IOException {
        return read(b, (int) 0, b.length);
    }


    @Override
    public int read(byte[] bytes, int off, int len)
        throws IOException {
        getBuf(); // Check for closed stream
        if ((off | len | (off + len) | (bytes.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        int n = 0;
        for (;;) {
            DrillBuf byteBuf = allocator.buffer(len);
            int nread = readInternal(byteBuf, off + n, len - n);
            if (nread <= 0) {
                return (n == 0) ? nread : n;
            }
            byteBuf.nioBuffer().get(bytes, off + n, len - n);
            byteBuf.release();
            n += nread;
            if (n >= len) {
                return n;
            }
            // if not closed but no bytes available, return
            InputStream input = in;
            if (input != null && input.available() <= 0) {
                return n;
            }
        }
    }

        /**
         * Closes this input stream and releases any system resources
         * associated with the stream.
         * Once the stream has been closed, further read(), available(), reset(),
         * or skip() invocations will throw an IOException.
         * Closing a previously closed stream has no effect.
         *
         * @exception  IOException  if an I/O error occurs.
         */
    public void close() throws IOException {
        DrillBuf buffer;
        InputStream inp;
        if((inp = in) != null){
            in=null;
            inp.close();
        }
        if((buffer= internalBuffer)!=null){
            synchronized(this){
                internalBuffer = null;
                buffer.release();
            }
        }
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
                    BasicBufferedDirectBufInputStream reader =
                        new BasicBufferedDirectBufInputStream(inputStream, allocator, streamId, startOffset,
                            totalByteSize, BUFSZ, true);
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
