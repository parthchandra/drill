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

    private final long startOffset;
    private final long totalByteSize;

    protected BufferAllocator allocator;
    private final int bufSize;

    private FSDataInputStream getInputStream() throws IOException {
        // Make sure stream is open
        checkInputStreamState();
        return (FSDataInputStream)in;
    }

    private void checkInputStreamState() throws IOException{
        if(in == null){
            throw new IOException("Input stream is closed.");
        }
    }

    private void checkStreamSupportsByteBuffer() throws UnsupportedOperationException{
        // iCheck input stream supports ByteBuffer
        if ( !(in instanceof ByteBufferReadable) ) {
            throw new UnsupportedOperationException("The input stream is not ByteBuffer readable.");
        }
    }

    private DrillBuf getBuf() throws IOException {
        checkInputStreamState();
        if (internalBuffer == null) {
            throw new IOException("Input stream is closed.");
        }
        return internalBuffer;
    }

    /**
     * Creates a <code>BasicBufferedDirectBufInputStream</code>
     * and saves its  argument, the input stream
     * <code>in</code>, for later use. An internal
     * buffer array is created and  stored in <code>internalBuffer</code>.
     *
     * @param   in   the underlying input stream.
     */
    public BasicBufferedDirectBufInputStream(InputStream in, BufferAllocator allocator, String id,
        long startOffset, long totalByteSize, boolean enableHints) {
        this(in, allocator, id, startOffset, totalByteSize, defaultBufferSize, enableHints);
    }

    /**
     * Creates a <code>BasicBufferedDirectBufInputStream</code>
     * with the specified buffer size,
     * and saves its  argument, the input stream
     * <code>in</code>, for later use.  An internal
     * buffer of length  <code>size</code>
     * is created and stored in <code>internalBuffer</code>.
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

    public void init() throws UnsupportedOperationException, IOException{
        checkStreamSupportsByteBuffer();
        //do a late allocation of the buffer
        internalBuffer = allocator.buffer(bufSize);
        //try {
            if (enableHints) {
                fadviseIfAvailable(getInputStream(), startOffset, totalByteSize);
            }
            getInputStream().seek(startOffset);
            getNextBlock();
        //} catch (IOException e) {
        //TODO: Throw UserException here
        //}
    }

    /**
     * Fills the buffer with more data, taking into account
     * shuffling and other tricks for dealing with marks.
     * Assumes that it is being called by a synchronized method.
     * This method also assumes that all data has already been read in,
     * hence curPosInBuffer > count.
     */
    /*
    private void fill() throws IOException {
        DrillBuf buffer = getBuf();
        if (markpos < 0)
            curPosInBuffer = 0;            // no mark: throw away the buffer
        else if (curPosInBuffer >= buffer.length) //  no room left in buffer
            if (markpos > 0) {  // can throw away early part of the buffer
                int sz = curPosInBuffer - markpos;
                System.arraycopy(buffer, markpos, buffer, 0, sz);
                curPosInBuffer = sz;
                markpos = 0;
            } else if (buffer.length >= marklimit) {
                markpos = -1;   //  buffer got too big, invalidate mark
                curPosInBuffer = 0;        //  drop buffer contents
            } else {            //  grow buffer
                int nsz = curPosInBuffer * 2;
                if (nsz > marklimit)
                    nsz = marklimit;
                byte nbuf[] = new byte[nsz];
                System.arraycopy(buffer, 0, nbuf, 0, curPosInBuffer);
                if (!bufUpdater.compareAndSet(this, buffer, nbuf)) {
                    // Can't replace internalBuffer if there was an async close.
                    // Note: This would need to be changed if fill()
                    // is ever made accessible to multiple threads.
                    // But for now, the only way CAS can fail is via close.
                    // assert internalBuffer == null;
                    throw new IOException("Stream closed");
                }
                buffer = nbuf;
            }
        count = curPosInBuffer;
        int n = getInputStream().read(buffer, curPosInBuffer, buffer.length - curPosInBuffer);
        if (n > 0)
            count = n + curPosInBuffer;
    }
    */
    /**
     * Read one more block from the underlying stream.
     * Assumes we have reached the end of buffered date
     * Assumes it is being called from a synchronized block.
     */
    private void getNextBlock() throws IOException{
        Preconditions.checkState(this.curPosInBuffer >= this.count,
            "Internal error: Buffered stream has not been consumed and trying to read more from underlying stream");
        DrillBuf buffer = getBuf();
        count = curPosInBuffer = 0;


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
        if (bytesToRead > 0) {
            int n = CompatibilityUtil.getBuf(getInputStream(), directBuffer, bytesToRead);
            if (n > 0) {
                buffer.writerIndex(n);
                count = n + curPosInBuffer;
            }
        }
        this.curPosInStream = getInputStream().getPos();

    }

    /*
    private void fill() throws IOException {
        DrillBuf buffer = getBuf();
        //if (curPosInBuffer >= buffer.capacity()) { //  no room left in buffer
        //    int nsz = curPosInBuffer * 2;
        //    DrillBuf nbuf = allocator.buffer(nsz);
        //    buffer.getBytes(0, nbuf, 0, curPosInBuffer); // Copy bytes into new buffer
        //    synchronized(this) {
        //        //if (!bufUpdater.compareAndSet(this, buffer, nbuf)) {
        //            // Can't replace internalBuffer if there was an async close.
        //            // Note: This would need to be changed if fill()
        //            // is ever made accessible to multiple threads.
        //            // But for now, the only way CAS can fail is via close.
        //            // assert internalBuffer == null;
        //        if(internalBuffer != null) {
        //            internalBuffer = nbuf;
        //        } else {
        //           throw new IOException("Stream closed");
        //        }
        //        buffer.release();
        //    }
        //    buffer = nbuf;
        //}
        count = curPosInBuffer = 0;

        int remainingCapacity = buffer.capacity() - curPosInBuffer;
        long currentPos = getInputStream().getPos();

        //int bytesToRead = remainingCapacity <= (totalByteSize + startOffset - currentPos ) ?
        //    remainingCapacity :
        //    (int) (totalByteSize + startOffset - currentPos );
        int bytesToRead = remainingCapacity;

        ByteBuffer directBuffer = buffer.nioBuffer(curPosInBuffer, bytesToRead);
        // The DFS can return *more* bytes than requested if the capacity of the buffer is greater.
        // i.e 'n' can be greater than bytes requested which is pretty stupid and violates
        // the API contract; but we still have to deal with it.i Se we make sure the size of the
        // buffer is exactly the same as the number of bytes requested
        if (bytesToRead > 0) {
            int n = CompatibilityUtil.getBuf(getInputStream(), directBuffer, bytesToRead);
            if (n > 0) {
                buffer.writerIndex(n);
                count = n + curPosInBuffer;
            }
        }
    }
    */

    /**
     * See
     * the general contract of the <code>read</code>
     * method of <code>InputStream</code>.
     *
     * @return     the next byte of data, or <code>-1</code> if the end of the
     *             stream is reached.
     * @exception  IOException  if this input stream has been closed by
     *                          invoking its {@link #close()} method,
     *                          or an I/O error occurs.
     * @see        java.io.FilterInputStream#in
     */
    public synchronized int read() throws IOException {
        if (curPosInBuffer >= count) {
            getNextBlock();
            if (curPosInBuffer >= count) {
                return -1;
            }
        }
        curPosInBuffer++;
        return getBuf().nioBuffer().get() & 0xff;
    }

    /**
     * Read characters into a portion of an array, reading from the underlying
     * stream at most once if necessary.
     */
    private int read1(DrillBuf b, int off, int len) throws IOException {
        int avail = count - curPosInBuffer;
        if (avail <= 0) {
            /* If the requested length is at least as large as the buffer, and
               if there is no mark/reset activity, do not bother to copy the
               bytes into the local buffer.  In this way buffered streams will
               cascade harmlessly. */
            if (len >= getBuf().capacity() ) {
                long currentPos = getInputStream().getPos();
                //int bytesToRead = len <= (totalByteSize + startOffset - currentPos ) ?
                //    len :
                //    (int) (totalByteSize + startOffset - currentPos );
                int bytesToRead = len;
                ByteBuffer directBuffer = b.nioBuffer(off, bytesToRead);
                if (bytesToRead > 0) {
                    int n = CompatibilityUtil.getBuf(getInputStream(), directBuffer, bytesToRead);
                    if (n > 0) {
                        b.writerIndex(n);
                    }else{
                        n = 0;
                    }
                    return n;
                }
            }
            getNextBlock();
            avail = count - curPosInBuffer;
            if (avail <= 0)  {
                return -1;
            }
        }
        int cnt = (avail < len) ? avail : len;
        //System.arraycopy(getBuf(), curPosInBuffer, b, off, cnt);
        //TODO: Avoid this copy ...
        getBuf().getBytes(curPosInBuffer, b, off, cnt); // Copy bytes into new buffer
        b.writerIndex(off+cnt);
        curPosInBuffer += cnt;
        return cnt;
    }

    /**
     * Reads bytes from this byte-input stream into the specified byte array,
     * starting at the given offset.
     *
     * <p> This method implements the general contract of the corresponding
     * <code>{@link InputStream#read(byte[], int, int) read}</code> method of
     * the <code>{@link InputStream}</code> class.  As an additional
     * convenience, it attempts to read as many bytes as possible by repeatedly
     * invoking the <code>read</code> method of the underlying stream.  This
     * iterated <code>read</code> continues until one of the following
     * conditions becomes true: <ul>
     *
     *   <li> The specified number of bytes have been read,
     *
     *   <li> The <code>read</code> method of the underlying stream returns
     *   <code>-1</code>, indicating end-of-file, or
     *
     *   <li> The <code>available</code> method of the underlying stream
     *   returns zero, indicating that further input requests would block.
     *
     * </ul> If the first <code>read</code> on the underlying stream returns
     * <code>-1</code> to indicate end-of-file then this method returns
     * <code>-1</code>.  Otherwise this method returns the number of bytes
     * actually read.
     *
     * <p> Subclasses of this class are encouraged, but not required, to
     * attempt to read as many bytes as possible in the same fashion.
     *
     * @param      b     destination buffer.
     * @param      off   offset at which to start storing bytes.
     * @param      len   maximum number of bytes to read.
     * @return     the number of bytes read, or <code>-1</code> if the end of
     *             the stream has been reached.
     * @exception  IOException  if this input stream has been closed by
     *                          invoking its {@link #close()} method,
     *                          or an I/O error occurs.
     */
    public synchronized int read(DrillBuf b, int off, int len) throws IOException {
        getBuf(); // Check for closed stream
        if ((off | len | (off + len) | (b.capacity() - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        int n = 0;
        for (;;) {
            int nread = read1(b, off + n, len - n);
            if (nread <= 0) {
                return (n == 0) ? nread : n;
            }
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
     * See the general contract of the <code>skip</code>
     * method of <code>InputStream</code>.
     *
     * @exception  IOException  if the stream does not support seek,
     *                          or if this input stream has been closed by
     *                          invoking its {@link #close()} method, or an
     *                          I/O error occurs.
     */
    public synchronized long skip(long n) throws IOException {
        getBuf(); // Check for closed stream
        if (n <= 0) {
            return 0;
        }
        long avail = count - curPosInBuffer;

        if (avail <= 0) {

            // Fill in buffer to save bytes for reset
            getNextBlock();
            avail = count - curPosInBuffer;
            if (avail <= 0) {
                return 0;
            }
        }

        long skipped = (avail < n) ? avail : n;
        curPosInBuffer += skipped;
        return skipped;
    }

    /**
     * Returns an estimate of the number of bytes that can be read (or
     * skipped over) from this input stream without blocking by the next
     * invocation of a method for this input stream. The next invocation might be
     * the same thread or another thread.  A single read or skip of this
     * many bytes will not block, but may read or skip fewer bytes.
     * <p>
     * This method returns the sum of the number of bytes remaining to be read in
     * the buffer (<code>count&nbsp;- curPosInBuffer</code>) and the result of calling the
     * {@link java.io.FilterInputStream#in in}.available().
     *
     * @return     an estimate of the number of bytes that can be read (or skipped
     *             over) from this input stream without blocking.
     * @exception  IOException  if this input stream has been closed by
     *                          invoking its {@link #close()} method,
     *                          or an I/O error occurs.
     */
    public synchronized int available() throws IOException {
        int n = count - curPosInBuffer;
        int avail = getInputStream().available();
        return n > (Integer.MAX_VALUE - avail)
                    ? Integer.MAX_VALUE
                    : n + avail;
    }

    /**
     * See the general contract of the <code>mark</code>
     * method of <code>InputStream</code>.
     *
     * @param   readlimit   the maximum limit of bytes that can be read before
     *                      the mark position becomes invalid.
     * @see     java.io.BufferedInputStream#reset()
     */
    public synchronized void mark(int readlimit) {
        throw new UnsupportedOperationException("Mark/reset is not supported.");
    }

    /**
     * See the general contract of the <code>reset</code>
     * method of <code>InputStream</code>.
     * <p>
     * If <code>markpos</code> is <code>-1</code>
     * (no mark has been set or the mark has been
     * invalidated), an <code>IOException</code>
     * is thrown. Otherwise, <code>curPosInBuffer</code> is
     * set equal to <code>markpos</code>.
     *
     * @exception  IOException  if this stream has not been marked or,
     *                  if the mark has been invalidated, or the stream
     *                  has been closed by invoking its {@link #close()}
     *                  method, or an I/O error occurs.
     * @see        java.io.BufferedInputStream#mark(int)
     */
    public synchronized void reset() throws IOException {
        throw new UnsupportedOperationException("Mark/reset is not supported.");
    }

    /**
     * Tests if this input stream supports the <code>mark</code>
     * and <code>reset</code> methods. The <code>markSupported</code>
     * method of <code>BasicBufferedDirectBufInputStream</code> returns
     * <code>true</code>.
     *
     * @return  a <code>boolean</code> indicating if this stream type supports
     *          the <code>mark</code> and <code>reset</code> methods.
     * @see     InputStream#mark(int)
     * @see     InputStream#reset()
     */
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
            int nread = read1(byteBuf, off + n, len - n);
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
