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
package parquet.FileReader;

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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * A <code>BasicBufferedDirectBufInputStream</code> adds
 * functionality to another input stream-namely,
 * the ability to buffer the input and to
 * support the <code>mark</code> and <code>reset</code>
 * methods. When  the <code>BasicBufferedDirectBufInputStream</code>
 * is created, an internal buffer array is
 * created. As bytes  from the stream are read
 * or skipped, the internal buffer is refilled
 * as necessary  from the contained input stream,
 * many bytes at a time. The <code>mark</code>
 * operation  remembers a point in the input
 * stream and the <code>reset</code> operation
 * causes all the  bytes read since the most
 * recent <code>mark</code> operation to be
 * reread before new bytes re  taken from
 * the contained input stream.
 *
 * @author  Arthur van Hoff
 * @since   JDK1.0
 */
public
class BasicBufferedDirectBufInputStream extends BufferedDirectBufInputStream implements Closeable {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicBufferedDirectBufInputStream.class);

    private static int defaultBufferSize = 8192*1024; // 8 MB

    /**
     * The internal buffer array where the data is stored. When necessary,
     * it may be replaced by another array of
     * a different size.
     */
    protected volatile DrillBuf buf;

    /**
     * Atomic updater to provide compareAndSet for buf. This is
     * necessary because closes can be asynchronous. We use nullness
     * of buf[] as primary indicator that this stream is closed. (The
     * "in" field is also nulled out on close.)
     */
    private static final
        AtomicReferenceFieldUpdater<BasicBufferedDirectBufInputStream, DrillBuf> bufUpdater =
        AtomicReferenceFieldUpdater.newUpdater
        (BasicBufferedDirectBufInputStream.class,  DrillBuf.class, "buf");

    /**
     * The index one greater than the index of the last valid byte in
     * the buffer.
     * This value is always
     * in the range <code>0</code> through <code>buf.length</code>;
     * elements <code>buf[0]</code>  through <code>buf[count-1]
     * </code>contain buffered input data obtained
     * from the underlying  input stream.
     */
    protected int count;

    /**
     * The current position in the buffer. This is the index of the next
     * character to be read from the <code>buf</code> array.
     * <p>
     * This value is always in the range <code>0</code>
     * through <code>count</code>. If it is less
     * than <code>count</code>, then  <code>buf[pos]</code>
     * is the next byte to be supplied as input;
     * if it is equal to <code>count</code>, then
     * the  next <code>read</code> or <code>skip</code>
     * operation will require more bytes to be
     * read from the contained  input stream.
     *
     * @see     java.io.BufferedInputStream#buf
     */
    protected int pos;

    /**
     * The value of the <code>pos</code> field at the time the last
     * <code>mark</code> method was called.
     * <p>
     * This value is always
     * in the range <code>-1</code> through <code>pos</code>.
     * If there is no marked position in  the input
     * stream, this field is <code>-1</code>. If
     * there is a marked position in the input
     * stream,  then <code>buf[markpos]</code>
     * is the first byte to be supplied as input
     * after a <code>reset</code> operation. If
     * <code>markpos</code> is not <code>-1</code>,
     * then all bytes from positions <code>buf[markpos]</code>
     * through  <code>buf[pos-1]</code> must remain
     * in the buffer array (though they may be
     * moved to  another place in the buffer array,
     * with suitable adjustments to the values
     * of <code>count</code>,  <code>pos</code>,
     * and <code>markpos</code>); they may not
     * be discarded unless and until the difference
     * between <code>pos</code> and <code>markpos</code>
     * exceeds <code>marklimit</code>.
     *
     * @see     java.io.BufferedInputStream#mark(int)
     * @see     java.io.BufferedInputStream#pos
     */
    //protected int markpos = -1;

    /**
     * The maximum read ahead allowed after a call to the
     * <code>mark</code> method before subsequent calls to the
     * <code>reset</code> method fail.
     * Whenever the difference between <code>pos</code>
     * and <code>markpos</code> exceeds <code>marklimit</code>,
     * then the  mark may be dropped by setting
     * <code>markpos</code> to <code>-1</code>.
     *
     * @see     java.io.BufferedInputStream#mark(int)
     * @see     java.io.BufferedInputStream#reset()
     */
    //protected int marklimit;

    protected int curpos; // current offset in the input stream

    protected String streamId; // a name for logging purposes only

    private final long startOffset;
    private final long totalByteSize;

    protected BufferAllocator allocator;
    private final int bufSize;

    /**
     * Check to make sure that underlying input stream has not been
     * nulled out due to close; if not return it;
     */
    private FSDataInputStream getInIfOpen() throws IOException {
        InputStream input = in;
        if (input == null) {
            throw new IOException("Stream closed");
        }
        // Make input stream a stream that supports ByteBuffer
        if ( !(in instanceof ByteBufferReadable) ) {
            throw new UnsupportedOperationException("The input stream is not ByteBuffer readable.");
        }

        return (FSDataInputStream)input;
    }

    /**
     * Check to make sure that buffer has not been nulled out due to
     * close; if not return it;
     */
    private DrillBuf getBufIfOpen() throws IOException {
        DrillBuf buffer = buf;
        if (buffer == null) {
            throw new IOException("Stream closed");
        }
        return buf;
    }

    /**
     * Creates a <code>BasicBufferedDirectBufInputStream</code>
     * and saves its  argument, the input stream
     * <code>in</code>, for later use. An internal
     * buffer array is created and  stored in <code>buf</code>.
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
     * buffer array of length  <code>size</code>
     * is created and stored in <code>buf</code>.
     *
     * @param   in     the underlying input stream.
     * @param   bufSize   the buffer size.
     * @exception IllegalArgumentException if size <= 0.
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

    public void init() {
        //do a late allocation of the buffer
        buf = allocator.buffer(bufSize);
        //TODO: issue fadvise here.
        // Need a reflection based call here.
        try {
            if (enableHints) {
                fadviseIfAvailable(getInIfOpen(), startOffset, totalByteSize);
            }
            getInIfOpen().seek(startOffset);
            fill();
        } catch (IOException e) {
            //TODO: Throw UserException here
        }
    }

    /**
     * Fills the buffer with more data, taking into account
     * shuffling and other tricks for dealing with marks.
     * Assumes that it is being called by a synchronized method.
     * This method also assumes that all data has already been read in,
     * hence pos > count.
     */
    /*
    private void fill() throws IOException {
        DrillBuf buffer = getBufIfOpen();
        if (markpos < 0)
            pos = 0;            // no mark: throw away the buffer
        else if (pos >= buffer.length) //  no room left in buffer
            if (markpos > 0) {  // can throw away early part of the buffer
                int sz = pos - markpos;
                System.arraycopy(buffer, markpos, buffer, 0, sz);
                pos = sz;
                markpos = 0;
            } else if (buffer.length >= marklimit) {
                markpos = -1;   //  buffer got too big, invalidate mark
                pos = 0;        //  drop buffer contents
            } else {            //  grow buffer
                int nsz = pos * 2;
                if (nsz > marklimit)
                    nsz = marklimit;
                byte nbuf[] = new byte[nsz];
                System.arraycopy(buffer, 0, nbuf, 0, pos);
                if (!bufUpdater.compareAndSet(this, buffer, nbuf)) {
                    // Can't replace buf if there was an async close.
                    // Note: This would need to be changed if fill()
                    // is ever made accessible to multiple threads.
                    // But for now, the only way CAS can fail is via close.
                    // assert buf == null;
                    throw new IOException("Stream closed");
                }
                buffer = nbuf;
            }
        count = pos;
        int n = getInIfOpen().read(buffer, pos, buffer.length - pos);
        if (n > 0)
            count = n + pos;
    }
    */
    private void fill() throws IOException {
        DrillBuf buffer = getBufIfOpen();
        if (pos >= buffer.capacity()) { //  no room left in buffer
            int nsz = pos * 2;
            DrillBuf nbuf = allocator.buffer(nsz);
            //System.arraycopy(buffer, 0, nbuf, 0, pos);
            buffer.getBytes(0, nbuf, 0, pos); // Copy bytes into new buffer
            buf.release();
            if (!bufUpdater.compareAndSet(this, buffer, nbuf)) {
                // Can't replace buf if there was an async close.
                // Note: This would need to be changed if fill()
                // is ever made accessible to multiple threads.
                // But for now, the only way CAS can fail is via close.
                // assert buf == null;
                throw new IOException("Stream closed");
            }
            buffer = nbuf;
        }
        count = pos;

        int remainingCapacity = buffer.capacity() - pos;
        long currentPos = getInIfOpen().getPos();

        //int bytesToRead = remainingCapacity <= (totalByteSize + startOffset - currentPos ) ?
        //    remainingCapacity :
        //    (int) (totalByteSize + startOffset - currentPos );
        int bytesToRead = remainingCapacity;

        ByteBuffer directBuffer = buffer.nioBuffer(pos, bytesToRead);
        // The DFS can return *more* bytes than requested if the capacity of the buffer is greater.
        // i.e 'n' can be greater than bytes requested which is pretty stupid and violates
        // the API contract; but we still have to deal with it.i Se we make sure the size of the
        // buffer is exactly the same as the number of bytes requested
        if (bytesToRead > 0) {
            int n = CompatibilityUtil.getBuf(getInIfOpen(), directBuffer, bytesToRead);
            if (n > 0) {
                buffer.writerIndex(n);
                count = n + pos;
            }
        }
    }

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
        if (pos >= count) {
            fill();
            if (pos >= count) {
                return -1;
            }
        }
        pos++;
        return getBufIfOpen().nioBuffer().get() & 0xff;
    }

    /**
     * Read characters into a portion of an array, reading from the underlying
     * stream at most once if necessary.
     */
    private int read1(DrillBuf b, int off, int len) throws IOException {
        int avail = count - pos;
        if (avail <= 0) {
            /* If the requested length is at least as large as the buffer, and
               if there is no mark/reset activity, do not bother to copy the
               bytes into the local buffer.  In this way buffered streams will
               cascade harmlessly. */
            if (len >= getBufIfOpen().capacity() ) {
                long currentPos = getInIfOpen().getPos();
                //int bytesToRead = len <= (totalByteSize + startOffset - currentPos ) ?
                //    len :
                //    (int) (totalByteSize + startOffset - currentPos );
                int bytesToRead = len;
                ByteBuffer directBuffer = b.nioBuffer(off, bytesToRead);
                if (bytesToRead > 0) {
                    int n = CompatibilityUtil.getBuf(getInIfOpen(), directBuffer, bytesToRead);
                    if (n > 0) {
                        b.writerIndex(n);
                    }else{
                        n = 0;
                    }
                    return n;
                }
            }
            fill();
            avail = count - pos;
            if (avail <= 0)  {
                return -1;
            }
        }
        int cnt = (avail < len) ? avail : len;
        //System.arraycopy(getBufIfOpen(), pos, b, off, cnt);
        //TODO: Avoid this copy ...
        getBufIfOpen().getBytes(pos, b, off, cnt); // Copy bytes into new buffer
        b.writerIndex(off+cnt);
        pos += cnt;
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
        getBufIfOpen(); // Check for closed stream
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
        getBufIfOpen(); // Check for closed stream
        if (n <= 0) {
            return 0;
        }
        long avail = count - pos;

        if (avail <= 0) {

            // Fill in buffer to save bytes for reset
            fill();
            avail = count - pos;
            if (avail <= 0) {
                return 0;
            }
        }

        long skipped = (avail < n) ? avail : n;
        pos += skipped;
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
     * the buffer (<code>count&nbsp;- pos</code>) and the result of calling the
     * {@link java.io.FilterInputStream#in in}.available().
     *
     * @return     an estimate of the number of bytes that can be read (or skipped
     *             over) from this input stream without blocking.
     * @exception  IOException  if this input stream has been closed by
     *                          invoking its {@link #close()} method,
     *                          or an I/O error occurs.
     */
    public synchronized int available() throws IOException {
        int n = count - pos;
        int avail = getInIfOpen().available();
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
     * is thrown. Otherwise, <code>pos</code> is
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
        return getInIfOpen().getPos();
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
        getBufIfOpen(); // Check for closed stream
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
        while ( (buffer = buf) != null) {
            buf.release();
            if (bufUpdater.compareAndSet(this, buffer, null)) {
                InputStream input = in;
                in = null;
                if (input != null) {
                    input.close();
                }
                return;
            }
            // Else retry in case a new buf wa
            // CASed in fill()
        }
        getInIfOpen().close();
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
