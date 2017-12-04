/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.store.parquet.columnreaders;

import java.nio.ByteBuffer;

import org.apache.drill.exec.store.parquet.columnreaders.VLColumnBulkInput.ColumnPrecisionInfo;
import org.apache.drill.exec.store.parquet.columnreaders.VLColumnBulkInput.PageDataInfo;
import org.apache.drill.exec.util.MemoryUtils;

/** Abstract class for sub-classes implementing several algorithms for loading a Bulk Entry */
abstract class VLAbstractEntryReader {

  /** byte buffer used for buffering page data */
  protected final ByteBuffer buffer;
  /** Page Data Information */
  protected final PageDataInfo pageInfo;
  /** expected precision type: fixed or variable length */
  protected final ColumnPrecisionInfo columnPrecInfo;
  /** Bulk entry */
  protected final VLColumnBulkEntry entry;

  /**
   * CTOR.
   * @param _buffer byte buffer for data buffering (within CPU cache)
   * @param _pageInfo page being processed information
   * @param _columnPrecInfo column precision information
   * @param _entry reusable bulk entry object
   */
  VLAbstractEntryReader(ByteBuffer _buffer,
    PageDataInfo _pageInfo,
    ColumnPrecisionInfo _columnPrecInfo,
    VLColumnBulkEntry _entry) {

    this.buffer         = _buffer;
    this.pageInfo       = _pageInfo;
    this.columnPrecInfo = _columnPrecInfo;
    this.entry          = _entry;
  }

  /**
   * @param valuesToRead maximum values to read within the current page
   * @return a bulk entry object
   */
  abstract VLColumnBulkEntry getEntry(int valsToReadWithinPage);

  /**
   * Indicates whether to use bulk processing
   */
  protected final boolean bulkProcess() {
    return columnPrecInfo.bulkProcess;
  }

  /**
   * Loads new data into the buffer if empty or the force flag is set.
   *
   * @param force flag to force loading new data into the buffer
   */
  protected final boolean load(boolean force) {

    if (!force && buffer.hasRemaining()) {
      return true; // NOOP
    }

    // We're here either because the buffer is empty or we want to force a new load operation.
    // In the case of force, there might be unprocessed data (still in the buffer) which is fine
    // since the caller updates the page data buffer's offset only for the data it has consumed; this
    // means unread data will be loaded again but this time will be positioned in the beginning of the
    // buffer. This can happen only for the last entry in the buffer when either of its length or value
    // is incomplete.
    buffer.clear();

    int remaining = remainingPageData();
    int toCopy    = remaining > buffer.capacity() ? buffer.capacity() : remaining;

    if (toCopy == 0) {
      return false;
    }

    pageInfo.pageData.getBytes(pageInfo.pageDataOff, buffer.array(), buffer.position(), toCopy);

    buffer.limit(toCopy);

    // At this point the buffer position is 0 and its limit set to the number of bytes copied.

    return true;
  }

  /**
   * @return remaining data in current page
   */
  protected final int remainingPageData() {
    return pageInfo.pageDataLen - pageInfo.pageDataOff;
  }

  /**
   * @param buff source buffer
   * @param pos start position
   * @return an integer encoded as a low endian
   */
  protected final int getInt(final byte[] buff, final int pos) {
    return MemoryUtils.getInt(buff, pos);
  }

  /**
   * Copy data with a length less or equal to a long
   *
   * @param src source buffer
   * @param srcIndex source index
   * @param dest destination buffer
   * @param destIndex destination buffer
   * @param length length to copy (in bytes)
   */
  static final void vlCopyLELong(byte[] src, int srcIndex, byte[] dest, int destIndex, int length) {
    assert length >= 0 && length <= MemoryUtils.LONG_NUM_BYTES;

    if (length == 1) {
      dest[destIndex] = src[srcIndex];

    } else if (length == 2) {
      MemoryUtils.putShort(src, srcIndex, dest, destIndex);

    } else if (length == 3) {
      dest[destIndex] = src[srcIndex];
      MemoryUtils.putShort(src, srcIndex+1, dest, destIndex+1);

    } else if (length == 4) {
      MemoryUtils.putInt(src, srcIndex, dest, destIndex);

    } else if (length == 5) {
      dest[destIndex] = src[srcIndex];
      MemoryUtils.putInt(src, srcIndex+1, dest, destIndex+1);

    } else if (length == 6) {
      MemoryUtils.putShort(src, srcIndex, dest, destIndex);
      MemoryUtils.putInt(src, srcIndex+2, dest, destIndex+2);

    } else if (length == 7) {
      dest[destIndex] = src[srcIndex];
      MemoryUtils.putShort(src, srcIndex+1, dest, destIndex+1);
      MemoryUtils.putInt(src, srcIndex+3, dest, destIndex+3);

    } else {
      MemoryUtils.putLong(src, srcIndex, dest, destIndex);
    }
  }

  /**
   * Copy data with a length greater than a long
   *
   * @param src source buffer
   * @param srcIndex source index
   * @param dest destination buffer
   * @param destIndex destination buffer
   * @param length length to copy (in bytes)
   */
  static final void vlCopyGTLong(byte[] src, int srcIndex, byte[] dest, int destIndex, int length) {
    assert length >= 0 && length > MemoryUtils.LONG_NUM_BYTES;

    final int numLongEntries = length / MemoryUtils.LONG_NUM_BYTES;
    final int remaining      = length % MemoryUtils.LONG_NUM_BYTES;

    if (numLongEntries == 1) {
      MemoryUtils.putLong(src, srcIndex, dest, destIndex);

    } else if (numLongEntries == 2) {
      MemoryUtils.putLong(src, srcIndex, dest, destIndex);
      MemoryUtils.putLong(src, srcIndex + MemoryUtils.LONG_NUM_BYTES, dest, destIndex + MemoryUtils.LONG_NUM_BYTES);

    } else if (numLongEntries == 3) {
      MemoryUtils.putLong(src, srcIndex, dest, destIndex);
      MemoryUtils.putLong(src, srcIndex + MemoryUtils.LONG_NUM_BYTES, dest, destIndex + MemoryUtils.LONG_NUM_BYTES);
      MemoryUtils.putLong(src, srcIndex + 2 * MemoryUtils.LONG_NUM_BYTES, dest, destIndex + 2 * MemoryUtils.LONG_NUM_BYTES);

    } else if (numLongEntries == 4) {
      MemoryUtils.putLong(src, srcIndex, dest, destIndex);
      MemoryUtils.putLong(src, srcIndex + MemoryUtils.LONG_NUM_BYTES, dest, destIndex + MemoryUtils.LONG_NUM_BYTES);
      MemoryUtils.putLong(src, srcIndex + 2 * MemoryUtils.LONG_NUM_BYTES, dest, destIndex + 2 * MemoryUtils.LONG_NUM_BYTES);
      MemoryUtils.putLong(src, srcIndex + 3 * MemoryUtils.LONG_NUM_BYTES, dest, destIndex + 3 * MemoryUtils.LONG_NUM_BYTES);

    } else {
      for (int idx = 0; idx < numLongEntries; ++idx) {
        MemoryUtils.putLong(src, srcIndex + idx * MemoryUtils.LONG_NUM_BYTES, dest, destIndex + idx * MemoryUtils.LONG_NUM_BYTES);
      }
    }

    if (remaining > 0) {
      vlCopyLELong(src, srcIndex + numLongEntries * MemoryUtils.LONG_NUM_BYTES, dest, destIndex + numLongEntries * MemoryUtils.LONG_NUM_BYTES, remaining);
    }
  }

}
