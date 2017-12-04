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

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.store.parquet.columnreaders.VLColumnBulkInput.ColumnPrecisionInfo;
import org.apache.drill.exec.store.parquet.columnreaders.VLColumnBulkInput.PageDataInfo;
import org.apache.drill.exec.util.MemoryUtils;

/** Handles variable data types. */
final class VLNullableEntryReader extends VLAbstractEntryReader {

  VLNullableEntryReader(ByteBuffer _buffer,
      PageDataInfo _pageInfo,
      ColumnPrecisionInfo _columnPrecInfo,
      VLColumnBulkEntry _entry) {

    super(_buffer, _pageInfo, _columnPrecInfo, _entry);
  }

  /** {@inheritDoc} */
  @Override
  VLColumnBulkEntry getEntry(int valuesToRead) {
    // Bulk processing is effective for smaller precisions
    if (bulkProcess()) {
      return getEntryBulk(valuesToRead);
    }
    return getEntrySingle(valuesToRead);
  }

  VLColumnBulkEntry getEntryBulk(int valuesToRead) {

    load(true); // load new data to process

    final int[] value_lengths = entry.getValuesLength();
    final int readBatch    = Math.min(entry.getMaxEntries(), valuesToRead);
    final byte[] tgt_buff  = entry.getInternalDataArray();
    final byte[] src_buff  = buffer.array();
    final int src_len      = buffer.remaining();
    final int tgt_len      = tgt_buff.length;

    // Counters
    int numValues = 0;
    int numNulls  = 0;
    int tgt_pos   = 0;
    int src_pos   = 0;

    if (readBatch > 0) {
      // Initialize the reader if needed
      pageInfo.definitionLevels.readFirstIntegerIfNeeded();
    }

    for (; numValues < readBatch; ) {
      // Non-null entry
      if (pageInfo.definitionLevels.readCurrInteger() == 1) {
        if (src_pos > src_len -4) {
          break;
        }

        final int data_len = getInt(src_buff, src_pos);
        src_pos           += 4;

        if (src_len < (src_pos + data_len)
         || tgt_len < (tgt_pos + data_len)) {

          break;
        }

        value_lengths[numValues++] = data_len;

        if (data_len > 0) {
          if (data_len <= 8) {
            if (src_pos+7 < src_len) {
              MemoryUtils.putLong(src_buff, src_pos, tgt_buff, tgt_pos);
            } else {
              vlCopyLELong(src_buff, src_pos, tgt_buff, tgt_pos, data_len);
            }

          } else {
            vlCopyGTLong(src_buff, src_pos, tgt_buff, tgt_pos, data_len);
          }
        }

        // Update the counters
        src_pos += data_len;
        tgt_pos += data_len;

      } else { // Null value
        value_lengths[numValues++] = -1;
        ++numNulls;
      }

      // read the next definition-level value since we know the current entry has been processed
      pageInfo.definitionLevels.nextInteger();
    }

    // We're here either because a) the Parquet metadata is wrong (advertises more values than the real count)
    // or the first value being processed ended up to be too long for the buffer.
    if (numValues == 0) {
      return getEntrySingle(valuesToRead);
    }

    // Update the page data buffer offset
    pageInfo.pageDataOff += ((numValues - numNulls) * 4 + tgt_pos);

    if (remainingPageData() < 0) {
      final String message = String.format("Invalid Parquet page data offset [%d]..", pageInfo.pageDataOff);
      throw new DrillRuntimeException(message);
    }

    // Now set the bulk entry
    entry.set(0, tgt_pos, numValues, numNulls > 0);

    return entry;
  }

  VLColumnBulkEntry getEntrySingle(int valuesToRead) {

    final int[] value_lengths = entry.getValuesLength();

    if (pageInfo.definitionLevels.readCurrInteger() == 1) {

      if (remainingPageData() < 4) {
        final String message = String.format("Invalid Parquet page metadata; cannot process advertised page count..");
        throw new DrillRuntimeException(message);
      }

      final int data_len  = pageInfo.pageData.getInt(pageInfo.pageDataOff);

      if (remainingPageData() < (4 + data_len)) {
        final String message = String.format("Invalid Parquet page metadata; cannot process advertised page count..");
        throw new DrillRuntimeException(message);
      }

      // Register the length
      value_lengths[0] = data_len;

      // Now set the bulk entry
      entry.set(pageInfo.pageDataOff + 4, data_len, 1, false, pageInfo.pageData);

      // Update the page data buffer offset
      pageInfo.pageDataOff += (data_len + 4);

    } else { // Null value
      value_lengths[0] = -1;

      // Now set the bulk entry
      entry.set(0, 0, 1, true);
    }

    // read the next definition-level value since we know the current entry has been processed
    pageInfo.definitionLevels.nextInteger();

    return entry;
  }


}
