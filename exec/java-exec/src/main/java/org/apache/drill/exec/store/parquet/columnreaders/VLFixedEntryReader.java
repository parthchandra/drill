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

/** Handles fixed data types that have been erroneously tagged as Variable Length. */
final class VLFixedEntryReader extends VLAbstractEntryReader {

  VLFixedEntryReader(ByteBuffer _buffer,
    PageDataInfo _pageInfo,
    ColumnPrecisionInfo _columnPrecInfo,
    VLColumnBulkEntry _entry) {

    super(_buffer, _pageInfo, _columnPrecInfo, _entry);
  }

  /** {@inheritDoc} */
  @Override
  final VLColumnBulkEntry getEntry(int valuesToRead) {
    assert columnPrecInfo.precision >= 0 : "Fixed length precision cannot be lower than zero";

    load(true); // load new data to process

    final int expected_data_len = columnPrecInfo.precision;
    final int entry_sz          = 4 + columnPrecInfo.precision;
    final int max_values        = Math.min(entry.getMaxEntries(), (pageInfo.pageDataLen-pageInfo.pageDataOff)/entry_sz);
    final int read_batch        = Math.min(max_values, valuesToRead);
    final int[] value_lengths   = entry.getValuesLength();
    final byte[] tgt_buff       = entry.getInternalDataArray();
    final byte[] src_buff       = buffer.array();
    int idx                     = 0;

    for ( ; idx < read_batch; ++idx) {
      final int curr_pos = idx * entry_sz;
      final int data_len = getInt(src_buff, curr_pos);

      if (data_len != expected_data_len) {
        return null; // this is a soft error; caller needs to revert to variable length processing
      }

      value_lengths[idx] = data_len;
      final int tgt_pos  = idx * expected_data_len;

      if (expected_data_len > 0) {
        if (expected_data_len <= 8) {
          if (idx < (read_batch-2) ) {
            MemoryUtils.putLong(src_buff, curr_pos+4, tgt_buff, tgt_pos);
          } else {
            vlCopyLELong(src_buff, curr_pos+4, tgt_buff, tgt_pos, data_len);
          }

        } else {
          vlCopyGTLong(src_buff, curr_pos+4, tgt_buff, tgt_pos, data_len);
        }
      }
    }

    // Update the page data buffer offset
    pageInfo.pageDataOff += idx * entry_sz;

    // Now set the bulk entry
    entry.set(0, idx * expected_data_len, idx, false);

    return entry;
  }
}
