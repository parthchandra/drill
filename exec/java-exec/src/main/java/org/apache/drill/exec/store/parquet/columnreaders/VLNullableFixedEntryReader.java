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
import org.apache.parquet.column.values.ValuesReader;

/** Handles nullable fixed data types that have been erroneously tagged as Variable Length. */
final class VLNullableFixedEntryReader extends VLAbstractEntryReader {

  VLNullableFixedEntryReader(ByteBuffer _buffer,
      PageDataInfo _pageInfo,
      ColumnPrecisionInfo _columnPrecInfo,
      VLColumnBulkEntry _entry) {

    super(_buffer, _pageInfo, _columnPrecInfo, _entry);
  }

  /** {@inheritDoc} */
  @Override
  final VLColumnBulkEntry getEntry(int valuesToRead) {
    assert columnPrecInfo.precision >= 0 : "Fixed length precision cannot be lower than zero";

    // TODO - We should not use force reload for sparse columns (values with lot of nulls)
    load(true); // load new data to process

    final int expected_data_len = columnPrecInfo.precision;
    final int entry_sz          = 4 + columnPrecInfo.precision;
    final int read_batch        = Math.min(entry.getMaxEntries(), valuesToRead);
    final int[] value_lengths   = entry.getValuesLength();
    final byte[] tgt_buff       = entry.getInternalDataArray();
    final byte[] src_buff       = buffer.array();
    int nonNullValues           = 0;
    int idx                     = 0;

    // Fixed precision processing can directly operate on the raw definition-level reader as no peeking
    // is needed.
    final ValuesReader definitionLevels = pageInfo.definitionLevels.getUnderlyingReader();

    for ( ; idx < read_batch; ++idx) {
      if (definitionLevels.readInteger() == 1) {

        final int curr_pos = nonNullValues * entry_sz;
        final int data_len = getInt(src_buff, curr_pos);

        if (data_len != expected_data_len) {
          return null; // this is a soft error; caller needs to revert to variable length processing
        }

        value_lengths[idx] = data_len;
        final int tgt_pos  = nonNullValues * expected_data_len;

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

        // Increase the non null values counter
        ++nonNullValues;

      } else { // Null value
        value_lengths[idx] = -1; // to mark a null value
      }
    }

    // Update the page data buffer offset
    pageInfo.pageDataOff += nonNullValues * entry_sz;

    // Now set the bulk entry
    entry.set(0, nonNullValues * expected_data_len, idx, idx != nonNullValues);

    return entry;
  }

}
