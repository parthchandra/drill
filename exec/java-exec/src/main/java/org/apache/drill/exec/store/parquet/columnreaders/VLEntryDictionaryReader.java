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
import org.apache.parquet.io.api.Binary;

/** Handles variable data types using a dictionary */
final class VLEntryDictionaryReader extends VLAbstractEntryReader {

  VLEntryDictionaryReader(ByteBuffer _buffer,
    PageDataInfo _pageInfo,
    ColumnPrecisionInfo _columnPrecInfo,
    VLColumnBulkEntry _entry) {

    super(_buffer, _pageInfo, _columnPrecInfo, _entry);
  }

  /** {@inheritDoc} */
  @Override
  final VLColumnBulkEntry getEntry(int valuesToRead) {
    // Bulk processing is effecting for smaller precisions
    if (bulkProcess()) {
      return getEntryBulk(valuesToRead);
    }
    return getEntrySingle(valuesToRead);
  }

  private final VLColumnBulkEntry getEntryBulk(int valuesToRead) {
    final ValuesReader valueReader = pageInfo.dictionaryValueReader;
    final int[] valueLengths       = entry.getValuesLength();
    final int readBatch            = Math.min(entry.getMaxEntries(), valuesToRead);
    final byte[] tgtBuff           = entry.getInternalDataArray();
    final int tgtLen               = tgtBuff.length;

    // Counters
    int numValues = 0;
    int tgtPos    = 0;

    for (int idx = 0; idx < readBatch; ++idx ) {
      final Binary currEntry = valueReader.readBytes();
      final int dataLen      = currEntry.length();

      if (tgtLen < (tgtPos + dataLen)) {
        break;
      }

      valueLengths[numValues++] = dataLen;

      if (dataLen <= 8) {
        vlCopyLELong(currEntry.getBytes(), 0, tgtBuff, tgtPos, dataLen);
      } else {
        vlCopyGTLong(currEntry.getBytes(), 0, tgtBuff, tgtPos, dataLen);
      }

      // Update the counters
      tgtPos += dataLen;
    }

    // We're here either because a) the Parquet metadata is wrong (advertises more values than the real count)
    // or the first value being processed ended up to be too long for the buffer.
    if (numValues == 0) {
      return getEntrySingle(valuesToRead);
    }

    // Now set the bulk entry
    entry.set(0, tgtPos, numValues, false);

    return entry;
  }

  private final VLColumnBulkEntry getEntrySingle(int valsToReadWithinPage) {
    final ValuesReader valueReader = pageInfo.dictionaryValueReader;
    final int[] valueLengths       = entry.getValuesLength();
    final Binary currEntry         = valueReader.readBytes();
    final int dataLen              = currEntry.length();

    // Set the value length
    valueLengths[0] = dataLen;

    // Now set the bulk entry
    entry.set(0, dataLen, 1, false, currEntry.getBytes());

    return entry;
  }

}
