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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.store.parquet.columnreaders.VLColumnBulkInput.ColumnPrecisionInfo;
import org.apache.drill.exec.store.parquet.columnreaders.VLColumnBulkInput.ColumnPrecisionType;
import org.apache.drill.exec.store.parquet.columnreaders.VLColumnBulkInput.PageDataInfo;
import org.apache.drill.exec.store.parquet.columnreaders.VLColumnBulkInput.VLColumnBulkInputCallback;

/** Provides bulk reads when accessing Parquet's page payload for variable length columns */
final class VLBulkPageReader {

  /**
   * Using small buffers so that they could fit in the L1 cache
   * NOTE - This buffer size is used in several places of the bulk processing implementation; please analyze
   *        the impact of changing this buffer size.
   */
  static final int BUFF_SZ = 1024 << 2; // 4k
  /** byte buffer used for buffering page data */
  private final ByteBuffer buffer = ByteBuffer.allocate(BUFF_SZ);
  /** Page Data Information */
  private final PageDataInfo pageInfo = new PageDataInfo();
  /** expected precision type: fixed or variable length */
  private final ColumnPrecisionInfo columnPrecInfo;
  /** Bulk entry */
  private final VLColumnBulkEntry entry;
  /** A callback to allow bulk readers interact with their container */
  private final VLColumnBulkInputCallback containerCallback;

  // Various BulkEntry readers
  final VLAbstractEntryReader fixedReader;
  final VLAbstractEntryReader nullableFixedReader;
  final VLAbstractEntryReader variableLengthReader;
  final VLAbstractEntryReader nullableVLReader;
  final VLAbstractEntryReader dictionaryReader;
  final VLAbstractEntryReader nullableDictionaryReader;

  VLBulkPageReader(
    PageDataInfo _pageInfo,
    ColumnPrecisionInfo _columnPrecInfo,
    VLColumnBulkInputCallback _containerCallback) {

    // Set the buffer to the native byte order
    buffer.order(ByteOrder.nativeOrder());

    this.pageInfo.pageData          = _pageInfo.pageData;
    this.pageInfo.pageDataOff       = _pageInfo.pageDataOff;
    this.pageInfo.pageDataLen       = _pageInfo.pageDataLen;
    this.pageInfo.numPageFieldsRead = _pageInfo.numPageFieldsRead;
    this.pageInfo.definitionLevels  = _pageInfo.definitionLevels;
    pageInfo.dictionaryValueReader = _pageInfo.dictionaryValueReader;
    this.columnPrecInfo             = _columnPrecInfo;
    this.entry                      = new VLColumnBulkEntry(this.columnPrecInfo);
    this.containerCallback          = _containerCallback;

    // Initialize the Variable Length Entry Readers
    fixedReader              = new VLFixedEntryReader(buffer, pageInfo, columnPrecInfo, entry);
    nullableFixedReader      = new VLNullableFixedEntryReader(buffer, pageInfo, columnPrecInfo, entry);
    variableLengthReader     = new VLEntryReader(buffer, pageInfo, columnPrecInfo, entry);
    nullableVLReader         = new VLNullableEntryReader(buffer, pageInfo, columnPrecInfo, entry);
    dictionaryReader         = new VLEntryDictionaryReader(buffer, pageInfo, columnPrecInfo, entry);
    nullableDictionaryReader = new VLNullableDictionaryReader(buffer, pageInfo, columnPrecInfo, entry);
  }

  final void set(PageDataInfo _pageInfo) {
    pageInfo.pageData              = _pageInfo.pageData;
    pageInfo.pageDataOff           = _pageInfo.pageDataOff;
    pageInfo.pageDataLen           = _pageInfo.pageDataLen;
    pageInfo.numPageFieldsRead     = _pageInfo.numPageFieldsRead;
    pageInfo.definitionLevels      = _pageInfo.definitionLevels;
    pageInfo.dictionaryValueReader = _pageInfo.dictionaryValueReader;

    buffer.clear();
  }

  final VLColumnBulkEntry getEntry(int valuesToRead) {
    VLColumnBulkEntry entry = null;

    if (ColumnPrecisionType.isPrecTypeFixed(columnPrecInfo.columnPrecisionType)) {
      if ((entry = getFixedEntry(valuesToRead)) == null) {
        // The only reason for a null to be returned is when the "getFixedEntry" method discovers
        // the column is not fixed length; this false positive happens if the sample data was not
        // representative of all the column values.

        // If this is an optional column, then we need to reset the definition-level reader
        if (pageInfo.definitionLevels.hasDefinitionLevels()) {
          try {
            containerCallback.resetDefinitionLevelReader(pageInfo.numPageFieldsRead);
            // Update the definition level object reference
            pageInfo.definitionLevels.set(containerCallback.getDefinitionLevelsReader(), pageInfo.numPageValues);

          } catch (IOException ie) {
            throw new DrillRuntimeException(ie);
          }
        }

        columnPrecInfo.columnPrecisionType = ColumnPrecisionType.DT_PRECISION_IS_VARIABLE;
        entry                              = getVLEntry(valuesToRead);
      }

    } else {
      entry = getVLEntry(valuesToRead);
    }

    if (entry != null) {
      pageInfo.numPageFieldsRead += entry.getNumValues();
    }
    return entry;
  }

  private final VLColumnBulkEntry getFixedEntry(int valuesToRead) {
    if (pageInfo.definitionLevels.hasDefinitionLevels()) {
      return nullableFixedReader.getEntry(valuesToRead);
    } else {
      return fixedReader.getEntry(valuesToRead);
    }
  }

  private final VLColumnBulkEntry getVLEntry(int valuesToRead) {
    if (pageInfo.dictionaryValueReader == null) {
      if (pageInfo.definitionLevels.hasDefinitionLevels()) {
        return nullableVLReader.getEntry(valuesToRead);
      } else {
        return variableLengthReader.getEntry(valuesToRead);
      }
    } else {
      if (pageInfo.definitionLevels.hasDefinitionLevels()) {
        return nullableDictionaryReader.getEntry(valuesToRead);
      } else {
        return dictionaryReader.getEntry(valuesToRead);
      }
    }
  }

}