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
package org.apache.drill.exec.store.parquet.columnreaders;

import java.io.IOException;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.NullableVectorDefinitionSetter;
import org.apache.drill.exec.vector.ValueVector;

import parquet.column.ColumnDescriptor;
import parquet.format.SchemaElement;
import parquet.hadoop.metadata.ColumnChunkMetaData;

abstract class NullableColumnReader<V extends ValueVector> extends ColumnReader<V>{

  private int nullsFound;
  // used to skip nulls found
  private int rightBitShift;
  // used when copying less than a byte worth of data at a time, to indicate the number of used bits in the current byte
  private int bitsUsed;
  BaseDataValueVector castedBaseVector;
  NullableVectorDefinitionSetter castedVectorMutator;
  long definitionLevelsRead;
  long totalDefinitionLevelsRead;

  NullableColumnReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
               boolean fixedLength, V v, SchemaElement schemaElement) throws ExecutionSetupException {
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    castedBaseVector = (BaseDataValueVector) v;
    castedVectorMutator = (NullableVectorDefinitionSetter) v.getMutator();
    totalDefinitionLevelsRead = 0;
  }


  @Override
  public void processPages(long recordsToReadInThisPass) throws IOException {
    readStartInBytes = 0;
    readLength = 0;
    readLengthInBits = 0;
    recordsReadInThisIteration = 0;
    vectorData = castedBaseVector.getBuffer();

    // values need to be spaced out where nulls appear in the column
    // leaving blank space for nulls allows for random access to values
    // to optimize copying data out of the buffered disk stream, runs of defined values
    // are located and copied together, rather than copying individual values

    long runStart = pageReader.readPosInBytes;
    int runLength = -1;     // number of non-null records in this pass.
    int nullRunLength = -1; // number of consecutive null records that we read.
    int currentDefinitionLevel = -1;
    int readCount = 0; // the record number we last read.
    int writeCount = 0; // the record number we last wrote to the value vector.

    boolean haveMoreData;


    while (readCount < recordsToReadInThisPass && writeCount < valueVec.getValueCapacity()) {
      // read a page if needed
      if (!pageReader.hasPage()
              || ((readStartInBytes + readLength >= pageReader.byteLength && bitsUsed == 0) &&
              definitionLevelsRead >= pageReader.currentPageCount)) {
        if (!pageReader.next()) {
          break;
        }
        definitionLevelsRead = 0;
      }

      nullsFound=nullRunLength = 0;
      runLength = 0;

      //
      // Let's skip the next run of nulls if any ...
      //
      // If we are reentering this loop, the currentDefinitionLevel has already been read
      if (currentDefinitionLevel < 0) {
        currentDefinitionLevel = pageReader.definitionLevels.readInteger();
      }
      haveMoreData=readCount < recordsToReadInThisPass
              && writeCount + nullRunLength < valueVec.getValueCapacity()
              && definitionLevelsRead <= pageReader.currentPageCount;
      while (haveMoreData && currentDefinitionLevel < columnDescriptor.getMaxDefinitionLevel()
              ) {
        readCount++;
        definitionLevelsRead++;
        totalDefinitionLevelsRead++;
        nullsFound++;
        nullRunLength++;
        haveMoreData=readCount < recordsToReadInThisPass
                && writeCount + nullRunLength < valueVec.getValueCapacity()
                && definitionLevelsRead <= pageReader.currentPageCount;
        if(haveMoreData) {
          currentDefinitionLevel = pageReader.definitionLevels.readInteger();
        }
      }
      //
      // Write the nulls if any
      //
      if(nullRunLength>0) {
        int writerIndex = ((BaseDataValueVector) valueVec).getBuffer().writerIndex();
        if (dataTypeLengthInBits > 8 || (dataTypeLengthInBits < 8 && totalValuesRead + runLength % 8 == 0)) {
          castedBaseVector.getBuffer().setIndex(0, writerIndex + (int) Math.ceil(nullsFound * dataTypeLengthInBits / 8.0));
        } else if (dataTypeLengthInBits < 8) {
          rightBitShift += dataTypeLengthInBits * nullsFound;
        }
        writeCount += nullsFound; // TODO: nullsFound seems to be the same as nullRunLength - consolidate
        valuesReadInCurrentPass += nullsFound;
        recordsReadInThisIteration += nullsFound;
      }

      //
      // Handle the run of non-null values
      //
      //while (currentDefinitionLevel >= columnDescriptor.getMaxDefinitionLevel()
      //        && readCount < recordsToReadInThisPass
      //        && writeCount + runLength < valueVec.getValueCapacity()
      //        && definitionLevelsRead <= pageReader.currentPageCount
      haveMoreData=readCount < recordsToReadInThisPass
              && writeCount + runLength < valueVec.getValueCapacity() // note: writeCount+runLength
              && definitionLevelsRead <= pageReader.currentPageCount;
      while (haveMoreData && currentDefinitionLevel >= columnDescriptor.getMaxDefinitionLevel()
              ) {
        readCount++;
        definitionLevelsRead++;
        totalDefinitionLevelsRead++;
        runLength++;
        castedVectorMutator.setIndexDefined(writeCount+runLength-1); //set the nullable bit to indicate a non-null value
        haveMoreData=readCount < recordsToReadInThisPass
                && writeCount + runLength < valueVec.getValueCapacity()
                && definitionLevelsRead <= pageReader.currentPageCount;
        if(haveMoreData) {
          currentDefinitionLevel = pageReader.definitionLevels.readInteger();
        }
        //if( readCount < recordsToReadInThisPass
        //        && writeCount + runLength < valueVec.getValueCapacity()
        //        && definitionLevelsRead <= pageReader.currentPageCount) {
        //  currentDefinitionLevel = pageReader.definitionLevels.readInteger();
        //}
      }

      //
      // Write the non-null values
      //
      if(runLength>0) {
        // set up metadata
        //this.readStartInBytes = pageReader.readPosInBytes;
        //this.readLengthInBits = recordsReadInThisIteration * dataTypeLengthInBits;
        //this.readLength = (int) Math.ceil(readLengthInBits / 8.0);

        // This _must_ be set so that the call to readField works correctly for all datatypes
        this.recordsReadInThisIteration += runLength;

        this.readStartInBytes = pageReader.readPosInBytes;
        this.readLengthInBits = runLength * dataTypeLengthInBits;
        this.readLength = (int) Math.ceil(readLengthInBits / 8.0);
        readField(runLength);


        writeCount += runLength;
        valuesReadInCurrentPass += runLength;



      }

      totalValuesRead += recordsReadInThisIteration;
      pageReader.valuesRead += recordsReadInThisIteration;
      pageReader.readPosInBytes = readStartInBytes + readLength;

    }
    valuesReadInCurrentPass = writeCount; // TODO: validate this
    valueVec.getMutator().setValueCount(valuesReadInCurrentPass);
  }

  @Override
  protected abstract void readField(long recordsToRead);
}
