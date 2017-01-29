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
package org.apache.drill.exec.store.mapr.db.json;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.mapr.db.MapRDB;
import com.mapr.db.impl.IdCodec;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.join.HashJoinBatch;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.store.mapr.db.MapRDBFormatPluginConfig;
import org.apache.drill.exec.store.mapr.db.MapRDBSubScanSpec;
import org.apache.drill.exec.store.mapr.db.RestrictedMapRDBSubScanSpec;
import org.apache.drill.exec.vector.BaseValueVector;

import com.google.common.base.Stopwatch;
import com.mapr.db.ojai.DBDocumentReaderBase;
import com.mapr.db.Table;
import org.apache.drill.exec.vector.complex.impl.MapOrListWriterImpl;
import org.ojai.DocumentReader;



public class RestrictedJsonRecordReader extends MaprDBJsonRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RestrictedJsonRecordReader.class);

  public RestrictedJsonRecordReader(MapRDBSubScanSpec subScanSpec,
      MapRDBFormatPluginConfig formatPluginConfig,
      List<SchemaPath> projectedColumns, FragmentContext context) {
    super(subScanSpec, formatPluginConfig, projectedColumns, context);
   }

  public void readToInitSchema() {
    DBDocumentReaderBase reader = null;
    vectorWriter.setPosition(0);
    try {
      reader = (DBDocumentReaderBase) table.find().iterator().next().asReader();
      MapOrListWriterImpl writer = new MapOrListWriterImpl(vectorWriter.rootAsMap());
      if (getIdOnly()) {
        writeId(writer, reader.getId());
      } else {
        if (reader.next() != DocumentReader.EventType.START_MAP) {
          throw dataReadError("The document did not start with START_MAP!");
        }
      }
      writeToListOrMap(writer, reader);
    }
    catch(UserException e) {
      throw UserException.unsupportedError(e)
          .addContext(String.format("Table: %s, document id: '%s'",
              getTable().getPath(),
              reader == null ? null : IdCodec.asString(reader.getId())))
          .build(logger);
    } catch (SchemaChangeException e) {
      if (getIgnoreSchemaChange()) {
        logger.warn("{}. Dropping the row from result.", e.getMessage());
        logger.debug("Stack trace:", e);
      } else {
        throw dataReadError(e);
      }
    }
    finally {
      vectorWriter.setPosition(0);
    }
  }

  @Override
  public int next() {
    Stopwatch watch = Stopwatch.createUnstarted();
    watch.start();
    RestrictedMapRDBSubScanSpec rss = ((RestrictedMapRDBSubScanSpec) this.subScanSpec);

    vectorWriter.allocate();
    vectorWriter.reset();

    int recordCount = 0;

    DBDocumentReaderBase reader = null;

    if(!rss.readyToGetRowKey()) {
      //not ready to get rowkey, so we just load a record to initialize schema
      readToInitSchema();
      return 0;
    }
    Stopwatch timer1 = Stopwatch.createUnstarted();
    Stopwatch timer2 = Stopwatch.createUnstarted();
    while (recordCount < BaseValueVector.INITIAL_VALUE_ALLOCATION && rss.hasRowKey()) {
      while (rss.hasRowKey()) {
        try {
          timer1.start();
          String strRowkey = rss.nextRowKey();
          timer1.stop();
          timer2.start();
          vectorWriter.setPosition(recordCount);
          reader = (DBDocumentReaderBase) table.findById(strRowkey).asReader();
          MapOrListWriterImpl writer = new MapOrListWriterImpl(vectorWriter.rootAsMap());
          if (getIdOnly()) {
            writeId(writer, reader.getId());
          } else {
            if (reader.next() != DocumentReader.EventType.START_MAP) {
              throw dataReadError("The document did not start with START_MAP!");
            }
          }
          writeToListOrMap(writer, reader);
          recordCount++;
          timer2.stop();
        } catch (UserException e) {
          throw UserException.unsupportedError(e)
              .addContext(String.format("Table: %s, document id: '%s'",
                  getTable().getPath(),
                  reader == null ? null : IdCodec.asString(reader.getId())))
              .build(logger);
        } catch (SchemaChangeException e) {
          if (getIgnoreSchemaChange()) {
            logger.warn("{}. Dropping the row from result.", e.getMessage());
            logger.debug("Stack trace:", e);
          } else {
            throw dataReadError(e);
          }
        }
      }
    }
    vectorWriter.setValueCount(recordCount);
    logger.debug("Took {} ms to get {} records, getrowkey {}, getDoc {}",
        watch.elapsed(TimeUnit.MILLISECONDS), recordCount,
        timer1.elapsed(TimeUnit.MILLISECONDS), timer2.elapsed(TimeUnit.MILLISECONDS));
    return recordCount;
  }

  @Override
  public boolean hasNext() {
    //TODO: need to consider and test the case when there are multiple batches of rowkeys
    RestrictedMapRDBSubScanSpec rss = ((RestrictedMapRDBSubScanSpec) this.subScanSpec);

    HashJoinBatch hjBatch = rss.getJoinForSubScan();
    if (hjBatch == null) {
      return false;
    }
    AbstractRecordBatch.BatchState state = rss.getJoinForSubScan().getState();
    if ( state == AbstractRecordBatch.BatchState.BUILD_SCHEMA ) {
      return true;
    }
     if( state == AbstractRecordBatch.BatchState.FIRST) {
       rss.getJoinForSubScan().setState(AbstractRecordBatch.BatchState.NOT_FIRST);
      return true;
    }
    return false;
  }
}
