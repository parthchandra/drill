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

import static org.apache.drill.exec.store.mapr.PluginErrorHandler.dataReadError;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.mapr.db.impl.BaseJsonTable;
import com.mapr.db.impl.MultiGet;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.join.RowKeyJoin;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.store.mapr.db.MapRDBFormatPlugin;
import org.apache.drill.exec.store.mapr.db.MapRDBSubScanSpec;
import org.apache.drill.exec.store.mapr.db.RestrictedMapRDBSubScanSpec;
import org.apache.drill.exec.vector.BaseValueVector;

import com.google.common.base.Stopwatch;
import com.mapr.db.Table;
import com.mapr.db.impl.IdCodec;
import com.mapr.db.ojai.DBDocumentReaderBase;

import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.FieldPath;


public class RestrictedJsonRecordReader extends MaprDBJsonRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RestrictedJsonRecordReader.class);

  private int batchSize; // batchSize for rowKey based document get

  private String [] projections = null; // multiGet projections
  public RestrictedJsonRecordReader(MapRDBSubScanSpec subScanSpec,
                                    MapRDBFormatPlugin formatPlugin,
                                    List<SchemaPath> projectedColumns, FragmentContext context) {

    super(subScanSpec, formatPlugin, projectedColumns, context);
    batchSize = (int)context.getOptions().getOption(ExecConstants.QUERY_ROWKEYJOIN_BATCHSIZE);
    int idx = 0;
    FieldPath[] scannedFields = this.getScannedFields();

    // only populate projections for non-star query (for star, null is interpreted as all fields)
    if (!this.isStarQuery() && scannedFields != null && scannedFields.length > 0) {
      projections = new String[scannedFields.length];
      for (FieldPath path : scannedFields) {
        projections[idx] = path.asPathString();
        ++idx;
      }
    }
  }

  public void readToInitSchema() {
    DBDocumentReaderBase reader = null;
    vectorWriter.setPosition(0);

    try (DocumentStream dstream = table.find()) {
      reader = (DBDocumentReaderBase) dstream.iterator().next().asReader();
      documentWriter.writeDBDocument(vectorWriter, reader);
    } catch(UserException e) {
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
        throw dataReadError(logger, e);
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

    if (!rss.readyToGetRowKey()) {
      // not ready to get rowkey, so we just load a record to initialize schema; only do this
      // when we are in the build schema phase
      if (rss.isBuildSchemaPhase()) {
        readToInitSchema();
      }
      return 0;
    }

    Table table = super.formatPlugin.getJsonTableCache().getTable(subScanSpec.getTableName(), subScanSpec.getUserName());
    final MultiGet multiGet = new MultiGet((BaseJsonTable) table, condition, false, projections);
    int recordCount = 0;
    DBDocumentReaderBase reader = null;

    Stopwatch timer = Stopwatch.createUnstarted();

    while (recordCount < BaseValueVector.INITIAL_VALUE_ALLOCATION) {
      ByteBuffer rowKeyIds[] = rss.getRowKeyIdsToRead(batchSize);
      if (rowKeyIds == null) {
        break;
      }
      try {
        timer.start();
        final List<Document> docList = multiGet.doGet(rowKeyIds);
        int index = 0;
        while (index < docList.size()) {
          vectorWriter.setPosition(recordCount);
          reader = (DBDocumentReaderBase) docList.get(index).asReader();
          documentWriter.writeDBDocument(vectorWriter, reader);
          recordCount++;
          index++;
        }
        timer.stop();
      } catch (UserException e) {
        throw UserException.unsupportedError(e).addContext(String.format("Table: %s, document id: '%s'",
          getTable().getPath(), reader == null ? null : IdCodec.asString(reader.getId()))).build(logger);
      } catch (SchemaChangeException e) {
        if (getIgnoreSchemaChange()) {
          logger.warn("{}. Dropping the row from result.", e.getMessage());
          logger.debug("Stack trace:", e);
        } else {
          throw dataReadError(logger, e);
        }
      }
    }

    vectorWriter.setValueCount(recordCount);
    logger.debug("Took {} ms to get {} records, getrowkey {}", watch.elapsed(TimeUnit.MILLISECONDS), recordCount, timer.elapsed(TimeUnit.MILLISECONDS));
    return recordCount;
  }

  @Override
  public boolean hasNext() {
    RestrictedMapRDBSubScanSpec rss = ((RestrictedMapRDBSubScanSpec) this.subScanSpec);

    RowKeyJoin rjBatch = rss.getJoinForSubScan();
    if (rjBatch == null) {
      return false;
    }

    boolean hasMore = false;
    AbstractRecordBatch.BatchState state = rss.getJoinForSubScan().getBatchState();
    RowKeyJoin.RowKeyJoinState rkState = rss.getJoinForSubScan().getRowKeyJoinState();
    if ( state == AbstractRecordBatch.BatchState.BUILD_SCHEMA ) {
      hasMore = true;
    } else if ( state == AbstractRecordBatch.BatchState.FIRST) {
       rss.getJoinForSubScan().setBatchState(AbstractRecordBatch.BatchState.NOT_FIRST);
       rss.getJoinForSubScan().setRowKeyJoinState(RowKeyJoin.RowKeyJoinState.PROCESSING);
       hasMore = true;
    } else if ( rkState == RowKeyJoin.RowKeyJoinState.INITIAL) {
      rss.getJoinForSubScan().setRowKeyJoinState(RowKeyJoin.RowKeyJoinState.PROCESSING);
      hasMore = true;
    }

    logger.debug("restricted reader hasMore = {}", hasMore);

    return hasMore;
  }

}
