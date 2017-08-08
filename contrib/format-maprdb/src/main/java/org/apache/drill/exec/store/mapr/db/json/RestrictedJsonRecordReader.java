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


public class RestrictedJsonRecordReader extends MaprDBJsonRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RestrictedJsonRecordReader.class);

  private int batchSize; // batchSize for rowKey based document get

  public RestrictedJsonRecordReader(MapRDBSubScanSpec subScanSpec,
                                    MapRDBFormatPlugin formatPlugin,
                                    List<SchemaPath> projectedColumns, FragmentContext context) {

    super(subScanSpec, formatPlugin, projectedColumns, context);
    batchSize = (int)context.getOptions().getOption(ExecConstants.QUERY_ROWKEYJOIN_BATCHSIZE);
  }

  public void readToInitSchema() {
    DBDocumentReaderBase reader = null;
    vectorWriter.setPosition(0);
    try {
      reader = (DBDocumentReaderBase) table.find().iterator().next().asReader();
      documentWriter.writeDBDocument(vectorWriter, reader);
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
      //not ready to get rowkey, so we just load a record to initialize schema
      readToInitSchema();
      return 0;
    }

    Table table = super.formatPlugin.getJsonTableCache().getTable(subScanSpec.getTableName(), subScanSpec.getUserName());
    int idx = 0;
    String [] projections = new String[this.getColumns().size()];
    for (SchemaPath path : this.getColumns()) {
      projections[idx] = FieldPathHelper.schemaPath2FieldPath(path).asPathString();
      ++idx;
    }
    final MultiGet multiGet = new MultiGet((BaseJsonTable) table, null, false, projections);
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
    //TODO: need to consider and test the case when there are multiple batches of rowkeys
    RestrictedMapRDBSubScanSpec rss = ((RestrictedMapRDBSubScanSpec) this.subScanSpec);

    RowKeyJoin rjBatch = rss.getJoinForSubScan();
    if (rjBatch == null) {
      return false;
    }

    boolean hasMore = false;
    AbstractRecordBatch.BatchState state = rss.getJoinForSubScan().getBatchState();
    if ( state == AbstractRecordBatch.BatchState.BUILD_SCHEMA ) {
      hasMore = true;
    } else if ( state == AbstractRecordBatch.BatchState.FIRST) {
       rss.getJoinForSubScan().setBatchState(AbstractRecordBatch.BatchState.NOT_FIRST);
       hasMore = true;
    }

    logger.debug("restricted reader hasMore = {}", hasMore);

    return hasMore;
  }
}
