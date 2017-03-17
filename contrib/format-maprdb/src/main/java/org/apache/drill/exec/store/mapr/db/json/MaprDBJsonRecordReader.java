/*
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

import static org.apache.drill.exec.store.mapr.PluginConstants.DOCUMENT_SCHEMA_PATH;
import static org.apache.drill.exec.store.mapr.PluginErrorHandler.dataReadError;
import static org.ojai.DocumentConstants.ID_FIELD;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.mapr.db.MapRDBFormatPlugin;
import org.apache.drill.exec.store.mapr.db.MapRDBSubScanSpec;
import org.apache.drill.exec.util.EncodedSchemaPathSet;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.hadoop.fs.Path;
import org.ojai.DocumentReader;
import org.ojai.DocumentStream;
import org.ojai.FieldPath;
import org.ojai.FieldSegment;
import org.ojai.store.QueryCondition;
import org.ojai.util.FieldProjector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import com.mapr.db.Table.TableOption;
import com.mapr.db.exceptions.DBException;
import com.mapr.db.impl.IdCodec;
import com.mapr.db.ojai.DBDocumentReaderBase;
import com.mapr.db.util.ByteBufs;

import io.netty.buffer.DrillBuf;

public class MaprDBJsonRecordReader extends AbstractRecordReader {
  private static final Logger logger = LoggerFactory.getLogger(MaprDBJsonRecordReader.class);

  protected static final FieldPath[] ID_ONLY_PROJECTION = { ID_FIELD };

  protected Table table;
  private QueryCondition condition;

  /**
   * A set of projected FieldPaths that are pushed into MapR-DB Scanner
   */
  private FieldPath[] scannedFields;

  private final Path tablePath;
  private final String indexFid;
  private OperatorContext operatorContext;
  protected VectorContainerWriter vectorWriter;

  private DrillBuf buffer;

  private DocumentStream documentStream;

  private Iterator<DocumentReader> documentReaderIterators;

  private boolean includeId;
  private boolean idOnly;

  private boolean projectWholeDocument;
  private FieldProjector projector;

  private final boolean unionEnabled;
  private final boolean readNumbersAsDouble;
  private boolean disablePushdown;
  private final boolean allTextMode;
  private final boolean ignoreSchemaChange;
  private final boolean disableCountOptimization;

  protected final MapRDBSubScanSpec subScanSpec;
  protected final MapRDBFormatPlugin formatPlugin;
  
  protected OjaiValueWriter valueWriter;
  protected DocumentReaderVectorWriter documentWriter;

  public MaprDBJsonRecordReader(MapRDBSubScanSpec subScanSpec, MapRDBFormatPlugin formatPlugin,
                                List<SchemaPath> projectedColumns, FragmentContext context) {
    buffer = context.getManagedBuffer();
    scannedFields = null;
    tablePath = new Path(Preconditions.checkNotNull(subScanSpec, "MapRDB reader needs a sub-scan spec").getTableName());
    this.subScanSpec = subScanSpec;
    this.formatPlugin = formatPlugin;
    indexFid = subScanSpec.getIndexFid();
    documentReaderIterators = null;
    projectWholeDocument = false;
    includeId = false;
    idOnly    = false;
    byte[] serializedFilter = subScanSpec.getSerializedFilter();
    condition = null;

    if (serializedFilter != null) {
      condition = com.mapr.db.impl.ConditionImpl.parseFrom(ByteBufs.wrap(serializedFilter));
    }

    disableCountOptimization = formatPlugin.getConfig().shouldDisableCountOptimization();
    setColumns(projectedColumns);
    unionEnabled = context.getOptions().getOption(ExecConstants.ENABLE_UNION_TYPE);
    readNumbersAsDouble = formatPlugin.getConfig().isReadAllNumbersAsDouble();
    allTextMode = formatPlugin.getConfig().isAllTextMode();
    ignoreSchemaChange = formatPlugin.getConfig().isIgnoreSchemaChange();
    disablePushdown = !formatPlugin.getConfig().isEnablePushdown();
  }

  @Override
  protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> columns) {
    Set<SchemaPath> transformed = Sets.newLinkedHashSet();
    Set<SchemaPath> encodedSchemaPathSet = Sets.newLinkedHashSet();

    if (disablePushdown) {
      transformed.add(AbstractRecordReader.STAR_COLUMN);
      includeId = true;
    } else {
      if (isStarQuery()) {
        transformed.add(AbstractRecordReader.STAR_COLUMN);
        includeId = true;
        if (isSkipQuery() && !disableCountOptimization) {
          // `SELECT COUNT(*)` query
          idOnly = true;
          scannedFields = ID_ONLY_PROJECTION;
        }
      } else {
        Set<FieldPath> scannedFieldsSet = Sets.newTreeSet();
        Set<FieldPath> projectedFieldsSet = null;

        for (SchemaPath column : columns) {
          if (EncodedSchemaPathSet.isEncodedSchemaPath(column)) {
            encodedSchemaPathSet.add(column);
          } else {
            transformed.add(column);
            if (!DOCUMENT_SCHEMA_PATH.equals(column)) {
              FieldPath fp = getFieldPathForProjection(column);
              scannedFieldsSet.add(fp);
            } else {
              projectWholeDocument = true;
            }
          }
        }
        if (projectWholeDocument) {
          // we do not want to project the fields from the encoded field path list
          // hence make a copy of the scannedFieldsSet here for projection.
          projectedFieldsSet = new ImmutableSet.Builder<FieldPath>()
              .addAll(scannedFieldsSet).build();
        }

        if (encodedSchemaPathSet.size() > 0) {
          Collection<SchemaPath> decodedSchemaPaths = EncodedSchemaPathSet.decode(encodedSchemaPathSet);
          // now we look at the fields which are part of encoded field set and either 
          // add them to scanned set or clear the scanned set if all fields were requested.
          for (SchemaPath column : decodedSchemaPaths) {
            if (column.equals(AbstractRecordReader.STAR_COLUMN)) {
              includeId = true;
              scannedFieldsSet.clear();
              break;
            }
            scannedFieldsSet.add(getFieldPathForProjection(column));
          }
        }

        if (scannedFieldsSet.size() > 0) {
          if (includesIdField(scannedFieldsSet)) {
            includeId = true;
          }
          scannedFields = scannedFieldsSet.toArray(new FieldPath[scannedFieldsSet.size()]);
        }

        if (disableCountOptimization) {
          idOnly = (scannedFields == null);
        }

        if(projectWholeDocument) {
          projector = new FieldProjector(projectedFieldsSet);
        }

      }
    }
    return transformed;
  }

  protected boolean getIdOnly() {
    return idOnly;
  }

  protected Table getTable() {
    return table;
  }

  protected boolean getIgnoreSchemaChange() {
    return ignoreSchemaChange;
  }

  @Override
  @SuppressWarnings("deprecation")
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    this.vectorWriter = new VectorContainerWriter(output, unionEnabled);
    this.operatorContext = context;

    try {
      table = indexFid == null ? formatPlugin.getJsonTableCache().getTable(tablePath.toString()) : MapRDB.getIndexTable(tablePath, indexFid, "");
      table.setOption(TableOption.EXCLUDEID, !includeId);
      documentStream = table.find(condition, scannedFields);
      documentReaderIterators = documentStream.documentReaders().iterator();
      
      if (allTextMode) {
        valueWriter = new AllTextValueWriter(buffer);
      } else if (readNumbersAsDouble) {
        valueWriter = new NumbersAsDoubleValueWriter(buffer);
      } else {
        valueWriter = new OjaiValueWriter(buffer);
      }

      if (projectWholeDocument) {
        documentWriter = new ProjectionPassthroughVectorWriter(valueWriter, projector, includeId);
      } else if (isSkipQuery()) {
        documentWriter = new RowCountVectorWriter(valueWriter);
      } else if (idOnly) {
        documentWriter = new IdOnlyVectorWriter(valueWriter);
      } else {
        documentWriter = new FieldTransferVectorWriter(valueWriter);
      }
    } catch (DBException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public int next() {
    Stopwatch watch = Stopwatch.createUnstarted();
    watch.start();

    vectorWriter.allocate();
    vectorWriter.reset();

    int recordCount = 0;
    DBDocumentReaderBase reader = null;

    while(recordCount < BaseValueVector.INITIAL_VALUE_ALLOCATION) {
      vectorWriter.setPosition(recordCount);
      try {
        reader = nextDocumentReader();
        if (reader == null) {
          break; // no more documents for this reader
        } else {
          documentWriter.writeDBDocument(vectorWriter, reader);
        }
        recordCount++;
      } catch (UserException e) {
        throw UserException.unsupportedError(e)
            .addContext(String.format("Table: %s, document id: '%s'",
                table.getPath(),
                reader == null ? null : IdCodec.asString(reader.getId())))
            .build(logger);
      } catch (SchemaChangeException e) {
        String err_row = reader.getId().asJsonString();
        if (ignoreSchemaChange) {
          logger.warn("{}. Dropping row '{}' from result.", e.getMessage(), err_row);
          logger.debug("Stack trace:", e);
        } else {
          throw dataReadError(logger, e, "SchemaChangeException for row '{}'.", err_row);
        }
      }
    }

    vectorWriter.setValueCount(recordCount);
    logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), recordCount);
    return recordCount;
  }

  protected DBDocumentReaderBase nextDocumentReader() {
    final OperatorStats operatorStats = operatorContext == null ? null : operatorContext.getStats();
    try {
      if (operatorStats != null) {
        operatorStats.startWait();
      }
      try {
        if (!documentReaderIterators.hasNext()) {
          return null;
        } else {
          return (DBDocumentReaderBase) documentReaderIterators.next();
        }
      } finally {
        if (operatorStats != null) {
          operatorStats.stopWait();
        }
      }
    } catch (DBException e) {
      throw dataReadError(logger, e);
    }
  }

  /*
   * Extracts contiguous named segments from the SchemaPath, starting from the
   * root segment and build the FieldPath from it for projection.
   *
   * This is due to bug 22726 and 22727, which cause DB's DocumentReaders to
   * behave incorrectly for sparse lists, hence we avoid projecting beyond the
   * first encountered ARRAY field and let Drill handle the projection.
   */
  private static FieldPath getFieldPathForProjection(SchemaPath column) {
    Stack<PathSegment.NameSegment> pathSegments = new Stack<PathSegment.NameSegment>();
    PathSegment seg = column.getRootSegment();
    while (seg != null && seg.isNamed()) {
      pathSegments.push((PathSegment.NameSegment) seg);
      seg = seg.getChild();
    }
    FieldSegment.NameSegment child = null;
    while (!pathSegments.isEmpty()) {
      child = new FieldSegment.NameSegment(pathSegments.pop().getPath(), child, false);
    }
    return new FieldPath(child);
  }

  public static boolean includesIdField(Collection<FieldPath> projected) {
    return Iterables.tryFind(projected, new Predicate<FieldPath>() {
      @Override
      public boolean apply(FieldPath path) {
        return Preconditions.checkNotNull(path).equals(ID_FIELD);
      }
    }).isPresent();
  }

  @Override
  public void close() {
    if (documentStream != null) {
      documentStream.close();
    }
    formatPlugin.getJsonTableCache().closeTable(table);
  }

}
