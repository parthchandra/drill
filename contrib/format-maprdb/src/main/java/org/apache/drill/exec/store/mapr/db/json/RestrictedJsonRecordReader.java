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

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.store.mapr.db.MapRDBFormatPluginConfig;
import org.apache.drill.exec.store.mapr.db.MapRDBSubScanSpec;
import org.apache.drill.exec.vector.BaseValueVector;

import com.google.common.base.Stopwatch;
import com.mapr.db.ojai.DBDocumentReaderBase;

public class RestrictedJsonRecordReader extends MaprDBJsonRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RestrictedJsonRecordReader.class);

  public RestrictedJsonRecordReader(MapRDBSubScanSpec subScanSpec,
      MapRDBFormatPluginConfig formatPluginConfig,
      List<SchemaPath> projectedColumns, FragmentContext context) {
    super(subScanSpec, formatPluginConfig, projectedColumns, context);
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
      // TODO: new reader logic that iterates over the row keys from the build side of hash join for
      // this subscan (for this, we need to propagate the reference to the HashJoinBatch that is maintained
      // by RestrictedMapRDBSubScan
      
    }

    vectorWriter.setValueCount(recordCount);
    logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), recordCount);
    return recordCount;
  }

}
