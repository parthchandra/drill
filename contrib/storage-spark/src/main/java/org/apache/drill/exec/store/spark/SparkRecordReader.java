/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.spark;


import java.util.List;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.RawFragmentBatch;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.spark.SparkSubScan.SparkSubScanSpec;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.work.DataPushConnectionManager;
import org.apache.drill.exec.work.batch.UnlimitedRawBatchBufferNoAck;

import com.google.common.collect.Lists;

/**
 * SparkrecordReader is the class that is responsible for reading
 * data that is received from Spark via DrillClient protocol
 * not read from disk or some metastore
 *
 */
public class SparkRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SparkRecordReader.class);

  private SparkSubScanSpec subScanSpec;
  private FragmentContext fragmentContext;
  private OperatorContext operatorContext;
  private UnlimitedRawBatchBufferNoAck batchProvider;
  private RecordBatchLoader recordBatchLoader;

  private OutputMutator outputMutator;

  


  public SparkRecordReader(SparkSubScanSpec subScanSpec, FragmentContext fragmentContext) {
    this.subScanSpec = subScanSpec;
    this.fragmentContext = fragmentContext;
    this.batchProvider = DataPushConnectionManager.getInstance().
    		getRawBatchBuffer(fragmentContext.getHandle()); 
    this.batchProvider.setFragmentCount(subScanSpec.getAssignedPartitions().length);
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.outputMutator = output;
  }

  @Override
  public void setOperatorContext(OperatorContext operatorContext) {
    this.operatorContext = operatorContext;
    this.recordBatchLoader = new RecordBatchLoader(this.operatorContext.getAllocator());
  }

  @Override
  public int next() {
	RawFragmentBatch rawFragmentBatch = batchProvider.getNext();
	if ( rawFragmentBatch == null ) {
	  return 0;
	}
	
	try {
	  recordBatchLoader.load(rawFragmentBatch.getHeader().getDef(), rawFragmentBatch.getBody());
	  if (rawFragmentBatch.getBody() != null) rawFragmentBatch.getBody().release();
	} catch (SchemaChangeException e) {
	  logger.error("SchemaChangeException", e);
	  throw new DrillRuntimeException(e);
	}
  List<ValueVector> vvList = Lists.newArrayList();
	for (VectorWrapper<?> vectorWrapper : recordBatchLoader) {
	  vvList.add(vectorWrapper.getValueVector());
  }

    this.outputMutator.addFields(vvList);
    return recordBatchLoader.getRecordCount();
  }

  @Override
  public void cleanup() {
    this.batchProvider.cleanup();
    DataPushConnectionManager.getInstance().cleanRawBatchBuffer(fragmentContext.getHandle());
    if ( this.recordBatchLoader != null ) {
    	this.recordBatchLoader.clear();
    }
  }
  
}
