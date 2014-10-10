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
package org.apache.drill.exec.record;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.Test;

import java.util.List;

public class TestRecordBatchLoaderFactory extends BaseTestQuery {
  @Test
  public void test() throws Exception {
    String plan = Files.toString(FileUtils.getResourceAsFile("/multi_batch_input.json"), Charsets.UTF_8);
    RecordBatchLoaderFactory factory = new RecordBatchLoaderFactory(allocator);

    List<QueryResultBatch> results = testPhysicalWithResults(plan);

    for (QueryResultBatch result : results) {
      RecordBatchLoader loader = factory.load(result.getHeader().getDef(), result.getData());
      List<ValueVector> vvList = Lists.newArrayList();
      VectorContainer container = loader.getContainer();
      for(VectorWrapper vw : container) {
        vvList.add(vw.getValueVector());
      }

      for(ValueVector vv : vvList) {
        container.remove(vv);
      }
      vvList.clear();
    }

    factory.close();
  }
}
