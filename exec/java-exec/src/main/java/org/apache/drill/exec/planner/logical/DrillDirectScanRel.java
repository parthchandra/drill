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
package org.apache.drill.exec.planner.logical;

import com.google.common.collect.Iterators;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.planner.physical.DrillScanPrel;
import org.apache.drill.exec.planner.physical.PhysicalPlanCreator;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.visitor.PrelVisitor;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.direct.DirectGroupScan;

import java.io.IOException;
import java.util.Iterator;

/**
 * Logical and physical RelNode representing a {@link DirectGroupScan}. This is not backed by a {@link DrillTable},
 * unlike {@link DrillScanRel}.
 */
public class DrillDirectScanRel extends AbstractRelNode implements DrillScanPrel, DrillRel {

  private final DirectGroupScan groupScan;
  private final RelDataType rowType;

  public DrillDirectScanRel(RelOptCluster cluster, RelTraitSet traitSet, DirectGroupScan directGroupScan,
                            RelDataType rowType) {
    super(cluster, traitSet);
    this.groupScan = directGroupScan;
    this.rowType = rowType;
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    return creator.addMetadata(this, groupScan);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitPrel(this, value);
  }

  @Override
  public BatchSchema.SelectionVectorMode[] getSupportedEncodings() {
    return BatchSchema.SelectionVectorMode.DEFAULT;
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    return BatchSchema.SelectionVectorMode.NONE;
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return false;
  }

  @Override
  public Iterator<Prel> iterator() {
    return Iterators.emptyIterator();
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    return null;
  }

  @Override
  public DirectGroupScan getGroupScan() {
    return groupScan;
  }

  @Override
  public RelDataType deriveRowType() {
    return this.rowType;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("DirectGroupScan", groupScan.getDigest());
  }

  @Override
  public double getRows() {
    final PlannerSettings settings = PrelUtil.getPlannerSettings(getCluster());
    return groupScan.getScanStats(settings).getRecordCount();
  }
}
