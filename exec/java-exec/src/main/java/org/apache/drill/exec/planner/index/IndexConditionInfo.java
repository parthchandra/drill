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
package org.apache.drill.exec.planner.index;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.BitSets;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.partition.FindPartitionConditions;
import org.apache.drill.exec.planner.logical.partition.RewriteCombineBinaryOperators;
import org.apache.drill.exec.planner.physical.ScanPrel;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IndexConditionInfo {
  public final RexNode indexCondition;
  public final RexNode remainderCondition;
  public final boolean hasIndexCol;

  public IndexConditionInfo(RexNode indexCondition, RexNode remainderCondition, boolean hasIndexCol) {
    this.indexCondition = indexCondition;
    this.remainderCondition = remainderCondition;
    this.hasIndexCol = hasIndexCol;
  }

  public static Comparator<IndexDescriptor> indexFieldsDescComparator = new Comparator<IndexDescriptor>() {
    @Override
    public int compare(IndexDescriptor o1, IndexDescriptor o2) {
      return - (o1.getIndexColumns().size() - o2.getIndexColumns().size());
    }
  };

  public static Builder newBuilder(RexNode condition,
                                   Iterable<IndexDescriptor> indexes,
                                   RexBuilder builder,
                                   DrillScanRel scan) {
    return new Builder(condition, indexes, builder, scan);
  }

  public static class Builder {
    final RexBuilder builder;
    final DrillScanRel scan;
    final Iterable<IndexDescriptor> indexes;
    private RexNode condition;

    public Builder(RexNode condition,
                   Iterable<IndexDescriptor> indexes,
                   RexBuilder builder,
                   DrillScanRel scan
    ) {
      this.condition = condition;
      this.builder = builder;
      this.scan = scan;
      this.indexes = indexes;
    }

    public Builder(RexNode condition,
                   IndexDescriptor index,
                   RexBuilder builder,
                   DrillScanRel scan
    ) {
      this.condition = condition;
      this.builder = builder;
      this.scan = scan;
      this.indexes = Lists.newArrayList(index);
    }

    /**
     * Get a single IndexConditionInfo in which indexCondition has field  on all indexes in this.indexes
     * @return
     */
    public IndexConditionInfo getCollectiveInfo() {
      Set<LogicalExpression> paths = Sets.newLinkedHashSet();
      for ( IndexDescriptor index : indexes ) {
        paths.addAll(index.getIndexColumns());
        //paths.addAll(index.getNonIndexColumns());
      }
      return indexConditionRelatedToFields(Lists.newArrayList(paths), condition);
    }

    /**
     * Get a map of Index=>IndexConditionInfo, each IndexConditionInfo has the separated condition and remainder condition.
     * The map is ordered, so the last IndexDescriptor will have the final remainderCondition after separating conditions
     * that are relevant to this.indexes. The conditions are separated on LEADING index columns.
     * @return Map containing index{@link IndexDescriptor} and condition {@link IndexConditionInfo} pairs
     */
    public Map<IndexDescriptor, IndexConditionInfo> getLeadingKeyIndexConditionMap() {

      Map<IndexDescriptor, IndexConditionInfo> indexInfoMap = Maps.newLinkedHashMap();

      RexNode initCondition = condition;
      for(IndexDescriptor index : indexes) {
        List<LogicalExpression> leadingColumns = new ArrayList<>();
        if(initCondition.isAlwaysTrue()) {
          break;
        }
        //TODO: Ensure we dont get NULL pointer exceptions
        leadingColumns.add(index.getIndexColumns().get(0));
        IndexConditionInfo info = indexConditionRelatedToFields(leadingColumns, initCondition);
        if(info == null || info.hasIndexCol == false) {
          continue;
        }
        indexInfoMap.put(index, info);
        initCondition = info.remainderCondition;
      }
      return indexInfoMap;
    }

    public boolean isConditionPrefix(IndexDescriptor indexDesc, RexNode initCondition) {
      List<LogicalExpression> indexCols = indexDesc.getIndexColumns();
      boolean prefix = true;
      if (indexCols.size() > 0 && initCondition != null) {
        int i=0;
        while (prefix && i < indexCols.size()) {
          LogicalExpression p = indexCols.get(i++);
          List<LogicalExpression> prefixCol = ImmutableList.of(p);
          IndexConditionInfo info = indexConditionRelatedToFields(prefixCol, initCondition);
          if(info != null && info.hasIndexCol) {
            initCondition = info.remainderCondition;
            if (initCondition.isAlwaysTrue()) {
              // all filter conditions are accounted for, so if the remainder is TRUE, set it to NULL because
              // we don't need to keep track of it for rest of the index selection
              initCondition = null;
              break;
            }
          } else {
            prefix = false;
          }
        }
      }
      return prefix;
    }

    /**
     * Get a map of Index=>IndexConditionInfo, each IndexConditionInfo has the separated condition and remainder condition.
     * The map is ordered, so the last IndexDescriptor will have the final remainderCondition after separating conditions
     * that are relevant to this.indexes. The conditions are separated based on index columns.
     * @return Map containing index{@link IndexDescriptor} and condition {@link IndexConditionInfo} pairs
     */
    public Map<IndexDescriptor, IndexConditionInfo> getIndexConditionMap() {

      //sort indexes by indexed fields number in desc order
      List<IndexDescriptor> sortedIndex = Lists.newArrayList(indexes);
      Collections.sort(sortedIndex, indexFieldsDescComparator);

      return indexConditionMapFromSortedIndexes(sortedIndex);
    }

    public Map<IndexDescriptor, IndexConditionInfo> getIndexConditionMap(List<IndexDescriptor> indexList) {

      //sort indexes by indexed fields number in desc order
      List<IndexDescriptor> sortedIndex = Lists.newArrayList(indexList);
      Collections.sort(sortedIndex, indexFieldsDescComparator);

      return indexConditionMapFromSortedIndexes(sortedIndex);
    }

    public IndexConditionInfo getIndexConditionInfo(IndexDescriptor index) {
      if(!isConditionPrefix(index, condition)) {
        return null;
      }
      return indexConditionRelatedToFields(index.getIndexColumns(), condition);
    }

    private Map<IndexDescriptor, IndexConditionInfo> indexConditionMapFromSortedIndexes(List<IndexDescriptor> sortedIndex) {
      Map<IndexDescriptor, IndexConditionInfo> indexInfoMap = Maps.newLinkedHashMap();
      RexNode initCondition = condition;
      for(IndexDescriptor index : sortedIndex) {
        if(initCondition.isAlwaysTrue()) {
          break;
        }
        if(!isConditionPrefix(index, initCondition)) {
          continue;
        }

        IndexConditionInfo info = indexConditionRelatedToFields(index.getIndexColumns(), initCondition);
        if(info == null || info.hasIndexCol == false) {
          continue;
        }
        indexInfoMap.put(index, info);
        initCondition = info.remainderCondition;
      }
      return indexInfoMap;
    }
    /**
     * Given a list of Index Expressions(usually indexed fields/functions from one or a set of indexes),
     * separate a filter condition into
     *     1), relevant subset of conditions (by relevant, it means at least one given index Expression was found) and,
     *     2), the rest in remainderCondition
     * @param relevantPaths
     * @param condition
     * @return
     */
    public IndexConditionInfo indexConditionRelatedToFields(List<LogicalExpression> relevantPaths, RexNode condition) {
      // Use the same filter analyzer that is used for partitioning columns
      RewriteCombineBinaryOperators reverseVisitor =
          new RewriteCombineBinaryOperators(true, builder);

      condition = condition.accept(reverseVisitor);

      RexSeparator separator = new RexSeparator(relevantPaths, scan, builder);
      RexNode indexCondition = separator.getSeparatedCondition(condition);

      if (indexCondition == null) {
        return new IndexConditionInfo(null, null, false);
      }

      List<RexNode> conjuncts = RelOptUtil.conjunctions(condition);
      List<RexNode> indexConjuncts = RelOptUtil.conjunctions(indexCondition);
      for(RexNode indexConjunction: indexConjuncts) {
        RexUtil.removeAll(conjuncts, indexConjunction);
      }

      RexNode remainderCondition = RexUtil.composeConjunction(builder, conjuncts, false);

      indexCondition = indexCondition.accept(reverseVisitor);

      return new IndexConditionInfo(indexCondition, remainderCondition, true);
    }

  }
}
