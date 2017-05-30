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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.PrelUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class IndexSelector  {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IndexSelector.class);

  private RexNode indexCondition;
  private RelCollation requiredCollation;
  private double totalRows;
  private Statistics stats;         // a Statistics instance that will be used to get estimated rowcount for filter conditions
  private IndexConditionInfo.Builder builder;
  private RelOptPlanner planner;
  private List<IndexProperties> indexPropList;
  private DrillScanRel primaryTableScan;

  public IndexSelector(RexNode indexCondition,
      RelCollation requiredCollation,
      IndexCollection collection,
      Statistics stats,
      RexBuilder rexBuilder,
      RelOptPlanner planner,
      double totalRows,
      DrillScanRel scan) {
    this.indexCondition = indexCondition;
    this.requiredCollation = requiredCollation;
    this.totalRows = totalRows;
    this.stats = stats;
    this.builder =
        IndexConditionInfo.newBuilder(indexCondition, collection, rexBuilder, scan);
    this.planner = planner;
    this.primaryTableScan = scan;
    this.indexPropList = Lists.newArrayList();
  }

  public void addIndex(IndexDescriptor indexDesc, boolean isCovering, int numProjectedFields) {
    IndexProperties indexProps = new IndexProperties(indexDesc, isCovering,
        numProjectedFields, totalRows, primaryTableScan);
    indexPropList.add(indexProps);
  }

  /**
   * This method analyzes an index's columns and starting from the first column, checks
   * which part of the filter condition matches that column.  This process continues with
   * subsequent columns.  The goal is to identify the portion of the filter condition that
   * match the prefix columns.  If there are additional conditions that don't match prefix
   * columns, that condition is set as a remainder condition.
   * @param indexProps
   */
  public void analyzePrefixMatches(IndexProperties indexProps) {
    RexNode initCondition = indexCondition.isAlwaysTrue() ? null : indexCondition;
    Map<LogicalExpression, RexNode> leadingPrefixMap = Maps.newHashMap();
    List<LogicalExpression> indexCols = indexProps.getIndexDesc().getIndexColumns();
    boolean satisfiesCollation = false;

    if (indexCols.size() > 0) {
      if (initCondition != null) { // check filter condition
        if (!checkCollation()) {  // no collation requirement
          boolean prefix = true;
          int i=0;
          while (prefix && i < indexCols.size()) {
            LogicalExpression p = indexCols.get(i++);
            List<LogicalExpression> prefixCol = ImmutableList.of(p);
            IndexConditionInfo info = builder.indexConditionRelatedToFields(prefixCol, initCondition);
            if(info != null && info.hasIndexCol) {
              // the col had a match with one of the conditions; save the information about
              // indexcol --> condition mapping
              leadingPrefixMap.put(p, info.indexCondition);
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
        } else { // has collation requirement

        }
      } else if (checkCollation()) { // no filter condition, only collation requirement
        // compare collation of the index with the required collation
        List<RelFieldCollation> indexFieldCollations = indexProps.getIndexDesc().getCollation().getFieldCollations();
        List<RelFieldCollation> requiredFieldCollations =
            convertRequiredCollation(indexProps.getIndexDesc(), requiredCollation.getFieldCollations());
        if (Util.startsWith(indexFieldCollations, requiredFieldCollations)) {
          satisfiesCollation = true;
        }
      }
    }

    logger.debug("Index {}: leading prefix map: {}, remainder condition: {}", indexProps.getIndexDesc().getIndexName(),
        leadingPrefixMap, initCondition);
    indexProps.setProperties(leadingPrefixMap, satisfiesCollation, initCondition /* the remainder condition */, stats);
  }

  private boolean checkCollation() {
    return requiredCollation != null;
  }


  /**
   * Convert the required field collations that are in terms of the original plan into the one
   * that is specific to a particular index
   * @param required
   * @return
   */
  private List<RelFieldCollation> convertRequiredCollation(IndexDescriptor indexDesc,
      List<RelFieldCollation> required) {
    // TODO: implement
    return required;
  }

  /**
   * Run the index selection algorithm and return the top N indexes
   */
  public void getCandidateIndexes(List<IndexDescriptor> coveringIndexes,
      List<IndexDescriptor> nonCoveringIndexes) {

    logger.debug("Analyzing indexes for prefix matches");
    // analysis phase
    for (IndexProperties p : indexPropList) {
      analyzePrefixMatches(p);
    }

    int max_candidate_indexes = (int)PrelUtil.getPlannerSettings(planner).getMaxCandidateIndexesPerTable();
    // ranking phase; only needed if num indexes is greater than MAX_CANDIDATE_INDEXES
    if (indexPropList.size() > max_candidate_indexes) {
      Collections.sort(indexPropList, new IndexComparator(planner));
    }

    logger.debug("The top ranked indexes are: ");

    int count = 0;

    // pick the best N indexes
    for (int i=0; i < indexPropList.size(); i++) {
      IndexProperties index = indexPropList.get(i);
      if (index.isCovering()) {
        coveringIndexes.add(index.getIndexDesc());
        logger.debug("name: {}, covering: true, leadingSelectivity: {}, cost: {}",
            index.getIndexDesc().getIndexName(), index.getLeadingSelectivity(),
            index.getSelfCost(planner));
        count++;
      } else {
        nonCoveringIndexes.add(indexPropList.get(i).getIndexDesc());
        logger.debug("name: {}, covering: false, leadingSelectivity: {}, cost: {}",
            index.getIndexDesc().getIndexName(), index.getLeadingSelectivity(),
            index.getSelfCost(planner));
        count++;
      }
      if (count == max_candidate_indexes) {
        break;
      }
    }
  }

  public static class IndexComparator implements Comparator<IndexProperties> {

    private RelOptPlanner planner;
    private PlannerSettings settings;

    public IndexComparator(RelOptPlanner planner) {
      this.planner = planner;
      this.settings = PrelUtil.getPlannerSettings(planner);
    }

    @Override
    public int compare(IndexProperties o1, IndexProperties o2) {
      // given a covering and a non-covering index, prefer covering index unless the
      // difference in their selectivity is bigger than a configurable factor
      if (o1.isCovering() && !o2.isCovering()) {
        if (o1.getLeadingSelectivity()/o2.getLeadingSelectivity() < settings.getIndexCoveringToNonCoveringFactor()) {
          return -1;  // covering is ranked higher (better) than non-covering
        }
      }

      if (o2.isCovering() && !o1.isCovering()) {
        if (o2.getLeadingSelectivity()/o1.getLeadingSelectivity() < settings.getIndexCoveringToNonCoveringFactor()) {
          return 1;  // covering is ranked higher (better) than non-covering
        }
      }

      if (o1.satisfiesCollation() && !o2.satisfiesCollation()) {
        return -1;  // index with collation is ranked higher (better) than one without collation
      } else if (o2.satisfiesCollation() && !o1.satisfiesCollation()) {
        return 1;
      }

      DrillCostBase cost1 = (DrillCostBase)(o1.getSelfCost(planner));
      DrillCostBase cost2 = (DrillCostBase)(o2.getSelfCost(planner));

      if (cost1.isLt(cost2)) {
        return -1;
      } else if (cost1.isEqWithEpsilon(cost2)) {
        // TODO: if costs are same, use a rule based criteria
        return 0;
      } else {
        return 1;
      }
    }
  }

  /**
   * IndexProperties encapsulates the various metrics of a single index that are related to
   * the current query. These metrics are subsequently used to rank the index in comparison
   * with other indexes.
   */
  public static class IndexProperties  {
    private IndexDescriptor indexDescriptor; // index descriptor

    private double leadingSel = 1.0;    // selectivity of leading satisfiable conjunct
    private double remainderSel = 1.0;  // selectivity of all remainder satisfiable conjuncts
    private boolean satisfiesCollation = false; // whether index satisfies collation
    private boolean isCovering = false;         // whether index is covering
    private double  indexSize;                  // size in bytes of the selected part of index
    private int numMatchingConjuncts = 0;       // number of matching conjuncts

    private int numProjectedFields;
    private double totalRows;
    private DrillScanRel primaryTableScan = null;
    private RelOptCost selfCost = null;

    public List<RexNode> leadingFilters = Lists.newArrayList();
    public Map<LogicalExpression, RexNode> leadingPrefixMap;
    public RexNode remainderFilter = null;

    public IndexProperties(IndexDescriptor indexDescriptor,
        boolean isCovering,
        int numProjectedFields,
        double totalRows,
        DrillScanRel primaryTableScan) {
      this.indexDescriptor = indexDescriptor;
      this.isCovering = isCovering;
      this.numProjectedFields = numProjectedFields;
      this.totalRows = totalRows;
      this.primaryTableScan = primaryTableScan;
    }

    public void setProperties(Map<LogicalExpression, RexNode> prefixMap,
        boolean satisfiesCollation,
        RexNode remainderFilter,
        Statistics stats) {
      this.remainderFilter = remainderFilter;
      this.satisfiesCollation = satisfiesCollation;
      leadingPrefixMap = prefixMap;

      // iterate over the columns in the index descriptor and lookup from the leadingPrefixMap
      // the corresponding conditions
      for (LogicalExpression p : indexDescriptor.getIndexColumns()) {
        RexNode n;
        if ((n = leadingPrefixMap.get(p)) != null) {
          leadingFilters.add(n);
        } else {
          break; // break since the prefix property will not be preserved
        }
      }

      // compute the estimated row count by calling the statistics APIs
      for (RexNode filter : leadingFilters) {
        leadingSel *= (stats.getRowCount(filter, primaryTableScan))/totalRows;
      }
      if (remainderFilter != null) {
        remainderSel = stats.getRowCount(remainderFilter, primaryTableScan);
      }

    }

    public double getLeadingSelectivity() {
      return leadingSel;
    }

    public double getRemainderSelectivity() {
      return remainderSel;
    }

    public boolean isCovering() {
      return isCovering;
    }

    public double getTotalRows() {
      return totalRows;
    }

    public IndexDescriptor getIndexDesc() {
      return indexDescriptor;
    }

    public RexNode getRemainderFilter() {
      return remainderFilter;
    }

    public boolean satisfiesCollation() {
      return satisfiesCollation;
    }

    public RelOptCost getSelfCost(RelOptPlanner planner) {
      if (selfCost != null) {
        return selfCost;
      }
      selfCost = indexDescriptor.getCost(this, planner, numProjectedFields, primaryTableScan.getGroupScan());
      return selfCost;
    }

  }

}
