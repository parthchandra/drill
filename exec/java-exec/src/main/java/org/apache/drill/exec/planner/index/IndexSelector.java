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
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ScanPrel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class IndexSelector  {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IndexSelector.class);

  private RexNode indexCondition;
  private double totalRows;
  private Statistics stats;         // a Statistics instance that will be used to get estimated rowcount for filter conditions
  private IndexConditionInfo.Builder builder;
  private List<IndexProperties> indexPropList;
  private DrillScanRel primaryTableScan;
  private IndexPlanCallContext indexContext;

  public IndexSelector(RexNode indexCondition,
      IndexPlanCallContext indexContext,
      IndexCollection collection,
      RexBuilder rexBuilder,
      double totalRows) {
    this.indexCondition = indexCondition;
    this.indexContext = indexContext;
    this.totalRows = totalRows;
    this.stats = ((DbGroupScan) (indexContext.scan).getGroupScan()).getStatistics();
    this.builder =
        IndexConditionInfo.newBuilder(indexCondition, collection, rexBuilder, indexContext.scan);
    this.primaryTableScan = indexContext.scan;
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
      }
      if (requiredCollation()) {
        satisfiesCollation = buildAndCheckCollation(indexProps);
      }
    }

    indexProps.setProperties(leadingPrefixMap, satisfiesCollation, initCondition /* the remainder condition */, stats);
  }

  private boolean requiredCollation() {
    if (indexContext.sort != null && indexContext.sort.getCollationList().size() > 0) {
      return true;
    }
    return false;
  }

  private boolean buildAndCheckCollation(IndexProperties indexProps) {
    IndexDescriptor indexDesc = indexProps.getIndexDesc();
    FunctionalIndexInfo functionInfo = indexDesc.getFunctionalInfo();

    RelCollation inputCollation;
    // for the purpose of collation we can assume that a covering index scan would provide
    // the collation property that would be relevant for non-covering as well
    ScanPrel indexScanPrel =
        IndexPlanUtils.buildCoveringIndexScan(indexContext.scan, indexDesc.getIndexGroupScan(), indexContext, indexDesc);
    inputCollation = indexScanPrel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);

    // we don't create collation for Filter because it will inherit the child's collation

    if (indexContext.lowerProject != null) {
      inputCollation =
          IndexPlanUtils.buildCollationProject(indexContext.lowerProject.getProjects(), null,
              indexScanPrel, functionInfo,indexContext);
    }

    if (indexContext.upperProject != null) {
      inputCollation =
          IndexPlanUtils.buildCollationProject(indexContext.upperProject.getProjects(), indexContext.lowerProject,
              indexContext.scan, functionInfo, indexContext);
    }

    if ( (inputCollation != null) && inputCollation.satisfies(indexContext.sort.getCollation())) {
      return true;
    }

    return false;

  }


  /**
   * Run the index selection algorithm and return the top N indexes
   */
  public void getCandidateIndexes(List<IndexDescriptor> coveringIndexes,
      List<IndexDescriptor> nonCoveringIndexes) {

    RelOptPlanner planner = indexContext.call.getPlanner();
    PlannerSettings settings = PrelUtil.getPlannerSettings(planner);
    List<IndexProperties> candidateIndexes = Lists.newArrayList();

    logger.info("Analyzing indexes for prefix matches");
    // analysis phase
    for (IndexProperties p : indexPropList) {
      analyzePrefixMatches(p);

      // only consider indexes that either have some leading prefix of the filter condition or
      // can satisfy required collation
      if (p.numLeadingFilters() > 0 || p.satisfiesCollation()) {
        double selThreshold = p.isCovering() ? settings.getCoveringIndexSelectivityFactor() :
          settings.getNonCoveringIndexSelectivityFactor();
        // only consider indexes whose selectivity is <= the configured threshold
        if (p.getLeadingSelectivity() <= selThreshold) {
          candidateIndexes.add(p);
        }
      }
    }

    int max_candidate_indexes = (int)PrelUtil.getPlannerSettings(planner).getMaxCandidateIndexesPerTable();
    // Ranking phase. Technically, we don't need to rank if there are fewer than max_candidate_indexes
    // but we do it anyways for couple of reasons: the log output will show the indexes in a properly ranked
    // order which helps diagnosing problems and secondly for internal unit/functional testing we want this code
    // to be exercised even for few indexes
    if (candidateIndexes.size() > 1) {
      Collections.sort(candidateIndexes, new IndexComparator(planner));
    }

    logger.info("The top ranked indexes are: ");

    int count = 0;
    boolean foundCovering = false;
    boolean foundCoveringCollation = false;

    // pick the best N indexes
    for (int i=0; i < candidateIndexes.size(); i++) {
      IndexProperties index = candidateIndexes.get(i);
      if (index.isCovering()) {
        if (foundCoveringCollation) {
          // if previously we already found a higher ranked covering index that satisfies collation,
          // then skip this one (note that selectivity and cost considerations were already handled
          // by the ranking phase)
          continue;
        }
        coveringIndexes.add(index.getIndexDesc());
        logger.info("name: {}, covering, collation: {}, leadingSelectivity: {}, cost: {}",
            index.getIndexDesc().getIndexName(),
            index.satisfiesCollation(),
            index.getLeadingSelectivity(),
            index.getSelfCost(planner));
        count++;
        foundCovering = true;
        if (index.satisfiesCollation()) {
          foundCoveringCollation = true;
        }
      } else {  // non-covering

        // skip this non-covering index if (a) there was a higher ranked covering index
        // with collation or (b) there was a higher ranked ranked covering index and this
        // non-covering index does not have collation
        if (foundCoveringCollation ||
            (foundCovering && !index.satisfiesCollation())) {
          continue;
        }

        // all other non-covering indexes can be added to the list because 2 or more non-covering index could
        // be considered for intersection later; currently the index selector is not costing the index intersection
        // TODO: enhance index selector for doing cost-based analysis of index intersection
        nonCoveringIndexes.add(index.getIndexDesc());
        logger.info("name: {}, non-covering, collation: {}, leadingSelectivity: {}, cost: {}",
            index.getIndexDesc().getIndexName(),
            index.satisfiesCollation(),
            index.getLeadingSelectivity(),
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
        if (o1.numLeadingFilters() > o2.numLeadingFilters()) {
          return -1;
        } else if (o1.numLeadingFilters() < o2.numLeadingFilters()) {
          return 1;
        }
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
    private double avgRowSize;          // avg row size in bytes of the selected part of index

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

      logger.debug("Index {}: leading prefix map: {}, whether satisfies collation: {}, remainder condition: {}",
          indexDescriptor.getIndexName(), leadingPrefixMap, satisfiesCollation, remainderFilter);

      // iterate over the columns in the index descriptor and lookup from the leadingPrefixMap
      // the corresponding conditions
      if (leadingPrefixMap.size() > 0) {
        for (LogicalExpression p : indexDescriptor.getIndexColumns()) {
          RexNode n;
          if ((n = leadingPrefixMap.get(p)) != null) {
            leadingFilters.add(n);
          } else {
            break; // break since the prefix property will not be preserved
          }
        }
      }

      // compute the estimated row count by calling the statistics APIs
      // NOTE: the calls to stats.getRowCount() below supply the primary table scan
      // which is ok because its main use is to convert the ordinal-based filter
      // to a string representation for stats lookup.
      for (RexNode filter : leadingFilters) {
        double filterRows = stats.getRowCount(filter, primaryTableScan /* see comment above */, false);
        double sel = 1.0;
        if (filterRows != Statistics.ROWCOUNT_UNKNOWN) {
          sel = filterRows/totalRows;
          logger.debug("Filter: {}, filterRows = {}, totalRows = {}, selectivity = {}",
              filter, filterRows, totalRows, sel);
        } else {
          logger.warn("Filter row count is UNKNOWN for filter: {}", filter);
        }
        leadingSel *= sel;
      }

      logger.debug("Combined selectivity of all leading filters: {}", leadingSel);

      if (remainderFilter != null) {
        remainderSel = stats.getRowCount(remainderFilter, primaryTableScan, false);
        logger.debug("Selectivity of remainder filters: {}", remainderSel);
      }

      // get the average row size based on the leading column filter
      avgRowSize = stats.getAvgRowSize(leadingFilters.size() > 0 ? leadingFilters.get(0) : null,
          primaryTableScan, false);
      logger.debug("Average row size based on leading filter: {}", avgRowSize);
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

    public int numLeadingFilters() {
      return leadingFilters.size();
    }

    public double getAvgRowSize() {
      return avgRowSize;
    }

  }

}
