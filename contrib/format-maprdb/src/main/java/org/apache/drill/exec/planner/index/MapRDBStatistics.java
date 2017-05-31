/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.index;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.expression.ExpressionStringBuilder;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.store.hbase.HBaseRegexParser;
import org.apache.drill.exec.store.mapr.db.json.JsonTableGroupScan;
import org.apache.hadoop.hbase.HConstants;
import org.ojai.store.QueryCondition;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

public class MapRDBStatistics implements Statistics {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MapRDBStatistics.class);
  double tableCostPrefFactor = 1.0;
  /*
   * The computed statistics are cached in <statsCache> so that any subsequent calls are returned
   * from the cache. The <statsCache> is a map of <RexNode, Rowcount>. The <RexNode> does not have
   * a comparator so it is converted to a String for serving as a Map key. This may result in
   * logically equivalent conditions considered differently e.g. sal<10 OR sal>100, sal>100 OR sal<10
   */
  private Map<String, Double> statsCache;
  /*
   * The mapping between <QueryCondition> and <RexNode> is kept in <conditionRexNodeMap>. This mapping
   * is useful to obtain rowCount for condition specified as <QueryCondition> required during physical
   * planning. Again, both the <QueryCondition> and <RexNode> are converted to Strings for the lack
   * of a comparator.
   */
  private Map<String, String> conditionRexNodeMap;

  public MapRDBStatistics() {
    statsCache = new HashMap<>();
    conditionRexNodeMap = new HashMap<>();
  }

  public double getTableCostPrefFactor() {
    return tableCostPrefFactor;
  }

  @Override
  public double getRowCount(RexNode condition, DrillScanRel scanRel, boolean isTableScan) {
    double costFactor = 1.0;
    PlannerSettings settings = PrelUtil.getPlannerSettings(scanRel.getCluster().getPlanner());
    if (isTableScan) {
      costFactor = tableCostPrefFactor;
    }
    if (scanRel.getGroupScan() instanceof DbGroupScan) {
      if (condition == null) {
        if (statsCache.get(null)!= null) {
          return costFactor * statsCache.get(null);
        }
      } else {
        String conditionAsStr = convertRexToString(condition, scanRel);
        if (statsCache.get(conditionAsStr) != null) {
          return costFactor * statsCache.get(conditionAsStr);
        }
      }
    }
    return ROWCOUNT_UNKNOWN;
  }

  /** Returns the number of rows satisfying the given FILTER condition
   *  @param condition - FILTER specified as a {@link QueryCondition}
   *  @param isTableScan - Whether the rowcounts are requested by an TableScan
   * @return approximate rows satisfying the filter
   */
  public double getRowCount(QueryCondition condition, boolean isTableScan) {
    double costFactor = 1.0;
    // For table scan multiply the rowcounts with the planner.fts_cost_factor to make
    // index scans less costly and bias towards index plans.
    if (isTableScan) {
      costFactor = tableCostPrefFactor;
    }
    if (condition != null
        && conditionRexNodeMap.get(condition.toString()) != null) {
      String rexConditionAsString = conditionRexNodeMap.get(condition.toString());
      if (statsCache.get(rexConditionAsString) != null) {
        return costFactor * statsCache.get(rexConditionAsString);
      }
    } else if (condition == null
        // We have full table rows mapping i.e. <NULL> QueryCondition, <NULL> RexNode pair
        && conditionRexNodeMap.get(condition) == null) {
      if (statsCache.get(null) != null) {
        return costFactor * statsCache.get(null);
      }
    }
    return ROWCOUNT_UNKNOWN;
  }

  public boolean initialize(RexNode condition, DrillScanRel scanRel, IndexPlanCallContext context) {
    GroupScan scan;
    PlannerSettings settings = PrelUtil.getPlannerSettings(scanRel.getCluster().getPlanner());
    tableCostPrefFactor = settings.getTableCostPrefFactor();
    if (!settings.isDisableScanStatistics()
      && scanRel.getGroupScan() instanceof DbGroupScan) {
      String conditionAsStr = convertRexToString(condition, scanRel);
      scan = scanRel.getGroupScan();
      if (statsCache.get(conditionAsStr) == null) {
        IndexCollection indexes = ((DbGroupScan)scan).getSecondaryIndexCollection(scanRel);
        populateRowCount(condition, indexes, scanRel, context);
        return true;
      }
    }
    return false;
  }

  private double populateRowCount(RexNode condition, IndexCollection indexes, DrillScanRel scanRel,
                                  IndexPlanCallContext context) {
    JsonTableGroupScan jTabGrpScan;
    double totalRows;
    Map<IndexDescriptor, IndexConditionInfo> leadingKeyIdxConditionMap;
    Map<IndexDescriptor, IndexConditionInfo> idxConditionMap;
    String conditionAsStr = convertRexToString(condition, scanRel);

    if (statsCache.get(conditionAsStr) != null) {
      return statsCache.get(conditionAsStr);
    }

    IndexCollection keyRepIndexes = rowKeyRepIndexes(indexes);
    if (scanRel.getGroupScan() instanceof JsonTableGroupScan) {
      jTabGrpScan = (JsonTableGroupScan) scanRel.getGroupScan();
    } else {
      return ROWCOUNT_UNKNOWN;
    }

    RexBuilder builder = scanRel.getCluster().getRexBuilder();
    PlannerSettings settings = PrelUtil.getSettings(scanRel.getCluster());
    IndexConditionInfo.Builder infoBuilder = IndexConditionInfo.newBuilder(condition,
        keyRepIndexes, builder, scanRel);
    idxConditionMap = infoBuilder.getIndexConditionMap();
    leadingKeyIdxConditionMap = infoBuilder.getLeadingKeyIndexConditionMap();
    // Get total rows in the table
    totalRows = jTabGrpScan.getEstimatedRowCount(null, null, scanRel);

    for (IndexDescriptor idx : leadingKeyIdxConditionMap.keySet()) {
      RexNode idxCondition = leadingKeyIdxConditionMap.get(idx).indexCondition;
      /* Use the pre-processed condition only for getting actual statistic from MapR-DB APIs. Use the
       * original condition everywhere else (cache store/lookups) since the RexNode condition and its
       * corresponding QueryCondition will be used to get statistics. e.g. we convert LIKE into RANGE
       * condition to get statistics. However, statistics are always asked for LIKE and NOT the RANGE
       */
      RexNode preProcIdxCondition = convertToStatsCondition(idxCondition, idx, context, scanRel,
          Arrays.asList(SqlKind.CAST, SqlKind.LIKE));
      RelDataType newRowType;
      FunctionalIndexInfo functionInfo = idx.getFunctionalInfo();
      if (functionInfo.hasFunctional()) {
        newRowType = FunctionalIndexHelper.rewriteFunctionalRowType(scanRel, context, functionInfo);
      } else {
        newRowType = scanRel.getRowType();
      }

      QueryCondition queryCondition = jTabGrpScan.convertToQueryCondition(
          convertToLogicalExpression(preProcIdxCondition, newRowType, settings, builder));
      // Cap rows at total rows in case of issues with DB APIs
      double rowCount = Math.min(jTabGrpScan.getEstimatedRowCount(queryCondition, idx, scanRel),
          totalRows);
      addToCache(idxCondition, idx, context, rowCount, jTabGrpScan, scanRel, newRowType);
    }
    /* Add the row count for index conditions on all indexes. Stats are only computed for leading
     * keys but index conditions can be pushed and would be required for access path costing
     */
    RexNode idxRemCondition = null;
    RelDataType newRowType = null;
    IndexDescriptor candIdx = null;
    for (IndexDescriptor idx : idxConditionMap.keySet()) {
      candIdx = idx;
      RexNode idxCondition = idxConditionMap.get(idx).indexCondition;
      idxRemCondition = idxConditionMap.get(idx).remainderCondition;
      // Ignore conditions which always evaluate to true
      if (idxCondition.isAlwaysTrue()) {
        continue;
      }
      FunctionalIndexInfo functionInfo = idx.getFunctionalInfo();
      if (functionInfo.hasFunctional()) {
        newRowType = FunctionalIndexHelper.rewriteFunctionalRowType(scanRel, context, functionInfo);
      } else {
        newRowType = scanRel.getRowType();
      }
      double rowCount = totalRows * computeSelectivity(idxCondition, totalRows, scanRel);
      addToCache(idxCondition, idx, context, rowCount, jTabGrpScan, scanRel, newRowType);
    }
    // Add the rowCount for non-pushable predicates
    if (idxRemCondition != null) {
      double rowCount = totalRows * computeSelectivity(idxRemCondition, totalRows, scanRel);
      addToCache(idxRemCondition, candIdx, context, rowCount, jTabGrpScan, scanRel, newRowType);
    }
    // Add the rowCount for the complete condition - based on table
    double rowCount = totalRows * computeSelectivity(condition, totalRows, scanRel);
    addToCache(condition, null, null, rowCount, jTabGrpScan, scanRel, scanRel.getRowType());
    // Add the full table rows while we are at it - represented by <NULL> RexNode, <NULL> QueryCondition
    addToCache(null, null, null, totalRows, jTabGrpScan, scanRel, scanRel.getRowType());
    return statsCache.get(conditionAsStr);
  }

  /*
   * Adds the statistic(row count) to the cache. Also adds the corresponding QueryCondition->RexNode
   * condition mapping.
   */
  private void addToCache(RexNode condition, IndexDescriptor idx, IndexPlanCallContext context, double rowcount,
      JsonTableGroupScan jTabGrpScan, DrillScanRel scanRel, RelDataType rowType) {
    if (condition != null
        && !condition.isAlwaysTrue()) {
      RexBuilder builder = scanRel.getCluster().getRexBuilder();
      PlannerSettings settings = PrelUtil.getSettings(scanRel.getCluster());
      String conditionAsStr = convertRexToString(condition, scanRel);
      if (statsCache.get(conditionAsStr) == null
              && rowcount != Statistics.ROWCOUNT_UNKNOWN) {
        statsCache.put(conditionAsStr, rowcount);
        logger.debug("StatsCache:<{}, {}>",conditionAsStr, rowcount);
        // Always pre-process CAST conditions - Otherwise queryCondition will not be generated correctly
        RexNode preProcIdxCondition = convertToStatsCondition(condition, idx, context, scanRel,
            Arrays.asList(SqlKind.CAST));
        QueryCondition queryCondition =
            jTabGrpScan.convertToQueryCondition(convertToLogicalExpression(preProcIdxCondition,
                rowType, settings, builder));
        if (queryCondition != null) {
          String queryConditionAsStr = queryCondition.toString();
          if (conditionRexNodeMap.get(queryConditionAsStr) == null) {
            conditionRexNodeMap.put(queryConditionAsStr, conditionAsStr);
            logger.debug("QCRNCache:<{}, {}>",queryConditionAsStr, conditionAsStr);
          }
        } else {
          logger.debug("QCRNCache: Unable to generate QueryCondition for {}", conditionAsStr);
        }
      } else {
        logger.debug("QCRNCache: Unable to generate QueryCondition for {}", conditionAsStr);
      }
    } else if (condition == null) {
      statsCache.put(null, rowcount);
      logger.debug("StatsCache:<{}, {}>","NULL", rowcount);
      conditionRexNodeMap.put(null, null);
      logger.debug("QCRNCache:<{}, {}>","NULL", "NULL");
    }
  }

  /*
   * Convert the given Rexnode to a String representation while also replacing the RexInputRef references
   * to actual column names. Since, we compare String representations of RexNodes, two equivalent RexNode
   * expressions may differ in the RexInputRef positions but otherwise the same.
   * e.g. $1 = 'CA' projection (State, Country) , $2 = 'CA' projection (Country, State)
   */
  private String convertRexToString(RexNode condition, DrillScanRel scanRel) {
    StringBuilder sb = new StringBuilder();
    if (condition == null) {
      return null;
    }
    if (condition.getKind() == SqlKind.AND) {
      boolean first = true;
      for(RexNode pred : RelOptUtil.conjunctions(condition)) {
        if (first) {
          sb.append(convertRexToString(pred, scanRel));
          first = false;
        } else {
          sb.append(" " + SqlKind.AND.toString() + " ");
          sb.append(convertRexToString(pred, scanRel));
        }
      }
      return sb.toString();
    } else if (condition.getKind() == SqlKind.OR) {
      boolean first = true;
      for(RexNode pred : RelOptUtil.disjunctions(condition)) {
        if (first) {
          sb.append(convertRexToString(pred, scanRel));
          first = false;
        } else {
          sb.append(" " + SqlKind.OR.toString() + " ");
          sb.append(convertRexToString(pred, scanRel));
        }
      }
      return sb.toString();
    } else {
      HashMap<String, String> inputRefMapping = new HashMap<>();
      /* Based on the rel projection the input reference for the same column may change
       * during planning. We want the cache to be agnostic to it. Hence, the entry stored
       * in the cache has the input reference ($i) replaced with the column name
       */
      getInputRefMapping(condition, scanRel, inputRefMapping);
      if (inputRefMapping.keySet().size() > 0) {
        //Found input ref - replace it
        String replCondition = condition.toString();
        for (String inputRef : inputRefMapping.keySet()) {
          replCondition = replCondition.replace(inputRef, inputRefMapping.get(inputRef));
        }
        return replCondition;
      } else {
        return condition.toString();
      }
    }
  }

  /*
   * Generate the input reference to column mapping for reference replacement. Please
   * look at the usage in convertRexToString() to understand why this mapping is required.
   */
  private void getInputRefMapping(RexNode condition, DrillScanRel scanRel,
      HashMap<String, String> mapping) {
    if (condition instanceof RexCall) {
      for (RexNode op : ((RexCall) condition).getOperands()) {
        getInputRefMapping(op, scanRel, mapping);
      }
    } else if (condition instanceof RexInputRef) {
      mapping.put(condition.toString(),
          scanRel.getRowType().getFieldNames().get(condition.hashCode()));
    }
  }

  /*
   * Additional pre-processing may be required for LIKE/CAST predicates in order to compute statistics.
   * e.g. A LIKE predicate should be converted to a RANGE predicate for statistics computation. MapR-DB
   * does not yet support computing statistics for LIKE predicates.
   */
  private RexNode convertToStatsCondition(RexNode condition, IndexDescriptor index,
      IndexPlanCallContext context, DrillScanRel scanRel, List<SqlKind>typesToProcess) {
    RexBuilder builder = scanRel.getCluster().getRexBuilder();
    if (condition.getKind() == SqlKind.AND) {
      final List<RexNode> conditions = Lists.newArrayList();
      for(RexNode pred : RelOptUtil.conjunctions(condition)) {
        conditions.add(convertToStatsCondition(pred, index, context, scanRel, typesToProcess));
      }
      return RexUtil.composeConjunction(builder, conditions, false);
    } else if (condition.getKind() == SqlKind.OR) {
      final List<RexNode> conditions = Lists.newArrayList();
      for(RexNode pred : RelOptUtil.disjunctions(condition)) {
        conditions.add(convertToStatsCondition(pred, index, context, scanRel, typesToProcess));
      }
      return RexUtil.composeDisjunction(builder, conditions, false);
    } else if (condition instanceof RexCall) {
      // LIKE operator - convert to a RANGE predicate, if possible
      if (typesToProcess.contains(SqlKind.LIKE)
          && ((RexCall) condition).getOperator().getKind() == SqlKind.LIKE) {
        return convertLikeToRange((RexCall)condition, builder);
      } else if (typesToProcess.contains(SqlKind.CAST)
          && hasCastExpression(condition)) {
        return convertCastForFIdx(((RexCall) condition), index, context, scanRel);
      }
      else {
        return condition;
      }
    }
    return condition;
  }

  /*
   * Determines whether the given expression contains a CAST expression. Assumes that the
   * given expression is a valid expression.
   * Returns TRUE, if it finds at least one instance of CAST operator.
   */
  private boolean hasCastExpression(RexNode condition) {
    if (condition instanceof RexCall) {
      if (((RexCall) condition).getOperator().getKind() == SqlKind.CAST) {
        return true;
      }
      for (RexNode op : ((RexCall) condition).getOperands()) {
        if (hasCastExpression(op)) {
          return true;
        }
      }
    }
    return false;
  }
  /*
   * CAST expressions are not understood by MAPR-DB as-is. Hence, we must convert them before passing them
   * onto MAPR-DB for statistics. Given a functional index, the given expression is converted into an
   * expression on the `expression` column of the functional index.
   */
  private RexNode convertCastForFIdx(RexCall condition, IndexDescriptor index,
                                     IndexPlanCallContext context, DrillScanRel origScan) {
    if (index == null) {
      return condition;
    }
    FunctionalIndexInfo functionInfo = index.getFunctionalInfo();
    if (!functionInfo.hasFunctional()) {
      return condition;
    }
    // The functional index has a different row-type than the original scan. Use the index row-type when
    // converting the condition
    RelDataType newRowType = FunctionalIndexHelper.rewriteFunctionalRowType(origScan, context, functionInfo);
    RexBuilder builder = origScan.getCluster().getRexBuilder();
    return FunctionalIndexHelper.convertConditionForIndexScan(condition,
        origScan, newRowType, builder, functionInfo);
  }

  /*
   * Helper function to perform additional pre-processing for LIKE predicates
   */
  private RexNode convertLikeToRange(RexCall condition, RexBuilder builder) {
    Preconditions.checkArgument(condition.getOperator().getKind() == SqlKind.LIKE,
        "Unable to convertLikeToRange: argument is not a LIKE condition!");
    HBaseRegexParser parser = null;
    RexNode arg = null;
    RexLiteral pattern = null, escape = null;
    String patternStr = null, escapeStr = null;
    if (condition.getOperands().size() == 2) {
      // No escape character specified
      for (RexNode op : condition.getOperands()) {
        if (op.getKind() == SqlKind.LITERAL) {
          pattern = (RexLiteral) op;
        } else {
          arg = op;
        }
      }
      // Get the PATTERN strings from the corresponding RexLiteral
      if (pattern.getTypeName() == SqlTypeName.DECIMAL ||
          pattern.getTypeName() == SqlTypeName.INTEGER) {
        patternStr = pattern.getValue().toString();
      } else if (pattern.getTypeName() == SqlTypeName.CHAR) {
        patternStr = pattern.getValue2().toString();
      }
      if (patternStr != null) {
        parser = new HBaseRegexParser(patternStr);
      }
    } else if (condition.getOperands().size() == 3) {
      // Escape character specified
      for (RexNode op : condition.getOperands()) {
        if (op.getKind() == SqlKind.LITERAL) {
          // Assume first literal specifies PATTERN and the second literal specifies the ESCAPE char
          if (pattern == null) {
            pattern = (RexLiteral) op;
          } else {
            escape = (RexLiteral) op;
          }
        } else {
          arg = op;
        }
      }
      // Get the PATTERN and ESCAPE strings from the corresponding RexLiteral
      if (pattern.getTypeName() == SqlTypeName.DECIMAL ||
          pattern.getTypeName() == SqlTypeName.INTEGER) {
        patternStr = pattern.getValue().toString();
      } else if (pattern.getTypeName() == SqlTypeName.CHAR) {
        patternStr = pattern.getValue2().toString();
      }
      if (escape.getTypeName() == SqlTypeName.DECIMAL ||
          escape.getTypeName() == SqlTypeName.INTEGER) {
        escapeStr = escape.getValue().toString();
      } else if (escape.getTypeName() == SqlTypeName.CHAR) {
        escapeStr = escape.getValue2().toString();
      }
      if (patternStr != null && escapeStr != null) {
        parser = new HBaseRegexParser(patternStr, escapeStr.toCharArray()[0]);
      }
    }
    if (parser != null) {
      parser.parse();
      String prefix = parser.getPrefixString();
      /*
       * If there is a literal prefix, convert it into an EQUALITY or RANGE predicate
       */
      if (prefix != null) {
        if (prefix.equals(parser.getLikeString())) {
          // No WILDCARD present. This turns the LIKE predicate to EQUALITY predicate
          if (arg != null) {
            return builder.makeCall(SqlStdOperatorTable.EQUALS, arg, pattern);
          }
        } else {
          // WILDCARD present. This turns the LIKE predicate to RANGE predicate
          byte[] startKey = HConstants.EMPTY_START_ROW;
          byte[] stopKey = HConstants.EMPTY_END_ROW;
          startKey = prefix.getBytes(Charsets.UTF_8);
          stopKey = startKey.clone();
          boolean isMaxVal = true;
          for (int i = stopKey.length - 1; i >= 0 ; --i) {
            int nextByteValue = (0xff & stopKey[i]) + 1;
            if (nextByteValue < 0xff) {
              stopKey[i] = (byte) nextByteValue;
              isMaxVal = false;
              break;
            } else {
              stopKey[i] = 0;
            }
          }
          if (isMaxVal) {
            stopKey = HConstants.EMPTY_END_ROW;
          }
          try {
            // TODO: This maybe a potential bug since we assume UTF-8 encoding. However, we follow the
            // current DB implementation. See HBaseFilterBuilder.createHBaseScanSpec "like" CASE statement
            RexLiteral startKeyLiteral = builder.makeLiteral(new String(startKey,
                Charsets.UTF_8.toString()));
            RexLiteral stopKeyLiteral = builder.makeLiteral(new String(stopKey,
                Charsets.UTF_8.toString()));
            if (arg != null) {
              RexNode startPred = builder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                  arg, startKeyLiteral);
              RexNode stopPred = builder.makeCall(SqlStdOperatorTable.LESS_THAN, arg, stopKeyLiteral);
              return builder.makeCall(SqlStdOperatorTable.AND, startPred, stopPred);
            }
          } catch (UnsupportedEncodingException ex) {
            // Encoding not supported - Do nothing!
            logger.debug("convertLikeToRange: Unsupported Encoding Exception -> {}", ex.getMessage());
          }
        }
      }
    }
    // Could not convert - return condition as-is.
    return condition;
  }

  /*
   * Compute the selectivity of the given rowCondition. Retrieve the selectivity
   * for index conditions from the cache
   */
  private double computeSelectivity(RexNode condition, double totalRows, DrillScanRel scanRel) {
    double selectivity;
    String conditionAsStr = convertRexToString(condition, scanRel);
    if (statsCache.get(conditionAsStr) != null) {
      selectivity = statsCache.get(conditionAsStr)/totalRows;
      logger.debug("computeSelectivity: Cache HIT: Found {} -> {}", conditionAsStr, selectivity);
      return selectivity;
    } else if (condition.getKind() == SqlKind.AND) {
      selectivity = 1.0;
      for(RexNode pred : RelOptUtil.conjunctions(condition)) {
        selectivity *= computeSelectivity(pred, totalRows, scanRel);
      }
    } else if (condition.getKind() == SqlKind.OR) {
      selectivity = 0.0;
      for(RexNode pred : RelOptUtil.disjunctions(condition)) {
        selectivity += computeSelectivity(pred, totalRows, scanRel);
      }
    } else {
      selectivity = RelMdUtil.guessSelectivity(condition);
    }
    // Cap selectivity to be between 0.0 and 1.0
    selectivity = Math.min(1.0, selectivity);
    selectivity = Math.max(0.0, selectivity);
    logger.debug("computeSelectivity: Cache MISS: Computed {} -> {}", conditionAsStr, selectivity);
    return selectivity;
  }

  /*
   * Filters out indexes from the given collection based on the row key of indexes i.e. after filtering
   * the given collection would contain only one index for each distinct row key in the collection
   */
  private IndexCollection rowKeyRepIndexes(IndexCollection indexes) {
    Iterator<IndexDescriptor> iterator = indexes.iterator();
    Map<String, Boolean> rowKeyCols = new HashMap<>();
    while (iterator.hasNext()) {
      IndexDescriptor index = iterator.next();
      // If index has columns - the first column is the leading column for the index
      if (index.getIndexColumns() != null) {
        if (rowKeyCols.get(convertLExToStr(index.getIndexColumns().get(0))) != null) {
          iterator.remove();
        } else {
          rowKeyCols.put(convertLExToStr(index.getIndexColumns().get(0)), true);
        }
      }
    }
    return indexes;
  }

  /*
   * Returns the String representation for the given Logical Expression
   */
  private String convertLExToStr(LogicalExpression lex) {
    StringBuilder sb = new StringBuilder();
    ExpressionStringBuilder esb = new ExpressionStringBuilder();
    lex.accept(esb, sb);
    return sb.toString();
  }

  /*
   * Converts the given RexNode condition into a Drill logical expression.
   */
  private LogicalExpression convertToLogicalExpression(RexNode condition,
      RelDataType type, PlannerSettings settings, RexBuilder builder) {
    LogicalExpression conditionExp;
    try {
      conditionExp = DrillOptiq.toDrill(new DrillParseContext(settings), type, builder, condition);
    } catch (ClassCastException e) {
      return null;
    }
    return conditionExp;
  }
}
