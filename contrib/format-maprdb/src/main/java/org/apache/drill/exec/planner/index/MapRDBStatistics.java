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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.Scan;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.hbase.HBaseRegexParser;
import org.apache.drill.exec.store.mapr.db.json.JsonTableGroupScan;
import org.apache.hadoop.hbase.HConstants;
import org.ojai.store.QueryCondition;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MapRDBStatistics implements Statistics {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MapRDBStatistics.class);
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
  private RexBuilder builder;
  private PlannerSettings settings;

  public MapRDBStatistics() {
    statsCache = new HashMap<>();
    conditionRexNodeMap = new HashMap<>();
  }

  public void init(RexBuilder builder, PlannerSettings settings) {
    this.builder = builder;
    this.settings = settings;
  }

  public RexBuilder getRexBuilder() {return builder;}

  public PlannerSettings getSettings() { return settings; }

  @Override
  public double getRowCount(RexNode condition, ScanPrel scanPrel) {
    GroupScan scan;
    Preconditions.checkNotNull(builder, "Unable to getRowCount: Statistics not initialized properly(builder)");
    Preconditions.checkNotNull(settings, "Unable to getRowCount: Statistics not initialized properly(settings)");
    if (scanPrel.getGroupScan() instanceof DbGroupScan) {
      String conditionAsStr = convertRexToString(condition, scanPrel);
      scan = scanPrel.getGroupScan();
      if (statsCache.get(conditionAsStr) != null) {
        return statsCache.get(conditionAsStr);
      }
      IndexCollection indexes = ((DbGroupScan)scan).getSecondaryIndexCollection(scanPrel);
      return computeRowCount(condition, indexes, scanPrel);
    } else {
      return ROWCOUNT_UNKNOWN;
    }
  }

  /** Returns the statistics given the specified filter condition
   *  @param condition - Filter specified as a {@link QueryCondition}
   */
  public double getRowCount(QueryCondition condition) {
    if (condition != null
        && conditionRexNodeMap.get(condition.toString()) != null) {
      String rexConditionAsString = conditionRexNodeMap.get(condition.toString());
      if (statsCache.get(rexConditionAsString) != null) {
        return statsCache.get(rexConditionAsString);
      }
    }
    return ROWCOUNT_UNKNOWN;
  }

  private double computeRowCount(RexNode condition, IndexCollection indexes, ScanPrel scanPrel) {
    double totalRows, selectivity;
    JsonTableGroupScan jTabGrpScan;
    Map<IndexDescriptor, IndexConditionInfo> leadingKeyIdxConditionMap;
    Map<IndexDescriptor, IndexConditionInfo> idxConditionMap;
    String conditionAsStr = convertRexToString(condition, scanPrel);
    if (statsCache.get(conditionAsStr) != null) {
      return statsCache.get(conditionAsStr);
    }
    IndexCollection keyRepIndexes = rowKeyRepIndexes(indexes);
    if (scanPrel.getGroupScan() instanceof JsonTableGroupScan) {
      jTabGrpScan = (JsonTableGroupScan) scanPrel.getGroupScan();
    } else {
      return ROWCOUNT_UNKNOWN;
    }
    IndexConditionInfo.Builder infoBuilder = IndexConditionInfo.newBuilder(condition,
        keyRepIndexes, builder, scanPrel);
    idxConditionMap = infoBuilder.getIndexConditionMap();
    leadingKeyIdxConditionMap = infoBuilder.getLeadingKeyIndexConditionMap();

    for (IndexDescriptor idx : leadingKeyIdxConditionMap.keySet()) {
      RexNode idxCondition = leadingKeyIdxConditionMap.get(idx).indexCondition;
      // Use the pre-processed condition only for getting actual statistic from MapR-DB APIs. Use the
      // original condition everywhere else (cache store/lookups)
      RexNode preProcIdxCondition = preProcessCondition(idxCondition);
      QueryCondition queryCondition = jTabGrpScan.convertToQueryCondition(
          convertToLogicalExpression(preProcIdxCondition, scanPrel));
      double rowCount = jTabGrpScan.getEstimatedRowCount(queryCondition, idx, scanPrel);
      addToCache(idxCondition, rowCount, jTabGrpScan, scanPrel);
    }
    // Get total rows in the table
    totalRows = jTabGrpScan.getRowCount(null, scanPrel);
    // Add the row count for index conditions on all indexes. Stats are only computed for leading keys but
    // index conditions can be pushed and would be required for access path costing
    RexNode idxRemCondition = null;
    for (IndexDescriptor idx : idxConditionMap.keySet()) {
      RexNode idxCondition = idxConditionMap.get(idx).indexCondition;
      idxRemCondition = idxConditionMap.get(idx).remainderCondition;
      double rowCount = totalRows * computeSelectivity(idxCondition, totalRows, scanPrel);
      addToCache(idxCondition, rowCount, jTabGrpScan, scanPrel);
    }
    // Add the rowCount for non-pushable predicates
    if (idxRemCondition != null && !idxRemCondition.isAlwaysTrue()) {
      double rowCount = totalRows * computeSelectivity(idxRemCondition, totalRows, scanPrel);
      addToCache(idxRemCondition, rowCount, jTabGrpScan, scanPrel);
    }
    // Add the rowCount for the complete condition
    double rowCount = totalRows * computeSelectivity(condition, totalRows, scanPrel);
    addToCache(condition, rowCount, jTabGrpScan, scanPrel);
    return statsCache.get(conditionAsStr);
  }

  /* Add the statistic(row count) to the cache.
   * Also add the QueryCondition->RexNode condition mapping.
   */
  private void addToCache(RexNode condition, double rowcount, JsonTableGroupScan jTabGrpScan,
      ScanPrel scanPrel) {
    if (condition != null) {
      String conditionAsStr = convertRexToString(condition, scanPrel);
      if (statsCache.get(conditionAsStr) == null) {
        statsCache.put(conditionAsStr, rowcount);
        logger.debug("StatsCache:<{}, {}>",conditionAsStr, rowcount);
      }
      QueryCondition queryCondition =
          jTabGrpScan.convertToQueryCondition(convertToLogicalExpression(condition, scanPrel));
      if (queryCondition != null) {
        String queryConditionAsStr = queryCondition.toString();
        if (conditionRexNodeMap.get(queryConditionAsStr) == null) {
          conditionRexNodeMap.put(queryConditionAsStr, conditionAsStr);
          logger.debug("QCRNCache:<{}, {}>",queryConditionAsStr, conditionAsStr);
        }
      } else {
        logger.debug("QCRNCache: Unable to generate QueryCondition for {}", conditionAsStr);
      }
    }
  }

  /*private String convertRexToString(RexNode condition, ScanPrel scanPrel) {
    LogicalExpression expr = DrillOptiq.toDrill(
        new DrillParseContext(PrelUtil.getPlannerSettings(scanPrel.getCluster())), scanPrel, condition);
    return expr.toString();
  }*/
  /*
   * Convert the given Rexnode to a String representation while also replacing the RexInputRef references
   * to actual column names. Since, we compare String representations of RexNodes, two equivalent RexNode
   * expressions may differ in the RexInputRef positions but otherwise the same.
   * e.g. $1 = 'CA' projection (State, Country) , $2 = 'CA' projection (Country, State)
   */
  private String convertRexToString(RexNode condition, ScanPrel scanPrel) {
    StringBuilder sb = new StringBuilder();
    if (condition.getKind() == SqlKind.AND) {
      boolean first = true;
      for(RexNode pred : RelOptUtil.conjunctions(condition)) {
        if (first) {
          sb.append(convertRexToString(pred, scanPrel));
          first = false;
        } else {
          sb.append(" " + SqlKind.AND.toString() + " ");
          sb.append(convertRexToString(pred, scanPrel));
        }
      }
      return sb.toString();
    } else if (condition.getKind() == SqlKind.OR) {
      boolean first = true;
      for(RexNode pred : RelOptUtil.disjunctions(condition)) {
        if (first) {
          sb.append(convertRexToString(pred, scanPrel));
          first = false;
        } else {
          sb.append(" " + SqlKind.OR.toString() + " ");
          sb.append(convertRexToString(pred, scanPrel));
        }
      }
      return sb.toString();
    } else {
      HashMap<String, String> inputRefMapping = new HashMap<>();
      getInputRefMapping(condition, scanPrel, inputRefMapping);
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
      /*String inputRef, inputColumn;
      inputRef = inputColumn = null;
      if (condition instanceof RexCall) {
        for (RexNode op : ((RexCall) condition).getOperands()) {
          if (op instanceof RexInputRef) {
            inputRef = op.toString();
            inputColumn = scanPrel.getRowType().getFieldNames().get(op.hashCode());
          } else {
            convertRexToString(op, scanPrel);
          }
        }
        return condition.toString().replace(inputRef, inputColumn);
      }
      return condition.toString();*/
    }
  }

  private void getInputRefMapping(RexNode condition, ScanPrel scanPrel,
      HashMap<String, String> mapping) {
    if (condition instanceof RexCall) {
      for (RexNode op : ((RexCall) condition).getOperands()) {
        getInputRefMapping(op, scanPrel, mapping);
      }
    } else if (condition instanceof RexInputRef) {
      mapping.put(condition.toString(),
          scanPrel.getRowType().getFieldNames().get(condition.hashCode()));
    }
  }

  /* Additional pre-processing may be required for LIKE/CAST predicates in order to compute statistics.
  * e.g. A LIKE predicate should be converted to a RANGE predicate for statistics computation. MapR-DB
  * does not yet support computing statistics for LIKE predicates.
  */
  private RexNode preProcessCondition(RexNode condition) {
    if (condition.getKind() == SqlKind.AND) {
      final List<RexNode> conditions = Lists.newArrayList();
      for(RexNode pred : RelOptUtil.conjunctions(condition)) {
        conditions.add(preProcessCondition(pred));
      }
      return RexUtil.composeConjunction(builder, conditions, false);
    } else if (condition.getKind() == SqlKind.OR) {
      final List<RexNode> conditions = Lists.newArrayList();
      for(RexNode pred : RelOptUtil.disjunctions(condition)) {
        conditions.add(preProcessCondition(pred));
      }
      return RexUtil.composeDisjunction(builder, conditions, false);
    } else if (condition instanceof RexCall) {
      // LIKE operator - convert to a RANGE predicate, if possible
      if (((RexCall) condition).getOperator().getKind() == SqlKind.LIKE) {
        return preProcessLikeCondition((RexCall)condition);
      } else {
        return condition;
      }
    }
    return condition;
  }

  /* Helper function to perform additional pre-processing for LIKE predicates
   *
   */
  private RexNode preProcessLikeCondition(RexCall condition) {
    Preconditions.checkArgument(condition.getOperator().getKind() == SqlKind.LIKE,
        "Unable to preProcessLikeCondition: argument is not a LIKE condition!");
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
            // TODO: This maybe a potential bug since we assume UTF-8 encoding. However, we follow the current DB
            // implementation. See HBaseFilterBuilder.createHBaseScanSpec "like" CASE statement
            RexLiteral startKeyLiteral = builder.makeLiteral(new String(startKey, Charsets.UTF_8.toString()));
            RexLiteral stopKeyLiteral = builder.makeLiteral(new String(stopKey, Charsets.UTF_8.toString()));
            if (arg != null) {
              RexNode startPred = builder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, arg, startKeyLiteral);
              RexNode stopPred = builder.makeCall(SqlStdOperatorTable.LESS_THAN, arg, stopKeyLiteral);
              return builder.makeCall(SqlStdOperatorTable.AND, startPred, stopPred);
            }
          } catch (UnsupportedEncodingException ex) {
            // Encoding not supported - Do nothing!
          }
        }
      }
    }
    // Could not convert - return condition as-is.
    return condition;
  }

  /* Compute the selectivity of the given rowCondition. Retrieve the selectivity
   * for index conditions from the cache
   */
  private double computeSelectivity(RexNode condition, double totalRows, ScanPrel scanPrel) {
    double selectivity;
    String conditionAsStr = convertRexToString(condition, scanPrel);
    if (statsCache.get(conditionAsStr) != null) {
      selectivity = statsCache.get(conditionAsStr)/totalRows;
      logger.debug("computeSelectivity: Cache HIT: Found {} -> {}", conditionAsStr, selectivity);
      return selectivity;
    } else if (condition.getKind() == SqlKind.AND) {
      selectivity = 1.0;
      for(RexNode pred : RelOptUtil.conjunctions(condition)) {
        selectivity *= computeSelectivity(pred, totalRows, scanPrel);
      }
    } else if (condition.getKind() == SqlKind.OR) {
      selectivity = 0.0;
      for(RexNode pred : RelOptUtil.disjunctions(condition)) {
        selectivity += computeSelectivity(pred, totalRows, scanPrel);
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

  private IndexCollection rowKeyRepIndexes(IndexCollection indexes) {
    Iterator<IndexDescriptor> iterator = indexes.iterator();
    Map<SchemaPath, Boolean> rowKeyCols = new HashMap<>();
    while (iterator.hasNext()) {
      IndexDescriptor index = iterator.next();
      // If index has columns - the first column is the leading column for the index
      if (index.getIndexColumns() != null) {
        if (rowKeyCols.get(index.getIndexColumns().get(0)) != null) {
          iterator.remove();
        } else {
          rowKeyCols.put(index.getIndexColumns().get(0), true);
        }
      }
    }
    return indexes;
  }

  private LogicalExpression convertToLogicalExpression(RexNode condition, ScanPrel scanPrel) {
    LogicalExpression conditionExp = null;
    try {
      conditionExp = DrillOptiq.toDrill(new DrillParseContext(settings), scanPrel, condition);
    } catch (ClassCastException e) {
      return null;
    }
    return conditionExp;
  }
}
