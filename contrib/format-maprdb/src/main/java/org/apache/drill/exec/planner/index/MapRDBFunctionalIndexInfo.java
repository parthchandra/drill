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
package org.apache.drill.exec.planner.index;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;

import java.util.Map;
import java.util.Set;

public class MapRDBFunctionalIndexInfo implements FunctionalIndexInfo {

  final private IndexDescriptor indexDesc;

  private boolean hasFunctionalField = false;

  //when we scan schemaPath in groupscan's columns, we check if this column(schemaPath) should be rewritten to '$N',
  //When there are more than two functions on the same column in index, CAST(a.b as INT), CAST(a.b as VARCHAR),
  // then we should map SchemaPath a.b to a set of SchemaPath, e.g. $1, $2
  private Map<SchemaPath, Set<SchemaPath>> columnToConvert;

  //the map of expression to destination SchemaPath $N
  private Map<LogicalExpression, LogicalExpression> exprToConvert;

  //SchemaPath involved in a functional index
  private Map<LogicalExpression, Set<SchemaPath>> pathsInExpr;

  private Set<SchemaPath> newPathsForIndexedFunction;

  public MapRDBFunctionalIndexInfo(IndexDescriptor indexDesc) {
    this.indexDesc = indexDesc;
    columnToConvert = Maps.newHashMap();
    exprToConvert = Maps.newHashMap();
    pathsInExpr = Maps.newHashMap();
    newPathsForIndexedFunction = Sets.newLinkedHashSet();
    init();
  }

  private void init() {
    int count = 0;
    for(LogicalExpression indexedExpr : indexDesc.getIndexColumns()) {
      if( !(indexedExpr instanceof SchemaPath) ) {
        hasFunctionalField = true;
        SchemaPath functionalFieldPath = SchemaPath.getSimplePath("$"+count);
        newPathsForIndexedFunction.add(functionalFieldPath);

        //now we handle only cast expression
        if(indexedExpr instanceof CastExpression) {
          //We handle only CAST directly on SchemaPath for now.
          SchemaPath pathBeingCasted = (SchemaPath)((CastExpression) indexedExpr).getInput();
          addTargetPathForOriginalPath(pathBeingCasted, functionalFieldPath);
          addPathInExpr(indexedExpr, pathBeingCasted);
          exprToConvert.put(indexedExpr, functionalFieldPath);
        }

        count++;
      }
    }
  }

  private void addPathInExpr(LogicalExpression expr, SchemaPath path) {
    if (!pathsInExpr.containsKey(expr)) {
      Set<SchemaPath> newSet = Sets.newHashSet();
      newSet.add(path);
      pathsInExpr.put(expr, newSet);
    }
    else {
      pathsInExpr.get(expr).add(path);
    }
  }

  private void addTargetPathForOriginalPath(SchemaPath origPath, SchemaPath newPath) {
    if (!columnToConvert.containsKey(origPath)) {
      Set<SchemaPath> newSet = Sets.newHashSet();
      newSet.add(newPath);
      columnToConvert.put(origPath, newSet);
    }
    else {
      columnToConvert.get(origPath).add(newPath);
    }
  }


  public boolean hasFunctional() {
    return hasFunctionalField;
  }

  public IndexDescriptor getIndexDesc() {
    return indexDesc;
  }

  /**
   * getNewPath: for an original path, return new rename '$N' path, notice there could be multiple renamed paths
   * if the there are multiple functional indexes refer original path.
   * @param path
   * @return
   */
  public SchemaPath getNewPath(SchemaPath path) {
    if(columnToConvert.containsKey(path)) {
      return columnToConvert.get(path).iterator().next();
    }
    return null;
  }

  /**
   * return a plain field path if the incoming index expression 'expr' is replaced to be a plain field
   * @param expr suppose to be an indexed expression
   * @return the renamed schemapath in index table for the indexed expression
   */
  public SchemaPath getNewPathFromExpr(LogicalExpression expr) {
    if(exprToConvert.containsKey(expr)) {
      return (SchemaPath)exprToConvert.get(expr);
    }
    return null;
  }

  /**
   * @return the map of indexed expression --> the involved schema paths in a indexed expression
   */
  public Map<LogicalExpression, Set<SchemaPath>> getPathsInExpr() {
    return pathsInExpr;
  }


  public Map<LogicalExpression, LogicalExpression> getExprMap() {
    return exprToConvert;
  }

  public Set<SchemaPath> allNewSchemaPaths() {
    return newPathsForIndexedFunction;
  }

  public boolean supportEqualCharConvertToLike() {
    return true;
  }
}
