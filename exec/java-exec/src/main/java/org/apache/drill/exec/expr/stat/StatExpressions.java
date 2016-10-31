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
package org.apache.drill.exec.expr.stat;

import com.google.common.collect.Iterators;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.LogicalExpressionBase;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.common.types.TypeProtos;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;

import java.util.Iterator;


public class StatExpressions {

  public static abstract class StatExpression<V> extends LogicalExpressionBase {
    public final V min;
    public final V max;
    public final long numNulls;
    public final long numNonNulls;
    public final long numValues;
    public final TypeProtos.MajorType type;

    protected StatExpression(V min, V max, long numNulls, long numNonNulls, long numValues, TypeProtos.MajorType type) {
      super(ExpressionPosition.UNKNOWN);
      this.min = min;
      this.max = max;
      this.numNulls = numNulls;
      this.numNonNulls = numNonNulls;
      this.numValues = numValues;
      this.type = type;
    }

    @Override
    public Iterator<LogicalExpression> iterator() {
      return Iterators.emptyIterator();
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitUnknown(this, value);
    }
  }

  public static class IntStatExpression extends StatExpression<Integer> {
    public IntStatExpression(IntStatistics intStatistics, long numValues, TypeProtos.MajorType type) {
      super(intStatistics.getMin(), intStatistics.getMax(), intStatistics.getNumNulls(), numValues - intStatistics.getNumNulls(), numValues, type);
    }
  }

  public static class LongStatExpression extends StatExpression<Long> {
    public LongStatExpression(LongStatistics longStatistics, long numValues, TypeProtos.MajorType type) {
      super(longStatistics.getMin(), longStatistics.getMax(), longStatistics.getNumNulls(), numValues - longStatistics.getNumNulls(), numValues, type);
    }
  }

}
