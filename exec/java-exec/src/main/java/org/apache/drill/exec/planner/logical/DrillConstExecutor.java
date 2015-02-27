/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.planner.logical;

import com.google.common.collect.ImmutableList;
import net.hydromatic.avatica.ByteString;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.fn.interpreter.InterpreterEvaluator;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.NlsString;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.List;

public class DrillConstExecutor implements RelOptPlanner.Executor {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillConstExecutor.class);


  // This is a list of all types that cannot be folded at planning time for various reasons, most of the types are
  // currently not supported at all. The reasons for the others can be found in the evaluation code in the reduce method
  public static final List<Object> NON_REDUCIBLE_TYPES =
      ImmutableList.builder().add(TypeProtos.MinorType.INTERVAL, TypeProtos.MinorType.INTERVALYEAR, TypeProtos.MinorType.INTERVALDAY, TypeProtos.MinorType.MAP,
                                  TypeProtos.MinorType.LIST, TypeProtos.MinorType.TIMESTAMPTZ, TypeProtos.MinorType.TIMETZ, TypeProtos.MinorType.LATE,
                                  TypeProtos.MinorType.TINYINT, TypeProtos.MinorType.SMALLINT, TypeProtos.MinorType.GENERIC_OBJECT, TypeProtos.MinorType.NULL,
                                  TypeProtos.MinorType.DECIMAL28DENSE, TypeProtos.MinorType.DECIMAL38DENSE, TypeProtos.MinorType.MONEY, TypeProtos.MinorType.VARBINARY,
                                  TypeProtos.MinorType.FIXEDBINARY, TypeProtos.MinorType.FIXEDCHAR, TypeProtos.MinorType.FIXED16CHAR,
                                  TypeProtos.MinorType.VAR16CHAR, TypeProtos.MinorType.UINT1, TypeProtos.MinorType.UINT2, TypeProtos.MinorType.UINT4,
                                  TypeProtos.MinorType.UINT8, TypeProtos.MinorType.DECIMAL9, TypeProtos.MinorType.DECIMAL18,
                                  TypeProtos.MinorType.DECIMAL28SPARSE, TypeProtos.MinorType.DECIMAL38SPARSE).build();

  FunctionImplementationRegistry funcImplReg;
  UdfUtilities udfUtilities;
  BufferAllocator allocator;

  public DrillConstExecutor (FunctionImplementationRegistry funcImplReg, BufferAllocator allocator, UdfUtilities udfUtilities) {
    this.funcImplReg = funcImplReg;
    this.udfUtilities = udfUtilities;
    this.allocator = allocator;
  }

  @Override
  public void reduce(RexBuilder rexBuilder, List<RexNode> constExps, List<RexNode> reducedValues) {
    for (RexNode newCall : constExps) {
      LogicalExpression logEx = DrillOptiq.toDrill(new DrillParseContext(), null /* input rel */, newCall);

      ErrorCollectorImpl errors = new ErrorCollectorImpl();
      LogicalExpression materializedExpr = ExpressionTreeMaterializer.materialize(logEx, null, errors, funcImplReg);
      if (errors.getErrorCount() != 0) {
        logger.error("Failure while materializing expression [{}].  Errors: {}", newCall, errors);
      }

      final MaterializedField outputField = MaterializedField.create("outCol", materializedExpr.getMajorType());
      ValueVector vector = TypeHelper.getNewVector(outputField, allocator);
      vector.allocateNewSafe();
      InterpreterEvaluator.evaluateConstantExpr(vector, udfUtilities, materializedExpr);

      try {
        switch(materializedExpr.getMajorType().getMinorType()) {
          case INT:
            reducedValues.add(rexBuilder.makeExactLiteral(new BigDecimal((Integer)vector.getAccessor().getObject(0))));
            break;
          case BIGINT:
            reducedValues.add(rexBuilder.makeExactLiteral(new BigDecimal((Long)vector.getAccessor().getObject(0))));
            break;
          case FLOAT4:
            reducedValues.add(rexBuilder.makeApproxLiteral(new BigDecimal((Float)vector.getAccessor().getObject(0))));
            break;
          case FLOAT8:
            reducedValues.add(rexBuilder.makeApproxLiteral(new BigDecimal((Double)vector.getAccessor().getObject(0))));
            break;
          case VARCHAR:
            reducedValues.add(rexBuilder.makeCharLiteral(new NlsString(new String(((VarCharVector) vector).getAccessor().get(0), "UTF-8"), null, null)));
            break;
          case BIT:
            reducedValues.add(rexBuilder.makeLiteral(((BitVector) vector).getAccessor().get(0) == 1 ? true : false));
            break;
          case DATE:
            reducedValues.add(rexBuilder.makeDateLiteral(((DateTime)vector.getAccessor().getObject(0)).toCalendar(null)));
            break;
          case TIME:
            // TODO - review the given precision value, chose the maximum available on SQL server
            // https://msdn.microsoft.com/en-us/library/bb677243.aspx
            reducedValues.add(rexBuilder.makeTimeLiteral(((DateTime) vector.getAccessor().getObject(0)).toCalendar(null), 7));
          case TIMESTAMP:
            // TODO - review the given precision value, could not find a good recommendation, reusing value of 7 from time
            reducedValues.add(rexBuilder.makeTimestampLiteral(((DateTime) vector.getAccessor().getObject(0)).toCalendar(null), 7));
            break;

          case DECIMAL9:
          case DECIMAL18:
          case DECIMAL28SPARSE:
          case DECIMAL38SPARSE:
            // TODO - figure out the best thing to do here, had some issues with creating decimal literals, I'm not
            // sure the calcite code is correct here. Example expression that fails, 123456789.000000000 + 0
            // The call to bd.unscaledValue().longValue() on the passed BigDecial is returning a value that fails
            // the assert two lines down: assert BigDecimal.valueOf(l, scale).equals(bd);
            // currently to make this fold it must be put in the filter condition, project expression reduction is
            // not planning correctly.
            // Could not fix this by adding the decimal types to the list of NON_REDUCIBLE_TYPES, to resolve differences
            // in scale and precision, calcite appears to be inserting casts, which are always considered constant
            //    - this could be an issue for all of the NON_REDUCIBLE_TYPES, as any casts may be evaluated anyway and
            //      then fail here

            // TODO - fix - workaround for now, just create an approximate literal, this will break an equality check with
            // a decimal type
            reducedValues.add(rexBuilder.makeApproxLiteral((BigDecimal) vector.getAccessor().getObject(0)));
            break;

          // TODO - tried to test this with a call to convertToNullableVARBINARY, but the interpreter could not be found for it
          // disabling for now and adding VARBINARY to the list of unfoldable types
          case VARBINARY:
            reducedValues.add(rexBuilder.makeBinaryLiteral(new ByteString((byte[]) vector.getAccessor().getObject(0))));
            // fall through for now

          // TODO - not sure how to populate the SqlIntervalQualifier parameter of the rexBuilder.makeIntervalLiteral method
          // will make these non-reducible at planning time for now
          case INTERVAL:
          case INTERVALYEAR:
          case INTERVALDAY:
            // fall through for now
//            reducedValues.add(rexBuilder.makeIntervalLiteral(((Period) vector.getAccessor().getObject(0)));

          // TODO - map and list are used in Drill but currently not expressible as literals, these can however be
          // outputs of functions that take literals as inputs (such as a convert_fromJSON with a literal string
          // as input), so we need to identify functions with these return types as non-foldable until we have a
          // literal representation for them
          case MAP:
          case LIST:
            // fall through for now

          // currently unsupported types
          case TIMESTAMPTZ:
          case TIMETZ:
          case LATE:
          case TINYINT:
          case SMALLINT:
          case GENERIC_OBJECT:
          case NULL:
          case DECIMAL28DENSE:
          case DECIMAL38DENSE:
          case MONEY:
          case FIXEDBINARY:
          case FIXEDCHAR:
          case FIXED16CHAR:
          case VAR16CHAR:
          case UINT1:
          case UINT2:
          case UINT4:
          case UINT8:
            throw new DrillRuntimeException("Unsupported type returned during planning time constant expression folding: "
                + materializedExpr.getMajorType().getMinorType() );
        }
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException("Invalid string returned from constant expression evaluation");
      }
      vector.clear();
    }
  }
}


