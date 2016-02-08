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

<@pp.dropOutputFile />
<#assign className="TimestampArithmeticFunctions" />
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/${className}.java" />
<#include "/@includes/license.ftl" />

    package org.apache.drill.exec.expr.fn.impl;

    import org.apache.drill.exec.expr.DrillSimpleFunc;
    import org.apache.drill.exec.expr.annotations.FunctionTemplate;
    import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
    import org.apache.drill.exec.expr.annotations.Output;
    import org.apache.drill.exec.expr.annotations.Param;
    import org.apache.drill.exec.expr.annotations.Workspace;
    import org.apache.drill.exec.expr.holders.IntHolder;
    import org.apache.drill.exec.expr.holders.BigIntHolder;
    import org.apache.drill.exec.expr.holders.TimeStampHolder;
    import org.apache.drill.exec.expr.holders.VarCharHolder;
    import org.joda.time.MutableDateTime;


public class ${className} {

<#macro intervalBlock interval, tsi>
    try {
    String stringValue = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(${interval}.start, ${interval}.end, ${interval}.buffer);
    ${tsi} = org.apache.drill.exec.util.TSI.valueOf(stringValue);
    } catch (IllegalArgumentException e) {
    throw new IllegalArgumentException("Incorrect interval timestamp value. Should be one of the following: "
    + java.util.Arrays.toString(org.apache.drill.exec.util.TSI.values()));
    }
</#macro>

@FunctionTemplate(name = "timestampadd", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
public static class TimestampAdd implements DrillSimpleFunc {

  @Param VarCharHolder interval;
  @Param IntHolder count;
  @Param TimeStampHolder in;
  @Workspace MutableDateTime temp;
  @Output TimeStampHolder out;

  public void setup() {
    temp = new MutableDateTime(org.joda.time.DateTimeZone.UTC);
  }

  public void eval() {

    org.apache.drill.exec.util.TSI tsi;
    <@intervalBlock interval="interval" tsi="tsi"/>

    temp.setMillis(in.value);
    tsi.addCount(temp, count.value);
    out.value = temp.getMillis();

  }
}

@FunctionTemplate(name = "timestampdiff", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
public static class TimestampDiff implements DrillSimpleFunc {

  @Param VarCharHolder interval;
  @Param TimeStampHolder left;
  @Param TimeStampHolder right;
  @Output BigIntHolder out;

  public void setup() {
  }

  public void eval() {

    org.apache.drill.exec.util.TSI tsi;
    <@intervalBlock interval="interval" tsi="tsi"/>

    out.value = tsi.getDiff(new org.joda.time.Interval(right.value, left.value));

  }
}

}