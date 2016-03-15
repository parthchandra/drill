<#-- Licensed to the Apache Software Foundation (ASF) under one or more contributor
  license agreements. See the NOTICE file distributed with this work for additional
  information regarding copyright ownership. The ASF licenses this file to
  You under the Apache License, Version 2.0 (the "License"); you may not use
  this file except in compliance with the License. You may obtain a copy of
  the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required
  by applicable law or agreed to in writing, software distributed under the
  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
  OF ANY KIND, either express or implied. See the License for the specific
  language governing permissions and limitations under the License. -->


<#include "*/generic.ftl">
<#macro page_head>
</#macro>

<#macro page_body>
<a href="/queries">back</a><br/>
<div class="page-header">
</div>
<h4>Drillbits</h4>
<!--<div align="right">
    <a href="http://drill.apache.org/docs/planning-and-execution-options/">Documentation</a>
</div>
-->
<div class="table-responsive">
    <table class="table table-bordered text-right">
        <tbody>
          <tr>
            <th>Address</th>
            <th>Control Port</th>
            <th>Data Port</th>
            <th>User Port</th>
          </tr>
          <#list model as drillbit>
          <tr>
              <td>${drillbit.getAddress()}</td>
              <td>${drillbit.getControlPort()}</td>
              <td>${drillbit.getDataPort()}</td>
              <td>${drillbit.getUserPort()}</td>
          </tr>
          </#list>
        </tbody>
    </table>
</div>
</#macro>

<@page_html/>

