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
package org.apache.drill.exec.store.mapr;

import static org.ojai.DocumentConstants.ID_KEY;

import org.apache.drill.common.expression.SchemaPath;

import com.mapr.db.DBConstants;

public class PluginConstants {

  public static final SchemaPath ID_SCHEMA_PATH = SchemaPath.getSimplePath(ID_KEY);

  public static final SchemaPath DOCUMENT_SCHEMA_PATH = SchemaPath.getSimplePath(DBConstants.DOCUMENT_FIELD);

  public static final String JSON_TABLE_SCAN_SIZE_MB = "format-maprdb.json.scanSizeMB";
  public static final int JSON_TABLE_SCAN_SIZE_MB_DEFAULT = 128;
}
