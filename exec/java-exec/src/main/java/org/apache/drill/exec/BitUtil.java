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
package org.apache.drill.exec;

import java.lang.reflect.Field;

public class BitUtil {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BitUtil.class);

  public static void logMemoryInfo() {
    if (!logger.isDebugEnabled()) {
      return;
    }
    try {
      Class bitsClass = Class.forName("java.nio.Bits");
      Field reservedMemoryField = bitsClass.getDeclaredField("reservedMemory");
      reservedMemoryField.setAccessible(true);
      long reservedMemory = reservedMemoryField.getLong(null);
      Field totalCapacityField = bitsClass.getDeclaredField("totalCapacity");
      totalCapacityField.setAccessible(true);
      long totalCapacity = totalCapacityField.getLong(null);
      Field maxMemoryField = bitsClass.getDeclaredField("maxMemory");
      maxMemoryField.setAccessible(true);
      long maxMemory = maxMemoryField.getLong(null);
      logger.info("reservedMemory: {} totalCapacity: {} maxMemory: {}", reservedMemory, totalCapacity, maxMemory);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
