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
package org.apache.drill.exec.util;

import java.lang.reflect.Field;
import java.nio.ByteOrder;

import sun.misc.Unsafe;

/** Exposes advanced Memory Access APIs for Little-Endian / Unaligned platforms */
@SuppressWarnings("restriction")
public final class MemoryUtils {

  // Ensure this is a little-endian hardware */
  static {
    if (ByteOrder.nativeOrder() != ByteOrder.LITTLE_ENDIAN) {
      throw new IllegalStateException("Drill only runs on LittleEndian systems.");
    }
  }

  /** Java's unsafe object */
  private static Unsafe UNSAFE;

  static {
    try {
      Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
      theUnsafe.setAccessible(true);
      UNSAFE = (Unsafe) theUnsafe.get(null);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Byte arrays offset */
  private static final long BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

  /** Number of bytes in a long */
  public static final int LONG_NUM_BYTES  = 8;
  /** Number of bytes in an int */
  public static final int INT_NUM_BYTES   = 4;
  /** Number of bytes in a short */
  public static final int SHORT_NUM_BYTES = 2;

//----------------------------------------------------------------------------
// APIs
//----------------------------------------------------------------------------

  /**
   * @param data source byte array
   * @param index index within the byte array
   * @return short value starting at data+index
   */
  public static short getShort(byte[] data, int index) {
    assert index >= 0 && (index + SHORT_NUM_BYTES) <= data.length;
    return UNSAFE.getShort(data, BYTE_ARRAY_OFFSET + index);
  }

  /**
   * @param data source byte array
   * @param index index within the byte array
   * @return integer value starting at data+index
   */
  public static int getInt(byte[] data, int index) {
    assert index >= 0 && (index + INT_NUM_BYTES) <= data.length;
    return UNSAFE.getInt(data, BYTE_ARRAY_OFFSET + index);
  }

  /**
   * @param data data source byte array
   * @param index index within the byte array
   * @return long value read at data_index
   */
  public static long getLong(byte[] data, int index) {
    assert index >= 0 && (index + LONG_NUM_BYTES) <= data.length;
    return UNSAFE.getLong(data, BYTE_ARRAY_OFFSET + index);
  }

  /**
   * Read a short at position src+srcIndex and copy it to the dest+destIndex
   *
   * @param src source byte array
   * @param srcIndex source index
   * @param dest destination byte array
   * @param destIndex destination index
   */
  public static void putShort(byte[] src, int srcIndex, byte[] dest, int destIndex) {
    assert srcIndex >= 0  && (srcIndex  + SHORT_NUM_BYTES) <= src.length;
    assert destIndex >= 0 && (destIndex + SHORT_NUM_BYTES) <= dest.length;

    short value = UNSAFE.getShort(src, BYTE_ARRAY_OFFSET + srcIndex);
    UNSAFE.putShort(dest, (BYTE_ARRAY_OFFSET + destIndex), value);
  }

  /**
   * Read an integer at position src+srcIndex and copy it to the dest+destIndex
   *
   * @param src source byte array
   * @param srcIndex source index
   * @param dest destination byte array
   * @param destIndex destination index
   */
  public static void putInt(byte[] src, int srcIndex, byte[] dest, int destIndex) {
    assert srcIndex >= 0  && (srcIndex  + INT_NUM_BYTES) <= src.length;
    assert destIndex >= 0 && (destIndex + INT_NUM_BYTES) <= dest.length;

    int value = UNSAFE.getInt(src, BYTE_ARRAY_OFFSET + srcIndex);
    UNSAFE.putInt(dest, (BYTE_ARRAY_OFFSET + destIndex), value);
  }

  /**
   * Read a long at position src+srcIndex and copy it to the dest+destIndex
   *
   * @param src source byte array
   * @param srcIndex source index
   * @param dest destination byte array
   * @param destIndex destination index
   */
  public static void putLong(byte[] src, int srcIndex, byte[] dest, int destIndex) {
    assert srcIndex >= 0  && (srcIndex  + LONG_NUM_BYTES) <= src.length;
    assert destIndex >= 0 && (destIndex + LONG_NUM_BYTES) <= dest.length;

    long value = UNSAFE.getLong(src, BYTE_ARRAY_OFFSET + srcIndex);
    UNSAFE.putLong(dest, (BYTE_ARRAY_OFFSET + destIndex), value);
  }

  /**
   * Copy a short value to the dest+destIndex
   *
   * @param dest destination byte array
   * @param destIndex destination index
   * @param value a short value
   */
  public static void putShort(byte[] dest, int destIndex, short value) {
    assert destIndex >= 0 && (destIndex + SHORT_NUM_BYTES) <= dest.length;
    UNSAFE.putShort(dest, (BYTE_ARRAY_OFFSET + destIndex), value);
  }

  /**
   * Copy an integer value to the dest+destIndex
   *
   * @param dest destination byte array
   * @param destIndex destination index
   * @param value an int value
   */
  public static void putInt(byte[] dest, int destIndex, int value) {
    assert destIndex >= 0 && (destIndex + INT_NUM_BYTES) <= dest.length;
    UNSAFE.putInt(dest, (BYTE_ARRAY_OFFSET + destIndex), value);
  }

  /**
   * Copy a long value to the dest+destIndex
   *
   * @param dest destination byte array
   * @param destIndex destination index
   * @param value a long value
   */
  public static void putLong(byte[] dest, int destIndex, long value) {
    assert destIndex >= 0 && (destIndex + LONG_NUM_BYTES) <= dest.length;
    UNSAFE.putLong(dest, (BYTE_ARRAY_OFFSET + destIndex), value);
  }

// ----------------------------------------------------------------------------
// Local Implementation
// ----------------------------------------------------------------------------

  /** Disable class instantiation */
  private MemoryUtils() {
  }

}
