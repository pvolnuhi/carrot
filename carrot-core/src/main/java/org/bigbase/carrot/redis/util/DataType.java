/**
 *    Copyright (C) 2021-present Carrot, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 */
package org.bigbase.carrot.redis.util;

import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Data types of Key-Values
 */
public enum DataType {
  SYSTEM, STRING, LIST, SET, ZSET, HASH, SBITMAP, BTREE;
  
  public static DataType getDataType(long keyPtr) {
    int ordinal = UnsafeAccess.toByte(keyPtr);
    if (ordinal >= 0 && ordinal <= DataType.BTREE.ordinal()) { 
      return DataType.values()[ordinal];
    } else {
      return null;
    }
  }
  
  /**
   * Translate internal key address to external key address
   * @param ptr internal key address
   * @return external key address
   */
  public static long internalKeyToExternalKeyAddress(long ptr) {
    return ptr + Utils.SIZEOF_BYTE + Utils.SIZEOF_INT;
  }
  
  /**
   * Returns size of an external key
   * @param ptr internal key address
   * @return size
   */
  public static int externalKeyLength (long ptr) {
    return UnsafeAccess.toInt(ptr + Utils.SIZEOF_BYTE);
  }
  
}
