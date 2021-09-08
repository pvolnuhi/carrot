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
package org.bigbase.carrot.util;

/**
 * Key - Value class
 * 
 *
 */
public class KeyValue implements Comparable<KeyValue>{

  /*
   * Key address
   */
  public long keyPtr;
  /*
   * Key size
   */
  public int keySize;
  /*
   * Value address
   */
  public long valuePtr;
  /*
   * Value size 
   */
  public int valueSize;
  
 /**
  * Constructor
  * @param keyPtr key address
  * @param keySize key size
  * @param valuePtr value address
  * @param valueSize value size
  */
  public KeyValue(long keyPtr, int keySize, long valuePtr, int valueSize) {
    this.keyPtr = keyPtr;
    this.keySize = keySize;
    this.valuePtr = valuePtr;
    this.valueSize = valueSize;
  }
  
  @Override
  public int compareTo(KeyValue o) {
    return Utils.compareTo(keyPtr, keySize, o.keyPtr, o.keySize);
  }

  @Override
  public int hashCode() {
    return Math.abs(Utils.murmurHash(keyPtr, keySize, 0));
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || ! (o instanceof KeyValue)) {
      return false;
    }
    return compareTo((KeyValue) o) == 0;
  }
  
  public void free() {
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(valuePtr);
  }
  @Override
  public String toString() {
    return Utils.toString(keyPtr, keySize) + " "+ Utils.toString(valuePtr, valueSize);
  }  
  
}
