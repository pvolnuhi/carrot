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
package org.bigbase.carrot.redis.strings;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Sets or clears the bit at offset in the string value stored at key.
 * The bit is either set or cleared depending on value, which can be either 0 or 1.
 * When key does not exist, a new string value is created. The string is grown to make 
 * sure it can hold a bit at offset. The offset argument is required to be greater than or 
 * equal to 0, and smaller than 232 (this limits bitmaps to 512MB). When the string at key 
 * is grown, added bits are set to 0.
 * 
 *
 */
public class StringSetBit extends Operation {

  long offset;
  int bit;
  int oldBit;
  
  @Override
  public boolean execute() {
    this.updatesCount = 0;
//    if (foundRecordAddress < 0) {
//      // Yes we return true
//      return true;
//    }
//    long foundKeyPtr = DataBlock.keyAddress(foundRecordAddress);
//    int foundKeySize = DataBlock.keyLength(foundRecordAddress);
//    boolean existKey = true;
//    if (Utils.compareTo(foundKeyPtr, foundKeySize, keyAddress, keySize) != 0) {
//      // Key not found
//      existKey = false;
//    }
    long valuePtr = 0;
    int valueSize = 0;
    if (foundRecordAddress > 0) {
      valuePtr = DataBlock.valueAddress(foundRecordAddress);
      valueSize = DataBlock.valueLength(foundRecordAddress);
      if (offset >= ((long) valueSize) * Utils.BITS_PER_BYTE) {
        int newSize = (int)(offset / Utils.BITS_PER_BYTE) + 1;
        long oldValuePtr = valuePtr;
        valuePtr = UnsafeAccess.mallocZeroed(newSize);
        UnsafeAccess.copy(oldValuePtr, valuePtr, valueSize);
        valueSize = newSize;
        this.updatesCount = 1;
        this.keys[0] = keyAddress;
        this.keySizes[0] = keySize;
        this.values[0] = valuePtr;
        this.valueSizes[0] = valueSize;
        //TODO: check if we have no memory leaks here
        this.reuseValues[0] = true;
      }
    } else {
      // new K-V
      valueSize = (int)(offset / Utils.BITS_PER_BYTE) + 1;
      valuePtr = UnsafeAccess.mallocZeroed(valueSize);
      this.updatesCount = 1;
      this.keys[0] = keyAddress;
      this.keySizes[0] = keySize;
      this.values[0] = valuePtr;
      this.valueSizes[0] = valueSize;
      this.reuseValues[0] = true;

    }
    if (this.updatesCount == 0) {
      setUpdateInPlace(true);
    }
    this.bit = getsetbit(valuePtr, valueSize);
    return true;
  }
  
  private int getsetbit(long valuePtr, int valueSize) {
    int n = (int)(this.offset / Utils.BITS_PER_BYTE);
    int rem = (int)(offset - ((long) n) * Utils.BITS_PER_BYTE);
    byte b = UnsafeAccess.toByte(valuePtr + n);
    oldBit = (b >>> (7 - rem)) & 1;
    if (bit == 1) {
      b |= bit << (7 - rem);
    } else {
      b &= ~(1 << (7 - rem));
    }
    UnsafeAccess.putByte(valuePtr + n, b);
    return oldBit;
  }

  @Override
  public void reset() {
    super.reset();
    this.bit = 0;
    this.oldBit = 0;
    this.offset = 0;
  }
  
  /**
   * Set offset for this operation
   * @param offset offset in bits
   */
  public void setOffset (long offset) {
    // Offset is always >= 0;
    this.offset = offset;
  }
  
  /**
   * Sets new bit
   * @param bit value
   */
  public void setBit(int bit) {
    this.bit = bit;
  }
  /**
   * Returns old bit value at offset
   * @return bit value: 0 or 1
   */
  public int getOldBit() {
    return oldBit;
  }

}
