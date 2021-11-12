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
package org.bigbase.carrot.ops;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * This example of specific Update - atomic counter 
 * 
 *
 */
public class IncrementLong extends Operation {
  
  static ThreadLocal<Long> buffer = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(Utils.SIZEOF_LONG);
    }
  };
  
  long value;
  
  public IncrementLong() {
    setReadOnly(true);
  }
  
  /**
   * Set increment value
   * @param v value
   */
  public void setIncrement(long v) {
    this.value = v;
  }
  
  /**
   * Get increment value
   * @return
   */
  public long getIncrement() {
    return this.value;
  }
  /**
   * Get value after increment
   * @return value after increment
   */
  public long getValue() {
    return this.value;
  }
  
  @Override
  public void reset() {
    super.reset();
    value = 0;
    setReadOnly(true);
  }
  
  @Override
  public boolean execute() {
    long v = 0;
    if (foundRecordAddress > 0) {
      int vSize = DataBlock.valueLength(foundRecordAddress);
      if (vSize != Utils.SIZEOF_LONG /*long size*/) {
        return false;
      }
      long ptr = DataBlock.valueAddress(foundRecordAddress);
      v = UnsafeAccess.toLong(ptr);
      this.value += v;
      UnsafeAccess.putLong(ptr, value);
      this.updatesCount = 0;
      return true;
    }
    this.value += v;
    UnsafeAccess.putLong(buffer.get(), value);
    // set updateCounts to 0 - we update in place
    this.updatesCount = 1;
    this.keys[0] = keyAddress;
    this.keySizes[0] = keySize;
    this.values[0] = buffer.get();
    this.valueSizes[0] = Utils.SIZEOF_LONG;
    return true;
  }
}
