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

/**
 * 
 * String GETSET operation.
 * Atomically sets key to value and returns the 
 * old value stored at key. Returns an error when
 * key exists but does not hold a string value.
 * 
 * @author Vladimir Rodionov
 *
 */
public class StringGetSet extends Operation {

  private long bufferPtr;
  private int bufferSize;
  private int oldSize = -1; // means did not existed
  private long valuePtr;
  private int valueSize;
  
  @Override
  public boolean execute() {
    if (foundRecordAddress > 0) {
      int vLength = DataBlock.valueLength(foundRecordAddress);
      this.oldSize = vLength;
      if (this.oldSize > this.bufferSize) {
        this.updatesCount = 0;
        return false;
      }
      long vPtr = DataBlock.valueAddress(foundRecordAddress);
      UnsafeAccess.copy(vPtr, bufferPtr, vLength);
    }
    // now update
    this.updatesCount = 1;
    this.keys[0] = keyAddress;
    this.keySizes[0] = keySize;
    this.values[0] = valuePtr;
    this.valueSizes[0] = valueSize;
    return true;
  }
  
  public void setBuffer(long ptr, int size) {
    this.bufferPtr = ptr;
    this.bufferSize = size;
  }
  
  public void setValue(long ptr, int size) {
    this.valuePtr = ptr;
    this.valueSize = size;
  }
  
  /**
   * Get value previous size
   * @return size
   */
  public int getPreviousVersionLength() {
    return this.oldSize;
  }
  
  @Override
  public void reset() {
    super.reset();
    this.bufferPtr = 0;
    this.bufferSize = 0;
    this.oldSize = -1; // means did not existed
    this.valuePtr = 0;
    this.valueSize = 0;
  }
}
