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
 * String GETDEL operation.
 * Atomically gets key value and deletes key. 
 * Returns an error when key exists but does not hold a string value.
 * 
 * @author Vladimir Rodionov
 *
 */
public class StringGetDelete extends Operation {

  private long bufferPtr;
  private int bufferSize;
  private int size = -1; // means did not exist
  
  @Override
  public boolean execute() {
    if (foundRecordAddress > 0) {
      int vLength = DataBlock.valueLength(foundRecordAddress);
      this.size = vLength;
      if (this.size > this.bufferSize) {
        this.updatesCount = 0;
        return false;
      }
      long vPtr = DataBlock.valueAddress(foundRecordAddress);
      UnsafeAccess.copy(vPtr, bufferPtr, vLength);
    } else {
      // Does not exist
      this.updatesCount = 0;
      return false;
    }
    // now delete it
    this.updatesCount = 1;
    this.keys[0] = keyAddress;
    this.keySizes[0] = keySize;
    this.updateTypes[0] = true;// DELETE
    return true;
  }
  
  public void setBuffer(long ptr, int size) {
    this.bufferPtr = ptr;
    this.bufferSize = size;
  }
  
  /**
   * Get value previous size
   * @return size
   */
  public int getValueLength() {
    return this.size;
  }
  
  @Override
  public void reset() {
    super.reset();
    this.bufferPtr = 0;
    this.bufferSize = 0;
    this.size = -1; // means did not existed
  }
}
