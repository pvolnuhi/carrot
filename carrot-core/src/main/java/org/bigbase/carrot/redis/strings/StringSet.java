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
import org.bigbase.carrot.redis.MutationOptions;
import org.bigbase.carrot.util.Utils;

/**
 * String SET operation.
 * Set key to hold the string value. If key already holds a value, it is overwritten, 
 * regardless of its type. Any previous time to live associated with the key is 
 * discarded on successful SET operation (if keepTTL == false).
 * @author Vladimir Rodionov
 *
 */
public class StringSet extends Operation {


  private long valuePtr;
  private int valueSize;
  private boolean keepTTL = false;
  private MutationOptions opts;
  
  public StringSet() {
    //TODO remove after testing
    //setFloorKey(true);
  }
  
  @Override
  public boolean execute() {
    boolean keyExists = foundRecordAddress > 0; 
    if (keyExists && opts == MutationOptions.NX ||
        !keyExists && opts == MutationOptions.XX) {
      return false;
    }
    if (keyExists && keepTTL) {
      this.expire = DataBlock.getRecordExpire(foundRecordAddress);
    }
    // now update
    this.updatesCount = 1;
    this.keys[0] = keyAddress;
    this.keySizes[0] = keySize;
    this.values[0] = valuePtr;
    this.valueSizes[0] = valueSize;
    return true;
  }
  
  /**
   * Set keep TimeToLive
   * @param b value
   */
  public void setKeepTTL(boolean b) {
    this.keepTTL = b;
  }
  
  /**
   * set mutation options
   * @param opts options
   */
  public void setMutationOptions(MutationOptions opts) {
    this.opts = opts;
  }
  
  /**
   * Set value
   * @param ptr value address
   * @param size value size
   */
  public void setValue(long ptr, int size) {
    this.valuePtr = ptr;
    this.valueSize = size;
  }
  
  @Override
  public void reset() {
    super.reset();
    this.valuePtr = 0;
    this.valueSize = 0;
    this.keepTTL = false;
    this.opts = null;
  }
}
