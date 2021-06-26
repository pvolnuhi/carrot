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
 * String value length operation.
 * Returns the length of the string value stored at key. 
 * An error is returned when key holds a non-string value.
 * @author Vladimir Rodionov
 *
 */
public class StringLength extends Operation {


  int strlen = -1;
  
  public StringLength() {
    setReadOnlyOrUpdateInPlace(true);
  }
  
  @Override
  public boolean execute() {
    this.updatesCount = 0;
    if (foundRecordAddress < 0) {
      // Yes we return true
      return true;
    }
    
    int valueSize = DataBlock.valueLength(foundRecordAddress);
    this.strlen = valueSize;  
    return true;
  }
  

  @Override
  public void reset() {
    super.reset();
    this.strlen = -1;
    setReadOnlyOrUpdateInPlace(true);
  }
  
  /**
   * Returns string value 
   * @return value length or 0 , if not found
   */
  public int getLength() {
    return this.strlen;
  }

}
