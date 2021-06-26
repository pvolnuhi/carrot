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
import org.bigbase.carrot.redis.Commons;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Bit count operation.
 * Count the number of set bits (population counting) in a string.
 * By default all the bytes contained in the string are examined. 
 * It is possible to specify the counting operation only in an interval 
 * passing the additional arguments start and end.
 * Like for the GETRANGE command start and end can contain negative values 
 * in order to index bytes starting from the end of the string, where -1 is 
 * the last byte, -2 is the penultimate, and so forth.
 * Non-existent keys are treated as empty strings, so the command will return zero.
 * @author Vladimir Rodionov
 *
 */
public class StringBitCount extends Operation {

  long start;
  long end;
  long count;
  boolean startEndSet = false;
  
  public StringBitCount() {
    setReadOnlyOrUpdateInPlace(true);
  }
  
  @Override
  public boolean execute() {
    this.updatesCount = 0;
    if (foundRecordAddress < 0) {
      // Yes we return true
      return true;
    }
    long foundKeyPtr = DataBlock.keyAddress(foundRecordAddress);
    int foundKeySize = DataBlock.keyLength(foundRecordAddress);
    //FIXME: do we need this code?
    if (Utils.compareTo(foundKeyPtr, foundKeySize, keyAddress, keySize) != 0) {
      // Key not found
      return true;
    }
   
    long valuePtr = DataBlock.valueAddress(foundRecordAddress);
    int valueSize =DataBlock.valueLength(foundRecordAddress);
    
    if (startEndSet) {
      if (start == Commons.NULL_LONG) {
        start = 0;
      }
      // sanity checks
      if (start < 0) {
        start = valueSize + start;
      }
      // still < 0?
      if (start < 0) {
        start = 0;
      } else if (start >= valueSize) {
        return true;
      }
      
      if (end == Commons.NULL_LONG) {
        end = valueSize - 1;
      }
      
      if (end < 0) {
        end = valueSize + end;
      }
      // still < 0?
      if (end < 0) {
        return true;
      }
      
      if (end >= valueSize) {
        end = valueSize -1 ;
      }
      
      if (start > end) {
        // 0
        return true;
      }
    } else {
      start = 0;
      end = valueSize -1;
    }
    this.count = Utils.bitcount(valuePtr + start, (int)(end - start + 1));
    return true;
  }
  
 

  @Override
  public void reset() {
    super.reset();
    this.start = 0;
    this.end = 0;
    this.count = 0;
    this.startEndSet = false;
    setReadOnlyOrUpdateInPlace(true);
  }
  
  
  public void setStartEnd (long from, long to) {
    this.start = from;
    this.end = to;
    this.startEndSet = true;
  }
  
  public long getBitCount() {
    return count;
  }

}
