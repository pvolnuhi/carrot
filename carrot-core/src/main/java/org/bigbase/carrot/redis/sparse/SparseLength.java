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
package org.bigbase.carrot.redis.sparse;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.util.Utils;

/**
 * String value length operation.
 * Returns the length of the string value stored at key (in bytes). 
 * An error is returned when key holds a non-string value.
 * 
 *
 */
public class SparseLength extends Operation {


  long strlen;
  
  public SparseLength() {
    setFloorKey(true);
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
    long foundValuePtr = DataBlock.valueAddress(foundRecordAddress);
    int foundValueSize = DataBlock.valueLength(foundRecordAddress);
    
    if (Utils.compareTo(foundKeyPtr, foundKeySize - Utils.SIZEOF_LONG, 
      keyAddress, keySize - Utils.SIZEOF_LONG) != 0) {
      // Key not found - this is not a sparse bitmap key
      return true;
    }
    long offset = SparseBitmaps.getChunkOffsetFromKey(foundKeyPtr, foundKeySize);
    boolean isCompressed = SparseBitmaps.isCompressed(foundValuePtr);
    foundValuePtr = isCompressed? 
        SparseBitmaps.decompress(foundValuePtr, foundValueSize - SparseBitmaps.HEADER_SIZE): 
          foundValuePtr;
    if (isCompressed) {
      foundValueSize = SparseBitmaps.CHUNK_SIZE;
    }
    this.strlen = offset/Utils.BITS_PER_BYTE + (Utils.lastBitOffset(foundValuePtr + 
      SparseBitmaps.HEADER_SIZE, foundValueSize - SparseBitmaps.HEADER_SIZE)/ Utils.BITS_PER_BYTE + 1) ;  
    return true;
  }
  
  @Override
  public void reset() {
    super.reset();
    this.strlen = 0;
    setFloorKey(true);
    setReadOnlyOrUpdateInPlace(true);
  }
  
  /**
   * Returns string value 
   * @return value length or 0 , if not found
   */
  public long getLength() {
    return this.strlen;
  }

}
