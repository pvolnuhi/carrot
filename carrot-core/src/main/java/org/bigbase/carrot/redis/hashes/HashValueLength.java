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
package org.bigbase.carrot.redis.hashes;

import static org.bigbase.carrot.redis.util.Commons.elementAddressFromKey;
import static org.bigbase.carrot.redis.util.Commons.elementSizeFromKey;
import static org.bigbase.carrot.redis.util.Commons.keySizeWithPrefix;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.util.Utils;

public class HashValueLength extends Operation{

  private int foundValueSize = -1;
  
  public HashValueLength() {
    setFloorKey(true);
    setReadOnlyOrUpdateInPlace(true);
  }

  @Override
  public void reset() {
    super.reset();
    setFloorKey(true);
    foundValueSize = -1;
    setReadOnlyOrUpdateInPlace(true);
  }
    
  public int getFoundValueSize() {
    return foundValueSize;
  }
  
  @Override
  public boolean execute() {
    if (foundRecordAddress <=0) {
      return false;
    }
    // check prefix
    int setKeySize = keySizeWithPrefix(keyAddress);
    int foundKeySize = DataBlock.keyLength(foundRecordAddress);
    if (foundKeySize <= setKeySize) {
      return false;
    }
    long foundKeyAddress = DataBlock.keyAddress(foundRecordAddress);
    // Prefix keys must be equals
    if (Utils.compareTo(keyAddress, setKeySize, foundKeyAddress, 
      setKeySize) != 0) {
      return false;
    }
    
    long fieldPtr = elementAddressFromKey(keyAddress);
    int fieldSize = elementSizeFromKey(keyAddress, keySize);
    // Set no updates
    updatesCount = 0;
    long address = Hashes.exactSearch(foundRecordAddress, fieldPtr, fieldSize);
    if (address < 0) {
      this.foundValueSize = -1;
      return false;
    }
    // size of a field-value pair
    foundValueSize = Hashes.getValueSize(address);    
    return true;
  }
}
