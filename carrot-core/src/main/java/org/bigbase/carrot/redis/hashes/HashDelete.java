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

import static org.bigbase.carrot.redis.util.Commons.addNumElements;
import static org.bigbase.carrot.redis.util.Commons.elementAddressFromKey;
import static org.bigbase.carrot.redis.util.Commons.elementSizeFromKey;
import static org.bigbase.carrot.redis.util.Commons.isFirstKey;
import static org.bigbase.carrot.redis.util.Commons.keySize;
import static org.bigbase.carrot.redis.util.Commons.keySizeWithPrefix;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.redis.util.Commons;
import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;


/**
 * This read-modify-write mutation is executed atomically and isolated
 * It deletes field from a given Hash, defined by a Key
 * @author Vladimir Rodionov
 *
 */
public class HashDelete extends Operation{

  /**
   * Parent big sorted map
   */
  BigSortedMap map;
  
  /**
   * Buffer to return deleted value to
   */
  long buffer;
  
  /**
   * Buffer size
   */
  int bufferSize;
  /**
   * Value size (deleted) full
   */
  int valueSize = 0;

  
  boolean checkForEmpty = false;
  /**
   * Constructor
   */
  
  public HashDelete() {
    setFloorKey(true);
  }
  
  @Override
  public void reset() {
    super.reset();
    setFloorKey(true);
    this.map = null;
    this.buffer = 0;
    this.bufferSize = 0;
    this.valueSize = 0;
    this.checkForEmpty = false;
  }
  
  public boolean checkForEmpty() {
    return this.checkForEmpty;
  }
  
  /**
   * Gets value size
   * @return serialized value size
   */
  public int getValueSize() {
    return this.valueSize;
  }
  
  /**
   * Sets the buffer for return value
   * @param ptr buffer address
   * @param size buffer size
   */
  
  public void setBuffer(long ptr, int size) {
    this.buffer = ptr;
    this.bufferSize = size;
  }
  
  /**
   * Sets parent sorted map
   * @param map sorted map
   */
  public void setMap(BigSortedMap map) {
    this.map = map;
  }
  
  @Override
  public boolean execute() {
    if (foundRecordAddress <=0) {
      return false;
    }
    // check prefix
    int hashKeySizeWithPrefix = keySizeWithPrefix(keyAddress);
    int foundKeySize = DataBlock.keyLength(foundRecordAddress);
    if (foundKeySize <= hashKeySizeWithPrefix) {
      // Hash does not exists
      return false;
    }
    long foundKeyAddress = DataBlock.keyAddress(foundRecordAddress);
    int hashKeySize  = hashKeySizeWithPrefix - Commons.KEY_PREFIX_SIZE;
    boolean isFirstKey = isFirstKey(foundKeyAddress, foundKeySize, hashKeySize); 
    // Prefix keys must be equals
    if (Utils.compareTo(keyAddress, hashKeySizeWithPrefix , foundKeyAddress, 
      hashKeySizeWithPrefix) != 0) {
      // Hash does not exists
      return false;
    }    
    long fieldPtr = elementAddressFromKey(keyAddress);
    int fieldSize = elementSizeFromKey(keyAddress, keySize);
    // First two bytes are number of elements in a value
    long addr = Hashes.exactSearch(foundRecordAddress, fieldPtr, fieldSize);
    if (addr < 0) {
      // Field not found
      return false;
    }
    // found
    int fieldSizeSize = Utils.sizeUVInt(fieldSize);
    int fieldValueSize = Utils.readUVInt(addr + fieldSizeSize);
    int fieldValueSizeSize = Utils.sizeUVInt(fieldValueSize);
    
    this.valueSize = fieldValueSize + fieldValueSizeSize;
    if (buffer > 0 && bufferSize >= fieldValueSize + fieldValueSizeSize) {
      // Copy value into the buffer
      Utils.writeUVInt(buffer, fieldValueSize);
      UnsafeAccess.copy(addr + fieldSize + fieldSizeSize + fieldValueSizeSize, 
        buffer + fieldValueSizeSize, fieldValueSize);
    } else if (buffer > 0 && bufferSize < fieldValueSize + fieldValueSizeSize) {
      return false;
    }
    
    int toCut = fieldSizeSize + fieldSize + fieldValueSizeSize + fieldValueSize;
    long valueAddress = DataBlock.valueAddress(foundRecordAddress);
    // decrement number of elements in this value
    int numElements = addNumElements(valueAddress, -1);
    if (numElements == 0) {
      this.checkForEmpty = true;
    } 
    int valueSize = DataBlock.valueLength(foundRecordAddress);
    int newValueSize = valueSize - toCut;
    Hashes.checkValueArena(newValueSize);
    long ptr = Hashes.valueArena.get();
    // TODO: check this
    UnsafeAccess.copy(valueAddress, ptr, addr - valueAddress);
    UnsafeAccess.copy(addr + toCut, ptr + addr - valueAddress, valueSize - toCut - (addr - valueAddress));
    
    // set # of updates to 1
    this.updatesCount = 1;
    this.keys[0] = foundKeyAddress;
    this.keySizes[0] = foundKeySize;
    this.values[0] = ptr;
    this.valueSizes[0] = valueSize - toCut;
    //TODO: Verify that we do not need to check canDelete to delete
    if (numElements == 0 && !isFirstKey /*&& canDelete(foundKeyAddress, foundKeySize)*/) {
      // Delete Key, b/c its empty
      //TODO: we postpone deleting empty first key
      this.updateTypes[0] = true;
    } 
    return true;
  }

}
