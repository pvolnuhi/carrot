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

import static org.bigbase.carrot.redis.util.Commons.KEY_SIZE;
import static org.bigbase.carrot.redis.util.Commons.NUM_ELEM_SIZE;
import static org.bigbase.carrot.redis.util.Commons.addNumElements;
import static org.bigbase.carrot.redis.util.Commons.canSplit;
import static org.bigbase.carrot.redis.util.Commons.elementAddressFromKey;
import static org.bigbase.carrot.redis.util.Commons.elementSizeFromKey;
import static org.bigbase.carrot.redis.util.Commons.keySize;
import static org.bigbase.carrot.redis.util.Commons.keySizeWithPrefix;
import static org.bigbase.carrot.redis.util.Commons.numElementsInValue;
import static org.bigbase.carrot.redis.util.Commons.setNumElements;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.redis.util.Commons;
import org.bigbase.carrot.redis.util.DataType;
import org.bigbase.carrot.redis.util.MutationOptions;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * This read-modify-write mutation is executed atomically and isolated
 * It adds new field - value to a given Hash, defined by a Key
 * 
 *
 */
public class HashSet extends Operation{
  
  private static ThreadLocal<Long> keyArena = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(512);
    }
  };
  
  private static ThreadLocal<Integer> keyArenaSize = new ThreadLocal<Integer>() {
    @Override
    protected Integer initialValue() {
      return 512;
    }
  };
  
  private long fieldValueAddress;
  
  private int fieldValueSize;
  
  private MutationOptions opts;
  
  private int added = 0;
  /**
   * Checks key arena size
   * @param required size
   */
  
  static void checkKeyArena (int required) {
    int size = keyArenaSize.get();
    if (size >= required ) {
      return;
    }
    long ptr = UnsafeAccess.realloc(keyArena.get(), required);
    keyArena.set(ptr);
    keyArenaSize.set(required);
  }
  
  /**
   * Build key for Set. It uses thread local key arena 
   * TODO: data type prefix
   * @param keyPtr original key address
   * @param keySize original key size
   * @param elPtr element address
   * @param elSize element size
   * @return new key size 
   */
    
   
  private static int buildKey( long keyPtr, int keySize, long fieldPtr, int fieldSize) {
    checkKeyArena(keySize + KEY_SIZE + fieldSize + Utils.SIZEOF_BYTE);
    long arena = keyArena.get();
    int kSize = KEY_SIZE + keySize + Utils.SIZEOF_BYTE;
    UnsafeAccess.putByte(arena, (byte) DataType.HASH.ordinal());
    UnsafeAccess.putInt(arena + Utils.SIZEOF_BYTE, keySize);
    UnsafeAccess.copy(keyPtr, arena + KEY_SIZE + Utils.SIZEOF_BYTE, keySize);
    if (fieldPtr > 0) {
      UnsafeAccess.copy(fieldPtr, arena + kSize, fieldSize);
      kSize += fieldSize;
    }
    return kSize;
  }
  
  public HashSet() {
    setFloorKey(true);
  }
  
  /**
   * Get number of added fields
   * @return number of added fields
   */
  public int getAdded() {
    return added;
  }
  
  @Override
  public void reset() {
    super.reset();
    setFloorKey(true);
    this.fieldValueAddress = 0;
    this.fieldValueSize = 0;
    this.opts = null;
    this.added = 0;
  }
  
  /**
   * Sets field's value address
   * @param address value address
   * @param size value size
   */
  public void setFieldValue(long address, int size) {
    this.fieldValueAddress = address;
    this.fieldValueSize = size;
  }
  
  public void setOptions(MutationOptions opts) {
    this.opts = opts;
  }
  
  @Override
  public boolean execute() {
    if (foundRecordAddress <= 0 && opts == MutationOptions.XX) {
      return false;
    }
    // check prefix
    int setKeySize = keySizeWithPrefix(keyAddress);
    int foundKeySize = foundRecordAddress > 0? DataBlock.keyLength(foundRecordAddress): 0;
    long foundKeyAddress = foundRecordAddress > 0? DataBlock.keyAddress(foundRecordAddress): 0;
    long fieldPtr = elementAddressFromKey(keyAddress);
    int fieldSize = elementSizeFromKey(keyAddress, keySize);
        
    // Prefix keys must be equals if set exists, otherwise insert new set KV
    if ((foundKeySize <= setKeySize) || 
        Utils.compareTo(keyAddress, setKeySize, foundKeyAddress, 
      setKeySize) != 0) {
      // Set does not exist yet
      // Insert new set KV if opts != XX
      if (opts != MutationOptions.XX) {
        insertFirstKVandFieldValue(fieldPtr, fieldSize);
        return true;
      } else {
        return false;
      }
    }
    // Set exists
    long valueAddress = DataBlock.valueAddress(foundRecordAddress);
    int valueSize = DataBlock.valueLength(foundRecordAddress);
    // First two bytes are number of elements in a value
    long addr = Hashes.insertSearch(foundRecordAddress, fieldPtr, fieldSize);
    boolean append = addr == (valueAddress + valueSize);
    // check if the same element
    boolean exists = append? false: Hashes.compareFields(addr, fieldPtr, fieldSize) == 0;
    if ( exists && opts == MutationOptions.NX || !exists && opts == MutationOptions.XX) {
      // Can not insert, because of mutation options
      return false;
    }
    // found
    int fieldSizeSize = Utils.sizeUVInt(fieldSize);
    int fieldValueSizeSize = Utils.sizeUVInt(fieldValueSize);
    int toAdd = !exists? fieldSizeSize + fieldSize + fieldValueSize + fieldValueSizeSize:
      fieldValueSize + fieldValueSizeSize - Hashes.getFulValueSize(addr);

    int newValueSize = valueSize + toAdd;
    
    boolean needSplit = DataBlock.mustStoreExternally(foundKeySize, newValueSize);
    if (!exists) added = 1;
    if (!needSplit) {
      Hashes.checkValueArena(newValueSize);
      insertFieldValue(valueAddress, valueSize, addr, fieldPtr, fieldSize);    
      // set # of updates to 1
      this.updatesCount = 1;
      this.keys[0] = foundKeyAddress; // use the key we found
      this.keySizes[0] = foundKeySize; // use the key we found
      this.values[0] = Hashes.valueArena.get();
      this.valueSizes[0] = newValueSize;
      return true;
    } else if (!canSplit(valueAddress)){
      // We can't split existing KV , so insert new one
      //TODO: UPDATE == true (exists == true)
      // WRONG CODE
      //TODO: TEST
      insertNewKVandFieldValue(fieldPtr, fieldSize);
      return true;
    } else {
      // Do split
      Hashes.checkValueArena(newValueSize + NUM_ELEM_SIZE);
      insertFieldValue(valueAddress, valueSize, addr, fieldPtr, fieldSize);
      // calculate new key size
      // This is value address for update #1
      long vPtr = Hashes.valueArena.get();
      int kSize = keySize(keyAddress);
      // This is value address for update #2
      long splitPos = Hashes.splitAddress(vPtr, newValueSize);
      int fSize = Hashes.getFieldSize(splitPos);
      long fPtr = Hashes.getFieldAddress(splitPos);
      // This is key size for update #2
      int totalKeySize = buildKey(keyAddress + KEY_SIZE + Utils.SIZEOF_BYTE, kSize, fPtr, fSize);
      // This is key address for update #2
      long kPtr = keyArena.get();
      int totalElNum = numElementsInValue(vPtr);
      int leftSplitElNum = Hashes.splitNumber(vPtr, newValueSize);
      int rightSplitElNum = totalElNum - leftSplitElNum;
      // Prepare value for split
      UnsafeAccess.copy(splitPos, splitPos + NUM_ELEM_SIZE, (vPtr + newValueSize - splitPos));
      
      setNumElements(vPtr, leftSplitElNum);
      setNumElements(splitPos, rightSplitElNum);
      // This is value size update #1
      int leftValueSize = (int)(splitPos - vPtr);
      // This is value size update #2
      int rightValueSize = (int)((vPtr + newValueSize - splitPos) + NUM_ELEM_SIZE);
      
      // Prepare updates
      this.updatesCount = 2;
      this.keys[0] = foundKeyAddress;
      this.keySizes[0] = foundKeySize;
      this.values[0] = vPtr;
      this.valueSizes[0] = leftValueSize;
      
      this.keys[1] = kPtr;
      this.keySizes[1] = totalKeySize;
      this.values[1] = splitPos;
      this.valueSizes[1] = rightValueSize;
      return true;
    }
  }
  
  /**
   * Insert new K-V with a given field-value pair
   * @param fieldPtr field address
   * @param fieldSize field size
   */
  private void insertNewKVandFieldValue(long fieldPtr, int fieldSize) {
    added = 1;
    // Get real keySize
    int kSize = keySize(keyAddress);
    checkKeyArena(kSize + KEY_SIZE + Utils.SIZEOF_BYTE + fieldSize); 
    // Build first key for the new set
    int totalKeySize = buildKey(keyAddress + KEY_SIZE + Utils.SIZEOF_BYTE, kSize, fieldPtr, fieldSize);
    long kPtr = keyArena.get();
    int fSizeSize = Utils.sizeUVInt(fieldSize);
    int vSizeSize = Utils.sizeUVInt(fieldValueSize);
    Hashes.checkValueArena(fieldSize + fSizeSize + fieldValueSize + vSizeSize + NUM_ELEM_SIZE);
    long vPtr = Hashes.valueArena.get();
    setNumElements(vPtr, 1);
    // Write field length
    Utils.writeUVInt(vPtr + NUM_ELEM_SIZE, fieldSize);
    // Write value length
    Utils.writeUVInt(vPtr + NUM_ELEM_SIZE + fSizeSize, fieldValueSize);
    // Copy field
    UnsafeAccess.copy(fieldPtr, vPtr + NUM_ELEM_SIZE + fSizeSize + vSizeSize, fieldSize);
    //Copy value
    UnsafeAccess.copy(fieldValueAddress, vPtr + NUM_ELEM_SIZE + fSizeSize + 
      vSizeSize + fieldSize, fieldValueSize);
    
    
    // set number of updates to 1
    this.updatesCount = 1;
    keys[0] = kPtr;
    keySizes[0] = totalKeySize;
    values[0] = vPtr;
    valueSizes[0] = fieldSize + fSizeSize + fieldValueSize + vSizeSize + NUM_ELEM_SIZE;
  }
  
  /**
   * Insert new K-V with a given field-value pair
   * @param fieldPtr field address
   * @param fieldSize field size
   */
  private void insertFirstKVandFieldValue(long fieldPtr, int fieldSize) {
    added = 1;
    // Get real keySize
    int kSize = keySize(keyAddress);
    checkKeyArena(kSize + KEY_SIZE + Utils.SIZEOF_BYTE + 1 /*ZERO*/); 
    // Build first key for the new set
    int totalKeySize = buildKey(keyAddress + KEY_SIZE + Utils.SIZEOF_BYTE, kSize, Commons.ZERO, 1);
    long kPtr = keyArena.get();
    int fSizeSize = Utils.sizeUVInt(fieldSize);
    int vSizeSize = Utils.sizeUVInt(fieldValueSize);
    Hashes.checkValueArena(fieldSize + fSizeSize + fieldValueSize + vSizeSize + NUM_ELEM_SIZE);
    long vPtr = Hashes.valueArena.get();
    setNumElements(vPtr, 1);
    // Write field length
    Utils.writeUVInt(vPtr + NUM_ELEM_SIZE, fieldSize);
    // Write value length
    Utils.writeUVInt(vPtr + NUM_ELEM_SIZE + fSizeSize, fieldValueSize);
    // Copy field
    UnsafeAccess.copy(fieldPtr, vPtr + NUM_ELEM_SIZE + fSizeSize + vSizeSize, fieldSize);
    //Copy value
    UnsafeAccess.copy(fieldValueAddress, vPtr + NUM_ELEM_SIZE + fSizeSize + 
      vSizeSize + fieldSize, fieldValueSize);
    
    
    // set number of updates to 1
    this.updatesCount = 1;
    keys[0] = kPtr;
    keySizes[0] = totalKeySize;
    values[0] = vPtr;
    valueSizes[0] = fieldSize + fSizeSize + fieldValueSize + vSizeSize + NUM_ELEM_SIZE;
  }
  /**
   * Insert field-value into value (valueAddress, valueSize)
   * insertion point is addr. Hashes.valueArena is used
   * @param valueAddress value address
   * @param valueSize current value size
   * @param addr insertion point
   * @param fieldPtr field pointer
   * @param fieldSize field size
   */
  private void insertFieldValue(long valueAddress, int valueSize, long addr, long fieldPtr, int fieldSize) {
    // increment number of elements in this value
    boolean update = false;
    int existRecLen = 0;
    if (addr < valueAddress + valueSize) {
      int fSize = Utils.readUVInt(addr);
      int fSizeSize = Utils.sizeUVInt(fSize);
      int vSize = Utils.readUVInt(addr + fSizeSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      update = Utils.compareTo(fieldPtr, fieldSize, addr + fSizeSize + vSizeSize, fSize) == 0;
      if (update) {
        existRecLen = fSize + vSize + fSizeSize + vSizeSize;
      }
    }
    
    if (!update) {
      addNumElements(valueAddress, 1);
    }
    long ptr = Hashes.valueArena.get();
    // Copy everything before addr
    UnsafeAccess.copy(valueAddress, ptr, addr - valueAddress);
    // Encode element size
    ptr += addr - valueAddress;
    int fSizeSize = Utils.writeUVInt(ptr, fieldSize);
    ptr += fSizeSize;
    int vSizeSize = Utils.writeUVInt(ptr, fieldValueSize);
    ptr += vSizeSize;
    // copy field
    UnsafeAccess.copy(fieldPtr, ptr, fieldSize);
    ptr += fieldSize;
    // copy value
    UnsafeAccess.copy(fieldValueAddress, ptr, fieldValueSize);
    ptr += fieldValueSize;
    // copy rest elements

    UnsafeAccess.copy(addr + existRecLen, ptr, valueSize - (addr - valueAddress) - existRecLen);
  }
  

}
