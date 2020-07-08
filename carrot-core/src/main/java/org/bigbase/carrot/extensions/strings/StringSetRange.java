package org.bigbase.carrot.extensions.strings;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * String SETRANGE operation.
 * Overwrites part of the string stored at key, starting at the specified offset, 
 * for the entire length of value. If the offset is larger than the current length of the string at key, 
 * the string is padded with zero-bytes to make offset fit. Non-existing keys are considered as empty 
 * strings, so this command will make sure it holds a string large enough to be able to set value at offset.
 * Note that the maximum offset that you can set is 229 -1 (536870911), as Redis Strings are limited
 * to 512 megabytes. If you need to grow beyond this size, you can use multiple keys.
 * @author Vladimir Rodionov
 *
 */
public class StringSetRange extends Operation {


  private long valuePtr;
  private int valueSize;
  private int offset;
  private int length;
  
  @Override
  public boolean execute() {
    if (foundRecordAddress < 0) {
      return false;
    }
    long kPtr = DataBlock.keyAddress(foundRecordAddress);
    int kSize = DataBlock.keyLength(foundRecordAddress);
    // compare keys
    boolean keyExists = Utils.compareTo(keyAddress, keySize, kPtr, kSize) ==0;
    long newPtr = 0;
    boolean reuseValue = false;
    if (keyExists) {
      int vSize = DataBlock.valueLength(foundRecordAddress);
      long vPtr = DataBlock.valueAddress(foundRecordAddress);
      if (vSize >= this.offset + this.valueSize) {
        // Just overwrite part of existing value
        UnsafeAccess.copy(valuePtr, vPtr + offset, valueSize);
        this.length = vSize;
        this.updatesCount = 0;
        return true;
      } else {
        this.length = offset + valueSize;
        // Allocate new value
        newPtr = UnsafeAccess.reallocZeroed(vPtr, vSize, this.length);
        UnsafeAccess.copy(valuePtr, newPtr + offset, valueSize);
        reuseValue = true;
      }
    } else {
      // key does not exists;
      this.length = offset + valueSize;
      // Allocate new value
      newPtr = UnsafeAccess.mallocZeroed(this.length);
      UnsafeAccess.copy(valuePtr, newPtr + offset, valueSize);
      reuseValue = true;
    }
    // now update
    this.updatesCount = 1;
    this.keys[0] = keyAddress;
    this.keySizes[0] = keySize;
    this.values[0] = newPtr;
    this.valueSizes[0] = length;
    this.reuseValues[0] = reuseValue;
    return true;
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
  
  public void setOffset(int offset) {
    this.offset = offset;
  }
  
  public int getValueLength() {
    return this.length;
  }
  
  @Override
  public void reset() {
    super.reset();
    this.valuePtr = 0;
    this.valueSize = 0;
    this.offset = 0;
    this.length = 0;
  }
}