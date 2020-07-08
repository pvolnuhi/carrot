package org.bigbase.carrot.extensions.strings;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
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
  private boolean notExists = false;
  private boolean keepTTL = false;
  
  @Override
  public boolean execute() {
    if (foundRecordAddress < 0) {
      return false;
    }
    long kPtr = DataBlock.keyAddress(foundRecordAddress);
    int kSize = DataBlock.keyLength(foundRecordAddress);
    // compare keys
    boolean keyExists = Utils.compareTo(keyAddress, keySize, kPtr, kSize) ==0;
    if ((keyExists && notExists) || (!keyExists && !notExists)) {
      return false;
    }
    if (keepTTL) {
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
  
  public void setKeepTTL(boolean b) {
    this.keepTTL = b;
  }
  
  public void setIfDoesNotExist(boolean b) {
    this.notExists = b;
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
    this.notExists = false;
  }
}
