package org.bigbase.carrot.extensions.strings;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Get bit by offset operation. 
 * Returns the bit value at offset in the string value stored at key.
 * When offset is beyond the string length, the string is assumed to be a 
 * contiguous space with 0 bits. When key does not exist it is assumed to be 
 * an empty string, so offset is always out of range and the value is also assumed 
 * to be a contiguous space with 0 bits.
 * @author Vladimir Rodionov
 *
 */
public class StringGetBit extends Operation {

  long offset;
  int bit;
  
  @Override
  public boolean execute() {
    this.updatesCount = 0;
    if (foundRecordAddress < 0) {
      // Yes we return true
      return true;
    }
    long foundKeyPtr = DataBlock.keyAddress(foundRecordAddress);
    int foundKeySize = DataBlock.keyLength(foundRecordAddress);
    if (Utils.compareTo(foundKeyPtr, foundKeySize, keyAddress, keySize) != 0) {
      // Key not found
      return true;
    }
    
    long valuePtr = DataBlock.valueAddress(foundRecordAddress);
    int valueSize = DataBlock.valueLength(foundRecordAddress);
    if (offset < 0 || offset > ((long)valueSize) * 8 - 1) {
      return true;
    }

    this.bit = getbit(valuePtr, valueSize);
    return true;
  }
  
  private int getbit(long valuePtr, int valueSize) {
    int n = (int)(this.offset >>>3);
    int rem = (int)(offset - ((long)n) * 8);
    byte b = UnsafeAccess.toByte(valuePtr + n);
    return b >>> (7 - rem);
  }

  @Override
  public void reset() {
    super.reset();
    this.bit = 0;
    this.offset = 0;
  }
  
  /**
   * Set offset for this operation
   * @param offset offset in bits
   */
  public void setOffset (long offset) {
    this.offset = offset;
  }
  /**
   * Returns bit value at offset
   * @return bit alue: 0 or 1
   */
  public int getBit() {
    return bit;
  }

}
