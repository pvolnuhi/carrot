package org.bigbase.carrot.redis.strings;

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
  
  public StringGetBit() {
    setReadOnlyOrUpdateInPlace(true);
  }
  
  @Override
  public boolean execute() {
    this.updatesCount = 0;
    if (foundRecordAddress < 0) {
      // Yes we return true
      return true;
    }
     
    long valuePtr = DataBlock.valueAddress(foundRecordAddress);
    int valueSize = DataBlock.valueLength(foundRecordAddress);
    if (offset < 0 || offset > ((long)valueSize) * Utils.BITS_PER_BYTE - 1) {
      return true;
    }
    this.bit = getbit(valuePtr, valueSize);
    return true;
  }
  
  private int getbit(long valuePtr, int valueSize) {
    int n = (int)(this.offset / Utils.BITS_PER_BYTE);
    int rem = (int)(offset - ((long) n) * Utils.BITS_PER_BYTE);
    byte b = UnsafeAccess.toByte(valuePtr + n);
    return (b >>> (7 - rem)) & 1;
  }

  @Override
  public void reset() {
    super.reset();
    this.bit = 0;
    this.offset = 0;
    setReadOnlyOrUpdateInPlace(true);
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
