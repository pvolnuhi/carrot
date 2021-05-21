package org.bigbase.carrot.redis.strings;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.redis.Commons;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Get string value range operation.
 * Returns the substring of the string value stored at key, determined by the offsets 
   * start and end (both are inclusive). Negative offsets can be used in order to provide 
   * an offset starting from the end of the string. So -1 means the last character, 
   * -2 the penultimate and so forth.
   * The function handles out of range requests by limiting the resulting range to the actual
   *  length of the string.
 * @author Vladimir Rodionov
 *
 */
public class StringGetRange extends Operation {

  long from;
  long to;
  long rangeSize = -1;
  long bufferPtr;
  int bufferSize;
  
  public StringGetRange() {
    setReadOnly(true);
  }
  
  @Override
  public boolean execute() {
    this.updatesCount = 0;
    if (foundRecordAddress < 0) {
      // Yes we return true
      return false;
    }
   
    long valuePtr = DataBlock.valueAddress(foundRecordAddress);
    int valueSize = DataBlock.valueLength(foundRecordAddress);
    this.rangeSize = 0; 
    
    if (from == Commons.NULL_LONG) {
      from = 0;
    }
    
    if (to == Commons.NULL_LONG) {
      to = valueSize - 1;
    }
    // sanity checks
    if (from < 0) {
      from = valueSize + from;
    }
    if (to < 0) {
      to = valueSize + to;
    }
    if (from < 0){
      from = 0;
    }
    if (from > valueSize - 1) {
      // out of range
      return false;
    }
    if (to < 0) {
      return false;
    }
    if (to > valueSize - 1) {
      to = valueSize - 1;
    }
    if (from > to) {
      return false;
    }
    this.rangeSize = to - from + 1;
    if (this.rangeSize > this.bufferSize) {
      return false;
    }
    UnsafeAccess.copy(valuePtr + from, bufferPtr, this.rangeSize);
    return true;
  }
  
  @Override
  public void reset() {
    super.reset();
    this.from = 0;
    this.to = 0;
    this.rangeSize = -1;
    this.bufferPtr = 0;
    this.bufferSize = 0;
    setReadOnly(true);
  }
  
  /**
   * Sets range
   * @param from from offset inclusive
   * @param to offset inclusive
   */
  public void setFromTo (long from, long to) {
    this.from = from;
    this.to = to;
  }
  /**
   * Sets buffer
   * @param ptr buffer address
   * @param size buffer size
   */
  public void setBuffer (long ptr, int size) {
    this.bufferPtr = ptr;
    this.bufferSize = size;
  }
  /**
   * Returns range length
   * @return length
   */
  public int getRangeLength() {
    return (int) rangeSize;
  }

}
