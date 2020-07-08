package org.bigbase.carrot.extensions.strings;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
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

  int from;
  int to;
  int rangeSize = -1;
  long bufferPtr;
  int bufferSize;
  
  @Override
  public boolean execute() {
    this.updatesCount = 0;
    if (foundRecordAddress < 0) {
      // Yes we return true
      return false;
    }
    long foundKeyPtr = DataBlock.keyAddress(foundRecordAddress);
    int foundKeySize = DataBlock.keyLength(foundRecordAddress);
    if (Utils.compareTo(foundKeyPtr, foundKeySize, keyAddress, keySize) != 0) {
      // Key not found
      return false;
    }
   
    long valuePtr = DataBlock.valueAddress(foundRecordAddress);
    int valueSize =DataBlock.valueLength(foundRecordAddress);
    
  
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
    if (from > valueSize -1) {
      from = valueSize -1;
    }
    
    if (to < 0) {
      to = 0;
    }
    
    if (to > valueSize -1) {
      to = valueSize -1;
    }
    if (from > to) {
      // 0
      rangeSize = 0;
      return false;
    }
    
    this.rangeSize = to - from +1;
    if (this.rangeSize > this.bufferSize) {
      return false;
    }
    UnsafeAccess.copy(valuePtr, bufferPtr, this.rangeSize);
    return true;
  }
  
  @Override
  public void reset() {
    super.reset();
    this.from = 0;
    this.to = 0;
    this.rangeSize = 0;
    this.bufferPtr = 0;
    this.bufferSize = 0;
  }
  
  /**
   * Sets range
   * @param from from offset inclusive
   * @param to offset inclusive
   */
  public void setFromTo (int from, int to) {
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
    return (int)rangeSize;
  }

}
