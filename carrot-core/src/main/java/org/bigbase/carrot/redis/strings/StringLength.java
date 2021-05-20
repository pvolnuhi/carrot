package org.bigbase.carrot.redis.strings;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * String value length operation.
 * Returns the length of the string value stored at key. 
 * An error is returned when key holds a non-string value.
 * @author Vladimir Rodionov
 *
 */
public class StringLength extends Operation {


  int strlen = 0;
  
  @Override
  public boolean execute() {
    this.updatesCount = 0;
    if (foundRecordAddress < 0) {
      // Yes we return true
      return true;
    }
//TODO: remove this code after testing    
//    long foundKeyPtr = DataBlock.keyAddress(foundRecordAddress);
//    int foundKeySize = DataBlock.keyLength(foundRecordAddress);
//    if (Utils.compareTo(foundKeyPtr, foundKeySize, keyAddress, keySize) != 0) {
//      // Key not found
//      return true;
//    }
    
    int valueSize = DataBlock.valueLength(foundRecordAddress);
    this.strlen = valueSize;  
    return true;
  }
  

  @Override
  public void reset() {
    super.reset();
    this.strlen = 0;
  }
  
  /**
   * Returns string value 
   * @return value length or 0 , if not found
   */
  public int getLength() {
    return this.strlen;
  }

}
