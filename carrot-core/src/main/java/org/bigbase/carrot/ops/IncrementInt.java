package org.bigbase.carrot.ops;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * This example of specific Update - atomic counter 
 * @author Vladimir Rodionov
 *
 */
public class IncrementInt extends Operation {
  
  int value;
  
  public IncrementInt() {
    setReadOnlyOrUpdateInPlace(true);
  }
  
  /**
   * Set increment value
   * @param v value
   */
  public void setIncrement(int v) {
    this.value = v;
  }
  
  /**
   * Get increment value
   * @return value
   */
  public int getIncrement() {
    return value;
  }
  
  /**
   * Value after increment
   * @return value after increment
   */
  public int getValue() {
    return this.value;
  }
  
  @Override
  public void reset() {
    super.reset();
    value = 0;
    setReadOnlyOrUpdateInPlace(true);
  }
  
  @Override
  public boolean execute() {
    if (foundRecordAddress <= 0) {
      return false;
    }
    int vSize = DataBlock.valueLength(foundRecordAddress);
    if (vSize != Utils.SIZEOF_INT /*long size*/) {
      return false;
    }
    long ptr = DataBlock.valueAddress(foundRecordAddress);
    int v = UnsafeAccess.toInt(ptr);
    value += v;
    UnsafeAccess.putInt(ptr, value);
    // set updateCounts to 0 - we update in place
    this.updatesCount = 0;
    return true;
  }

}
