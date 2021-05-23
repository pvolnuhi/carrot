package org.bigbase.carrot.ops;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * This example of specific Update - atomic counter 
 * @author Vladimir Rodionov
 *
 */
public class IncrementLong extends Operation {
  
  long value;
  
  public IncrementLong() {
    //setReadOnlyOrUpdateInPlace(true);
  }
  
  /**
   * Set increment value
   * @param v value
   */
  public void setIncrement(long v) {
    this.value = v;
  }
  
  /**
   * Get increment value
   * @return
   */
  public long getIncrement() {
    return this.value;
  }
  /**
   * Get value after increment
   * @return value after increment
   */
  public long getValue() {
    return this.value;
  }
  
  @Override
  public void reset() {
    super.reset();
    value = 0;
    //setReadOnlyOrUpdateInPlace(true);
  }
  
  @Override
  public boolean execute() {
    if (foundRecordAddress <= 0) {
      return false;
    }
    int vSize = DataBlock.valueLength(foundRecordAddress);
    if (vSize != Utils.SIZEOF_LONG /*long size*/) {
      return false;
    }
    long ptr = DataBlock.valueAddress(foundRecordAddress);
    long v = UnsafeAccess.toLong(ptr);
    this.value += v;
    UnsafeAccess.putLong(ptr, v + value);
    // set updateCounts to 0 - we update in place
    this.updatesCount = 0;
    return true;
  }

}
