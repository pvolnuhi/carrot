package org.bigbase.carrot.ops;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.util.UnsafeAccess;

/**
 * This example of specific Update - atomic counter 
 * @author Vladimir Rodionov
 *
 */
public class Increment extends Operation {
  
  long value;
  
  public void setIncrement(long v) {
    this.value = v;
  }
  
  public long getIncrement() {
    return value;
  }
  
  @Override
  public void reset() {
    super.reset();
    value = 0;
  }
  
  @Override
  public boolean execute() {
    if (foundRecordAddress <= 0) {
      return false;
    }
    int vSize = DataBlock.valueLength(foundRecordAddress);
    if (vSize != 8 /*long size*/) {
      return false;
    }
    long ptr = DataBlock.valueAddress(foundRecordAddress);
    long v = UnsafeAccess.toLong(ptr);
    UnsafeAccess.putLong(ptr, v + value);
    // set updateCounts to 0 - we update in place
    this.updatesCount = 0;
    return true;
  }

}
