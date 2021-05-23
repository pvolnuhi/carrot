package org.bigbase.carrot.ops;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * This example of specific Update - atomic counter 
 * @author Vladimir Rodionov
 *
 */
public class IncrementDouble extends Operation {
  
  double value;
  
  public IncrementDouble() {
    setReadOnlyOrUpdateInPlace(true);
  }
  
  public void setIncrement(double v) {
    this.value = v;
  }
  
  public double getIncrement() {
    return this.value;
  }
  
  public double getValue() {
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
    if (vSize != Utils.SIZEOF_DOUBLE /*long size*/) {
      return false;
    }
    long ptr = DataBlock.valueAddress(foundRecordAddress);
    long v = UnsafeAccess.toLong(ptr);
    double dv = Double.longBitsToDouble(v);
    value += dv;
    UnsafeAccess.putLong(ptr, Double.doubleToLongBits(value));
    // set updateCounts to 0 - we update in place
    this.updatesCount = 0;
    return true;
  }

}
