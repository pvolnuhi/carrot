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
  
  static ThreadLocal<Long> buffer = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(Utils.SIZEOF_DOUBLE);
    }
  };
  
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
    double dv = 0;
    if (foundRecordAddress > 0) {
      int vSize = DataBlock.valueLength(foundRecordAddress);
      if (vSize != Utils.SIZEOF_DOUBLE /*long size*/) {
        return false;
      }
      long ptr = DataBlock.valueAddress(foundRecordAddress);
      long v = UnsafeAccess.toLong(ptr);
      dv = Double.longBitsToDouble(v);
      value += dv;
      UnsafeAccess.putLong(ptr, Double.doubleToLongBits(value));
      this.updatesCount = 0;
      return true;
    }
    value += dv;
    UnsafeAccess.putLong(buffer.get(), Double.doubleToLongBits(value));
    this.updatesCount = 1;
    this.keys[0] = keyAddress;
    this.keySizes[0] = keySize;
    this.values[0] = buffer.get();
    this.valueSizes[0] = Utils.SIZEOF_DOUBLE;
    return true;
  }

}
