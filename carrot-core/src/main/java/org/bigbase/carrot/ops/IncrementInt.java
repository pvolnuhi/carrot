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
  
  static ThreadLocal<Long> buffer = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(Utils.SIZEOF_INT);
    }
  };
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
    int v = 0;
    if (foundRecordAddress > 0) {
      int vSize = DataBlock.valueLength(foundRecordAddress);
      if (vSize != Utils.SIZEOF_INT /*long size*/) {
        return false;
      }
      long ptr = DataBlock.valueAddress(foundRecordAddress);
      v = UnsafeAccess.toInt(ptr);
      value += v;
      UnsafeAccess.putInt(ptr, value);
      this.updatesCount = 0;
      return true;
    }
    value += v;
    UnsafeAccess.putInt(buffer.get(), value);
    
    this.updatesCount = 1;
    this.keys[0] = keyAddress;
    this.keySizes[0] = keySize;
    this.values[0] = buffer.get();
    this.valueSizes[0] = Utils.SIZEOF_INT;
    
    return true;
  }

}
