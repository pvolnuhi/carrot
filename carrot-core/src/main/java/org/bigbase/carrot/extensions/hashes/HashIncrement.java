package org.bigbase.carrot.extensions.hashes;

import static org.bigbase.carrot.extensions.Commons.KEY_SIZE;
import static org.bigbase.carrot.extensions.Commons.elementAddressFromKey;
import static org.bigbase.carrot.extensions.Commons.elementSizeFromKey;
import static org.bigbase.carrot.extensions.Commons.keySize;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.extensions.IncrementType;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class HashIncrement extends Operation{ 
  
  private IncrementType type;
  private int intValue;
  private long longValue;
  private float floatValue;
  private double doubleValue;
  
  public HashIncrement() {
    setFloorKey(true);
  }

  @Override
  public void reset() {
    super.reset();
    setFloorKey(true);
    type = null;
    intValue = 0;
    longValue = 0;
    floatValue = 0;
    doubleValue = 0;
  }
    
  /**
   * Sets increment type
   * @param type increment type
   */
  public void setIncrementType(IncrementType type) {
    this.type = type;
  }
  /**
   * Sets integer increment value
   * @param v increment value
   */
  public void setIntValue(int v) {
    this.intValue = v;
  }
  
  /**
   * Gets post increment value
   * @return new value
   */
  public int getIntPostIncrement() {
    return this.intValue;
  }
  
  /**
   * Sets long increment value
   * @param v increment value
   */
  public void setLongValue(long v) {
    this.longValue = v;
  }
  
  /**
   * Gets post increment value
   * @return new value
   */
  public long getLongPostIncrement() {
    return this.longValue;
  }
  
  /**
   * Sets float increment value
   * @param v increment value
   */
  public void setFloatValue(float v) {
    this.floatValue = v;
  }
  
  /**
   * Gets post increment value
   * @return new value
   */
  public float getFloatPostIncrement() {
    return this.floatValue;
  }
  
  /**
   * Sets long increment value
   * @param v increment value
   */
  public void setDoubleValue(double v) {
    this.doubleValue = v;
  }
  
  /**
   * Gets post increment value
   * @return new value
   */
  public double getDoublePostIncrement() {
    return this.doubleValue;
  }
  
  @Override
  public boolean execute() {
    if (foundRecordAddress <=0) {
      return false;
    }
    // check prefix
    int setKeySize = keySize(keyAddress);
    int foundKeySize = DataBlock.keyLength(foundRecordAddress);
    if (foundKeySize <= setKeySize + KEY_SIZE) {
      return false;
    }
    long foundKeyAddress = DataBlock.keyAddress(foundRecordAddress);
    // Prefix keys must be equals
    if (Utils.compareTo(keyAddress, setKeySize +  KEY_SIZE, foundKeyAddress, 
      setKeySize +  KEY_SIZE) != 0) {
      return false;
    }
    
    long fieldPtr = elementAddressFromKey(keyAddress);
    int fieldSize = elementSizeFromKey(keyAddress, keySize);
    // Set no updates
    updatesCount = 0;
    long address = Hashes.exactSearch(foundRecordAddress, fieldPtr, fieldSize);
    if (address < 0) {
      return false;
    }
    // size of a field-value pair
    int foundValueSize = Hashes.getValueSize(address);    
    return increment(address, foundValueSize);
  }

  private boolean increment(long address, int size) {
    
    switch(type) {
      case INTEGER:
        if (size != 4) {
          return false;
        }
        int v = UnsafeAccess.toInt(address);
        this.intValue += v;
        UnsafeAccess.putInt(address,  intValue);
        return true;
          
      case LONG:
        if (size != 8) {
          return false;
        }
        long v1 = UnsafeAccess.toLong(address);
        this.longValue += v1;
        UnsafeAccess.putLong(address,  longValue);
        return true;
      case FLOAT:
        if (size != 4) {
          return false;
        }
        int v2 = UnsafeAccess.toInt(address);
        float f2 = Float.intBitsToFloat(v2);
        this.floatValue += f2;
        UnsafeAccess.putInt(address,  Float.floatToIntBits(this.floatValue));
        return true;
      case DOUBLE:   
        if (size != 8) {
          return false;
        }
        long v3 = UnsafeAccess.toLong(address);
        double d2 = Double.longBitsToDouble(v3);
        this.doubleValue += d2;
        UnsafeAccess.putLong(address,  Double.doubleToLongBits(this.doubleValue));
        return true;
    }
    return false;
  }
}
