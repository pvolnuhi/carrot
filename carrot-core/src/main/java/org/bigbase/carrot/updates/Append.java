package org.bigbase.carrot.updates;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.util.UnsafeAccess;

public class Append extends Update {
  
  long appendValuePtr;
  int appendValueSize;
  
  static ThreadLocal<Long> buffer = new ThreadLocal<Long>() {

    @Override
    protected Long initialValue() {
      long ptr = UnsafeAccess.malloc(4096);
      return ptr;
    }
  };
  
  static ThreadLocal<Integer> bufferSize = new ThreadLocal<Integer>() {

    @Override
    protected Integer initialValue() {
      return 4096;
    }    
  };
  
  @Override
  public void reset() {
    super.reset();
    appendValuePtr = 0;
    appendValueSize = 0;
  }
  
  public void setAppendValue(long ptr, int size) {
    this.appendValuePtr = ptr;
    this.appendValueSize = size;
  }
  
  private void checkBuffer(int required) {
    int size = bufferSize.get();
    if (size >= required) {
      return;
    }
    UnsafeAccess.free(buffer.get());
    long ptr = UnsafeAccess.malloc(required);
    buffer.set(ptr);
    bufferSize.set(required);
  }
  
  @Override
  public boolean execute() {
    if (foundRecordAddress <=0) {
      return false;
    }
    int vsize = DataBlock.valueLength(foundRecordAddress);
    checkBuffer(vsize + appendValueSize);
    long ptr = DataBlock.valueAddress(foundRecordAddress);
    long bufferPtr = buffer.get();
    UnsafeAccess.copy(ptr, bufferPtr, vsize);
    UnsafeAccess.copy(appendValuePtr, bufferPtr + vsize, appendValueSize);
    // set key
    keys[0] = keyAddress; // original key
    keySizes[0] = keySize;
    values[0] = bufferPtr;
    valueSizes[0] = vsize + appendValueSize;
    // Set update count to 1
    updatesCount = 1;
    return true;
  }

}
