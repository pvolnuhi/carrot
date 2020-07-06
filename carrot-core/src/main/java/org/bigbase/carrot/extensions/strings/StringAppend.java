package org.bigbase.carrot.extensions.strings;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * String append mutation.
 * If key already exists and is a string, this command appends the value at the end of the string. 
 * If key does not exist it is created and set as an empty string, 
 * so APPEND will be similar to SET in this special case.
 * @author Vladimir Rodionov
 *
 */
public class StringAppend extends Operation {
  
  /*
   * Append value address
   */
  long appendValuePtr;
  /*
   * Append value size
   */
  int appendValueSize;
  /*
   * Size of a value after append
   */
  int sizeAfterAppend;
  
  /*
   * Thread local buffer 
   */
  static ThreadLocal<Long> buffer = new ThreadLocal<Long>() {

    @Override
    protected Long initialValue() {
      long ptr = UnsafeAccess.malloc(4096);
      return ptr;
    }
  };
  
  /* 
   * Thread local buffer size
   * 
   */
  static ThreadLocal<Integer> bufferSize = new ThreadLocal<Integer>() {

    @Override
    protected Integer initialValue() {
      return 4096;
    }    
  };
  
  public StringAppend() {
    setFloorKey(true);
  }
  
  @Override
  public void reset() {
    super.reset();
    appendValuePtr = 0;
    appendValueSize = 0;
    sizeAfterAppend=0;
  }
  /**
   * Sets append value address and size
   * @param ptr append value address
   * @param size append value size
   */
  public void setAppendValue(long ptr, int size) {
    this.appendValuePtr = ptr;
    this.appendValueSize = size;
  }
  
  /**
   * Gets value size after append
   * @return value size after append operation 
   */
  public int getSizeAfterAppend() {
    return this.sizeAfterAppend;
  }
  
  /**
   * Ensure enough space in a thread local buffer
   * @param required required size
   */
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
    long foundKeyPtr = DataBlock.keyAddress(foundRecordAddress);
    int foundKeySize = DataBlock.keyLength(foundRecordAddress);
    
    boolean keyExists = Utils.compareTo(keyAddress, keySize, foundKeyPtr, foundKeySize) == 0;
    int vsize = appendValueSize;
    if (keyExists) {
      vsize = DataBlock.valueLength(foundRecordAddress);
      checkBuffer(vsize + appendValueSize);
      long ptr = DataBlock.valueAddress(foundRecordAddress);
      long bufferPtr = buffer.get();
      UnsafeAccess.copy(ptr, bufferPtr, vsize);
      UnsafeAccess.copy(appendValuePtr, bufferPtr + vsize, appendValueSize);
      vsize += appendValueSize;
      // set key
      keys[0] = keyAddress; // original key
      keySizes[0] = keySize;
      values[0] = bufferPtr;
      valueSizes[0] = vsize;
    } else {
      // set key
      keys[0] = keyAddress; // original key
      keySizes[0] = keySize;
      values[0] = appendValuePtr;
      valueSizes[0] = vsize;
    }
    this.sizeAfterAppend = vsize;
    // Set update count to 1
    updatesCount = 1;
    return true;
  }

}
