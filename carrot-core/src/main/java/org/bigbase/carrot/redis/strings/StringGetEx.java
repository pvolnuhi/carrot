package org.bigbase.carrot.redis.strings;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.util.UnsafeAccess;

/**
 * 
 * String GETEX operation.
 * Atomically gets key value and updates expire. 
 * Returns an error when key exists but does not hold a string value.
 * 
 * @author Vladimir Rodionov
 *
 */
public class StringGetEx extends Operation {

  private long bufferPtr;
  private int bufferSize;
  private int size = -1; // means did not exist
  
  @Override
  public boolean execute() {
    if (foundRecordAddress > 0) {
      int vLength = DataBlock.valueLength(foundRecordAddress);
      this.size = vLength;
      if (this.size > this.bufferSize) {
        this.updatesCount = 0;
        return false;
      }
      long vPtr = DataBlock.valueAddress(foundRecordAddress);
      UnsafeAccess.copy(vPtr, bufferPtr, vLength);
      // Update expire field
      DataBlock.setRecordExpire(foundRecordAddress, this.expire);
    } else {
      // Does not exist
      this.updatesCount = 0;
      return false;
    }
    return true;
  }
  
  public void setBuffer(long ptr, int size) {
    this.bufferPtr = ptr;
    this.bufferSize = size;
  }
  
  /**
   * Get value previous size
   * @return size
   */
  public int getValueLength() {
    return this.size;
  }
  
  @Override
  public void reset() {
    super.reset();
    this.bufferPtr = 0;
    this.bufferSize = 0;
    this.size = -1; // means did not existed
  }
}
