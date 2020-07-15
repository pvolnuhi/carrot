package org.bigbase.carrot.redis.sparse;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.util.Utils;

/**
 * String value length operation.
 * Returns the length of the string value stored at key. 
 * An error is returned when key holds a non-string value.
 * @author Vladimir Rodionov
 *
 */
public class SparseLength extends Operation {


  long strlen;
  
  @Override
  public boolean execute() {
    this.updatesCount = 0;
    if (foundRecordAddress < 0) {
      // Yes we return true
      return true;
    }
    long foundKeyPtr = DataBlock.keyAddress(foundRecordAddress);
    int foundKeySize = DataBlock.keyLength(foundRecordAddress);
    if (Utils.compareTo(foundKeyPtr, foundKeySize - Utils.SIZEOF_LONG, 
      keyAddress, keySize - Utils.SIZEOF_LONG) != 0) {
      // Key not found
      return true;
    }
    long offset = SparseBitmaps.getChunkOffsetFromKey(foundKeyPtr, foundKeySize);
    this.strlen = offset/Utils.SIZEOF_BYTE + SparseBitmaps.CHUNK_SIZE - SparseBitmaps.BITS_SIZE;  
    return true;
  }
  

  @Override
  public void reset() {
    super.reset();
    this.strlen = 0;
  }
  
  /**
   * Returns string value 
   * @return value length or 0 , if not found
   */
  public long getLength() {
    return this.strlen;
  }

}
