package org.bigbase.carrot.redis.sparse;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Get bit by offset operation. 
 * Returns the bit value at offset in the string value stored at key.
 * When offset is beyond the string length, the string is assumed to be a 
 * contiguous space with 0 bits. When key does not exist it is assumed to be 
 * an empty string, so offset is always out of range and the value is also assumed 
 * to be a contiguous space with 0 bits.
 * @author Vladimir Rodionov
 *
 */
public class SparseGetBit extends Operation {

  long offset;
  int bit = 0;
  
  public SparseGetBit() {
    setFloorKey(true);
  }
  
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
      // sparse bitmap Key not found
      return true;
    }
    
    long valuePtr = DataBlock.valueAddress(foundRecordAddress);
    int valueSize = DataBlock.valueLength(foundRecordAddress);
    boolean isCompressed = SparseBitmaps.isCompressed(valuePtr);
    if (isCompressed) {
      valuePtr = SparseBitmaps.decompress(valuePtr, valueSize - SparseBitmaps.HEADER_SIZE);
    }

    this.bit = getbit(valuePtr);
    return true;
  }
  
  //TODO: Test
  private int getbit(long valuePtr) {
    long chunkOffset = this.offset / SparseBitmaps.BITS_PER_CHUNK;
    int off = (int)(this.offset - chunkOffset * SparseBitmaps.BITS_PER_CHUNK);
    int n = (int)(off / Utils.BITS_PER_BYTE);
    int rem = (int)(off - ((long)n) * Utils.BITS_PER_BYTE);
    byte b = UnsafeAccess.toByte(valuePtr + n + SparseBitmaps.HEADER_SIZE);
    return (b >>> (7 - rem)) & 1;
  }

  @Override
  public void reset() {
    super.reset();
    this.bit = 0;
    this.offset = 0;
  }
  
  /**
   * Set offset for this operation
   * @param offset offset in bits
   */
  public void setOffset (long offset) {
    this.offset = offset;
  }
  /**
   * Returns bit value at offset
   * @return bit alue: 0 or 1
   */
  public int getBit() {
    return bit;
  }

}
