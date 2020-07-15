package org.bigbase.carrot.redis.sparse;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Sets or clears the bit at offset in the string value stored at key.
 * The bit is either set or cleared depending on value, which can be either 0 or 1.
 * When key does not exist, a new string value is created. The string is grown to make 
 * sure it can hold a bit at offset. The offset argument is required to be greater than or 
 * equal to 0, and smaller than 232 (this limits bitmaps to 512MB). When the string at key 
 * is grown, added bits are set to 0.
 * @author Vladimir Rodionov
 *
 */
public class SparseSetBit extends Operation {

  static ThreadLocal<Long> buffer = new ThreadLocal<Long>() {

    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(SparseBitmaps.BUFFER_CAPACITY);
    }    
  };
  
  long offset;
  int bit;
  int oldBit;
  
  public SparseSetBit() {
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
    boolean existKey = true;
    if (Utils.compareTo(foundKeyPtr, foundKeySize, keyAddress, keySize) != 0) {
      // Key not found
      existKey = false;
    }
    long valuePtr = 0;
    int valueSize = 0;
    if (existKey) {
      valuePtr = DataBlock.valueAddress(foundRecordAddress);
      valueSize = DataBlock.valueLength(foundRecordAddress);
      if (SparseBitmaps.isCompressed(valuePtr)) {
        valuePtr = SparseBitmaps.decompress(valuePtr, valueSize - SparseBitmaps.BITS_SIZE);
        this.updatesCount = 1; 
      }
    } else {
      // new K-V
      valueSize = SparseBitmaps.CHUNK_SIZE;
      valuePtr = UnsafeAccess.mallocZeroed(valueSize);
      this.updatesCount = 1; 
    }
    this.bit = getsetbit(valuePtr, valueSize);
    int popCount = SparseBitmaps.getBitCount(valuePtr);
    if (SparseBitmaps.shouldCompress(popCount)) {
      int bitCount = SparseBitmaps.getBitCount(valuePtr);
      int compSize = SparseBitmaps.compress(valuePtr + + SparseBitmaps.BITS_SIZE, bitCount, buffer.get());
      valueSize = compSize + SparseBitmaps.BITS_SIZE;
      valuePtr = buffer.get();
      this.updatesCount = 1; 
    } else {
      valueSize = SparseBitmaps.CHUNK_SIZE;
    }
    this.keys[0] = keyAddress;
    this.keySizes[0] = keySize;

    this.values[0] = valuePtr;
    this.valueSizes[0] = valueSize;
    return true;
  }
  
  private int getsetbit(long valuePtr, int valueSize) {
    long chunkOffset = this.offset / SparseBitmaps.BITS_PER_CHUNK;
    int off = (int)(this.offset - chunkOffset * SparseBitmaps.BITS_PER_CHUNK);
    int n = (int)(off >>>3);
    int rem = (int)(off - ((long)n) * Utils.SIZEOF_BYTE);
    byte b = UnsafeAccess.toByte(valuePtr + n);
    oldBit = b >>> (7 - rem);
    b |= bit << (7 - rem);
    UnsafeAccess.putByte(valuePtr + n, b);
    return oldBit;
  }

  @Override
  public void reset() {
    super.reset();
    this.bit = 0;
    this.oldBit = 0;
    this.offset = 0;
  }
  
  /**
   * Set offset for this operation
   * @param offset offset in bits
   */
  public void setOffset (long offset) {
    // Offset is always >= 0;
    this.offset = offset;
  }
  
  /**
   * Sets new bit
   * @param bit value
   */
  public void setBit(int bit) {
    this.bit = bit;
  }
  /**
   * Returns old bit value at offset
   * @return bit value: 0 or 1
   */
  public int getOldBit() {
    return oldBit;
  }

}
