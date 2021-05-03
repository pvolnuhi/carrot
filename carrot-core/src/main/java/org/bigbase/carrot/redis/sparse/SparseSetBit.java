package org.bigbase.carrot.redis.sparse;

import org.bigbase.carrot.BigSortedMap;
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
    boolean existKey = true;
    boolean newChunk = false;
    long foundKeyPtr = 0;
    int foundKeySize = 0;
    if (foundRecordAddress < 0) {
      existKey = false;
    } else {
      foundKeyPtr = DataBlock.keyAddress(foundRecordAddress);
      foundKeySize = DataBlock.keyLength(foundRecordAddress);
      if (foundKeySize <= Utils.SIZEOF_LONG || 
          Utils.compareTo(foundKeyPtr, foundKeySize - Utils.SIZEOF_LONG, 
            keyAddress, keySize - Utils.SIZEOF_LONG) != 0) {
        // Key not found - not a sparse bitmap key
        existKey = false;
      }
    }
    if (existKey) {
      long existOffsetFromKey = SparseBitmaps.getChunkOffsetFromKey(foundKeyPtr, foundKeySize);
      if (existOffsetFromKey + SparseBitmaps.BITS_PER_CHUNK <= offset) {
        // Yes, this is the wrong chunk
        existKey = false; 
      }
    }
    if (!existKey && this.bit == 0) {
      this.oldBit = 0;
      // do nothing
      return true;
    }
    long valuePtr = 0;
    int valueSize = 0;
    if (existKey) {
      valuePtr = DataBlock.valueAddress(foundRecordAddress);
      valueSize = DataBlock.valueLength(foundRecordAddress);
      if (SparseBitmaps.isCompressed(valuePtr)) {
        valuePtr = SparseBitmaps.decompress(valuePtr, 
          valueSize - SparseBitmaps.HEADER_SIZE);
        this.updatesCount = 1; 
      } else {
        // we do update in place
      }
    } else {
      // new K-V
      valueSize = SparseBitmaps.CHUNK_SIZE;
      valuePtr = UnsafeAccess.mallocZeroed(valueSize);
      this.updatesCount = 1; 
      newChunk = true;
      // Update memory statistics
      BigSortedMap.totalAllocatedMemory.addAndGet(SparseBitmaps.CHUNK_SIZE);
    }
    this.oldBit = getsetbit(valuePtr, valueSize);
    int bitCount = existKey? SparseBitmaps.getBitCount(valuePtr): 1;
    if (bitCount > 0 && SparseBitmaps.shouldCompress(bitCount)) {
      int compSize = SparseBitmaps.compress(valuePtr, bitCount, newChunk, buffer.get());
      valueSize = compSize + SparseBitmaps.HEADER_SIZE;
      valuePtr = buffer.get();
      this.updatesCount = 1; 
    } else if (bitCount > 0){
      valueSize = SparseBitmaps.CHUNK_SIZE;
    }
    this.keys[0] = keyAddress;
    this.keySizes[0] = keySize;

    this.values[0] = valuePtr;
    this.valueSizes[0] = valueSize;
    if (bitCount == 0) {
      this.updateTypes[0] = true; // DELETE
    }
    return true;
  }
  
  //TODO: TEST
  private int getsetbit(long valuePtr, int valueSize) {
    long chunkOffset = this.offset / SparseBitmaps.BITS_PER_CHUNK;
    int off = (int)(this.offset - chunkOffset * SparseBitmaps.BITS_PER_CHUNK);
    int n = (int)(off / Utils.BITS_PER_BYTE);
    int rem = (int)(off - n * Utils.BITS_PER_BYTE);
    byte b = UnsafeAccess.toByte(valuePtr + n + SparseBitmaps.HEADER_SIZE);
    oldBit = (b >>> (7 - rem)) & 1;
    if (this.bit == 1) {
      b |= 1 << (7 - rem);
    } else {
      b &= ~(1 << (7 - rem));
    }
    UnsafeAccess.putByte(valuePtr + n + SparseBitmaps.HEADER_SIZE, b);
    if (oldBit == 0 && bit == 1) {
      SparseBitmaps.incrementBitCount(valuePtr, 1);
    } else if (oldBit == 1 && bit == 0) {
      SparseBitmaps.incrementBitCount(valuePtr, -1);
    }
    return oldBit;
  }

  @Override
  public void reset() {
    super.reset();
    this.bit = 0;
    this.oldBit = 0;
    this.offset = 0;
    setFloorKey(true);
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
