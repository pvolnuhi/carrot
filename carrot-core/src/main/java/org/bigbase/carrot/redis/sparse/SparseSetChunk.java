package org.bigbase.carrot.redis.sparse;

import static  org.bigbase.carrot.redis.sparse.SparseBitmaps.HEADER_SIZE;
import static  org.bigbase.carrot.redis.sparse.SparseBitmaps.BUFFER_CAPACITY;
import static  org.bigbase.carrot.redis.sparse.SparseBitmaps.CHUNK_SIZE;

import static  org.bigbase.carrot.redis.sparse.SparseBitmaps.compress;
import static  org.bigbase.carrot.redis.sparse.SparseBitmaps.setBitCount;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;



/**
 * Inserts new or overwrites existing chunk of a bitmap
 * @author Vladimir Rodionov
 *
 */

public class SparseSetChunk extends Operation {
  
  static ThreadLocal<Long> buffer = new ThreadLocal<Long>() {

    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(BUFFER_CAPACITY);
    }    
  };
  /*
   * Address of a chunk
   */
  long ptr;
  // chunk size is CHUNK_SIZE - BITS_SIZE
  
  /*
   * Offset to/from overwrite
   */
  int offset = -1;
  
  /*
   * Overwrite before or after offset
   */
  boolean before = false;
  
  public SparseSetChunk() {
    setFloorKey(true);
  }

  @Override
  public boolean execute() {
    
    long valuePtr;
    int valueSize;
    this.updatesCount = 1; 
    
    if (this.foundRecordAddress > 0) {
      
      long kPtr = DataBlock.keyAddress(this.foundRecordAddress);
      int kSize = DataBlock.keyLength(this.foundRecordAddress);
      // Check if it is the same sparse bitmap key
      if (kSize > Utils.SIZEOF_LONG && Utils.compareTo(kPtr, kSize - Utils.SIZEOF_LONG , 
          keyAddress, keySize - Utils.SIZEOF_LONG) == 0) {
        long vPtr = DataBlock.valueAddress(this.foundRecordAddress);
        int vSize = DataBlock.valueLength(this.foundRecordAddress);
        vPtr = SparseBitmaps.isCompressed(vPtr)? SparseBitmaps.decompress(vPtr, vSize - SparseBitmaps.HEADER_SIZE): vPtr;
        if (this.offset > 0) {
          if (before) {
            // Copy from existing till offset (exclusive)
            UnsafeAccess.copy(vPtr + SparseBitmaps.HEADER_SIZE, ptr + SparseBitmaps.HEADER_SIZE, this.offset);
          } else {
            // Copy from existing after offset (inclusive)
            UnsafeAccess.copy(vPtr + SparseBitmaps.HEADER_SIZE + this.offset, 
              ptr + SparseBitmaps.HEADER_SIZE + this.offset, SparseBitmaps.BYTES_PER_CHUNK - this.offset);
          }
        }
      }
    }
    
    int popCount = (int) Utils.bitcount(this.ptr, SparseBitmaps.BYTES_PER_CHUNK);
    if (SparseBitmaps.shouldCompress(popCount)) {
      // we set newChunk = false to save thread local buffer
      int compSize = compress(ptr, popCount, false, buffer.get());
      valueSize = compSize + HEADER_SIZE;
      valuePtr = buffer.get();
    } else {
      // WHY do we do copy?
      //valuePtr = buffer.get();
      //UnsafeAccess.copy(ptr, valuePtr + HEADER_SIZE, SparseBitmaps.BYTES_PER_CHUNK);
      valuePtr = ptr;
      setBitCount(valuePtr, popCount, false);
      valueSize = CHUNK_SIZE;
    }
    this.keys[0] = keyAddress;
    this.keySizes[0] = keySize;

    this.values[0] = valuePtr;
    this.valueSizes[0] = valueSize;
    return true;
    
  }

  @Override
  public void reset() {
    super.reset();
    setFloorKey(true);
    this.ptr = 0;
    this.offset = -1;
    this.before = false;
  }
  
  /**
   * Set before
   * @param v
   */
  public void setBefore(boolean v) {
    this.before = v;
  }
  
  /**
   * Set offset
   * @param offset
   */
  public void setOffset (int offset) {
    this.offset = offset;
  }
  
  /**
   * Set chunk address
   * @param ptr address of a chunk
   */
  public void setChunkAddress(long ptr) {
    this.ptr = ptr;
  }
}
