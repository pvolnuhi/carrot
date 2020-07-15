package org.bigbase.carrot.redis.sparse;

import static  org.bigbase.carrot.redis.sparse.SparseBitmaps.BITS_SIZE;
import static  org.bigbase.carrot.redis.sparse.SparseBitmaps.BUFFER_CAPACITY;
import static  org.bigbase.carrot.redis.sparse.SparseBitmaps.CHUNK_SIZE;

import static  org.bigbase.carrot.redis.sparse.SparseBitmaps.compress;
import static  org.bigbase.carrot.redis.sparse.SparseBitmaps.setBitCount;

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
  
  public SparseSetChunk() {
    setFloorKey(true);
  }

  @Override
  public boolean execute() {
    if (foundRecordAddress <=0) {
      return false;
    }
    long valuePtr;
    int valueSize;
    this.updatesCount = 1; 

    int popCount = (int) Utils.bitcount(ptr, CHUNK_SIZE - BITS_SIZE);
    if (SparseBitmaps.shouldCompress(popCount)) {
      int compSize = compress(ptr, popCount, buffer.get());
      valueSize = compSize + BITS_SIZE;
      valuePtr = buffer.get();
    } else {
      valuePtr = buffer.get();
      UnsafeAccess.copy(ptr, valuePtr + BITS_SIZE, CHUNK_SIZE - BITS_SIZE);
      setBitCount(valuePtr, popCount, false);
      valueSize = CHUNK_SIZE;
    }
    this.keys[0] = keyAddress;
    this.keySizes[0] = keySize;

    this.values[0] = valuePtr;
    this.valueSizes[0] = valueSize;
    return true;
    
  }

  public void setChunkAddress(long ptr) {
    this.ptr = ptr;
  }
}
