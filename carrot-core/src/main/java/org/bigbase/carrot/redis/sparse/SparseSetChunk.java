/**
 *    Copyright (C) 2021-present Carrot, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 */
package org.bigbase.carrot.redis.sparse;

import static org.bigbase.carrot.redis.sparse.SparseBitmaps.BITS_PER_CHUNK;
import static org.bigbase.carrot.redis.sparse.SparseBitmaps.BUFFER_CAPACITY;
import static org.bigbase.carrot.redis.sparse.SparseBitmaps.BYTES_PER_CHUNK;
import static org.bigbase.carrot.redis.sparse.SparseBitmaps.CHUNK_SIZE;
import static org.bigbase.carrot.redis.sparse.SparseBitmaps.HEADER_SIZE;
import static org.bigbase.carrot.redis.sparse.SparseBitmaps.compress;
import static org.bigbase.carrot.redis.sparse.SparseBitmaps.setBitCount;
import static org.bigbase.carrot.util.Utils.BITS_PER_BYTE;
import static org.bigbase.carrot.util.Utils.SIZEOF_LONG;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Inserts new or overwrites existing chunk of a bitmap
 * @author Vladimir Rodionov
 */

public class SparseSetChunk extends Operation {

  /*
   * Buffer for local thread operations 
   */
  static ThreadLocal<Long> buffer = new ThreadLocal<Long>() {

    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(BUFFER_CAPACITY);
    }
  };
  /*
   * Address of a chunk
   */
  long chunkPtr;
  /*
   * chunk length - actual length is always CHUNK_SIZE,
   * offset and chunkLength controls overwrite region location
   */
  int chunkLength;
  /*
   * Offset to/from overwrite (in bytes)
   */
  long offset = -1;

  public SparseSetChunk() {
    setFloorKey(true);
  }

  @Override
  public boolean execute() {

    long valuePtr;
    int valueSize;
    this.updatesCount = 1;
    boolean sameChunk = false;
    if (this.foundRecordAddress > 0) {

      long kPtr = DataBlock.keyAddress(this.foundRecordAddress);
      int kSize = DataBlock.keyLength(this.foundRecordAddress);
      // Check if it is the same sparse bitmap key
      if (kSize > SIZEOF_LONG
          && Utils.compareTo(kPtr, kSize - SIZEOF_LONG, keyAddress, keySize - SIZEOF_LONG) == 0) {
        // The same sparse bitmap
        long foundOffset = SparseBitmaps.getChunkOffsetFromKey(kPtr, kSize);
        if (foundOffset == (this.offset * BITS_PER_BYTE / BITS_PER_CHUNK) * BITS_PER_CHUNK) {
          // Overwrite existing chunk
          sameChunk = true;
          long vPtr = DataBlock.valueAddress(this.foundRecordAddress);
          int vSize = DataBlock.valueLength(this.foundRecordAddress);
          vPtr = SparseBitmaps.isCompressed(vPtr) ? SparseBitmaps.decompress(vPtr, vSize - HEADER_SIZE, buffer.get()): vPtr;
          int preCopy = (int) (this.offset - (this.offset / BYTES_PER_CHUNK * BYTES_PER_CHUNK));
          // Copy from existing till offset (exclusive)
          UnsafeAccess.copy(vPtr + HEADER_SIZE, chunkPtr + HEADER_SIZE, preCopy);
          // Copy from existing after offset + chunkLength(inclusive)
          if (preCopy + this.chunkLength < BYTES_PER_CHUNK) {
            int postCopy = BYTES_PER_CHUNK - preCopy - this.chunkLength;
            UnsafeAccess.copy(vPtr + HEADER_SIZE + preCopy + this.chunkLength,
              chunkPtr + HEADER_SIZE + preCopy + this.chunkLength, postCopy);
          }
        }
      }
    }

    int popCount = (int) Utils.bitcount(this.chunkPtr + HEADER_SIZE, BYTES_PER_CHUNK);
    if (popCount == 0 && sameChunk) {
      // do not insert empty segment back - delete it
      this.updateTypes[0] = true;
    } else if (popCount == 0) {
      // do not insert new empty segment
      this.updatesCount = 0;
      return true;
    }
    if (SparseBitmaps.shouldCompress(popCount)) {
      // we set newChunk = false to save thread local buffer
      int compSize = compress(chunkPtr, popCount, false, buffer.get());
      valueSize = compSize + HEADER_SIZE;
      valuePtr = buffer.get();
    } else {
      valuePtr = chunkPtr;
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
    this.chunkPtr = 0;
    this.chunkLength = 0;
    this.offset = -1;
  }

  /**
   * Set absolute offset of a chunk in bytes
   * @param offset
   */
  public void setOffset(long offset) {
    this.offset = offset;
  }

  /**
   * Set chunk address
   * @param ptr address of a chunk
   */
  public void setChunkAddress(long ptr) {
    this.chunkPtr = ptr;
  }

  /**
   * Set chunk length
   * @param length chunk length
   */
  public void setChunkLength(int length) {
    this.chunkLength = length;
  }

}
