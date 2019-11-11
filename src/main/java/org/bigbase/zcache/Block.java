package org.bigbase.zcache;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.bigbase.carrot.RetryOperationException;
import org.bigbase.util.UnsafeAccess;
import org.bigbase.util.Utils;

/**
 * Records are sorted Format: <KV> + <KV> : 0 .. 1 - key size
 * (first bit is delete marker) 2 .. 3 - value size <KEY> <VALUE>
 */

public class Block implements Comparable<Block>{

  public final static int RECORD_PREFIX_LENGTH = 4;
  public final static long NOT_FOUND = -1L;
  public final static byte DELETED_MASK = (byte) (1 << 7);
  public final static double MIN_COMPACT_RATIO = 0.25d;
  public final static double MAX_MERGE_RATIO = 0.25d;
  
  public final static String MAX_BLOCK_SIZE_KEY = "max.block.size";
  public final static int DEFAULT_MAX_BLOCK_SIZE = 4096; 
    
  static ThreadLocal<Long> bufPtr = new ThreadLocal<Long>() {

	@Override
	protected Long initialValue() {
		int maxSize = BigSortedMapOld.getMaxBlockSize() + 80;
		return UnsafeAccess.malloc(maxSize);
	}
	  
  };
  /*
   * TODO: make this configurable
   */
  
  static float[] BLOCK_RATIOS = new float[] {0.25f, 0.5f, 0.75f, 1.0f};
  /*
   * Read-Write Lock
   * TODO: StampedLock (Java 8)
   */
  static ReentrantReadWriteLock[] locks = new ReentrantReadWriteLock[11113];
  static {
	  for (int i=0; i < locks.length; i++) {
		  locks[i] = new ReentrantReadWriteLock();
	  }
  }
  /**
   * Get total allocated memory
   * @return memory
   */
  static long getTotalAllocatedMemory() {
    return BigSortedMapOld.totalAllocatedMemory.get();
  }
  
  /**
   * Get total data size
   * @return total data size
   */
  static long getTotalDataSize() {
    return BigSortedMapOld.totalDataSize.get();
  }
  
  /**
   * Get min size greater than current
   * @param max - max size
   * @param current current size
   * @return min size or -1;
   */
  static int getMinSizeGreaterThan(int max, int current) {
    for (int i=0; i < BLOCK_RATIOS.length; i++) {
      int size = Math.round(max * BLOCK_RATIOS[i]);
      if (size > current) return size;
    }
    return -1;
  }
  
  /**
   * Get min size greater than current
   * @param max - max size
   * @param current current size
   * @return min size or -1;
   */
  static int getMinSizeGreaterOrEqualsThan(int max, int current) {
    for (int i=0; i < BLOCK_RATIOS.length; i++) {
      int size = Math.round(max * BLOCK_RATIOS[i]);
      if (size >= current) return size;
    }
    return -1;
  }
   
  /*
   * Block's address (current)
   */
  volatile long dataPtr;  
  
  /*
   * Current block size
   */
  int blockSize;

  /*
   * Data size (volatile?)
   */
  int dataSize = 0;
  /*
   * Number of KVs in a block - index size (volatile?)
   */
  short numRecords = 0;
  /*
   * Number of deleted records (volatile?)
   */
  short numDeletedRecords = 0;
  
  /*
   * Split/Merge sequence number. 
   */
  volatile short seqNumberSplitOrMerge; 
  
  /*
   * Is block compressed
   */
  boolean compressed = false;

  /*
   * Is thread safe block 
   */
  
  boolean threadSafe;


  /**
   * Constructor
   * @param initial size
   */
  Block(int size) {
    dataPtr = UnsafeAccess.malloc(size);
    if (dataPtr == 0) {
    	throw new RuntimeException("Failed to allocate "+ size + " bytes");
    }
    BigSortedMapOld.totalAllocatedMemory.addAndGet(size);
    this.blockSize = size;
  }
   

  /**
   * Expands block
   * @return true if success
   */
  boolean expand() {
     
      // Get next size
      int nextSize = getMinSizeGreaterThan(BigSortedMapOld.maxBlockSize, blockSize);
      if (nextSize < 0) {
        return false;
      }
      long newPtr = UnsafeAccess.malloc(nextSize);
      if (newPtr <= 0) {
        return false;
      }
      BigSortedMapOld.totalAllocatedMemory.addAndGet(nextSize - blockSize);
      // Do copy
      UnsafeAccess.copy(dataPtr, newPtr, dataSize);
      UnsafeAccess.free(dataPtr);
      this.dataPtr = newPtr;
      this.blockSize = nextSize;
      return true;
  }
  
  /**
   * Shrink block
   * @return true if success
   */
  boolean shrink() {
      // Get next size
      int nextSize = getMinSizeGreaterThan(BigSortedMapOld.maxBlockSize, dataSize);
      if (nextSize < 0) {
        return false;
      }
      long newPtr = UnsafeAccess.malloc(nextSize);
      if (newPtr <= 0) {
        return false;
      }
      BigSortedMapOld.totalAllocatedMemory.addAndGet(nextSize - blockSize);

      // Do copy
      UnsafeAccess.copy(dataPtr, newPtr, dataSize);
      UnsafeAccess.free(dataPtr);
      this.dataPtr = newPtr;
      this.blockSize = nextSize;
      return true;
  }
  
  /**
   * Set thread safe
   * @param b thread safe (true/false)
   */
  public void setThreadSafe(boolean b) {
    this.threadSafe = b;
  }
  
  /**
   * Is thread safe
   * @return
   */
  public boolean isThreadSafe() {
    return threadSafe;
  }
  
  public boolean isCompressed() {
	  return compressed;
  }
  
  /**
   * Reset block for reuse
   */
  public void reset() {
    this.seqNumberSplitOrMerge = 0;
    this.numRecords = 0;
    this.numDeletedRecords = 0;
    this.dataSize = 0;
    this.firstKey = null;
  }
  
  /**
   *  Read lock
   * @throws RetryOperationException 
   * @throws InterruptedException 
   */
  public void readLock() throws RetryOperationException {
    if (isThreadSafe()) return ;
    long before = this.seqNumberSplitOrMerge;
    int index = (hashCode() % locks.length);
    ReentrantReadWriteLock lock = locks[index];
    lock.readLock().lock();
    long after = this.seqNumberSplitOrMerge;
    if (before != after) {
      throw new RetryOperationException();
    }
  }
  
  /**
   * Read unlock
   */
  public void readUnlock() {
    if (isThreadSafe()) return;
    int index = (hashCode() % locks.length);
    ReentrantReadWriteLock lock = locks[index];
    lock.readLock().unlock();
  }
  
  /**
   * Write lock
   * @throws RetryOperationException 
   * @throws InterruptedException 
   */
  public void writeLock() throws RetryOperationException {
    if (isThreadSafe()) return ;
    long before = this.seqNumberSplitOrMerge;
    int index = (hashCode() % locks.length);
    ReentrantReadWriteLock lock = locks[index];
    lock.writeLock().lock();
    long after = this.seqNumberSplitOrMerge;
    if (before != after) {
      throw new RetryOperationException();
    }
  }
  
  /**
   * Write unlock
   */
  public void writeUnlock() {
    if (isThreadSafe()) return;
    int index = (hashCode() % locks.length);
    ReentrantReadWriteLock lock = locks[index];
    lock.writeLock().unlock();
  }  
      
  /**
   * Put operation
   * @param key
   * @param keyOffset
   * @param keyLength
   * @param value
   * @param valueOffset
   * @param valueLength
   * @return true, if success, false otherwise (no room, split block)
   */
  public boolean put(byte[] key, int keyOffset, int keyLength, byte[] value, int valueOffset,
      int valueLength) throws RetryOperationException {

    boolean onlyExactOverwrite = false;
    boolean isOverwrite = false;
    boolean keyOverwrite = false; // key the same, value is different in size
    try {
      writeLock();
      if (dataSize + keyLength + valueLength + RECORD_PREFIX_LENGTH > blockSize) {
        // try compact first (remove deleted)
        compact(true);
        if (dataSize + keyLength + valueLength + RECORD_PREFIX_LENGTH > blockSize) {
          // try to expand block
          expand();
          if (dataSize + keyLength + valueLength + RECORD_PREFIX_LENGTH > blockSize) {
            // Still not enough room
            onlyExactOverwrite = true;
          }
        }
      }

      long addr = search(key, keyOffset, keyLength);
      if (addr < dataPtr + dataSize) {
        short keylen = UnsafeAccess.toShort(addr);
        keylen = normalize(keylen);
        short vallen = UnsafeAccess.toShort(addr + 2);
        if (keylen == keyLength) {
          // Compare keys
          int res = Utils.compareTo(key, keyOffset, keyLength, addr + 4, keylen);
          if (res == 0 && vallen == valueLength) {
            isOverwrite = true;
          } else if (res == 0) {
            keyOverwrite = true;
          }
        } 
      }
      // Failed to put - split the block
      if (onlyExactOverwrite && !isOverwrite) {
        return false;
      }

      int moveDist = RECORD_PREFIX_LENGTH + keyLength + valueLength;

      if (!isOverwrite && !keyOverwrite) {
        // INSERT
        // move from offset to offset + moveDist
        UnsafeAccess.copy(addr, addr + moveDist, dataPtr + dataSize - addr);
        UnsafeAccess.copy(key, keyOffset, addr + RECORD_PREFIX_LENGTH, keyLength);
        UnsafeAccess.copy(value, valueOffset, addr + RECORD_PREFIX_LENGTH + keyLength, valueLength);
        // Update key-value length
        UnsafeAccess.putShort(addr, (short) keyLength);
        UnsafeAccess.putShort(addr + 2, (short) valueLength);
        // Set not deleted
        setDeleted(addr, false);
        numRecords++;
        dataSize += moveDist;
        if (isThreadSafe() == false) {
          BigSortedMapOld.totalDataSize.addAndGet(moveDist);
        }
      } else if (isOverwrite){
        // UPDATE existing
        // We do overwrite of existing record
        UnsafeAccess.copy(value, valueOffset, addr + RECORD_PREFIX_LENGTH + keyLength, valueLength);
        // Undelete if deleted
        // Set not deleted
        setDeleted(addr, false);
      } else {
        // keyOverwrite = true
        // delete existing, put new

        short keylen = UnsafeAccess.toShort(addr);
        keylen =normalize(keylen);
        short vallen = UnsafeAccess.toShort(addr + 2);
        moveDist = keylen + vallen + RECORD_PREFIX_LENGTH;
        int toMove = (valueLength - vallen);
        // move from offset to offset + moveDist
        UnsafeAccess.copy(addr + moveDist, addr + moveDist+ toMove, 
          dataPtr + dataSize - addr - moveDist);
        UnsafeAccess.copy(key, keyOffset, addr + RECORD_PREFIX_LENGTH, keyLength);
        UnsafeAccess.copy(value, valueOffset, addr + RECORD_PREFIX_LENGTH + keyLength, valueLength);
        // Update key-value length
        UnsafeAccess.putShort(addr, (short) keyLength);
        UnsafeAccess.putShort(addr + 2, (short) valueLength);
        // Set not deleted
        setDeleted(addr, false);
        dataSize += toMove;   
        if (isThreadSafe() == false) {
          BigSortedMapOld.totalDataSize.addAndGet(toMove);
        }
      }
      return true;
    } finally {
      writeUnlock();
    }
  }

  /**
   * Put k-v operation
   * @param keyPtr
   * @param keyLength
   * @param valuePtr
   * @param valueLength
   * @return true, if success, false otherwise
   * @throws RetryOperationException 
   */
  public boolean put(long keyPtr, int keyLength, long valuePtr, int valueLength) throws RetryOperationException {

    boolean onlyExactOverwrite = false;
    boolean isOverwrite = false;
    boolean keyOverwrite = false; // key the same, value is different in size

    try {

      writeLock();
      if (dataSize + keyLength + valueLength + RECORD_PREFIX_LENGTH > blockSize) {
        // try compact first
        compact(true);
        if (dataSize + keyLength + valueLength + RECORD_PREFIX_LENGTH > blockSize) {
          // try to expand block
          expand();
          if (dataSize + keyLength + valueLength + RECORD_PREFIX_LENGTH > blockSize) {
            // Still not enough room
            onlyExactOverwrite = true;
          }
        }
      }

      long addr = search(keyPtr, keyLength);
      if (addr < dataPtr + dataSize) {

        short keylen = UnsafeAccess.toShort(addr);
        keylen = normalize(keylen);
        short vallen = UnsafeAccess.toShort(addr + 2);
        if (keylen == keyLength) {
          // Compare keys
          int res = Utils.compareTo(keyPtr, keyLength, addr + RECORD_PREFIX_LENGTH, keylen);
          if (res == 0 && vallen == valueLength) {
            isOverwrite = true;
          } else if (res == 0) {
            keyOverwrite = true;
          }
        }
      }
      if (onlyExactOverwrite && !isOverwrite) {
        return false;
      }

      int moveDist = RECORD_PREFIX_LENGTH + keyLength + valueLength;

      if (!isOverwrite && !keyOverwrite) {
        // INSERT
        // move from offset to offset + moveDist
        UnsafeAccess.copy(addr, addr + moveDist, dataPtr + dataSize - addr);
        UnsafeAccess.copy(keyPtr, addr + RECORD_PREFIX_LENGTH, keyLength);
        UnsafeAccess.copy(valuePtr, addr + RECORD_PREFIX_LENGTH + keyLength, valueLength);
        // Update key-value length
        UnsafeAccess.putShort(addr, (short) keyLength);
        UnsafeAccess.putShort(addr + 2, (short) valueLength);
        // Set not deleted
        setDeleted(addr, false);
        dataSize += moveDist;
        if (isThreadSafe() == false) {
          BigSortedMapOld.totalDataSize.addAndGet(moveDist);
        }
      } else if (isOverwrite){
        // UPDATE existing
        // We do overwrite of existing record
        UnsafeAccess.copy(valuePtr, addr + RECORD_PREFIX_LENGTH + keyLength, valueLength);
        // Undelete if deleted
        // Set not deleted
        setDeleted(addr, false);
      } else {
        // keyOverwrite = true
        // delete existing, put new
        short keylen = UnsafeAccess.toShort(addr);
        keylen = normalize(keylen);
        short vallen = UnsafeAccess.toShort(addr + 2);
        moveDist = keylen + vallen + RECORD_PREFIX_LENGTH;
        int toMove = (valueLength - vallen);
        // move from offset to offset + moveDist
        UnsafeAccess.copy(addr + moveDist, addr + moveDist + toMove,
          dataPtr + dataSize - addr - moveDist);
        UnsafeAccess.copy(keyPtr, addr + RECORD_PREFIX_LENGTH, keyLength);
        UnsafeAccess.copy(valuePtr, addr + RECORD_PREFIX_LENGTH + keyLength, valueLength);
        // Update key-value length
        UnsafeAccess.putShort(addr, (short) keyLength);
        UnsafeAccess.putShort(addr + 2, (short) valueLength);
        // Set not deleted
        setDeleted(addr, false);
        dataSize += toMove;
        if (isThreadSafe() == false) {
          BigSortedMapOld.totalDataSize.addAndGet(toMove);
        }
      }
      return true;
    } finally {
      writeUnlock();
    }
  }
  
  /**
   * Search position of a first key which is greater or
   * equals to a given key
   * @param key
   * @param keyOffset
   * @param keyLength
   * @return address to insert (or update)
   */
  long search(byte[] key, int keyOffset, int keyLength) {
    long ptr = dataPtr;
    int count = 0;
    while (count++ < numRecords) {
      short keylen = UnsafeAccess.toShort(ptr);
      keylen = normalize(keylen);
      short vallen = UnsafeAccess.toShort(ptr + 2);
      int res = Utils.compareTo(key, keyOffset, keyLength, ptr + RECORD_PREFIX_LENGTH, keylen);
      if (res <= 0) {
        return ptr;
      }
      ptr += keylen + vallen + RECORD_PREFIX_LENGTH;
    }
    // after the last record
    return dataPtr + dataSize;
  }
 
  boolean isLargerThanMax(byte[] key, int keyOffset, int keyLength) {
    return search(key, keyOffset, keyLength) == dataPtr + dataSize;
  }
  
  boolean isLargerThanMax(long keyPtr, int keyLength ) {
    return search (keyPtr, keyLength) == dataPtr + dataSize;
  }
  
  /**
   * Search position of a first key which is greater or
   * equals to a given key
   * @param keyPtr
   * @param keyLength
   * @return address to insert (or update)
   */
  long search(long keyPtr, int keyLength) {
    long ptr = dataPtr;
    int count = 0;
    while (count++ < numRecords) {
      short keylen = UnsafeAccess.toShort(ptr);
      keylen = normalize(keylen);
      short vallen = UnsafeAccess.toShort(ptr + 2);
      int res = Utils.compareTo(keyPtr, keyLength,  ptr + RECORD_PREFIX_LENGTH, keylen);
      if (res <= 0) {
        return ptr;
      }
      ptr += keylen + vallen + RECORD_PREFIX_LENGTH;
    }
    // after the last record
    return dataPtr + dataSize;
  }
    
  /**
   * Set deleted (true, false)
   * @param addr address of k-v record
   * @param b 
   */
  private void setDeleted(long addr, boolean b) {
    byte v = UnsafeAccess.toByte(addr);
    boolean wasDeleted = v < 0;
    if (b) {
      UnsafeAccess.putByte(addr, (byte) (v | DELETED_MASK));
    } else {
      UnsafeAccess.putByte(addr, (byte) (v & ~DELETED_MASK));
    }
    if (wasDeleted && !b) {
      // undelete
      numDeletedRecords --;
    } else if (!wasDeleted && b) {
      // Deleted
      numDeletedRecords++;
    }
  }

  /**
   *  Check if record is deleted
   * @param addr record address
   * @return true, false
   */
  private boolean isDeleted(long addr) {
    return UnsafeAccess.toByte(addr) < 0;
  }
  
  /**
   * Delete operation
   * TODO: compact on deletion
   * @param key
   * @param keyOffset
   * @param keyLength
   * @return true, if success, false otherwise
   * @throws RetryOperationException 
   */
  
  public boolean delete(byte[] key, int keyOffset, int keyLength) throws RetryOperationException {
    
	try {
	  
	  long addr = get(key, keyOffset, keyLength);
      if (addr == NOT_FOUND) {
          return false;
      }
      writeLock();
      setDeleted(addr, true);
      return true;
    } finally {
      writeUnlock();
    }
  }
  
  /**
   * Delete operation
   * TODO: compact on deletion 
   * @param keyPtr
   * @param keyLength
   * @return true if success, false otherwise
   * @throws RetryOperationException 
   */
  public boolean delete(long keyPtr,  int keyLength) throws RetryOperationException {
	try {
      long addr = get(keyPtr, keyLength);
      if (addr == NOT_FOUND) {
        return false;
      }
      writeLock();
      setDeleted(addr, true);
      return true;
    } finally {
      writeUnlock();
    }
  }

  /**
   * Get key-value offset in a block
   * @param key
   * @param keyOffset
   * @param keyLength
   * @return record offset or -1 if not found 
   * @throws RetryOperationException 
   */
  public long get(byte[] key, int keyOffset, int keyLength) throws RetryOperationException {
    
	try {
      readLock();
      long ptr = dataPtr;
      int count = 0;
      while (count++ < numRecords) {
        short keylen = normalize(UnsafeAccess.toShort(ptr));
        short vallen = UnsafeAccess.toShort(ptr + 2);
        if (keylen < 0 || keylen != keyLength) {
          // deleted or different sizes
          ptr += keylen + vallen + RECORD_PREFIX_LENGTH;
          continue;
        }
        int res = Utils.compareTo(key, keyOffset, keyLength, ptr + RECORD_PREFIX_LENGTH, keyLength);
        if (res == 0 && !isDeleted(ptr)) {
          return ptr;
        } /*else if (res > 0) {
        	return NOT_FOUND;
        }*/
        // Advance pointer
        ptr += keylen + vallen + RECORD_PREFIX_LENGTH;
      }
      return NOT_FOUND;
    } finally {
      readUnlock();
    }
  }  
  
  /**
   * Get value by key in a block
   * @param key
   * @param keyOffset
   * @param keyLength
   * @return value length or NOT_FOUND if not found 
   * @throws RetryOperationException 
   */
  public long get(byte[] key, int keyOffset, int keyLength, byte[] valueBuf, 
      int valOffset) throws RetryOperationException {
    
	try {
      int maxValueLength = valueBuf.length - valOffset;
      readLock();
      long ptr = dataPtr;
      int count = 0;
      while (count++ < numRecords) {
        short keylen = UnsafeAccess.toShort(ptr);
        short vallen = UnsafeAccess.toShort(ptr + 2);
        if (keylen < 0 || keylen != keyLength) {
          // deleted or different sizes
          ptr += normalize(keylen) + vallen + RECORD_PREFIX_LENGTH;
          continue;
        }
        int res = Utils.compareTo(key, keyOffset, keyLength, ptr + RECORD_PREFIX_LENGTH, keyLength);
        if (res == 0 && !isDeleted(ptr)) {
          if (vallen <= maxValueLength) {
            UnsafeAccess.copy(ptr + RECORD_PREFIX_LENGTH + keyLength, valueBuf, valOffset, vallen);
          }
          return vallen;
        }
        // TODO FIX sign of comparison
//        } else if (res > 0) {
//        	return NOT_FOUND;
//        }
        // Advance pointer
        ptr += keylen + vallen + RECORD_PREFIX_LENGTH;
      }
      return NOT_FOUND;
    } finally {
      readUnlock();
    }
  }

  /**
   * Get key-value offset in a block
   * @param key
   * @param keyOffset
   * @param keyLength
   * @return record offset or NOT_FOUND if not found 
   * @throws RetryOperationException 
   */
  public long get(long keyPtr, int keyLength) throws RetryOperationException {
    
	try {
      readLock();
      long ptr = dataPtr;
      int count = 0;
      while (count++ < numRecords) {
        short keylen = normalize(UnsafeAccess.toShort(ptr));
        short vallen = UnsafeAccess.toShort(ptr + 2);
        if (keylen < 0 || keylen != keyLength) {
          // deleted or different sizes
          ptr += keylen + vallen + RECORD_PREFIX_LENGTH;
          continue;
        }
        int res = Utils.compareTo(keyPtr, keyLength, ptr + RECORD_PREFIX_LENGTH, keyLength);
        if (res == 0 && !isDeleted(ptr)) {
          return ptr;
        } /*else if (res > 0) {
        	return NOT_FOUND;
        }*/
        // Advance pointer
        ptr += keylen + vallen + RECORD_PREFIX_LENGTH;
      }
      return NOT_FOUND;
    } finally {
      readUnlock();
    }
  }
  
  /**
   * Get key-value offset in a block
   * @param key
   * @param keyOffset
   * @param keyLength
   * @param valueBufLength value buffer length
   * @return value length if found, or NOT_FOUND. if value length >  valueBufLength
   *          no copy will be made - one must repeat call with new value buffer
   * @throws RetryOperationException 
   */
  public long get(long keyPtr, int keyLength, long valueBuf, int valueBufLength) 
      throws RetryOperationException {
    
	try {
      
	  readLock();
      long ptr = dataPtr;
      int count = 0;
      while (count++ < numRecords) {
        short keylen = normalize(UnsafeAccess.toShort(ptr));
        short vallen = UnsafeAccess.toShort(ptr + 2);
        if (keylen < 0 || keylen != keyLength) {
          // deleted or different sizes
          ptr += keylen + vallen + RECORD_PREFIX_LENGTH;
          continue;
        }
        int res = Utils.compareTo(keyPtr, keyLength, ptr + RECORD_PREFIX_LENGTH, keyLength);
        if (res == 0 && !isDeleted(ptr)) {
          if (vallen <= valueBufLength) {
            UnsafeAccess.copy(ptr + RECORD_PREFIX_LENGTH + keyLength, valueBuf, vallen);
          }
          return vallen;
        }/* else if (res > 0) {
        	return NOT_FOUND;
        }*/
        // Advance pointer
        ptr += keylen + vallen + RECORD_PREFIX_LENGTH;
      }
      return NOT_FOUND;
    } finally {
      readUnlock();
    }
  }
  
  /**
   * Get block size
   * @return block size
   */
  public int getBlockSize() {
    return blockSize;
  }
  
  /**
   * Get max block size
   * @return maxBlockSize
   */
  public int getMaxBlockSize() {
    return BigSortedMapOld.maxBlockSize;
  }
  
  /**
   * Get data size
   * @return data size
   */
  public int getDataSize() {
    return dataSize;
  }
  
  /**
   * Get number of records in a block
   * @return number of records
   */
  public int getNumRecords() {
    return numRecords;
  }
  
  /**
   * Get number of deleted records in a block
   * @return number of records
   */
  public int getNumDeletedRecords() {
    return numDeletedRecords;
  }
  
  /**
   * Compact block (remove deleted k-vs)
   * @param force - if true - force operation
   * @throws RetryOperationException 
   */
  
  public void compact(boolean force) throws RetryOperationException {
      long oldRecords = numRecords;
      
      if (numRecords == 0 || numDeletedRecords == 0) {
        return;
      }
      double ratio = ((double) numDeletedRecords) / numRecords;
      if (!force && ratio < MIN_COMPACT_RATIO) return;
      
      short keylen = UnsafeAccess.toShort(dataPtr);
      keylen = normalize(keylen);
      short vallen = UnsafeAccess.toShort(dataPtr+2);
      //We skip first record because we need
      // at least one record in a block, even deleted one
      // for blocks comparisons
      int firstRecordLength = keylen + vallen + RECORD_PREFIX_LENGTH;
      long ptr = dataPtr + firstRecordLength;
      
      while (ptr < dataPtr + dataSize) {
        keylen = UnsafeAccess.toShort(ptr);
        vallen = UnsafeAccess.toShort(ptr + 2);
        if (isDeleted(ptr)) {
          keylen = normalize(keylen);
          int toMove = keylen + vallen + RECORD_PREFIX_LENGTH;
          long len = dataPtr + dataSize - toMove - ptr;
          UnsafeAccess.copy(ptr + toMove, ptr, len);
          numRecords--;
          numDeletedRecords--;
          dataSize -= toMove;
          BigSortedMapOld.totalDataSize.addAndGet(-toMove);

        } else {
       // Advance pointer
          ptr += keylen + vallen + RECORD_PREFIX_LENGTH;
        } 
      }
      if (isDeleted(dataPtr) && numRecords > 1) {
        UnsafeAccess.copy(dataPtr + firstRecordLength, dataPtr, dataSize - firstRecordLength);
        numRecords--;
        numDeletedRecords--;
        dataSize -= firstRecordLength;
        BigSortedMapOld.totalDataSize.addAndGet(-firstRecordLength);
        // TODO above
      }
      if (oldRecords != numRecords) {
          if (seqNumberSplitOrMerge == Short.MAX_VALUE -1) {
        	  seqNumberSplitOrMerge = 0;
          } else {
        	  seqNumberSplitOrMerge++;
          }
      }
  }
  
  public long getSeqNumber() {
    return seqNumberSplitOrMerge;
  }
  /**
   * Must always compact before splitting block 
   * @param forceCompact
   * @return new (left) block
   * @throws RetryOperationException 
   */
  public Block split(boolean forceCompact) throws RetryOperationException {

	try {
      writeLock();
      // Increment sequence number
      if (seqNumberSplitOrMerge == Short.MAX_VALUE -1) {
    	  seqNumberSplitOrMerge = 0;
      } else {
    	  seqNumberSplitOrMerge++;
      }
      long off = 0;
      long ptr = dataPtr;
      int numRecords = 0;
      // compact first
      if (forceCompact) {
        compact(true);
      }
      // Now we should have zero deleted records
      while (true) {
        short keylen = UnsafeAccess.toShort(ptr + off);
        keylen = normalize(keylen);
        short vallen = UnsafeAccess.toShort(ptr + 2 + off);
        long old = off;
        off += keylen + vallen + RECORD_PREFIX_LENGTH;
        if (off >= dataSize / 2) {
          off = old;
          break;
        }
        numRecords++;
      }
      int oldDataSize = this.dataSize;
      int oldNumRecords = this.numRecords;
      this.dataSize = (int) off;
      this.numRecords = (short) numRecords;
      int leftDataSize = oldDataSize - this.dataSize;
      int leftBlockSize = getMinSizeGreaterThan(getBlockSize(), leftDataSize);
      Block left = new Block(leftBlockSize);

      left.numRecords = (short)(oldNumRecords - this.numRecords);
      left.numDeletedRecords = 0;
      left.dataSize = oldDataSize - this.dataSize;
      UnsafeAccess.copy(dataPtr + off, left.dataPtr, left.dataSize);
      // shrink current
      shrink();
      return left;
    } finally {
      writeUnlock();
    }
  }
  
  /**
   * Should merge this block
   * @return true, false
   */
  public boolean shouldMerge() {
    return (double) dataSize / blockSize < MAX_MERGE_RATIO;
  }
  
  /**
   * Should compact this block
   * @return true, false
   */
  public boolean shouldCompact() {
    if (numRecords == 0) return false;
    return (double) numDeletedRecords/ numRecords > MIN_COMPACT_RATIO;
  }
  /**
   * Merge two adjacent blocks
   * @param left
   * @param forceCompact
   * @param forceMerge
   * @return true, if merge successful, false - otherwise
   * @throws RetryOperationException 
   */
  public boolean merge(Block left, boolean forceCompact, boolean forceMerge) throws RetryOperationException {
    
	try {
      writeLock();
      left.writeLock();
      //Increment sequence numbers
      if (seqNumberSplitOrMerge == Short.MAX_VALUE -1) {
    	  seqNumberSplitOrMerge = 0;
      } else {
    	  seqNumberSplitOrMerge++;
      }
      if (left.seqNumberSplitOrMerge == Short.MAX_VALUE -1) {
    	  left.seqNumberSplitOrMerge = 0;
      } else {
    	  left.seqNumberSplitOrMerge++;
      }
      left.seqNumberSplitOrMerge++;
      if (!forceMerge && (!shouldMerge() || !left.shouldMerge())) {
        return false;
      }
      if (forceCompact) {
        compact(true);
        left.compact(true);
      }
      // Check total size
      while (this.dataSize + left.dataSize >= this.blockSize) {
        boolean result = expand();
        if (result == false) {
          //expansion failed
          throw new RuntimeException("Can not expand block for merge");
        }
      }
      UnsafeAccess.copy(left.dataPtr, this.dataPtr + dataSize, left.dataSize);
      this.numRecords += left.numRecords;
      this.numDeletedRecords = 0;
      this.dataSize += left.dataSize;
      
      // After merge left block becomes invalid
      //TODO
      return true;
    } finally {
      left.writeUnlock();
      writeUnlock();
    }
  }
  
  /**
   * Get block address
   * @return address
   */
  public long getAddress() {
    return dataPtr;
  }
  
  /**
   * Free memory
   */
  public void free() {
    UnsafeAccess.free(dataPtr);
  }
  
  /**
   * Get first key
   * @param skipDeleted
   * @return first key in a block
   */
  public final byte[] getFirstKey(boolean skipDeleted) {
    if (numRecords == 0) return null;
    synchronized (this) {
      try {
        readLock();
        long addr = skipDeleted ? skipDeletedRecords(dataPtr) : dataPtr;
        if (addr == NOT_FOUND) {
          return null;
        }
        short keylen = UnsafeAccess.toShort(addr);
        keylen = normalize(keylen);
        byte[] firstKey = new byte[keylen];
        UnsafeAccess.copy(addr + RECORD_PREFIX_LENGTH, firstKey, 0, keylen);
        return firstKey;
      } finally {
        readUnlock();
      }
    }
  }
  
  public final byte[] getFirstKey1() {
    return getFirstKey(false);
  }
  
  /**
   * Skips deleted records
   */
  long skipDeletedRecords(long addr) {
    long curPtr = addr;
    while (curPtr - dataPtr < dataSize) {
      short keylen = UnsafeAccess.toShort(curPtr);
      if (keylen > 0) {
        return curPtr;
      }
      keylen = normalize(keylen);
      short vallen = UnsafeAccess.toShort(curPtr + 2);
      curPtr += keylen + vallen + RECORD_PREFIX_LENGTH;      
    }
    return NOT_FOUND;
  }
    
  /**
   * Get last key  
   * @param skipDeleted
   * @return last key
   */
  public byte[] getLastKey(boolean skipDeleted) {
    long ptr = dataPtr;
    long lastFound = 0;
    try {
      readLock();
      while(ptr < dataPtr + dataSize) {
        boolean isDeleted = isDeleted(ptr);
        if ((isDeleted && !skipDeleted) || !isDeleted) {
          lastFound = ptr;
        } 
        short keySize = UnsafeAccess.toShort(ptr);
        keySize = normalize(keySize);
        short valueSize = UnsafeAccess.toShort(ptr + 2);
        ptr += keySize + valueSize + RECORD_PREFIX_LENGTH;
      }
      if (lastFound == 0) {
        return null;
      } else {
        short keySize = UnsafeAccess.toShort(lastFound);
        keySize = normalize(keySize);
        byte[] ret = new byte[keySize];
        UnsafeAccess.copy(lastFound + RECORD_PREFIX_LENGTH, ret, 0, ret.length);
        return ret;
      }
    } finally {
      readUnlock();
    }
  }
  
  public int getLastKey(byte[] buffer, int offset, boolean skipDeleted) {
    long ptr = dataPtr;
    long lastFound = 0;
    try {
      readLock();
      while(ptr < dataPtr + dataSize) {
        boolean isDeleted = isDeleted(ptr);
        if ((isDeleted && !skipDeleted) || !isDeleted) {
          lastFound = ptr;
        } 
        short keySize = UnsafeAccess.toShort(ptr);
        keySize = normalize(keySize);
        short valueSize = UnsafeAccess.toShort(ptr + 2);
        ptr += keySize + valueSize + RECORD_PREFIX_LENGTH;
      }
      if (lastFound == 0) {
        return -1;
      } else {
        short keySize = UnsafeAccess.toShort(lastFound);
        keySize = normalize(keySize);
        if (keySize <= buffer.length - offset) {
          UnsafeAccess.copy(lastFound + RECORD_PREFIX_LENGTH, buffer, 0, keySize);
        }
        return keySize;
      }
    } finally {
      readUnlock();
    }
  }
  private short normalize(short len) {
    short mask = 0x7fff;
    return (short) (mask & len);
  }
  

  
  @Override
	public int compareTo(Block o) {
		if (this == o)
			return 0;
		// TODO: make sure that we do not touch first key on compaction

//		short keylen1 = UnsafeAccess.toShort(dataPtr);
//		short keylen2 = UnsafeAccess.toShort(o.dataPtr);
//		keylen1 = normalize(keylen1);
//		keylen2 = normalize(keylen2);
//		return Utils.compareTo(dataPtr + RECORD_PREFIX_LENGTH, keylen1, o.dataPtr + RECORD_PREFIX_LENGTH, keylen2);
		byte[] firstKey = getFirstKey();
		byte[] firstKey1 = o.getFirstKey();
		return Utils.compareTo(firstKey, 0, firstKey.length, firstKey1,  0, firstKey1.length);
	}
  
    byte[] firstKey;
    
    public byte[] getFirstKey() {
    	if (firstKey != null) {
    		return firstKey;
    	}
    	try {
    		readLock();
    		short keylen = UnsafeAccess.toShort(dataPtr);
    		keylen = normalize(keylen);
    		byte[] buf = new byte[keylen];
    		UnsafeAccess.copy(dataPtr + RECORD_PREFIX_LENGTH, buf, 0, buf.length);
    		firstKey = buf;
    		return firstKey;
    	} finally {
    		readUnlock();
    	}
    }
}
