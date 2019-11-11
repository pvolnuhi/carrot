package org.bigbase.carrot;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import org.bigbase.util.Bytes;
import org.bigbase.util.UnsafeAccess;
import org.bigbase.util.Utils;
import org.bigbase.zcache.BigSortedMapOld;

/**
 * Records are lexicographically sorted 
 * Format: <KV>+ 
 * <KV> : 
 * 0 .. 1 - key size (2 bytes: max key size = 32K) 
 * 2 .. 3 - value size (2 bytes: max embedded value size = 32K if value size == -1,
 * value is stored externally, we keep embedded 12 bytes for value in this case: 4 bytes - size; 8
 * bytes - address) 
 * <EXPIRATION> (8 bytes time in ms: 0 - never expires) 
 * <EVICTION> (8 bytes - used by eviction algorithm : LRU, LFU) 
 * <KEY> 
 * <SEQUENCEID> - 8 bytes
 * <TYPE> - 1 byte: 0 - DELETE, 1 - PUT (DELETE comes first) 
 * <VALUE> 
 * For ordering we use combination of <KEY><SEQUENCEID><TYPE> 
 * TODO: 
 * 1. Add support for type 
 * 2. Add support for sequenceid 
 * 3. Add support for expiration filed and API 
 * 4. Add support for eviction field 
 * 5 Optimize storeFence usage - make sure we call it once before we release write lock 
 * We need sequenceId & type to support snapshots & tx
 */
@SuppressWarnings("unused")
public class DataBlock  {
  private final static Logger LOG = Logger.getLogger(DataBlock.class.getName());
  
  public final static int KEY_SIZE_LENGTH = 2;
  public final static int VALUE_SIZE_LENGTH = 2;
  public final static int KV_SIZE_LENGTH = KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH;
  public final static int EXPIRE_SIZE_LENGTH = 8;
  public final static int EVICTION_SIZE_LENGTH = 8;
  public final static int RECORD_PREFIX_LENGTH =
      KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH + EXPIRE_SIZE_LENGTH + EVICTION_SIZE_LENGTH;
  public final static long NOT_FOUND = -1L;
  public final static int TYPE_SIZE = 1;
  public final static int SEQUENCEID_SIZE = 8;
  // Overhead for K-V = 13 bytes
  public final static int RECORD_TOTAL_OVERHEAD =
      RECORD_PREFIX_LENGTH + TYPE_SIZE + SEQUENCEID_SIZE;
  public final static byte DELETED_MASK = (byte) (1 << 7);
  public final static double MIN_COMPACT_RATIO = 0.25d;
  public final static double MAX_MERGE_RATIO = 0.25d;

  public final static String MAX_BLOCK_SIZE_KEY = "max.block.size";
  public static short MAX_BLOCK_SIZE = 4096;
  public final static long NO_VERSION = -1;
  
  static {
    String val = System.getProperty(MAX_BLOCK_SIZE_KEY);
    if (val != null) {
      MAX_BLOCK_SIZE = Short.parseShort(val);
    }
  }

  static ThreadLocal<Long> bufPtr = new ThreadLocal<Long>() {

    @Override
    protected Long initialValue() {
      int maxSize = BigSortedMap.getMaxBlockSize() + 80;
      return UnsafeAccess.malloc(maxSize);
    }

  };
  /*
   * TODO: make this configurable
   */

  static float[] BLOCK_RATIOS = new float[] { 0.25f, 0.5f, 0.75f, 1.0f };
  /*
   * Read-Write Lock TODO: StampedLock (Java 8)
   */
  static ReentrantReadWriteLock[] locks = new ReentrantReadWriteLock[11113];
  static {
    for (int i = 0; i < locks.length; i++) {
      locks[i] = new ReentrantReadWriteLock();
    }
  }

  /**
   * Get total allocated memory
   * @return memory
   */
  static long getTotalAllocatedMemory() {
    return BigSortedMap.totalAllocatedMemory.get();
  }

  /**
   * Get total data size
   * @return total data size
   */
  static long getTotalDataSize() {
    return BigSortedMap.totalDataSize.get();
  }

  /**
   * Get min size greater than current
   * @param max - max size
   * @param current current size
   * @return min size or -1;
   */
  static int getMinSizeGreaterThan(int max, int current) {
    for (int i = 0; i < BLOCK_RATIOS.length; i++) {
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
    for (int i = 0; i < BLOCK_RATIOS.length; i++) {
      int size = Math.round(max * BLOCK_RATIOS[i]);
      if (size >= current) return size;
    }
    return -1;
  }

  final static int dataPtrOffset = 0;

  /*
   * Current block size (2 bytes = 64K max, actually 32K)
   */
  final static int blockSizeOffset = 8;

  /*
   * Data size (volatile?) 2 bytes
   */
  final static int dataSizeOffset = 10;
  /*
   * Number of KVs in a block - index size (volatile?) - 2 bytes
   */
  final static int numRecordsOffset = 12;

  /*
   * Split/Merge sequence number. use 1 byte
   */
  final static int seqNumberSplitOrMergeOffset = 14;

  /*
   * Is block compressed (1 byte)
   */
  final static int compressedOffset = 15;

  /*
   * Number of deleted & updated records (2 bytes) - is used during compaction
   * actually, they are pending records waiting for compaction
   */
  final static int numDeletedAndUpdatedRecordsOffset = 16;

  /*
   * Block's address (current)
   */
  volatile long dataPtr = 0;

  /*
   * Initial block size
   */
  short blockSize;

  short dataSize;
  
  short numRecords;
  
  short numDeletedAndUpdatedRecords;
  
  
  byte seqNumberSplitOrMerge;
  
  boolean compressed;
  
  volatile boolean threadSafe;
  /*
   * Block header address (in index block)
   */

  volatile long indexPtr = 0;

  /*
   * Parent index block
   */

  IndexBlock indexBlock;


  /*
   * Current lock
   */
  ReentrantReadWriteLock curLock;

  volatile boolean valid = true;

  public void dump() {
    LOG.info("====================================");
    LOG.info("Address        =" + getDataPtr());
    LOG.info("Block size     =" + getBlockSize());
    LOG.info("Data size      =" + getDataSize());
    LOG.info("Number k/v's   =" + getNumberOfRecords());
    LOG.info("Number del/upd =" + getNumberOfDeletedAndUpdatedRecords());
    LOG.info("First key      =" + Bytes.toHex(getFirstKey()));
  }
  /**
   * Create new block with a given size
   * @param size of a block
   */
  DataBlock(int size) {
    dataPtr = UnsafeAccess.malloc(size);
    if (dataPtr == 0) {
      throw new RuntimeException("Failed to allocate " + size + " bytes");
    }
    BigSortedMap.totalAllocatedMemory.addAndGet(size);
    // TODO: why is index size the same?
    BigSortedMap.totalIndexSize.addAndGet(size);

    this.blockSize = (short)size;
  }

  DataBlock() {
    String val = System.getProperty(MAX_BLOCK_SIZE_KEY);
    short size = 0;
    if (val != null) {
      size = Short.parseShort(val);
    } else {
      size = MAX_BLOCK_SIZE;
    }
    dataPtr = UnsafeAccess.malloc(size);
    if (dataPtr == 0) {
      throw new RuntimeException("Failed to allocate " + size + " bytes");
    }
    BigSortedMap.totalAllocatedMemory.addAndGet(size);
    BigSortedMap.totalIndexSize.addAndGet(size);

    this.blockSize = size;
  }
  
  /**
   * Copy constructor
   * @param db
   */
  DataBlock(DataBlock db) {
    this();
    copyOf(db);
  }
  
  /**
   * For detached data block (used in IndexBlockScanner/DataBlockScanner)
   * No locking is applied, because this method is called
   * inside writeLock of an IndexBlock
   * @param b block
   */
  public void copyOf(DataBlock b) {
    this.dataSize = b.getDataSize();
    this.numRecords = b.getNumberOfRecords();
    this.numDeletedAndUpdatedRecords = b.getNumberOfDeletedAndUpdatedRecords();
    //TODO: always valid here?
    this.valid = b.isValid();
    this.compressed = b.isCompressed();
    this.seqNumberSplitOrMerge = b.getSeqNumberSplitOrMerge();
    // This block is going to be used by single thread
    this.threadSafe = true;
    UnsafeAccess.copy(b.dataPtr, this.dataPtr, this.dataSize);
  }
  
  public boolean isValid() {
    return valid;
  }

  public void setValid(boolean b) {
    this.valid = b;
  }
  
  public long getIndexPtr() {
    return indexPtr;
  }

  public void set(IndexBlock indexBlock, long off) {
    this.indexBlock = indexBlock;
    this.indexPtr = indexBlock.getAddress() + off;
    this.dataPtr = getDataPtr();
    this.valid = true;
  }

  /**
   *  Register new block
   * @param indexBlock - parent index block
   * @param off        - offset in bytes in parent index block
   */
  public void register(IndexBlock indexBlock, long off) {
    this.indexBlock = indexBlock;
    this.indexPtr = indexBlock.getAddress() + off;
    setDataPtr(this.dataPtr);
    setBlockSize(this.blockSize);
    setDataSize(dataSize);
    setNumberOfRecords(numRecords);
    setNumberOfDeletedAndUpdatedRecords(numDeletedAndUpdatedRecords);
    setSeqNumberSplitOrMerge(seqNumberSplitOrMerge);
    setThreadSafe(threadSafe);
    // TODO compression
    setCompressed(compressed);
  }

  public static final short keyLength(long ptr) {
    return UnsafeAccess.toShort(ptr);
  }
  
  public static final short valueLength(long ptr) {
    return UnsafeAccess.toShort(ptr + KEY_SIZE_LENGTH);
  }
  
  public static final long keyAddress(long ptr) {
    return ptr + RECORD_PREFIX_LENGTH;
  }
  
  public static final long version(long ptr) {
    short keylen = keyLength(ptr);
    return UnsafeAccess.toLong(ptr + RECORD_PREFIX_LENGTH + keylen);
  }
  
  public static final long valueAddress(long ptr) {
    return keyLength(ptr) + keyAddress(ptr) + SEQUENCEID_SIZE + TYPE_SIZE;
  }
  
  public static final Op type(long ptr) {
    short keylen = keyLength(ptr);
    byte v =UnsafeAccess.toByte(ptr + RECORD_PREFIX_LENGTH + keylen + SEQUENCEID_SIZE);
    return Op.values()[v];
  }
  
  /**
   * Get data pointer
   * @return data pointer
   */
  public final long getDataPtr() {
    if (!detached()) {
      return UnsafeAccess.toLong(this.indexPtr);
    } else {
      return this.dataPtr;
    }
  }

  /**
   * Set data ptr
   * @param ptr
   */
  public final void setDataPtr(long ptr) {
    UnsafeAccess.putLong(this.indexPtr, ptr);
    UnsafeAccess.storeFence();
  }

  /**
   * Get block size
   * @return data size
   */
  public final short getBlockSize() {
    if (!detached()) {
      return UnsafeAccess.toShort(this.indexPtr + blockSizeOffset);
    } else {
      return this.blockSize;
    }
  }

  private boolean detached() {
    return this.indexBlock == null;
  }
  
  /**
   * Set block size
   * @param v block size
   */
  public final void setBlockSize(short v) {
    UnsafeAccess.putShort(this.indexPtr + blockSizeOffset, v);
    // TODO SFENCE?
    UnsafeAccess.storeFence();
  }

  /**
   * Increment data size
   */
  public final void incrBlockSize(short val) {
    short v = getBlockSize();
    // TODO max size check
    setBlockSize((short) (v + val));
  }

  /**
   * Get data size
   * @return data size
   */
  public short getDataSize() {
    if (!detached()) {
      return UnsafeAccess.toShort(this.indexPtr + dataSizeOffset);
    } else {
      return this.dataSize;
    }
  }

  /**
   * Set data size
   * @param v data size
   */
  public void setDataSize(short v) {
    UnsafeAccess.putShort(indexPtr + dataSizeOffset, v);
    // TODO SFENCE?
    UnsafeAccess.storeFence();
  }

  /**
   * Increment data size
   */
  public void incrDataSize(short val) {
    short v = getDataSize();
    setDataSize((short) (v + val));
  }

  /**
   * Get number of records
   * @return number
   */
  public short getNumberOfRecords() {
    if (!detached()) {
      return UnsafeAccess.toShort(indexPtr + numRecordsOffset);
    } else {
      return this.numRecords;
    }
  }

  /**
   * Set number of records
   * @param v number
   */
  public void setNumberOfRecords(short v) {
    UnsafeAccess.putShort(indexPtr + numRecordsOffset, v);
    // TODO SFENCE?
    UnsafeAccess.storeFence();

  }

  /**
   * Increment number records
   */
  public void incrNumberOfRecords(short val) {
    short v = getNumberOfRecords();
    // TODO check if exceeds Short.MAX_VALUE
    setNumberOfRecords((short) (v + val));
  }

  /**
   * Get number of deleted records
   * @return number
   */
  public short getNumberOfDeletedAndUpdatedRecords() {
    if (!detached()) {
      return UnsafeAccess.toShort(indexPtr + numDeletedAndUpdatedRecordsOffset);
    } else {
      return this.numDeletedAndUpdatedRecords;
    }
  }

  /**
   * Set number of deleted records
   * @param v number
   */
  public void setNumberOfDeletedAndUpdatedRecords(short v) {
    UnsafeAccess.putShort(indexPtr + numDeletedAndUpdatedRecordsOffset, v);
    // TODO SFENCE?
    UnsafeAccess.storeFence();

  }

  /**
   * Increment number of deleted records
   */
  public void incrNumberDeletedAndUpdatedRecords(short val) {
    short v = getNumberOfDeletedAndUpdatedRecords();
    setNumberOfDeletedAndUpdatedRecords((short) (v + val));
  }

  /**
   * Get sequence number split or merge
   * @return sequence number
   */
  public final byte getSeqNumberSplitOrMerge() {
    if(!detached()) {
      return UnsafeAccess.toByte(indexPtr + seqNumberSplitOrMergeOffset);
    } else {
      return this.seqNumberSplitOrMerge;
    }
  }

  /**
   * Set sequence number split or merge
   * @param val
   */
  public final void setSeqNumberSplitOrMerge(byte val) {
    UnsafeAccess.putByte(indexPtr + seqNumberSplitOrMergeOffset, val);
    UnsafeAccess.storeFence();
  }

  /**
   * Increment sequence number split or merge
   * @param delta
   */
  public final void incrSeqNumberSplitOrMerge() {
    byte v = getSeqNumberSplitOrMerge();
    if (v == Byte.MAX_VALUE) {
      v = 0;
    } else {
      v += 1;
    }
    setSeqNumberSplitOrMerge(v);
  }

  /**
   * Is thread safe
   * @return thread safe
   */
  public final boolean isThreadSafe() {
    return threadSafe;
  }

  /**
   * Set thread safe
   * @param b thread safe (true/false)
   */
  public void setThreadSafe(boolean b) {
    threadSafe = b;
  }

  /**
   * Is block compressed
   * @return compressed
   */
  public boolean isCompressed() {
    return UnsafeAccess.toByte(indexPtr + compressedOffset) == 1;
  }

  /**
   * Set block is compressed
   * @param b
   */
  public void setCompressed(boolean b) {
    byte val = 0;
    if (b) val = (byte) 1;
    UnsafeAccess.putByte(indexPtr + compressedOffset, val);
    UnsafeAccess.storeFence();
  }

  /**
   * Reset block for reuse TODO: method is not used and is not correct?
   */
//  public void reset() {
//    // TODO: block size?
//    //TODO: dataPtr?
//    setSeqNumberSplitOrMerge((byte) 0);
//    setNumberOfRecords((short) 0);
//    setNumberOfDeletedAndUpdatedRecords((short) 0);
//    setDataSize((short) 0);
//    setCompressed(false);
//    setThreadSafe(false);
//    this.indexBlock = null;
//    //this.dataPtr = 0;
//    this.blockSize = 0;
//    this.numRecords = 0;
//    this.numDeletedAndUpdatedRecords = 0;
//    this.seqNumberSplitOrMerge = 0;
//    this.compressed = false;
//    this.threadSafe = false;
//  }

  /**
   * Expands block
   * @return true if success
   */
  boolean expand() {

    // Get next size
    int blockSize = getBlockSize();
    int dataSize = getDataSize();
    int nextSize = getMinSizeGreaterThan(BigSortedMap.maxBlockSize, blockSize);
    if (nextSize < 0 || nextSize < blockSize) {
      return false;
    }
    long newPtr = UnsafeAccess.malloc(nextSize);
    if (newPtr <= 0) {
      return false;
    }
    BigSortedMap.totalAllocatedMemory.addAndGet(nextSize - blockSize);
    // Do copy
    UnsafeAccess.copy(dataPtr, newPtr, dataSize);
    UnsafeAccess.free(dataPtr);
    this.dataPtr = newPtr;
    setDataPtr(newPtr);
    setBlockSize((short) nextSize);
    return true;
  }

  /**
   * Shrink block
   * @return true if success
   */
  boolean shrink() {
    // Get next size
    int dataSize = getDataSize();
    int blockSize = getBlockSize();
    int nextSize = getMinSizeGreaterThan(BigSortedMap.maxBlockSize, dataSize);
    if (nextSize < 0 || nextSize < dataSize) {
      return false;
    }
    long newPtr = UnsafeAccess.malloc(nextSize);
    if (newPtr <= 0) {
      return false;
    }
    BigSortedMap.totalAllocatedMemory.addAndGet(nextSize - blockSize);

    // Do copy
    UnsafeAccess.copy(dataPtr, newPtr, dataSize);
    UnsafeAccess.free(dataPtr);
    this.dataPtr = newPtr;
    setDataPtr(newPtr);
    setBlockSize((short) nextSize);
    return true;
  }

  /**
   * Read lock TODO: loop until we get correct lock TODO: make sure we release lock
   * @throws RetryOperationException
   * @throws InterruptedException
   */
  public void readLock() throws RetryOperationException {
    if (isThreadSafe()) return;
    long before = getSeqNumberSplitOrMerge();
    int index = (int) (getAddress() % locks.length);
    curLock = locks[index];
    curLock.readLock().lock();
    //*DEBUG*/ System.out.println("Read lock " + index+" "+ curLock );
    long after = getSeqNumberSplitOrMerge();
    if (before != after) {
      throw new RetryOperationException();
    }
  }

  /**
   * Read unlock
   */
  public void readUnlock() {
    if (isThreadSafe()) return;
    curLock.readLock().unlock();
    //*DEBUG*/ System.out.println("Read unlock lock " + curLock );
  }

  /**
   * Write lock
   * @throws RetryOperationException
   * @throws InterruptedException
   */
  public void writeLock() throws RetryOperationException {
    if (isThreadSafe()) return;
    long before = getSeqNumberSplitOrMerge();
    int index = (int) (getAddress() % locks.length);
    curLock = locks[index];
    //*DEBUG*/ System.out.println("about to write lock: "+ index);
    curLock.writeLock().lock();
    //*DEBUG*/ System.out.println("acquired write lock: "+ index);

    long after = getSeqNumberSplitOrMerge();
    //*DEBUG*/ System.out.println("before="+ before+" aftre="+ after);
    if (before != after) {
      throw new RetryOperationException();
    }
  }

  /**
   * Write unlock
   */
  public void writeUnlock() {
    if (isThreadSafe()) {
      ///*DEBUG*/ System.out.println("write unlock: thread safe");
      return;
    }
    if (curLock != null) {
      curLock.writeLock().unlock();
      ///*DEBUG*/ System.out.println("unlocked write lock: "+ curLock);
      UnsafeAccess.storeFence();
    }
  }

  /**
   * Put operation
   * @param key
   * @param keyOffset
   * @param keyLength
   * @param value
   * @param valueOffset
   * @param valueLength
   * @param version
   * @return true, if success, false otherwise (no room, split block)
   */
  public boolean put(byte[] key, int keyOffset, int keyLength, byte[] value, int valueOffset,
      int valueLength, long version, long expire) throws RetryOperationException {

    boolean onlyExactOverwrite = false;
    boolean recordOverwrite = false;
    boolean keyOverwrite = false; // key the same, value is different in size
    // Get the most recent active Tx Id or snapshot Id (scanner)
    long mostRecentActiveTxId = BigSortedMap.getMostRecentActiveTxSeqId();
    try {
      writeLock();
      int dataSize = getDataSize();
      int blockSize = getBlockSize();
      if (dataSize + keyLength + valueLength + RECORD_TOTAL_OVERHEAD > blockSize) {
        // try compact first (remove deleted, updated)
        compact(true);
        // Get data size again
        dataSize = getDataSize();
        if (dataSize + keyLength + valueLength + RECORD_TOTAL_OVERHEAD > blockSize) {
          // try to expand block
          boolean res = expand();
          blockSize = getBlockSize();
          if (!res || (dataSize + keyLength + valueLength + RECORD_TOTAL_OVERHEAD > blockSize)) {
            // Still not enough room
            // Only exact overwrite (key & value) and only if
            // there are no conflicting Tx or snapshots exist
            onlyExactOverwrite = true;
          }
        }
      }

      long addr = search(key, keyOffset, keyLength, version);
      // TODO: verify what search returns if not found
      long foundSeqId = -1;
      if (addr < dataPtr + dataSize) {
        short keylen = UnsafeAccess.toShort(addr);
        short vallen = UnsafeAccess.toShort(addr + KEY_SIZE_LENGTH);
        if (keylen == keyLength) {
          // Compare keys
          int res = Utils.compareTo(key, keyOffset, keyLength, addr + RECORD_PREFIX_LENGTH, keylen);
          if (res == 0 && vallen == valueLength) {
            recordOverwrite = true;
            keyOverwrite = true;
          } else if (res == 0) {
            keyOverwrite = true;
          }
          if (keyOverwrite) {
            // Get seqId of existing record
            foundSeqId = getRecordSeqId(addr, (short) keyLength);
          }
        }
      }

      int moveDist = RECORD_TOTAL_OVERHEAD + keyLength + valueLength;
      // Classical case: the same key is not found
      boolean insert = !keyOverwrite;
      // The same key was found but we have to preserve
      // old value due to active Tx or snapshot scanner
      insert = insert || (keyOverwrite && (foundSeqId < mostRecentActiveTxId));
      // Overwrite only if there are no conflicting Tx or snapshots
      boolean overwrite = recordOverwrite && (foundSeqId > mostRecentActiveTxId);

      if (onlyExactOverwrite && !overwrite && insert) {
        // Failed to put - split the block
        return false;
      }

      if (insert) {
        // New K-V INSERT or we can't overwrite because of active Tx or snapshot
        // move from offset to offset + moveDist
        UnsafeAccess.copy(addr, addr + moveDist, dataPtr + dataSize - addr);
        UnsafeAccess.copy(key, keyOffset, addr + RECORD_PREFIX_LENGTH, keyLength);
        UnsafeAccess.copy(value, valueOffset, addr + RECORD_TOTAL_OVERHEAD + keyLength,
          valueLength);
        // Update key-value length
        UnsafeAccess.putShort(addr, (short) keyLength);
        UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) valueLength);

        // Set version, expire, op type and eviction (0)
        setRecordSeqId(addr, (short) keyLength, version);
        setRecordEviction(addr, 0L);
        if (expire > 0) {
          setRecordExpire(addr, expire);
        }
        setRecordType(addr, (short) keyLength, Op.PUT);

        incrNumberOfRecords((short) 1);
        incrDataSize((short) moveDist);
        if (keyOverwrite) {
          incrNumberDeletedAndUpdatedRecords((short) 1);
        }
        if (isThreadSafe() == false) {
          // TODO: why is this check?
          BigSortedMap.totalDataSize.addAndGet(moveDist);
        }
      } else if (overwrite) {
        // UPDATE existing
        // We do overwrite of existing record
        UnsafeAccess.copy(value, valueOffset, addr + RECORD_TOTAL_OVERHEAD + keyLength,
          valueLength);
        // Set version, expire, op type and eviction (0)
        setRecordSeqId(addr, (short) keyLength, version);
        // Do not update eviction, because we overwrite existing record
        // setRecordEviction(addr, 0L);
        if (expire > 0) {
          setRecordExpire(addr, expire);
        }
        setRecordType(addr, (short) keyLength, Op.PUT);
      } else {
        // Neither insert nor overwrite - delete, then - insert
        // keyOverwrite = true
        // delete existing, put new

        short keylen = UnsafeAccess.toShort(addr);// must be equal to keyLength
        short vallen = UnsafeAccess.toShort(addr + KEY_SIZE_LENGTH);
        moveDist = keylen + vallen + RECORD_TOTAL_OVERHEAD;
        int toMove = (valueLength - vallen);
        if (onlyExactOverwrite && (dataSize + toMove > blockSize)) {
          // failed to insert, split is required
          return false;
        }
        // move from offset to offset + moveDist
        UnsafeAccess.copy(addr + moveDist, addr + moveDist + toMove,
          dataPtr + dataSize - addr - moveDist);
        // TODO: check if we need this
        UnsafeAccess.copy(key, keyOffset, addr + RECORD_PREFIX_LENGTH, keyLength);
        UnsafeAccess.copy(value, valueOffset, addr + RECORD_TOTAL_OVERHEAD + keyLength,
          valueLength);
        // Update key-value length
        UnsafeAccess.putShort(addr, (short) keyLength);
        UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) valueLength);

        // Set version, expire, op type and eviction (0)
        setRecordSeqId(addr, (short) keyLength, version);
        // Do not reset eviction field
        // setRecordEviction(addr, 0L);
        if (expire > 0) {
          setRecordExpire(addr, expire);
        }
        setRecordType(addr, (short) keyLength, Op.PUT);
        incrDataSize((short) toMove);
        if (isThreadSafe() == false) {
          BigSortedMap.totalDataSize.addAndGet(toMove);
        }
      }
      return true;
    } finally {
      writeUnlock();
    }
  }

  private static long getRecordSeqId(long recordAddress) {
    short keyLen = UnsafeAccess.toShort(recordAddress);
    return UnsafeAccess.toLong(recordAddress + RECORD_PREFIX_LENGTH + keyLen);
  }

  private static long getRecordSeqId(long recordAddress, short keyLen) {
    return UnsafeAccess.toLong(recordAddress + RECORD_PREFIX_LENGTH + keyLen);
  }

  private static void setRecordSeqId(long recordAddress, long seqId) {
    short keyLen = UnsafeAccess.toShort(recordAddress);
    UnsafeAccess.putLong(recordAddress + RECORD_PREFIX_LENGTH + keyLen, seqId);
  }

  private static void setRecordSeqId(long recordAddress, short keyLen, long seqId) {
    UnsafeAccess.putLong(recordAddress + RECORD_PREFIX_LENGTH + keyLen, seqId);
  }

  private static long getRecordExpire(long recordAddress) {
    return UnsafeAccess.toLong(recordAddress + KV_SIZE_LENGTH);
  }

  private static void setRecordExpire(long recordAddress, long time) {
    UnsafeAccess.putLong(recordAddress + KV_SIZE_LENGTH, time);
  }

  private static long getRecordEviction(long recordAddress) {
    return UnsafeAccess.toLong(recordAddress + KV_SIZE_LENGTH + EXPIRE_SIZE_LENGTH);
  }

  private static void setRecordEviction(long recordAddress, long value) {
    UnsafeAccess.putLong(recordAddress + KV_SIZE_LENGTH + EXPIRE_SIZE_LENGTH, value);
  }

  private static Op getRecordType(long recordAddress) {
    short keyLen = UnsafeAccess.toShort(recordAddress);
    int val = UnsafeAccess.toByte(recordAddress + RECORD_PREFIX_LENGTH + keyLen + SEQUENCEID_SIZE);
    return Op.values()[val];
  }

  private static Op getRecordType(long recordAddress, short keyLen) {
    int val = UnsafeAccess.toByte(recordAddress + RECORD_PREFIX_LENGTH + keyLen + SEQUENCEID_SIZE);
    return Op.values()[val];
  }

  private static void setRecordType(long recordAddress, Op type) {
    short keyLen = UnsafeAccess.toShort(recordAddress);
    UnsafeAccess.putByte(recordAddress + RECORD_PREFIX_LENGTH + keyLen + SEQUENCEID_SIZE,
      (byte) type.ordinal());
  }

  private static void setRecordType(long recordAddress, short keyLen, Op type) {
    UnsafeAccess.putByte(recordAddress + RECORD_PREFIX_LENGTH + keyLen + SEQUENCEID_SIZE,
      (byte) type.ordinal());
  }

  /**
   * Put k-v operation
   * @param keyPtr
   * @param keyLength
   * @param valuePtr
   * @param valueLength
   * @param version
   * @return true, if success, false otherwise
   * @throws RetryOperationException
   */
  public boolean put(long keyPtr, int keyLength, long valuePtr, int valueLength, long version,
      long expire) throws RetryOperationException {

    boolean onlyExactOverwrite = false;
    boolean recordOverwrite = false;
    boolean keyOverwrite = false; // key the same, value is different in size
    // Get the most recent active Tx Id or snapshot Id (scanner)
    long mostRecentActiveTxId = BigSortedMap.getMostRecentActiveTxSeqId();
    try {
      writeLock();
      int dataSize = getDataSize();
      int blockSize = getBlockSize();
      if (dataSize + keyLength + valueLength + RECORD_TOTAL_OVERHEAD > blockSize) {
        // try compact first (remove deleted, updated)
        compact(true);
        // Get data size again
        dataSize = getDataSize();
        if (dataSize + keyLength + valueLength + RECORD_TOTAL_OVERHEAD > blockSize) {
          // try to expand block
          boolean res = expand();
          blockSize = getBlockSize();
          if (!res || (dataSize + keyLength + valueLength + RECORD_TOTAL_OVERHEAD > blockSize)) {
            // Still not enough room
            // Only exact overwrite (key & value) and only if
            // there are no conflicting Tx or snapshots exist
            onlyExactOverwrite = true;
          }
        }
      }

      long addr = search(keyPtr, keyLength, version);
      // TODO: verify what search returns if not found
      long foundSeqId = -1;
      if (addr < dataPtr + dataSize) {
        short keylen = UnsafeAccess.toShort(addr);
        short vallen = UnsafeAccess.toShort(addr + KEY_SIZE_LENGTH);
        if (keylen == keyLength) {
          // Compare keys
          int res = Utils.compareTo(keyPtr, keyLength, addr + RECORD_PREFIX_LENGTH, keylen);
          if (res == 0 && vallen == valueLength) {
            recordOverwrite = true;
            keyOverwrite = true;
          } else if (res == 0) {
            keyOverwrite = true;
          }
          if (keyOverwrite) {
            // Get seqId of existing record
            foundSeqId = getRecordSeqId(addr, (short) keyLength);
          }
        }
      }

      int moveDist = RECORD_TOTAL_OVERHEAD + keyLength + valueLength;
      // Classical case: the same key is not found
      boolean insert = !keyOverwrite;
      // The same key was found but we have to preserve
      // old value due to active Tx or snapshot scanner
      insert = insert || (keyOverwrite && (foundSeqId < mostRecentActiveTxId));
      // Overwrite only if there are no conflicting Tx or snapshots
      boolean overwrite = recordOverwrite && (foundSeqId > mostRecentActiveTxId);

      if (onlyExactOverwrite && !overwrite && insert) {
        // Failed to put - split the block
        return false;
      }

      if (insert) {
        // New K-V INSERT or we can't overwrite because of active Tx or snapshot
        // move from offset to offset + moveDist
        UnsafeAccess.copy(addr, addr + moveDist, dataPtr + dataSize - addr);
        UnsafeAccess.copy(keyPtr, addr + RECORD_PREFIX_LENGTH, keyLength);
        UnsafeAccess.copy(valuePtr, addr + RECORD_TOTAL_OVERHEAD + keyLength, valueLength);
        // Update key-value length
        UnsafeAccess.putShort(addr, (short) keyLength);
        UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) valueLength);

        // Set version, expire, op type and eviction (0)
        setRecordSeqId(addr, (short) keyLength, version);
        setRecordEviction(addr, 0L);
        if (expire > 0) {
          setRecordExpire(addr, expire);
        }
        setRecordType(addr, (short) keyLength, Op.PUT);

        incrNumberOfRecords((short) 1);
        incrDataSize((short) moveDist);
        if (keyOverwrite) {
          incrNumberDeletedAndUpdatedRecords((short)1);
        }
        if (isThreadSafe() == false) {
          // TODO: why is this check?
          BigSortedMap.totalDataSize.addAndGet(moveDist);
        }
      } else if (overwrite) {
        // UPDATE existing
        // We do overwrite of existing record
        UnsafeAccess.copy(valuePtr, addr + RECORD_TOTAL_OVERHEAD + keyLength, valueLength);
        // Set version, expire, op type and eviction (0)
        setRecordSeqId(addr, (short) keyLength, version);
        // Do not update eviction, because we overwrite existing record
        // setRecordEviction(addr, 0L);
        if (expire > 0) {
          setRecordExpire(addr, expire);
        }
        setRecordType(addr, (short) keyLength, Op.PUT);
      } else {
        // Neither insert nor overwrite - delete, then - insert
        // keyOverwrite = true
        // delete existing, put new

        short keylen = UnsafeAccess.toShort(addr);// must be equal to keyLength
        short vallen = UnsafeAccess.toShort(addr + KEY_SIZE_LENGTH);
        moveDist = keylen + vallen + RECORD_TOTAL_OVERHEAD;
        int toMove = (valueLength - vallen);
        if (onlyExactOverwrite && (dataSize + toMove > blockSize)) {
          // failed to insert, split is required
          return false;
        }
        // move from offset to offset + moveDist
        UnsafeAccess.copy(addr + moveDist, addr + moveDist + toMove,
          dataPtr + dataSize - addr - moveDist);
        // TODO: check if we need this
        UnsafeAccess.copy(keyPtr, addr + RECORD_PREFIX_LENGTH, keyLength);
        UnsafeAccess.copy(valuePtr, addr + RECORD_TOTAL_OVERHEAD + keyLength, valueLength);
        // Update key-value length
        UnsafeAccess.putShort(addr, (short) keyLength);
        UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) valueLength);

        // Set version, expire, op type and eviction (0)
        setRecordSeqId(addr, (short) keyLength, version);
        // Do not reset eviction field
        // setRecordEviction(addr, 0L);
        if (expire > 0) {
          setRecordExpire(addr, expire);
        }
        setRecordType(addr, (short) keyLength, Op.PUT);
        incrDataSize((short) toMove);
        if (isThreadSafe() == false) {
          BigSortedMap.totalDataSize.addAndGet(toMove);
        }
      }
      return true;
    } finally {
      writeUnlock();
    }
  }

  /**
   * Search position of a first key which is greater or equals to a given key
   * @param key
   * @param keyOffset
   * @param keyLength
   * @param version
   * @return address to insert (or update)
   */
  long search(byte[] key, int keyOffset, int keyLength, long version) {
    long ptr = dataPtr;
    int count = 0;
    int numRecords = getNumberOfRecords();
    int dataSize = getDataSize();
    while (count++ < numRecords) {
      short keylen = UnsafeAccess.toShort(ptr);
      short vallen = UnsafeAccess.toShort(ptr + KEY_SIZE_LENGTH);
      int res = Utils.compareTo(key, keyOffset, keyLength, ptr + RECORD_PREFIX_LENGTH, keylen);
      if (res < 0 || (res == 0 && version == NO_VERSION)) {
        return ptr;
      } else if (res == 0) {
        // check versions
        long ver = getRecordSeqId(ptr);
        if (ver <= version) {
          return ptr;
        }
      }
      ptr += keylen + vallen + RECORD_TOTAL_OVERHEAD;
    }
    // after the last record
    return dataPtr + dataSize;
  }

  /**
   * Search position of a first key which is greater or equals to a given key
   * @param keyPtr
   * @param keyLength
   * @return address to insert (or update)
   */
  long search(long keyPtr, int keyLength, long version) {
    long ptr = dataPtr;
    int count = 0;
    final int numRecords = getNumberOfRecords();
    final int dataSize = getDataSize();

    while (count++ < numRecords) {
      short keylen = UnsafeAccess.toShort(ptr);
      short vallen = UnsafeAccess.toShort(ptr + KEY_SIZE_LENGTH);
      int res = Utils.compareTo(keyPtr, keyLength, ptr + RECORD_PREFIX_LENGTH, keylen);
      if (res < 0 || (res == 0 && version == NO_VERSION)) {
        return ptr;
      } else if (res == 0) {
        // check versions
        long ver = getRecordSeqId(ptr);
        if (ver <= version) {
          return ptr;
        }
      }
      ptr += keylen + vallen + RECORD_TOTAL_OVERHEAD;
    }
    // after the last record
    return dataPtr + dataSize;
  }

  /**
   * Checks if a given key is larger than maximum key 
   * in this data block
   * @param key
   * @param keyOffset
   * @param keyLength
   * @param version
   * @return
   */
  public boolean isLargerThanMax(byte[] key, int keyOffset, int keyLength, long version) {
    // TODO: Locking is required, make sure we hold read lock
    //try {
    //  readLock();
      long address = search(key, keyOffset, keyLength, version);
      return address == dataPtr + getDataSize();
    //} finally {
    //  readUnlock();
    //}
  }
  
  /**
   * Checks if a given key is larger than maximum key 
   * in this data block
   * @param key
   * @param keyOffset
   * @param keyLength
   * @param version
   * @return
   */
  public boolean isLargerThanMax(long keyPtr, int keyLength, long version) {
    // TODO: Locking is required, make sure we hold read lock
    //try {
    //  readLock();
      long address = search(keyPtr, keyLength, version);
      return address == dataPtr + getDataSize();
    //} finally {
    //  readUnlock();
    //}
  }
  
  /**
   * Check if record is deleted
   * @param addr record address
   * @return true, false
   */
  private boolean isDeleted(long addr) {
    return getRecordType(addr) == Op.DELETE;
  }

  /**
   * Delete operation TODO: compact on deletion
   * @param keyPtr
   * @param keyOffset
   * @param version
   * @return operation result:OK, NOT_FOUND, SPLIT_REQUIRED
   * @throws RetryOperationException
   */

  public OpResult delete(long keyPtr, int keyLength, long version) throws RetryOperationException {

    boolean insert = false;
    OpResult result = OpResult.NOT_FOUND;
    int valueLength = 0; // no value in delete op
    // Get the most recent active Tx Id or snapshot Id (scanner)
    long mostRecentActiveTxId = BigSortedMap.getMostRecentActiveTxSeqId();
    try {
      writeLock();
      int dataSize = getDataSize();
      int blockSize = getBlockSize();
      long foundSeqId;

      do {
        // We loop until we delete all eligible records with the same key
        // there can be multiple of them due to ongoing Tx and/or open snapshot scanners
        foundSeqId = -1;
        insert = false;
        long addr = search(keyPtr, keyLength, version);
        if (addr < dataPtr + dataSize) {
          short keylen = UnsafeAccess.toShort(addr);
          if (keylen == keyLength) {
            // Compare keys
            int res = Utils.compareTo(keyPtr, keyLength, addr + RECORD_PREFIX_LENGTH, keylen);
            if (res == 0) {
              // Get seqId of existing record
              foundSeqId = getRecordSeqId(addr, (short) keyLength);
            } else {
              return result; // first time its NOT_FOUND, all subsequent - OK
            }
          } else {
            return result;
          }
        } else {
          return result;
        }
        if (foundSeqId < mostRecentActiveTxId) {
          insert = true; // we can't delete immediately
        }
        if (insert) {
          if (dataSize + keyLength + valueLength + RECORD_TOTAL_OVERHEAD > blockSize) {
            // try compact first (remove deleted, updated)
            compact(true);
            // Get data size again
            dataSize = getDataSize();
            if (dataSize + keyLength + valueLength + RECORD_TOTAL_OVERHEAD > blockSize) {
              // try to expand block
              boolean res = expand();
              blockSize = getBlockSize();
              if (!res
                  || (dataSize + keyLength + valueLength + RECORD_TOTAL_OVERHEAD > blockSize)) {
                // Still not enough room - bail out (split is required)
                return OpResult.SPLIT_REQUIRED;
              }
            }
          }
        }

        int moveDist = RECORD_TOTAL_OVERHEAD + keyLength + valueLength;

        if (insert) {
          // New K-V INSERT or we can't overwrite because of active Tx or snapshot
          // move from offset to offset + moveDist
          UnsafeAccess.copy(addr, addr + moveDist, dataPtr + dataSize - addr);
          UnsafeAccess.copy(keyPtr, addr + RECORD_PREFIX_LENGTH, keyLength);
          // Update key-value length
          UnsafeAccess.putShort(addr, (short) keyLength);
          UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) 0);

          // Set version, expire, op type and eviction (0)
          setRecordSeqId(addr, (short) keyLength, version);
          setRecordEviction(addr, 0L);
          setRecordType(addr, (short) keyLength, Op.DELETE);

          incrNumberOfRecords((short) 1);
          incrDataSize((short) moveDist);
          incrNumberDeletedAndUpdatedRecords((short)1);
          if (isThreadSafe() == false) {
            // TODO: why is this check?
            BigSortedMap.totalDataSize.addAndGet(moveDist);
          }
          return OpResult.OK;
        } else {
          // delete existing record with the same key

          short keylen = (short) keyLength;// must be equal to keyLength
          short vallen = UnsafeAccess.toShort(addr + KEY_SIZE_LENGTH);
          moveDist = keylen + vallen + RECORD_TOTAL_OVERHEAD;

          // move from offset + moveDist to offset
          UnsafeAccess.copy(addr + moveDist, addr, dataPtr + dataSize - addr - moveDist);
          incrDataSize((short) -moveDist);
          incrNumberOfRecords((short) -1);

          // Update data size
          dataSize = getDataSize();

          if (isThreadSafe() == false) {
            BigSortedMap.totalDataSize.addAndGet(-moveDist);
          }
          result = OpResult.OK;
        }
      } while (foundSeqId >= 0);
      return result;
    } finally {
      writeUnlock();
    }
  }
  
  /**
   * Adds Delete tombstone to an empty block
   * @param key
   * @param keyOffset
   * @param keyLength
   * @param version
   */
  public void addDelete(byte[] key, int keyOffset, int keyLength, long version) {
    UnsafeAccess.putShort(dataPtr, (short) keyLength);
    UnsafeAccess.putShort(dataPtr + KEY_SIZE_LENGTH, (short) 0);
    UnsafeAccess.copy(key, keyOffset, dataPtr + RECORD_PREFIX_LENGTH, keyLength);
    setRecordSeqId(dataPtr, version);
    setRecordType(dataPtr, Op.DELETE);
  }

  /**
   * Adds Delete tombstone to an empty block
   * @param key
   * @param keyOffset
   * @param keyLength
   * @param version
   */
  public void addDelete(long keyPtr, int keyLength, long version) {
    UnsafeAccess.putShort(dataPtr, (short) keyLength);
    UnsafeAccess.putShort(dataPtr + KEY_SIZE_LENGTH, (short) 0);
    UnsafeAccess.copy(keyPtr, dataPtr + RECORD_PREFIX_LENGTH, keyLength);
    setRecordSeqId(dataPtr, version);
    setRecordType(dataPtr, Op.DELETE);
  }

  /**
   * Delete operation TODO: compact on deletion
   * @param key
   * @param keyPtr
   * @param keyLength
   * @param version
   * @return true if success, false otherwise
   * @throws RetryOperationException
   */
  public OpResult delete(byte[] key, int keyOffset, int keyLength, long version)
      throws RetryOperationException {
    boolean insert = false;
    OpResult result = OpResult.NOT_FOUND;
    int valueLength = 0; // no value in delete op
    // Get the most recent active Tx Id or snapshot Id (scanner)
    long mostRecentActiveTxId = BigSortedMap.getMostRecentActiveTxSeqId();
    try {
      writeLock();
      int dataSize = getDataSize();
      int blockSize = getBlockSize();
      long foundSeqId;

      do {
        // We loop until we delete all eligible records with the same key
        // there can be multiple of them due to ongoing Tx and/or open snapshot scanners
        // TODO: 2 blocks can start (theoretically) with the same key!!!
        foundSeqId = -1;
        insert = false;
        long addr = search(key, keyOffset, keyLength, version);
        if (addr < dataPtr + dataSize) {
          short keylen = UnsafeAccess.toShort(addr);
          if (keylen == keyLength) {
            // Compare keys
            int res =
                Utils.compareTo(key, keyOffset, keyLength, addr + RECORD_PREFIX_LENGTH, keylen);
            if (res == 0) {
              // Get seqId of existing record
              foundSeqId = getRecordSeqId(addr, (short) keyLength);
            } else {
              return result; // first time its NOT_FOUND, all subsequent - OK
            }
          } else {
            return result;
          }
        } else {
          return result;
        }
        if (foundSeqId < mostRecentActiveTxId) {
          insert = true; // we can't delete immediately
        }
        if (insert) {
          if (dataSize + keyLength + valueLength + RECORD_TOTAL_OVERHEAD > blockSize) {
            // try compact first (remove deleted, updated)
            compact(true);
            // Get data size again
            dataSize = getDataSize();
            if (dataSize + keyLength + valueLength + RECORD_TOTAL_OVERHEAD > blockSize) {
              // try to expand block
              boolean res = expand();
              blockSize = getBlockSize();
              if (!res
                  || (dataSize + keyLength + valueLength + RECORD_TOTAL_OVERHEAD > blockSize)) {
                // Still not enough room - bail out (split is required)
                return OpResult.SPLIT_REQUIRED;
              }
            }
          }
        }

        int moveDist = RECORD_TOTAL_OVERHEAD + keyLength + valueLength;

        if (insert) {
          // New K-V INSERT or we can't overwrite because of active Tx or snapshot
          // move from offset to offset + moveDist
          UnsafeAccess.copy(addr, addr + moveDist, dataPtr + dataSize - addr);
          UnsafeAccess.copy(key, keyOffset, addr + RECORD_PREFIX_LENGTH, keyLength);
          // Update key-value length
          UnsafeAccess.putShort(addr, (short) keyLength);
          UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) 0);

          // Set version, expire, op type and eviction (0)
          setRecordSeqId(addr, (short) keyLength, version);
          setRecordEviction(addr, 0L);
          setRecordType(addr, (short) keyLength, Op.DELETE);

          incrNumberOfRecords((short) 1);
          incrDataSize((short) moveDist);
          incrNumberDeletedAndUpdatedRecords((short)1);
          if (isThreadSafe() == false) {
            // TODO: why is this check?
            BigSortedMap.totalDataSize.addAndGet(moveDist);
          }
          return OpResult.OK;
        } else {
          // delete existing record with the same key

          short keylen = (short) keyLength;// must be equal to keyLength
          short vallen = UnsafeAccess.toShort(addr + KEY_SIZE_LENGTH);
          moveDist = keylen + vallen + RECORD_TOTAL_OVERHEAD;

          // move from offset + moveDist to offset
          UnsafeAccess.copy(addr + moveDist, addr, dataPtr + dataSize - addr - moveDist);
          incrDataSize((short) -moveDist);
          // Update data size
          dataSize = getDataSize();
          incrNumberOfRecords((short) -1);

          if (isThreadSafe() == false) {
            BigSortedMap.totalDataSize.addAndGet(-moveDist);
          }
          result = OpResult.OK;
        }
      } while (foundSeqId >= 0);
      return result;
    } finally {
      writeUnlock();
    }
  }

  /**
   * Set record's expiration time
   * @param key key buffer
   * @param keyOffset offset
   * @param keyLength key length
   * @param expire expiration time
   * @param version version
   * @return true if success, false - otherwise
   */
  public boolean setExpire(byte[] key, int keyOffset, int keyLength, long expire, long version) {
    try {
      writeLock();
      long addr = search(key, keyOffset, keyLength, version);
      int dataSize = getDataSize();
      if (addr < dataPtr + dataSize) {
        short keylen = UnsafeAccess.toShort(addr);
        int res = Utils.compareTo(key, keyOffset, keyLength, addr + RECORD_PREFIX_LENGTH, keylen);
        if (res == 0) {
          Op type = getRecordType(addr);
          if (type == Op.PUT) {
            setRecordExpire(addr, expire);
            return true;
          } else {
            // Delete
            return false;
          }
        }
        return false;
      } else {
        // key not found
        return false;
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Set record's expiration time
   * @param keyPtr key buffer address
   * @param keyLength key length
   * @param expire expiration time
   * @param version version
   * @return true if success, false - otherwise
   */
  public boolean setExpire(long keyPtr, int keyLength, long expire, long version) {
    try {
      writeLock();
      long addr = search(keyPtr, keyLength, version);
      int dataSize = getDataSize();
      if (addr < dataPtr + dataSize) {
        short keylen = UnsafeAccess.toShort(addr);
        int res = Utils.compareTo(keyPtr, keyLength, addr + RECORD_PREFIX_LENGTH, keylen);
        if (res == 0) {
          Op type = getRecordType(addr);
          if (type == Op.PUT) {
            setRecordExpire(addr, expire);
            return true;
          } else {
            // Delete
            return false;
          }
        }
        return false;
      } else {
        // key not found
        return false;
      }
    } finally {
      writeUnlock();
    }
  }

  
  /**
   * Get key-value offset in a block
   * @param key
   * @param keyOffset
   * @param keyLength
   * @return record address or -1 if not found
   * @throws RetryOperationException
   */
  public long get(byte[] key, int keyOffset, int keyLength, long version)
      throws RetryOperationException {

    try {
      readLock();
      long addr = search(key, keyOffset, keyLength, version);
      int dataSize = getDataSize();
      if (addr < dataPtr + dataSize) {
        short keylen = UnsafeAccess.toShort(addr);
        int res = Utils.compareTo(key, keyOffset, keyLength, addr + RECORD_PREFIX_LENGTH, keylen);
        if (res == 0) {
          // FOUND exact key
          Op type = getRecordType(addr);
          if (type == Op.PUT) {
            return addr;
          } else {
            // Delete
            return NOT_FOUND;
          }
        } else {
          return NOT_FOUND;
        }
      } else {
        return NOT_FOUND;
      }
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
  public long get(byte[] key, int keyOffset, int keyLength, byte[] valueBuf, int valOffset,
      long version) throws RetryOperationException {

    try {
      int maxValueLength = valueBuf.length - valOffset;
      readLock();
      long addr = search(key, keyOffset, keyLength, version);
      int dataSize = getDataSize();
      if (addr < dataPtr + dataSize) {
        short keylen = UnsafeAccess.toShort(addr);
        short vallen = UnsafeAccess.toShort(addr + KEY_SIZE_LENGTH);
        int res = Utils.compareTo(key, keyOffset, keyLength, addr + RECORD_PREFIX_LENGTH, keylen);
        if (res == 0) {
          // FOUND exact key
          Op type = getRecordType(addr);
          if (type == Op.PUT) {
            if (vallen <= maxValueLength) {
              // Copy value
              UnsafeAccess.copy(addr + keylen + RECORD_TOTAL_OVERHEAD, valueBuf, valOffset, vallen);
            }
            return vallen;
          } else {
            // Delete
            return NOT_FOUND;
          }
        } else {
          return NOT_FOUND;
        }
      } else {
        return NOT_FOUND;
      }
    } finally {
      readUnlock();
    }
  }

  /**
   * Get key-value offset in a block
   * @param key
   * @param keyOffset
   * @param keyLength
   * @return record address or NOT_FOUND if not found
   * @throws RetryOperationException
   */
  public long get(long keyPtr, int keyLength, long version) throws RetryOperationException {

    try {
      readLock();
      long addr = search(keyPtr, keyLength, version);
      int dataSize = getDataSize();
      if (addr < dataPtr + dataSize) {
        short keylen = UnsafeAccess.toShort(addr);
        int res = Utils.compareTo(keyPtr, keyLength, addr + RECORD_PREFIX_LENGTH, keylen);
        if (res == 0) {
          // FOUND exact key
          Op type = getRecordType(addr);
          if (type == Op.PUT) {
            return addr;
          } else {
            // Delete
            return NOT_FOUND;
          }
        } else {
          return NOT_FOUND;
        }
      } else {
        return NOT_FOUND;
      }
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
   * @return value length if found, or NOT_FOUND. if value length > valueBufLength no copy will be
   *         made - one must repeat call with new value buffer
   * @throws RetryOperationException
   */
  public long get(long keyPtr, int keyLength, long valueBuf, int valueBufLength, long version)
      throws RetryOperationException {

    try {
      readLock();
      long addr = search(keyPtr, keyLength, version);
      int dataSize = getDataSize();
      if (addr < dataPtr + dataSize) {
        short keylen = UnsafeAccess.toShort(addr);
        short vallen = UnsafeAccess.toShort(addr + KEY_SIZE_LENGTH);
        int res = Utils.compareTo(keyPtr, keyLength, addr + RECORD_PREFIX_LENGTH, keylen);
        if (res == 0) {
          // FOUND exact key
          Op type = getRecordType(addr);
          if (type == Op.PUT) {
            if (vallen <= valueBufLength) {
              UnsafeAccess.copy(addr + keylen + RECORD_TOTAL_OVERHEAD, valueBuf, vallen);
            }
            return vallen;
          } else {
            // Delete
            return NOT_FOUND;
          }
        } else {
          return NOT_FOUND;
        }
      } else {
        return NOT_FOUND;
      }
    } finally {
      readUnlock();
    }
  }

  /**
   * Get max block size
   * @return maxBlockSize
   */
  public int getMaxBlockSize() {
    return BigSortedMap.maxBlockSize;
  }

  /**
   * Compact block (remove deleted and collapse updates k-vs)
   * @param force - if true - force operation
   * @param seqId (TODO: do we need it?)
   * @throws RetryOperationException
   */

  public void compact(boolean force) throws RetryOperationException {
    long numRecords = getNumberOfRecords();
    long oldRecords = numRecords;

    long numDeletedRecords = getNumberOfDeletedAndUpdatedRecords();
    if (numRecords == 0 || numDeletedRecords == 0) {
      return;
    }
    int dataSize = getDataSize();
    double ratio = ((double) numDeletedRecords) / numRecords;
    if (!force && ratio < MIN_COMPACT_RATIO) return;

    long mostRecentTxId = BigSortedMap.getMostOldestActiveTxSeqId();
    long leastRecentTxId = BigSortedMap.getMostOldestActiveTxSeqId();
    // Algorithm:
    // 1. KV <- read next while not block end
    // 2. if KV.seqId is between mostRecenetTxId and leastRecentTxId (minTx, maxTx), continue 1.
    // 3. else if KV.type = DELETE: processDeleted, then goto 1
    // 4  else  if KV.type = PUT: processUpdates, then goto 1
    // 
    //  long processDeleted(long ptr):
    //  1. KV <- read next while not block's end
    //  2. if KEY is not equal to DELETE key -> delete start record (DELETE) and return ptr
    //  3. if KEY is equal and KV.seqId is between (minTx, maxTx) -> return ptr
    //  4. if KEY is equal and KV.seqId is not between (minTx, maxTx) - delete KV, goto 1
    // 
    //  long processUpdates(long ptr):
    //  1. KV <- read next while not block's end
    //  2. if KEY is not equal to start key ->  return ptr
    //  3. if KEY is equal and KV.seqId is between (minTx, maxTx) -> return ptr
    //  4. if KEY is equal and KV.seqId is not between (minTx, maxTx) - delete KV, goto 1
    short keylen = UnsafeAccess.toShort(dataPtr);
    short vallen = UnsafeAccess.toShort(dataPtr + 2);
    // We skip first record because we need
    // at least one record in a block, even deleted one
    // for blocks comparisons
    // TODO: actually - we do not?
    int firstRecordLength = keylen + vallen + RECORD_TOTAL_OVERHEAD;
    long ptr = dataPtr + firstRecordLength;

    while (ptr < dataPtr + dataSize) {
       if (isDeleted(ptr)) {
        ptr = processDeleted(ptr, leastRecentTxId, mostRecentTxId);
      } else {
        ptr = coalesceUpdates(ptr, leastRecentTxId, mostRecentTxId);
      }
      // get updated data size
      dataSize = getDataSize(); 
    }
    // get updated number of records
    numRecords = getNumberOfRecords();
    // Delete first record (deleted)
    if (isDeleted(dataPtr) && numRecords > 1) {
      UnsafeAccess.copy(dataPtr + firstRecordLength, dataPtr, dataSize - firstRecordLength);
      incrNumberOfRecords((short)-1);
      incrDataSize((short)-firstRecordLength);
      incrNumberDeletedAndUpdatedRecords((short)-1);
      BigSortedMap.totalDataSize.addAndGet(-firstRecordLength);
    }
    if (oldRecords != numRecords) {
      incrSeqNumberSplitOrMerge();
    }
  }

  private long processDeleted(long ptr, long minTxId, long maxTxId) {

    long startPtr = ptr;
    long ver = getRecordSeqId(ptr);
    boolean delPossible = ver < minTxId || ver > maxTxId;
    // the current record at startPtr is Delete
    short dataSize = getDataSize();
    short keylen = UnsafeAccess.toShort(ptr);
    short vallen = UnsafeAccess.toShort(ptr + KEY_SIZE_LENGTH);
    ptr += keylen + vallen + RECORD_TOTAL_OVERHEAD;
    boolean canDelete = true;
    int count = 1;
    while (ptr < dataPtr + dataSize) {
      if (!keysEquals(startPtr, ptr)) {
        break;
      }
      // key is still the same
      keylen = UnsafeAccess.toShort(ptr);
      vallen = UnsafeAccess.toShort(ptr + KEY_SIZE_LENGTH);
      long version = getRecordSeqId(ptr);
      if (version >= minTxId && version <= maxTxId) {
        canDelete = false;
      }
      count++;
      ptr += keylen + vallen + RECORD_TOTAL_OVERHEAD;
    }
    if (canDelete && delPossible) {
      short toDelete = (short) (ptr - startPtr);
      long len = dataPtr + dataSize - toDelete - startPtr;
      UnsafeAccess.copy(startPtr + toDelete, startPtr, len);
      incrNumberOfRecords((short) -count);
      incrDataSize((short) -toDelete);
      incrNumberDeletedAndUpdatedRecords((short) -(count - 1));
      BigSortedMap.totalDataSize.addAndGet(-toDelete);
    }
    return ptr;
  }
  
  private boolean keysEquals (long address1, long address2) {
    int length1 = UnsafeAccess.toShort(address1);
    int length2 = UnsafeAccess.toShort(address2);
    return Utils.compareTo(address1, length1, address2, length2) == 0;
  }
  
  private long coalesceUpdates(long ptr, long minTxId, long maxTxId) {
    long startPtr = ptr;
    long ver = getRecordSeqId(ptr);
    boolean canDelete = ver < minTxId || ver > maxTxId;
    // the current record at startPtr is Delete
    short dataSize = getDataSize();
    short keylen = UnsafeAccess.toShort(ptr);
    short vallen = UnsafeAccess.toShort(ptr + KEY_SIZE_LENGTH);
    ptr += keylen + vallen + RECORD_TOTAL_OVERHEAD;
    boolean inBetween = false;// flag, we set it if one of the records has version
    // between minTxId and maxTxId
    while (ptr < dataPtr + dataSize) {
      if (!keysEquals(startPtr, ptr)) {
        break;
      }
      // key is still the same
      keylen = UnsafeAccess.toShort(ptr);
      vallen = UnsafeAccess.toShort(ptr + KEY_SIZE_LENGTH);
      long version = getRecordSeqId(ptr);
      if ((version < minTxId || version > maxTxId) && canDelete) {
        // Delete current record? 
        if (!inBetween) {
          // Delete current record only if it is in safe zone
          // and inBetween = false
          int toDelete = keylen + vallen + RECORD_TOTAL_OVERHEAD;
          long len = dataPtr + dataSize - toDelete - ptr;
          UnsafeAccess.copy(ptr + toDelete, ptr, len);
          incrNumberOfRecords((short) -1);
          incrDataSize((short) -toDelete);
          incrNumberDeletedAndUpdatedRecords((short) - 1);
          BigSortedMap.totalDataSize.addAndGet(-toDelete);
          // Update data size
          dataSize = getDataSize();
        } else {
          inBetween = false;
        }
      } else if (canDelete) {
        inBetween = true;
      }
      ptr += keylen + vallen + RECORD_TOTAL_OVERHEAD;
    }
    return ptr;
  }
  
  /**
   * Before calling canSplit compaction must be called
   * @return true if split can be done
   */
  public boolean canSplit() {
    return getNumberOfRecords() > 1;
  }
  /**
   * TODO: split won't work if we have only 1 record
   * Must always compact before splitting block
   * @param forceCompact
   * @return new (right) block
   * @throws RetryOperationException
   */
  public DataBlock split(boolean forceCompact) throws RetryOperationException {

    try {
      writeLock();
      int oldNumRecords = getNumberOfRecords();
      if (oldNumRecords <= 1) {
        return null; // split is not possible
      }
      // compact first
      if (forceCompact) {
        compact(true);
        // again
        oldNumRecords = getNumberOfRecords();
        if (oldNumRecords <= 1) {
          return null; // split is not possible
        }
      }
      // Increment sequence number
      incrSeqNumberSplitOrMerge();
      int off = 0;
      long ptr = dataPtr;
      int num = 0;
      // Now we should have zero deleted records
      while (num < oldNumRecords/2) {
        short keylen = UnsafeAccess.toShort(ptr + off);
        short vallen = UnsafeAccess.toShort(ptr + off + KEY_SIZE_LENGTH);
        long old = off;
        off += keylen + vallen + RECORD_TOTAL_OVERHEAD;
        num++;
      }
      
      int oldDataSize = getDataSize();
      setDataSize((short) off);
      setNumberOfRecords((short)num);
      int rightDataSize = oldDataSize - off;
      int rightBlockSize = getMinSizeGreaterThan(getBlockSize(), rightDataSize);
      DataBlock right = new DataBlock((short)rightBlockSize);

      right.numRecords = (short)(oldNumRecords - num);
      right.numDeletedAndUpdatedRecords = (short)0;
      right.dataSize = (short)rightDataSize;
      right.seqNumberSplitOrMerge = 0;
      right.compressed = false;
      right.threadSafe = false;
      UnsafeAccess.copy(dataPtr + off, right.dataPtr, right.dataSize);
      // shrink current
      shrink();
      return right;
    } finally {
      writeUnlock();
    }
  }
  
  /**
   * Get first key length
   * @return length
   */
  public final int getFirstKeyLength() {
    return UnsafeAccess.toShort(dataPtr);
  }
  
  /**
   * Get first key address
   * @return address
   */
  public final long getFirstKeyAddress() {
    return dataPtr + RECORD_PREFIX_LENGTH;
  }
  
  /**
   * Get first key version
   * @return version
   */
  public final long getFirstKeyVersion() {
    int keylen = getFirstKeyLength();
    return UnsafeAccess.toLong(dataPtr +RECORD_PREFIX_LENGTH + keylen);
  }
  
  /**
   * Get first key type value
   * @return type 
   */
  public final Op getFirstKeyType() {
    int keylen = getFirstKeyLength();
    int v = UnsafeAccess.toByte(dataPtr +RECORD_PREFIX_LENGTH + keylen + SEQUENCEID_SIZE);
    return Op.values()[v];
  }
  
  /**
   * Utility method 
   * @param key key data array
   * @param off offset
   * @param len length of key
   * @return -1, 0, +1
   */
  public int compareTo(byte[] key, int off, int len, long version, Op type) {
    return compareTo(this.dataPtr, key, off, len, version, type);
  }
  
  /**
   * Utility method: compares given key with a key defined by address in this block 
   * @param addr address of a record
   * @param key key data array
   * @param off offset
   * @param len length of key
   * @return -1, 0, +1
   */
  public static int compareTo(long addr, byte[] key, int off, int len, long version, Op type) {
    int length = UnsafeAccess.toShort(addr);
    int res = Utils.compareTo(key, off, len, addr + RECORD_PREFIX_LENGTH, length);
    if (res == 0) {
      long ver = getRecordSeqId(addr);
      if (ver > version) {
        return -1;
      } else if (ver < version) {
        return 1;
      } else {
        Op _type = getRecordType(addr);
        if (_type.ordinal() <= type.ordinal()) {
          return 1; 
        } else {
          return -1;
        }
      }
    } else {
      return res;
    }
  }
  /**
   * Utility method 
   * @param key key data pointer
   * @param len length of key
   * @return -1, 0, +1
   */
  
  public int compareTo(long key, int len, long version, Op type) {
    return compareTo(this.dataPtr, key, len, version, type);
  }
  
  /**
   * Utility method 
   * @param addr address of a record
   * @param key key data pointer
   * @param len length of key
   * @param version version
   * @param type type
   * @return -1, 0, +1
   */
  
  public static int compareTo(long addr, long key, int len, long version, Op type) {
    int length = UnsafeAccess.toShort(addr);
    int res = Utils.compareTo(key, len, addr + RECORD_PREFIX_LENGTH, length);
    if (res == 0) {
      long ver = getRecordSeqId(addr);
      if (ver > version) {
        return -1;
      } else if (ver < version) {
        return 1;
      } else {
        Op _type = getRecordType(addr);
        if (_type.ordinal() <= type.ordinal()) {
          return 1; 
        } else {
          return -1;
        }
      }
    } else {
      return res;
    }
  }
  
  public long splitPos(boolean forceCompact) {
    long off = 0;
    long ptr = dataPtr;
    // compact first
    if (forceCompact) {
      compact(true);
    }
    // Now we should have zero deleted records
    int numRecords = getNumberOfRecords();
    int num = 0;
    while (num < numRecords/2) {
      short keylen = UnsafeAccess.toShort(ptr + off);
      short vallen = UnsafeAccess.toShort(ptr + off + KEY_SIZE_LENGTH);
      off += keylen + vallen + RECORD_TOTAL_OVERHEAD;
      num++;
    }
    return dataPtr + off;
  }

  /**
   * Should merge this block
   * @return true, false
   */
  public boolean shouldMerge() {
    return (double) getDataSize() / getBlockSize() < MAX_MERGE_RATIO;
  }

  /**
   * Should compact this block
   * @return true, false
   */
  public boolean shouldCompact() {
    int numRecords = getNumberOfRecords();
    int numDeletedRecords = getNumberOfDeletedAndUpdatedRecords();
    if (numRecords == 0) return false;
    return (double) numDeletedRecords / numRecords > MIN_COMPACT_RATIO;
  }

  /**
   * TODO: FINISH
   * Merge two adjacent blocks
   * @param left
   * @param forceCompact
   * @param forceMerge
   * @return true, if merge successful, false - otherwise
   * @throws RetryOperationException
   */
  public boolean merge(DataBlock left, boolean forceCompact, boolean forceMerge)
      throws RetryOperationException {

    try {
      writeLock();
      left.writeLock();
      // Increment sequence numbers

      incrSeqNumberSplitOrMerge();
      left.incrSeqNumberSplitOrMerge();

      if (!forceMerge && (!shouldMerge() || !left.shouldMerge())) {
        return false;
      }
      if (forceCompact) {
        compact(true);
        left.compact(true);
      }
      // Check total size
      int dataSize = getDataSize();
      int leftDataSize = left.getDataSize();
      int blockSize = getBlockSize();
      while (dataSize + leftDataSize >= blockSize) {
        boolean result = expand();
        if (result == false) {
          // expansion failed
          throw new RuntimeException("Can not expand block for merge");
        }
        blockSize = getBlockSize();
      }
      UnsafeAccess.copy(left.dataPtr, this.dataPtr + dataSize, leftDataSize);

      incrNumberOfRecords(left.getNumberOfRecords());
      setNumberOfDeletedAndUpdatedRecords((short)0);
      incrDataSize(left.getDataSize());

      // After merge left block becomes invalid
      // TODO
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
    valid = false;
  }

  /**
   * Used for testing only
   */
  public byte[] getFirstKey() {
    if (this.indexBlock != null) {
      if (getNumberOfRecords() == 0) return null;
    } else {
      if (this.numRecords == 0) return null;
    }
    int len = keyLength(dataPtr);
    long addr = keyAddress(dataPtr);
    byte[] buf = new byte[len];
    UnsafeAccess.copy(addr, buf, 0, len);
    return buf;
  }
}
