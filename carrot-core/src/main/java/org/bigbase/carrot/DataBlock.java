package org.bigbase.carrot;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Records are lexicographically sorted 
 * Format: <KV>+ 
 * <KV> : 
 * 0 .. 1 - key size (2 bytes: max key size = 32K) 
 * 2 .. 3 - value size (2 bytes: max embedded value size = 32K if value size == -1,
 * value is stored externally, 
 * If key size == -1, both key and value are stored externally.
 * We keep embedded 8 bytes for value in this case:  8 bytes - address of K-V pair 
 * Format for external K-V: 4-4-key-value (4 bytes- key/value sizes)
 * 
 * We store K-V externally if sizeOf(Key) + sizeOf(value) + RECORD_TOTAL_OVERHEAD > 0.5 * MAX_BLOCK_SIZE
 * 
 * <EXPIRATION> (8 bytes time in ms: 0 - never expires) 
 * <EVICTION> (8 bytes - used by eviction algorithm : LRU, LFU etc) 
 * <KEY> 
 * <SEQUENCEID> - 8 bytes
 * <TYPE> - 1 byte: 0 - DELETE, 1 - PUT (DELETE comes first) 
 * <VALUE> 
 * For ordering we use combination of <KEY><SEQUENCEID><TYPE> 
 * TODO: 
 * 1. Add support for type - DONE
 * 2. Add support for sequenceid - DONE
 * 3. Add support for expiration field and API - DONE 
 * 4. Add support for eviction field - DONE
 * 5 Optimize storeFence usage - make sure we call it once before we release write lock 
 * We need sequenceId & type to support snapshots & tx
 * 6. check usage blockKeyLength & blockValueLength
 * 7. Code REUSE/DECOMPOSITION (IMPORTANT)
 * 8. Implement Append (as Get/Delete/Put?)
 */
@SuppressWarnings("unused")
public final class DataBlock  {
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
  public final static int INT_SIZE = 4;
  public final static int ADDRESS_SIZE = 8;
  // Overhead for K-V = 13 bytes
  public final static int RECORD_TOTAL_OVERHEAD =
      RECORD_PREFIX_LENGTH + TYPE_SIZE + SEQUENCEID_SIZE;
  public final static byte DELETED_MASK = (byte) (1 << 7);
  public final static double MIN_COMPACT_RATIO = 0.25d;

  public final static String MAX_BLOCK_SIZE_KEY = "max.block.size";
  public static short MAX_BLOCK_SIZE = 4096;
  public final static long NO_VERSION = -1;
  // This is stored as key length
  public final static int EXTERNAL_KEY_VALUE = 0;
  // This is stored as value length
  public final static int EXTERNAL_VALUE = -1;

  
  static {
    String val = System.getProperty(MAX_BLOCK_SIZE_KEY);
    if (val != null) {
      MAX_BLOCK_SIZE = Short.parseShort(val);
    }
  }
  // The greater factor is the worse is overall performance
  private static double MIN_MERGE_FACTOR = 0.75; 
  /*
   * TODO: make this configurable
   * TODO: Optimal block ratios (check jemalloc sizes)
   * 512-4096 with step 256 - this is jemalloc specific
   * sizes of allocation 
   * 256 * 2, 3, 4, ... 16
   */
  static int BASE_SIZE = 256;
  static int[] BASE_MULTIPLIERS = new int[] {4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  /*
   * Read-Write Lock TODO: StampedLock (Java 8)
   */
  static ReentrantReadWriteLock[] locks = new ReentrantReadWriteLock[11113];
  static {
    for (int i = 0; i < locks.length; i++) {
      locks[i] = new ReentrantReadWriteLock();
    }
  }

  public static int getMaximumBlockSize() {
    return BASE_SIZE * BASE_MULTIPLIERS[BASE_MULTIPLIERS.length -1];
  }
  

  /**
   * Get min size greater than current
   * @param max - max size
   * @param current current size
   * @return min size or -1;
   */
  static int getMinSizeGreaterThan(int max, int current) {
    for (int i = 0; i < BASE_MULTIPLIERS.length; i++) {
      int size = BASE_SIZE * BASE_MULTIPLIERS[i];
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
    for (int i = 0; i < BASE_MULTIPLIERS.length; i++) {
      int size = BASE_SIZE * BASE_MULTIPLIERS[i];
      if (size >= current) return size;
    }
    return -1;
  }
  public static enum AllocType {
    EMBEDDED, EXT_VALUE, EXT_KEY_VALUE;
  }
  /**
   * Checks if K-V must be stored externally
   * @param keySize
   * @param valueSize
   * @return true, if - yes, false - otherwise
   */
  public static AllocType getAllocType (int keySize, int valueSize) {
    if( keySize + valueSize + RECORD_TOTAL_OVERHEAD < MAX_BLOCK_SIZE/2) {
      return AllocType.EMBEDDED;
    } else if (keySize + RECORD_TOTAL_OVERHEAD + ADDRESS_SIZE + INT_SIZE < MAX_BLOCK_SIZE/2) {
      return AllocType.EXT_VALUE;
    } else {
      return AllocType.EXT_KEY_VALUE;
    }
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
   * Auxiliary byte: is used to keep compression codec (lower 3 bits, 0 means 
   * no compression, total up to 7 codecs can be used), bit 4. Four upper bits are 
   * reserved for future use 
   * is used to keep long K-V presence
   */
  final static int auxOffset = 15;

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

  short dataInBlockSize;
  
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
    LOG.info("Data size      =" + getDataInBlockSize());
    LOG.info("Number k/v's   =" + getNumberOfRecords());
    LOG.info("Number del/upd =" + getNumberOfDeletedAndUpdatedRecords());
    LOG.info("First key      =" + Bytes.toHex(getFirstKey()));
  }
  /**
   * Create new block with a given size
   * @param size of a block
   */
  DataBlock(int size) {
    this.dataPtr = UnsafeAccess.malloc(size);
    if (dataPtr == 0) {
      throw new RuntimeException("Failed to allocate " + size + " bytes");
    }
    BigSortedMap.totalAllocatedMemory.addAndGet(size);
    BigSortedMap.totalBlockDataSize.addAndGet(size);
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
    BigSortedMap.totalBlockDataSize.addAndGet(size);
    this.blockSize = size;
  }
  
  protected boolean isFirstBlock() {
    // First key = {0}
    return keyLength(dataPtr) == 1 && UnsafeAccess.toByte(keyAddress(dataPtr)) == 0;
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
  private void copyOf(DataBlock b) {
    this.dataInBlockSize = b.getDataInBlockSize();
    this.numRecords = b.getNumberOfRecords();
    this.numDeletedAndUpdatedRecords = b.getNumberOfDeletedAndUpdatedRecords();
    //TODO: always valid here?
    this.valid = b.isValid();
    this.compressed = b.isCompressed();
    this.seqNumberSplitOrMerge = b.getSeqNumberSplitOrMerge();
    // This block is going to be used by single thread
    this.threadSafe = true;
    UnsafeAccess.copy(b.dataPtr, this.dataPtr, this.dataInBlockSize);
  }
  
  final boolean isValid() {
    return valid;
  }

  final void invalidate() {
    this.valid = false;;
  }
  
  final long getIndexPtr() {
    return indexPtr;
  }

  final void set(IndexBlock indexBlock, long off) {
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
  final void register(IndexBlock indexBlock, long off) {
    this.indexBlock = indexBlock;
    this.indexPtr = indexBlock.getAddress() + off;
    setDataPtr(this.dataPtr);
    setBlockSize(this.blockSize);
    setDataSize(dataInBlockSize);
    setNumberOfRecords(numRecords);
    setNumberOfDeletedAndUpdatedRecords(numDeletedAndUpdatedRecords);
    setSeqNumberSplitOrMerge(seqNumberSplitOrMerge);
    setThreadSafe(threadSafe);
    // TODO compression
    setCompressed(compressed);
  }

  
  public static AllocType getRecordAllocationType(long ptr) {
    if( blockKeyLength(ptr) == EXTERNAL_KEY_VALUE) {
      return AllocType.EXT_KEY_VALUE;
    } else if (UnsafeAccess.toShort(ptr + KEY_SIZE_LENGTH) == EXTERNAL_VALUE) {
      return AllocType.EXT_VALUE;
    } else {
      return AllocType.EMBEDDED;
    }
      
  }
  
  public static boolean isExternalAllocatedRecord(long ptr) {
    AllocType type = getRecordAllocationType(ptr);
    return type != AllocType.EMBEDDED;
  }
  
  public static short blockKeyLength(long ptr) {
    return UnsafeAccess.toShort(ptr);
  }
  
  // TODO
  private static final int externalKeyLength(long ptr) {
    long address = getExternalRecordAddress(ptr);
    return UnsafeAccess.toInt(address);
  }
  
  public static int keyLength(long ptr) {
    if (getRecordAllocationType(ptr) == AllocType.EXT_KEY_VALUE) {
      return externalKeyLength(ptr);
    } else {
      return blockKeyLength(ptr);
    }
  }
  
  public static short blockValueLength(long ptr) {
    short len = UnsafeAccess.toShort(ptr + KEY_SIZE_LENGTH);
    if (len == EXTERNAL_VALUE) return INT_SIZE + ADDRESS_SIZE;
    return len;
  }
  
  
  public static final int valueLength(long ptr) {
    AllocType type = getRecordAllocationType(ptr);
    if (type == AllocType.EXT_KEY_VALUE) {
      long address = getExternalRecordAddress(ptr);
      return UnsafeAccess.toInt(address + INT_SIZE);
    } else if (type == AllocType.EXT_VALUE){
      long address = getExternalRecordAddress(ptr);
      return UnsafeAccess.toInt(address);
    } else {
      return blockValueLength(ptr);
    }
  }
  
  public static long getExternalRecordAddress(long ptr) {
    int klen = blockKeyLength(ptr);
    return UnsafeAccess.toLong(ptr + RECORD_TOTAL_OVERHEAD + klen + INT_SIZE);
  }
  
  public static long keyAddress(long ptr) {
    AllocType type = getRecordAllocationType(ptr);
    if (type != AllocType.EXT_KEY_VALUE) {
      return ptr + RECORD_PREFIX_LENGTH;
    } else {
      long address = getExternalRecordAddress(ptr);
      return address + 2 * INT_SIZE; // 4- key_size, 4-value_size, key, value 
    }
  }
  
  public static long version(long ptr) {
    short keylen = blockKeyLength(ptr);
    return UnsafeAccess.toLong(ptr + RECORD_PREFIX_LENGTH + keylen);
  }
  
  public static long valueAddress(long ptr) {
    AllocType type = getRecordAllocationType(ptr);
    if (type == AllocType.EMBEDDED) {
      return blockKeyLength(ptr) + keyAddress(ptr) + SEQUENCEID_SIZE + TYPE_SIZE;
    } else if (type == AllocType.EXT_KEY_VALUE){
      long address = getExternalRecordAddress(ptr);
      return address + 2 * INT_SIZE + UnsafeAccess.toInt(address)/*key length*/;
    } else {
      long address = getExternalRecordAddress(ptr);
      return address + INT_SIZE;
    }
  }
  
  public static boolean mustStoreExternally (int keyLength, int valueLength) {
    return getAllocType(keyLength, valueLength) != AllocType.EMBEDDED;
  }
  
  public static Op type(long ptr) {
    short keylen = blockKeyLength(ptr);
    byte v =UnsafeAccess.toByte(ptr + RECORD_PREFIX_LENGTH + keylen + SEQUENCEID_SIZE);
    return Op.values()[v];
  }
  
  /**
   * Get data pointer
   * @return data pointer
   */
  final long getDataPtr() {
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
  final void setDataPtr(long ptr) {
    UnsafeAccess.putLong(this.indexPtr, ptr);
    UnsafeAccess.storeFence();
  }

  /**
   * Get block size
   * @return data size
   */
  final short getBlockSize() {
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
  final void setBlockSize(short v) {
    UnsafeAccess.putShort(this.indexPtr + blockSizeOffset, v);
    UnsafeAccess.storeFence();
  }

  /**
   * Increment data size
   */
  final void incrBlockSize(short val) {
    short v = getBlockSize();
    // TODO max size check
    setBlockSize((short) (v + val));
  }

  /**
   * Get data size
   * @return data size
   */
  final short getDataInBlockSize() {
    if (!detached()) {
      return UnsafeAccess.toShort(this.indexPtr + dataSizeOffset);
    } else {
      return this.dataInBlockSize;
    }
  }

  /**
   * Set data size
   * @param v data size
   */
  final void setDataSize(short v) {
    UnsafeAccess.putShort(indexPtr + dataSizeOffset, v);
    UnsafeAccess.storeFence();
  }

  /**
   * Increment data size
   */
  final void incrDataSize(short val) {
    short v = getDataInBlockSize();
    setDataSize((short) (v + val));
    if (!isThreadSafe()) {
      BigSortedMap.totalDataInDataBlocksSize.addAndGet(val);
      BigSortedMap.totalDataSize.addAndGet(val);     
    }
  }

  /**
   * Get number of records
   * @return number
   */
  final short getNumberOfRecords() {
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
  final void setNumberOfRecords(short v) {
    UnsafeAccess.putShort(indexPtr + numRecordsOffset, v);
    UnsafeAccess.storeFence();
  }

  /**
   * Increment number records
   */
  final void incrNumberOfRecords(short val) {
    short v = getNumberOfRecords();
    // TODO check if exceeds Short.MAX_VALUE
    setNumberOfRecords((short) (v + val));
  }

  /**
   * Get number of deleted records
   * @return number
   */
  final short getNumberOfDeletedAndUpdatedRecords() {
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
  final void setNumberOfDeletedAndUpdatedRecords(short v) {
    UnsafeAccess.putShort(indexPtr + numDeletedAndUpdatedRecordsOffset, v);
    UnsafeAccess.storeFence();

  }

  /**
   * Increment number of deleted records
   */
  final void incrNumberDeletedAndUpdatedRecords(short val) {
    short v = getNumberOfDeletedAndUpdatedRecords();
    setNumberOfDeletedAndUpdatedRecords((short) (v + val));
  }

  /**
   * Get sequence number split or merge
   * @return sequence number
   */
  final byte getSeqNumberSplitOrMerge() {
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
  final void setSeqNumberSplitOrMerge(byte val) {
    UnsafeAccess.putByte(indexPtr + seqNumberSplitOrMergeOffset, val);
    UnsafeAccess.storeFence();
  }

  /**
   * Increment sequence number split or merge
   * @param delta
   */
  final void incrSeqNumberSplitOrMerge() {
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
  final boolean isThreadSafe() {
    return threadSafe;
  }

  /**
   * Set thread safe
   * @param b thread safe (true/false)
   */
  final void setThreadSafe(boolean b) {
    threadSafe = b;
  }

  /**
   * Is block compressed
   * @return compressed
   */
  final boolean isCompressed() {
    return (UnsafeAccess.toByte(indexPtr + auxOffset) & 7) != 0;
  }

  /**
   * Set block is compressed
   * @param b compressed or not
   */
  final void setCompressed(boolean b) {
    setCompressionCodec(1);
  }

  /**
   * Returns compression codec id
   * @return codec id
   */
  final int getCompressionCodec() {
    return UnsafeAccess.toByte(indexPtr + auxOffset) & 7;
  }
  
  
  /**
   * Sets compression codec id (0-7)
   * @param codec id
   */
  final void setCompressionCodec(int codec) {
    // codec is between 0 and 7, 0 - no compression
    int v = UnsafeAccess.toByte(indexPtr + auxOffset);
    v &= 0xf8;
    v |= codec;
    UnsafeAccess.putByte(indexPtr + auxOffset, (byte)v);
    UnsafeAccess.storeFence();
  }
  

  /**
   * Expands block
   * @return true if success
   */
  final boolean expand(int required) {

    // Get next size
    int blockSize = getBlockSize();
    int dataSize = getDataInBlockSize();
    int nextSize = getMinSizeGreaterThan(BigSortedMap.maxBlockSize, required);
    if (nextSize < 0 || nextSize < blockSize) {
      return false;
    }
    long newPtr = UnsafeAccess.malloc(nextSize);
    if (newPtr <= 0) {
      return false;
    }
    BigSortedMap.totalAllocatedMemory.addAndGet(nextSize - blockSize);
    BigSortedMap.totalBlockDataSize.addAndGet(nextSize - blockSize);
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
  final boolean shrink() {
    // Get next size
    int dataSize = getDataInBlockSize();
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
    BigSortedMap.totalBlockDataSize.addAndGet(nextSize - blockSize);
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
  final void readLock() throws RetryOperationException {
  }

  /**
   * Read unlock
   */
  public final void readUnlock() {
  }

  /**
   * Write lock
   * @throws RetryOperationException
   * @throws InterruptedException
   */
  final void writeLock() throws RetryOperationException {
  }

  /**
   * Write unlock
   */
  final void writeUnlock() {
  }

  
  private boolean isForbiddenKey(byte[] key, int off, int len) {
    return len == 1 && key[off] == 0;
  }
  
  private boolean isForbiddenKey (long key, int len) {
    return len == 1 && UnsafeAccess.toByte(key) == 0;
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
  final boolean put(byte[] key, int keyOffset, int keyLength, byte[] value, int valueOffset,
      int valueLength, long version, long expire) throws RetryOperationException {

    if (getNumberOfRecords() > 0 && isForbiddenKey(key, keyOffset, keyLength)) {
      // Return success silently TODO
      return true;
    }
    
    if (mustStoreExternally(keyLength, valueLength)) {
      return putExternally(key, keyOffset, keyLength, value, valueOffset, 
        valueLength, version, expire);
    }
    boolean onlyExactOverwrite = false;
    boolean recordOverwrite = false;
    boolean keyOverwrite = false; // key the same, value is different in size
    // Get the most recent active Tx Id or snapshot Id (scanner)
    long mostRecentActiveTxId = BigSortedMap.getMostRecentActiveTxSeqId();
    try {
      writeLock();
      
      onlyExactOverwrite = compactExpandIfNecessary(keyLength + valueLength);
      int dataSize = getDataInBlockSize();
      int blockSize = getBlockSize();

      long addr = search(key, keyOffset, keyLength, version);
      if (addr == NOT_FOUND) {
        // There were conflicting updates at the same time and this came late
        // we silently return true to avoid further splitting
        // This is how we handle multiple updates to the same key at the same time
        // All succeed but the winner has the highest sequenceId
        return true;
      }
      // TODO: verify what search returns if not found
      long foundSeqId = -1;
      boolean foundExternal = false;
      if (addr < dataPtr + dataSize) {
        int keylen = keyLength(addr);
        int vallen = valueLength(addr);
        if (keylen == keyLength) {
          // Compare keys
          int res = Utils.compareTo(key, keyOffset, keyLength, keyAddress(addr), keylen);
          if (res == 0 && vallen == valueLength) {
            recordOverwrite = true;
            keyOverwrite = true;
          } else if (res == 0) {
            keyOverwrite = true;
            foundExternal = getRecordAllocationType(addr) != AllocType.EMBEDDED;
          }
          if (keyOverwrite) {
            // Get seqId of existing record
            foundSeqId = getRecordSeqId(addr);
          }
        }
      }

      int newRecLen = RECORD_TOTAL_OVERHEAD + keyLength + valueLength;
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
        UnsafeAccess.copy(addr, addr + newRecLen, dataPtr + dataSize - addr);
        UnsafeAccess.copy(key, keyOffset, addr + RECORD_PREFIX_LENGTH, keyLength);
        UnsafeAccess.copy(value, valueOffset, addr + RECORD_TOTAL_OVERHEAD + keyLength,
          valueLength);
        // Update key-value length
        UnsafeAccess.putShort(addr, (short) keyLength);
        UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) valueLength);

        // Set version, expire, op type and eviction (0)
        setRecordSeqId(addr, version);
        setRecordEviction(addr, 0L);
        if (expire > 0) {
          setRecordExpire(addr, expire);
        }
        setRecordType(addr, Op.PUT);

        incrNumberOfRecords((short) 1);
        incrDataSize((short) newRecLen);
        if (keyOverwrite) {
          incrNumberDeletedAndUpdatedRecords((short) 1);
        }
      } else if (overwrite) {
        // UPDATE existing
        // We do overwrite of existing record
        UnsafeAccess.copy(value, valueOffset, addr + RECORD_TOTAL_OVERHEAD + keyLength,
          valueLength);
        // Set version, expire, op type and eviction (0)
        setRecordSeqId(addr, version);
        // Do not update eviction, because we overwrite existing record
        // setRecordEviction(addr, 0L);
        if (expire > 0) {
          setRecordExpire(addr, expire);
        }
        setRecordType(addr, Op.PUT);
      } else {
        // Neither insert nor overwrite - delete, then - insert
        // keyOverwrite = true
        // delete existing, put new

        int keylen = foundExternal? blockKeyLength(addr): keyLength(addr);// must be equal to keyLength
        int vallen = foundExternal? blockValueLength(addr): valueLength(addr);
        int existRecLen = keylen + vallen + RECORD_TOTAL_OVERHEAD;

                
        int toMove = foundExternal? (keyLength + valueLength + RECORD_TOTAL_OVERHEAD - existRecLen):
          (valueLength - vallen);
        if (onlyExactOverwrite && (dataSize + toMove > blockSize)) {
          // failed to insert, split is required
          return false;
        }
        
        deallocateIfExternalRecord(addr);

        // move from offset to offset + moveDist
        UnsafeAccess.copy(addr + existRecLen, addr + existRecLen + toMove,
          dataPtr + dataSize - addr - existRecLen);
        // TODO: check if we need this
        UnsafeAccess.copy(key, keyOffset, addr + RECORD_PREFIX_LENGTH, keyLength);
        UnsafeAccess.copy(value, valueOffset, addr + RECORD_TOTAL_OVERHEAD + keyLength,
          valueLength);
        // Update key-value length
        UnsafeAccess.putShort(addr, (short) keyLength);
        UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) valueLength);

        // Set version, expire, op type and eviction (0)
        setRecordSeqId(addr, version);
        // Do not reset eviction field
        // setRecordEviction(addr, 0L);
        if (expire > 0) {
          setRecordExpire(addr, expire);
        }
        setRecordType(addr, Op.PUT);
        incrDataSize((short) toMove);

      }
      return true;
    } finally {
      writeUnlock();
    }
  }

  /**
   * Returns address of externally allocated record
   * @param key - key pointer
   * @param keyLength key length
   * @param value value pointer
   * @param valueLength value length
   * @return address of a record
   */
  private long allocateAndCopyExternalKeyValue(long keyPtr, int keyLength, long valuePtr, int valueLength) {
    long recAddress = UnsafeAccess.malloc(keyLength + valueLength + 2 * INT_SIZE);
    if (recAddress <=0) {
      return UnsafeAccess.MALLOC_FAILED;
    }
    largeKVs.incrementAndGet();
    BigSortedMap.totalAllocatedMemory.addAndGet(keyLength + valueLength + 2 * INT_SIZE);
    BigSortedMap.totalDataSize.addAndGet(keyLength + valueLength + 2 * INT_SIZE);
    UnsafeAccess.putInt(recAddress, keyLength);
    UnsafeAccess.putInt(recAddress + INT_SIZE, valueLength);
    UnsafeAccess.copy(keyPtr, recAddress + 2 * INT_SIZE, keyLength);
    UnsafeAccess.copy(valuePtr, recAddress + 2*INT_SIZE + keyLength, valueLength);
    return recAddress;
  }
  
  /**
   * Returns address of externally allocated record
   * @param ptr current allocation
   * @param key - key pointer
   * @param keyLength key length
   * @param value value pointer
   * @param valueLength value length
   * @return address of a record
   */
  private long reallocateAndCopyExternalKeyValue(long ptr, long keyPtr, int keyLength, long valuePtr, 
      int valueLength) {
    
    int kLen = keyLength(ptr);
    int vLen = valueLength(ptr);
    if (kLen + vLen > keyLength + valueLength) {
      deallocateIfExternalRecord(ptr);
      return allocateAndCopyExternalKeyValue(keyPtr, keyLength, valuePtr, valueLength);
    }
    long recAddress = UnsafeAccess.realloc(ptr,keyLength + valueLength + 2 * INT_SIZE);
    if (recAddress <=0) {
      return UnsafeAccess.MALLOC_FAILED;
    }
    largeKVs.incrementAndGet();
    BigSortedMap.totalAllocatedMemory.addAndGet(keyLength + valueLength + 2 * INT_SIZE);
    BigSortedMap.totalDataSize.addAndGet(keyLength + valueLength + 2 * INT_SIZE);
    UnsafeAccess.putInt(recAddress, keyLength);
    UnsafeAccess.putInt(recAddress + INT_SIZE, valueLength);
    UnsafeAccess.copy(keyPtr, recAddress + 2 * INT_SIZE, keyLength);
    UnsafeAccess.copy(valuePtr, recAddress + 2*INT_SIZE + keyLength, valueLength);
    return recAddress;
  }
  
  private long allocateAndCopyExternalValue(long valuePtr, int valueLength) {
    long recAddress = UnsafeAccess.malloc(valueLength + INT_SIZE);
    if (recAddress <=0) {
      return UnsafeAccess.MALLOC_FAILED;
    }
    largeKVs.incrementAndGet();
    BigSortedMap.totalAllocatedMemory.addAndGet(valueLength + INT_SIZE);
    //TODO dataSize ?
    BigSortedMap.totalDataSize.addAndGet(valueLength + INT_SIZE);
    UnsafeAccess.putInt(recAddress , valueLength);
    UnsafeAccess.copy(valuePtr, recAddress + INT_SIZE, valueLength);
    return recAddress;
  }
  
  private long reallocateAndCopyExternalValue(long ptr, long valuePtr, int valueLength) {
    int vLen = valueLength(ptr);
    if (vLen > valueLength) {
      deallocateIfExternalRecord(ptr);
      return allocateAndCopyExternalValue(valuePtr, valueLength);
    }
    long recAddress = UnsafeAccess.realloc(ptr, valueLength + INT_SIZE);
    if (recAddress <=0) {
      return UnsafeAccess.MALLOC_FAILED;
    }
    largeKVs.incrementAndGet();
    BigSortedMap.totalAllocatedMemory.addAndGet(valueLength + INT_SIZE);
    //TODO dataSize ?
    BigSortedMap.totalDataSize.addAndGet(valueLength + INT_SIZE);
    UnsafeAccess.putInt(recAddress , valueLength);
    UnsafeAccess.copy(valuePtr, recAddress + INT_SIZE, valueLength);
    return recAddress;
  }
  /**
   * Returns address of externally allocated record
   * @param key - key array
   * @param keyOffset key offset
   * @param keyLength key length
   * @param value value array
   * @param valueOffset value offset
   * @param valueLength value length
   * @return address of a record
   */
  private long allocateAndCopyExternalKeyValue(byte[] key, int keyOffset, int keyLength, byte[] value, int valueOffset,
      int valueLength) {
    long recAddress = UnsafeAccess.malloc(keyLength + valueLength + 2 * INT_SIZE);
    if (recAddress < 0) {
      return UnsafeAccess.MALLOC_FAILED;
    }
    largeKVs.incrementAndGet();
    BigSortedMap.totalAllocatedMemory.addAndGet(keyLength + valueLength + 2 * INT_SIZE);
    BigSortedMap.totalDataSize.addAndGet(keyLength + valueLength + 2 * INT_SIZE);
    
    UnsafeAccess.putInt(recAddress, keyLength);
    UnsafeAccess.putInt(recAddress + INT_SIZE, valueLength);
    UnsafeAccess.copy(key, keyOffset, recAddress + 2 * INT_SIZE, keyLength);
    UnsafeAccess.copy(value, valueOffset, recAddress + 2 * INT_SIZE + keyLength, valueLength);
    return recAddress;
  }
  
  /**
   * Returns address of externally allocated record
   * @param ptr current allocation
   * @param key - key array
   * @param keyOffset key offset
   * @param keyLength key length
   * @param value value array
   * @param valueOffset value offset
   * @param valueLength value length
   * @return address of a record
   */
  private long reallocateAndCopyExternalKeyValue(long ptr, byte[] key, int keyOffset, 
      int keyLength, byte[] value, int valueOffset,
      int valueLength) {
    int kLen = keyLength(ptr);
    int vLen = valueLength(ptr);
    if (kLen + vLen > keyLength + valueLength) {
      deallocateIfExternalRecord(ptr);
      return allocateAndCopyExternalKeyValue(key, keyOffset, keyLength, value, valueOffset, valueLength);
    }
    long recAddress = UnsafeAccess.realloc(ptr, keyLength + valueLength + 2 * INT_SIZE);
    if (recAddress < 0) {
      return UnsafeAccess.MALLOC_FAILED;
    }
    largeKVs.incrementAndGet();
    BigSortedMap.totalAllocatedMemory.addAndGet(keyLength + valueLength + 2 * INT_SIZE);
    BigSortedMap.totalDataSize.addAndGet(keyLength + valueLength + 2 * INT_SIZE);
    
    UnsafeAccess.putInt(recAddress, keyLength);
    UnsafeAccess.putInt(recAddress + INT_SIZE, valueLength);
    UnsafeAccess.copy(key, keyOffset, recAddress + 2 * INT_SIZE, keyLength);
    UnsafeAccess.copy(value, valueOffset, recAddress + 2 * INT_SIZE + keyLength, valueLength);
    return recAddress;
  }
  
  
  private long getExternalAllocationSize(long address, AllocType type) {
    switch(type) {
      case EXT_VALUE:
        return UnsafeAccess.toInt(address) + INT_SIZE;
      case EXT_KEY_VALUE:
        return UnsafeAccess.toInt(address) + UnsafeAccess.toInt(address + INT_SIZE) + 2 * INT_SIZE;
      default: return 0;  
    }
  }
  
  private long allocateAndCopyExternalValue( byte[] value, int valueOffset,
      int valueLength) {
    long recAddress = UnsafeAccess.malloc( valueLength +  INT_SIZE);
    if (recAddress < 0) {
      return UnsafeAccess.MALLOC_FAILED;
    }
    largeKVs.incrementAndGet();
    BigSortedMap.totalAllocatedMemory.addAndGet(valueLength + INT_SIZE);
    BigSortedMap.totalDataSize.addAndGet(valueLength + INT_SIZE);
    UnsafeAccess.putInt(recAddress, valueLength);
    UnsafeAccess.copy(value, valueOffset, recAddress + INT_SIZE, valueLength);
    return recAddress;
  }
  
  private long reallocateAndCopyExternalValue(long ptr, byte[] value, int valueOffset,
      int valueLength) {
    int vLen = valueLength(ptr);
    if (vLen > valueLength) {
      deallocateIfExternalRecord(ptr);
      return allocateAndCopyExternalValue(value, valueOffset, valueLength);
    }
    long recAddress = UnsafeAccess.realloc( ptr, valueLength +  INT_SIZE);
    if (recAddress < 0) {
      return UnsafeAccess.MALLOC_FAILED;
    }
    largeKVs.incrementAndGet();
    BigSortedMap.totalAllocatedMemory.addAndGet(valueLength + INT_SIZE);
    BigSortedMap.totalDataSize.addAndGet(valueLength + INT_SIZE);
    UnsafeAccess.putInt(recAddress, valueLength);
    UnsafeAccess.copy(value, valueOffset, recAddress + INT_SIZE, valueLength);
    return recAddress;
  }
  
  /**
   * FIXME: compact 
   * Performs block's compaction and expansion if necessary
   * @param kvLength - total length of key + value
   * @return true if only exact overwrite, false - otherwise
   */
  private boolean compactExpandIfNecessary(int kvLength) {
    int dataSize = getDataInBlockSize();
    int blockSize = getBlockSize();
    if (dataSize + kvLength + RECORD_TOTAL_OVERHEAD > blockSize) {
      // try compact first (remove deleted, updated)
      compact(true);
      // Get data size again
      dataSize = getDataInBlockSize();
      if (dataSize + kvLength + RECORD_TOTAL_OVERHEAD > blockSize) {
        // try to expand block
        boolean res = expand(dataSize + kvLength + RECORD_TOTAL_OVERHEAD);
        blockSize = getBlockSize();
        if (!res || (dataSize + kvLength + RECORD_TOTAL_OVERHEAD > blockSize)) {
          // Still not enough room
          // Only exact overwrite (key & value) and only if
          // there are no conflicting Tx or snapshots exist
          return true;
        }
      }
    }
    return false;
  }
  
  final boolean putExternally(byte[] key, int keyOffset, int keyLength, byte[] value, int valueOffset,
      int valueLength, long version, long expire) throws RetryOperationException {

    boolean onlyExactOverwrite = false;
    boolean recordOverwrite = false;
    boolean keyOverwrite = false; // key the same, value is different in size
    // Get the most recent active Tx Id or snapshot Id (scanner)
    long mostRecentActiveTxId = BigSortedMap.getMostRecentActiveTxSeqId();
    AllocType type = getAllocType(keyLength, valueLength);
    // TODO: result check
    int newKVLength = INT_SIZE + ADDRESS_SIZE +
        (type == AllocType.EXT_VALUE? keyLength:0);// 4 - contains overall length, 8 - address 
    try {
      writeLock();
      onlyExactOverwrite = compactExpandIfNecessary(newKVLength);
      int dataSize = getDataInBlockSize();
      int blockSize = getBlockSize();
      
      long addr = search(key, keyOffset, keyLength, version);
      if (addr == NOT_FOUND) {
        // There were conflicting updates at the same time and this came late
        // we silently return true to avoid further splitting
        // This is how we handle multiple updates to the same key at the same time
        // All succeed but the winner has the highest sequenceId
        return true;
      }
      boolean append = addr == (dataPtr + dataSize);
      boolean extAddr = !append && getRecordAllocationType(addr) != AllocType.EMBEDDED;
      // TODO: verify what search returns if not found
      long foundSeqId = -1;
      if (addr < dataPtr + dataSize) {
        int keylen = keyLength(addr);
        int vallen = valueLength(addr);

        if (keylen == keyLength) {
          // Compare keys
          int res = Utils.compareTo(key, keyOffset, keyLength, keyAddress(addr), keylen);
          if (res == 0 && vallen == valueLength) {
            recordOverwrite = true;
            keyOverwrite = true;
          } else if (res == 0) {
            keyOverwrite = true;
          }
          if (keyOverwrite) {
            // Get seqId of existing record
            foundSeqId = getRecordSeqId(addr);
          }
        }
      }
  
      int newRecLength = RECORD_TOTAL_OVERHEAD + newKVLength;
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
      long recAddress = 0;
      if (insert) {
        // New K-V INSERT or we can't overwrite because of active Tx or snapshot
        // move from offset to offset + moveDist
        if (type == AllocType.EXT_KEY_VALUE) {
          int kvSize = keyLength + valueLength + 2 *INT_SIZE;
          recAddress = allocateAndCopyExternalKeyValue(key, keyOffset, keyLength, value, valueOffset, valueLength);
        
          UnsafeAccess.copy(addr, addr + newRecLength, dataPtr + dataSize - addr);
          UnsafeAccess.putInt( addr + RECORD_TOTAL_OVERHEAD + 0 /*key length*/, kvSize);
          UnsafeAccess.putLong(addr + RECORD_TOTAL_OVERHEAD + INT_SIZE, recAddress);
          // Update key-value length
          UnsafeAccess.putShort(addr, (short) EXTERNAL_KEY_VALUE /* ==0*/);
          UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) (INT_SIZE + ADDRESS_SIZE));
        } else { // AllocType.EXT_VALUE
          recAddress = allocateAndCopyExternalValue(value, valueOffset, valueLength);
          
          UnsafeAccess.copy(addr, addr + newRecLength, dataPtr + dataSize - addr);
          // copy key
          UnsafeAccess.copy(key,  keyOffset, addr + RECORD_PREFIX_LENGTH, keyLength);
          // store value (size + address)
          UnsafeAccess.putInt( addr + RECORD_TOTAL_OVERHEAD + keyLength, valueLength + INT_SIZE);
          UnsafeAccess.putLong(addr + RECORD_TOTAL_OVERHEAD + keyLength + INT_SIZE, recAddress);
          // Update key-value length
          UnsafeAccess.putShort(addr, (short) keyLength);
          // Set value to external
          UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) EXTERNAL_VALUE);
        }
        // Set version, expire, op type and eviction (0)
        setRecordSeqId(addr, version);
        setRecordEviction(addr, 0L);
        if (expire > 0) {
          setRecordExpire(addr, expire);
        }
        setRecordType(addr, Op.PUT);

        incrNumberOfRecords((short) 1);
        incrDataSize((short) newRecLength);
        if (keyOverwrite) {
          incrNumberDeletedAndUpdatedRecords((short) 1);
        }
      } else if (overwrite) {
        // Keys are the same, values have the same size - both new and old records 
        // are external allocations of the same type
        // UPDATE existing - both are external records
        // We do overwrite of existing record and use existing external allocation
        recAddress = getExternalRecordAddress(addr);
        
        if (type == AllocType.EXT_KEY_VALUE) {
          UnsafeAccess.copy(value, valueOffset, recAddress + 2 * INT_SIZE + keyLength,
            valueLength);
        } else { // AllocType.EXT_VALUE
          UnsafeAccess.copy(value, valueOffset, recAddress + INT_SIZE,
            valueLength);
        }
        // Set version, expire, op type and eviction (0)
        setRecordSeqId(addr, version);
        // Do not update eviction, because we overwrite existing record
        // setRecordEviction(addr, 0L);
        if (expire > 0) {
          setRecordExpire(addr, expire);
        }
        setRecordType(addr, Op.PUT);
      } else if (!extAddr){
        // Keys are the same, values are not, old k-v is EMBEDDED
        // Neither insert nor overwrite - delete, then - insert
        // keyOverwrite = true
        // delete existing, put new
        // TODO check allocation
        int keylen =  keyLength(addr);// must be equal to keyLength
        int vallen =  valueLength(addr);
        int existRecLength = keylen + vallen + RECORD_TOTAL_OVERHEAD;
        
        int toMove = newRecLength - existRecLength;
        if (onlyExactOverwrite && (dataSize + toMove > blockSize)) {
          // failed to insert, split is required
          return false;
        }
        
        if (type == AllocType.EXT_KEY_VALUE) {
          recAddress = allocateAndCopyExternalKeyValue(key, keyOffset, keyLength, value, 
            valueOffset, valueLength);

          // move from offset to offset + moveDist
          UnsafeAccess.copy(addr + existRecLength, addr + existRecLength + toMove,
            dataPtr + dataSize - addr - existRecLength);
        
          UnsafeAccess.putInt( addr + RECORD_TOTAL_OVERHEAD + 0 /*key length*/, 
            keyLength + valueLength + 2 * INT_SIZE);
          UnsafeAccess.putLong(addr + RECORD_TOTAL_OVERHEAD + INT_SIZE, recAddress);
          // Update key-value length
          UnsafeAccess.putShort(addr, (short) EXTERNAL_KEY_VALUE);
          UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) (INT_SIZE + ADDRESS_SIZE));
        } else { // AlocType.EXT_VALUE
          
          recAddress = allocateAndCopyExternalValue(value, 
            valueOffset, valueLength);
         

          // move from offset to offset + moveDist
          UnsafeAccess.copy(addr + existRecLength, addr + existRecLength + toMove,
            dataPtr + dataSize - addr - existRecLength);
          // copy key
          UnsafeAccess.copy(key,  keyOffset, addr + RECORD_PREFIX_LENGTH, keyLength);
          UnsafeAccess.putInt( addr + RECORD_TOTAL_OVERHEAD + keyLength, 
            valueLength + INT_SIZE);
          UnsafeAccess.putLong(addr + RECORD_TOTAL_OVERHEAD + keyLength + INT_SIZE, recAddress);
          // Update key-value length
          UnsafeAccess.putShort(addr, (short) keyLength);
          UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) EXTERNAL_VALUE);

        }
        // Set version, expire, op type and eviction (0)
        setRecordSeqId(addr, version);
        // Do not reset eviction field
        // setRecordEviction(addr, 0L);
        if (expire > 0) {
          setRecordExpire(addr, expire);
        }
        setRecordType(addr, Op.PUT);
        incrDataSize((short) toMove);

      } else if (extAddr) {
        // Keys are the same, values are not, but old k-v is EXTERNAL type of allocation
        // As since keys are the same, both new and old records have the same type of 
        // external allocation, either EXT_VALUE or EXT_KEY_VALUE
        // Deallocate existing allocation
        //TODO: realloc reconsider
        //deallocateIfExternalRecord(addr);
        if (type == AllocType.EXT_KEY_VALUE) {
          recAddress = reallocateAndCopyExternalKeyValue(addr, key, keyOffset, keyLength, value, 
            valueOffset, valueLength);
          UnsafeAccess.putInt( addr + RECORD_TOTAL_OVERHEAD + 0 /*key length*/, 
            keyLength + valueLength + 2 * INT_SIZE);
          UnsafeAccess.putLong(addr + RECORD_TOTAL_OVERHEAD + INT_SIZE, recAddress);
          // Update key-value length
          UnsafeAccess.putShort(addr, (short) EXTERNAL_KEY_VALUE);
          UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) (INT_SIZE + ADDRESS_SIZE));
        } else { // AllocType.EXT_VALUE
          // As since keys are the same - no need to overwrite existing key
          recAddress = reallocateAndCopyExternalValue(addr, value, 
            valueOffset, valueLength);         
          UnsafeAccess.putInt( addr + RECORD_TOTAL_OVERHEAD + keyLength, 
            valueLength + INT_SIZE);
          UnsafeAccess.putLong(addr + RECORD_TOTAL_OVERHEAD + keyLength +INT_SIZE, recAddress);
          // Update key-value length
          UnsafeAccess.putShort(addr, (short) keyLength);
          UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) EXTERNAL_VALUE);
        }
        // Set version, expire, op type and eviction (0)
        setRecordSeqId(addr, version);
        // Do not reset eviction field
        // setRecordEviction(addr, 0L);
        if (expire > 0) {
          setRecordExpire(addr, expire);
        }
        setRecordType(addr, Op.PUT);
      }
      return true;
    } finally {
      writeUnlock();
    }
  }
  
  public static long getRecordSeqId(long recordAddress) {
    short keyLen = blockKeyLength(recordAddress);
    return UnsafeAccess.toLong(recordAddress + RECORD_PREFIX_LENGTH + keyLen);
  }


  public static void setRecordSeqId(long recordAddress, long seqId) {
    short keyLen = blockKeyLength(recordAddress);
    UnsafeAccess.putLong(recordAddress + RECORD_PREFIX_LENGTH + keyLen, seqId);
  }


  public static long getRecordExpire(long recordAddress) {
    return UnsafeAccess.toLong(recordAddress + KV_SIZE_LENGTH);
  }

  public static void setRecordExpire(long recordAddress, long time) {
    UnsafeAccess.putLong(recordAddress + KV_SIZE_LENGTH, time);
  }

  public static long getRecordEviction(long recordAddress) {
    return UnsafeAccess.toLong(recordAddress + KV_SIZE_LENGTH + EXPIRE_SIZE_LENGTH);
  }

  public static void setRecordEviction(long recordAddress, long value) {
    UnsafeAccess.putLong(recordAddress + KV_SIZE_LENGTH + EXPIRE_SIZE_LENGTH, value);
  }

  public static Op getRecordType(long recordAddress) {
    short keyLen = blockKeyLength(recordAddress);
    int val = UnsafeAccess.toByte(recordAddress + RECORD_PREFIX_LENGTH + keyLen + SEQUENCEID_SIZE);
    return Op.values()[val];
  }

  public static void setRecordType(long recordAddress, Op type) {
    short keyLen = blockKeyLength(recordAddress);
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
  final boolean put(long keyPtr, int keyLength, long valuePtr, int valueLength, long version,
      long expire) throws RetryOperationException {

    if (getNumberOfRecords() > 0 && isForbiddenKey(keyPtr, keyLength)) {
      // Return success silently TODO
      return true;
    }
    
    if (mustStoreExternally(keyLength, valueLength)) {
      return putExternally(keyPtr,  keyLength, valuePtr,  
        valueLength, version, expire);
    }
    boolean onlyExactOverwrite = false;
    boolean recordOverwrite = false;
    boolean keyOverwrite = false; // key the same, value is different in size
    // Get the most recent active Tx Id or snapshot Id (scanner)
    long mostRecentActiveTxId = BigSortedMap.getMostRecentActiveTxSeqId();
    try {
      writeLock();
      
      onlyExactOverwrite = compactExpandIfNecessary(keyLength + valueLength);
      int dataSize = getDataInBlockSize();
      int blockSize = getBlockSize();

      long addr = search(keyPtr, keyLength, version);
      if (addr == NOT_FOUND) {
        // There were conflicting updates at the same time and this came late
        // we silently return true to avoid further splitting
        // This is how we handle multiple updates to the same key at the same time
        // All succeed but the winner has the highest sequenceId
        return true;
      }
      // TODO: verify what search returns if not found
      long foundSeqId = -1;
      boolean foundExternal = false;
      if (addr < dataPtr + dataSize) {
        int keylen = keyLength(addr);
        int vallen = valueLength(addr);
        if (keylen == keyLength) {
          // Compare keys
          int res = Utils.compareTo(keyPtr, keyLength, keyAddress(addr), keylen);
          if (res == 0 && vallen == valueLength) {
            recordOverwrite = true;
            keyOverwrite = true;
          } else if (res == 0) {
            keyOverwrite = true;
            foundExternal = getRecordAllocationType(addr) != AllocType.EMBEDDED;
          }
          if (keyOverwrite) {
            // Get seqId of existing record
            foundSeqId = getRecordSeqId(addr);
          }
        }
      }

      int newRecLen = RECORD_TOTAL_OVERHEAD + keyLength + valueLength;
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
        UnsafeAccess.copy(addr, addr + newRecLen, dataPtr + dataSize - addr);
        UnsafeAccess.copy(keyPtr, addr + RECORD_PREFIX_LENGTH, keyLength);
        UnsafeAccess.copy(valuePtr, addr + RECORD_TOTAL_OVERHEAD + keyLength,
          valueLength);
        // Update key-value length
        UnsafeAccess.putShort(addr, (short) keyLength);
        UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) valueLength);

        // Set version, expire, op type and eviction (0)
        setRecordSeqId(addr, version);
        setRecordEviction(addr, 0L);
        if (expire > 0) {
          setRecordExpire(addr, expire);
        }
        setRecordType(addr, Op.PUT);

        incrNumberOfRecords((short) 1);
        incrDataSize((short) newRecLen);
        if (keyOverwrite) {
          incrNumberDeletedAndUpdatedRecords((short) 1);
        }
      } else if (overwrite) {
        // UPDATE existing
        // We do overwrite of existing record
        UnsafeAccess.copy(valuePtr, addr + RECORD_TOTAL_OVERHEAD + keyLength,
          valueLength);
        // Set version, expire, op type and eviction (0)
        setRecordSeqId(addr, version);
        // Do not update eviction, because we overwrite existing record
        // setRecordEviction(addr, 0L);
        if (expire > 0) {
          setRecordExpire(addr, expire);
        }
        setRecordType(addr, Op.PUT);
      } else {
        // Neither insert nor overwrite - delete, then - insert
        // keyOverwrite = true
        // delete existing, put new

        int keylen = foundExternal? blockKeyLength(addr): keyLength(addr);// must be equal to keyLength
        int vallen = foundExternal? blockValueLength(addr): valueLength(addr);
        int existRecLen = keylen + vallen + RECORD_TOTAL_OVERHEAD;

                
        int toMove = foundExternal? (keyLength + valueLength + RECORD_TOTAL_OVERHEAD - existRecLen):
          (valueLength - vallen);
        if (onlyExactOverwrite && (dataSize + toMove > blockSize)) {
          // failed to insert, split is required
          return false;
        }
        
        deallocateIfExternalRecord(addr);

        // move from offset to offset + moveDist
        UnsafeAccess.copy(addr + existRecLen, addr + existRecLen + toMove,
          dataPtr + dataSize - addr - existRecLen);
        // TODO: check if we need this
        UnsafeAccess.copy(keyPtr, addr + RECORD_PREFIX_LENGTH, keyLength);
        UnsafeAccess.copy(valuePtr, addr + RECORD_TOTAL_OVERHEAD + keyLength,
          valueLength);
        // Update key-value length
        UnsafeAccess.putShort(addr, (short) keyLength);
        UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) valueLength);

        // Set version, expire, op type and eviction (0)
        setRecordSeqId(addr, version);
        // Do not reset eviction field
        // setRecordEviction(addr, 0L);
        if (expire > 0) {
          setRecordExpire(addr, expire);
        }
        setRecordType(addr, Op.PUT);
        incrDataSize((short) toMove);

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
   * @param version
   * @return true, if success, false otherwise
   * @throws RetryOperationException
   */
  final boolean putExternally(long keyPtr, int keyLength, long valuePtr, int valueLength, long version,
      long expire) throws RetryOperationException {

    boolean onlyExactOverwrite = false;
    boolean recordOverwrite = false;
    boolean keyOverwrite = false; // key the same, value is different in size
    // Get the most recent active Tx Id or snapshot Id (scanner)
    long mostRecentActiveTxId = BigSortedMap.getMostRecentActiveTxSeqId();
    AllocType type = getAllocType(keyLength, valueLength);
    // TODO: result check
    int newKVLength = INT_SIZE + ADDRESS_SIZE +
        (type == AllocType.EXT_VALUE? keyLength:0);// 4 - contains overall length, 8 - address 
    try {
      writeLock();
      onlyExactOverwrite = compactExpandIfNecessary(newKVLength);
      int dataSize = getDataInBlockSize();
      int blockSize = getBlockSize();
      
      long addr = search(keyPtr, keyLength, version);
      if (addr == NOT_FOUND) {
        // There were conflicting updates at the same time and this came late
        // we silently return true to avoid further splitting
        // This is how we handle multiple updates to the same key at the same time
        // All succeed but the winner has the highest sequenceId
        return true;
      }
      boolean append = addr == (dataPtr + dataSize);
      boolean extAddr = !append && getRecordAllocationType(addr) != AllocType.EMBEDDED;
      // TODO: verify what search returns if not found
      long foundSeqId = -1;
      if (addr < dataPtr + dataSize) {
        int keylen = keyLength(addr);
        int vallen = valueLength(addr);

        if (keylen == keyLength) {
          // Compare keys
          int res = Utils.compareTo(keyPtr, keyLength, keyAddress(addr), keylen);
          if (res == 0 && vallen == valueLength) {
            recordOverwrite = true;
            keyOverwrite = true;
          } else if (res == 0) {
            keyOverwrite = true;
          }
          if (keyOverwrite) {
            // Get seqId of existing record
            foundSeqId = getRecordSeqId(addr);
          }
        }
      }
  
      int newRecLength = RECORD_TOTAL_OVERHEAD + newKVLength;
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
      long recAddress = 0;
      if (insert) {
        // New K-V INSERT or we can't overwrite because of active Tx or snapshot
        // move from offset to offset + moveDist
        if (type == AllocType.EXT_KEY_VALUE) {
          int kvSize = keyLength + valueLength + 2 *INT_SIZE;
          recAddress = allocateAndCopyExternalKeyValue(keyPtr, keyLength, valuePtr, valueLength);
        
          UnsafeAccess.copy(addr, addr + newRecLength, dataPtr + dataSize - addr);
          UnsafeAccess.putInt( addr + RECORD_TOTAL_OVERHEAD + 0 /*key length*/, kvSize);
          UnsafeAccess.putLong(addr + RECORD_TOTAL_OVERHEAD + INT_SIZE, recAddress);
          // Update key-value length
          UnsafeAccess.putShort(addr, (short) EXTERNAL_KEY_VALUE /* ==0*/);
          UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) (INT_SIZE + ADDRESS_SIZE));
        } else { // AllocType.EXT_VALUE
          recAddress = allocateAndCopyExternalValue(valuePtr, valueLength);
          
          UnsafeAccess.copy(addr, addr + newRecLength, dataPtr + dataSize - addr);
          // copy key
          UnsafeAccess.copy(keyPtr, addr + RECORD_PREFIX_LENGTH, keyLength);
          // store value (size + address)
          UnsafeAccess.putInt( addr + RECORD_TOTAL_OVERHEAD + keyLength, valueLength + INT_SIZE);
          UnsafeAccess.putLong(addr + RECORD_TOTAL_OVERHEAD + keyLength + INT_SIZE, recAddress);
          // Update key-value length
          UnsafeAccess.putShort(addr, (short) keyLength);
          // Set value to external
          UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) EXTERNAL_VALUE);
        }
        // Set version, expire, op type and eviction (0)
        setRecordSeqId(addr, version);
        setRecordEviction(addr, 0L);
        if (expire > 0) {
          setRecordExpire(addr, expire);
        }
        setRecordType(addr, Op.PUT);

        incrNumberOfRecords((short) 1);
        incrDataSize((short) newRecLength);
        if (keyOverwrite) {
          incrNumberDeletedAndUpdatedRecords((short) 1);
        }
      } else if (overwrite) {
        // Keys are the same, values have the same size - both new and old records 
        // are external allocations of the same type
        // UPDATE existing - both are external records
        // We do overwrite of existing record and use existing external allocation
        recAddress = getExternalRecordAddress(addr);
        
        if (type == AllocType.EXT_KEY_VALUE) {
          UnsafeAccess.copy(valuePtr, recAddress + 2 * INT_SIZE + keyLength,
            valueLength);
        } else { // AllocType.EXT_VALUE
          UnsafeAccess.copy(valuePtr, recAddress + INT_SIZE,
            valueLength);
        }
        // Set version, expire, op type and eviction (0)
        setRecordSeqId(addr, version);
        // Do not update eviction, because we overwrite existing record
        // setRecordEviction(addr, 0L);
        if (expire > 0) {
          setRecordExpire(addr, expire);
        }
        setRecordType(addr, Op.PUT);
      } else if (!extAddr){
        // Keys are the same, values are not, old k-v is EMBEDDED
        // Neither insert nor overwrite - delete, then - insert
        // keyOverwrite = true
        // delete existing, put new
        // TODO check allocation
        int keylen =  keyLength(addr);// must be equal to keyLength
        int vallen =  valueLength(addr);
        int existRecLength = keylen + vallen + RECORD_TOTAL_OVERHEAD;
        
        int toMove = newRecLength - existRecLength;
        if (onlyExactOverwrite && (dataSize + toMove > blockSize)) {
          // failed to insert, split is required
          return false;
        }
        
        if (type == AllocType.EXT_KEY_VALUE) {
          recAddress = allocateAndCopyExternalKeyValue(keyPtr, keyLength, valuePtr, 
            valueLength);

          // move from offset to offset + moveDist
          UnsafeAccess.copy(addr + existRecLength, addr + existRecLength + toMove,
            dataPtr + dataSize - addr - existRecLength);
        
          UnsafeAccess.putInt( addr + RECORD_TOTAL_OVERHEAD + 0 /*key length*/, 
            keyLength + valueLength + 2 * INT_SIZE);
          UnsafeAccess.putLong(addr + RECORD_TOTAL_OVERHEAD + INT_SIZE, recAddress);
          // Update key-value length
          UnsafeAccess.putShort(addr, (short) EXTERNAL_KEY_VALUE);
          UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) (INT_SIZE + ADDRESS_SIZE));
        } else { // AlocType.EXT_VALUE
          
          recAddress = allocateAndCopyExternalValue(valuePtr, valueLength);
         

          // move from offset to offset + moveDist
          UnsafeAccess.copy(addr + existRecLength, addr + existRecLength + toMove,
            dataPtr + dataSize - addr - existRecLength);
          // copy key
          UnsafeAccess.copy(keyPtr,  addr + RECORD_PREFIX_LENGTH, keyLength);
          UnsafeAccess.putInt( addr + RECORD_TOTAL_OVERHEAD + keyLength, 
            valueLength + INT_SIZE);
          UnsafeAccess.putLong(addr + RECORD_TOTAL_OVERHEAD + keyLength + INT_SIZE, recAddress);
          // Update key-value length
          UnsafeAccess.putShort(addr, (short) keyLength);
          UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) EXTERNAL_VALUE);

        }
        // Set version, expire, op type and eviction (0)
        setRecordSeqId(addr, version);
        // Do not reset eviction field
        // setRecordEviction(addr, 0L);
        if (expire > 0) {
          setRecordExpire(addr, expire);
        }
        setRecordType(addr, Op.PUT);
        incrDataSize((short) toMove);

      } else if (extAddr) {
        // Keys are the same, values are not, but old k-v is EXTERNAL type of allocation
        // As since keys are the same, both new and old records have the same type of 
        // external allocation, either EXT_VALUE or EXT_KEY_VALUE
        // Deallocate existing allocation

        //deallocateIfExternalRecord(addr);
        if (type == AllocType.EXT_KEY_VALUE) {
          //TODO: realloc reconsider

          recAddress = reallocateAndCopyExternalKeyValue(addr,keyPtr, keyLength, valuePtr, valueLength);
          UnsafeAccess.putInt( addr + RECORD_TOTAL_OVERHEAD + 0 /*key length*/, 
            keyLength + valueLength + 2 * INT_SIZE);
          UnsafeAccess.putLong(addr + RECORD_TOTAL_OVERHEAD + INT_SIZE, recAddress);
          // Update key-value length
          UnsafeAccess.putShort(addr, (short) EXTERNAL_KEY_VALUE);
          UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) (INT_SIZE + ADDRESS_SIZE));
        } else { // AllocType.EXT_VALUE
          // As since keys are the same - no need to overwrite existing key
          //TODO: realloc reconsider
          recAddress = reallocateAndCopyExternalValue(addr, valuePtr, valueLength);         
          UnsafeAccess.putInt( addr + RECORD_TOTAL_OVERHEAD + keyLength, 
            valueLength + INT_SIZE);
          UnsafeAccess.putLong(addr + RECORD_TOTAL_OVERHEAD + keyLength +INT_SIZE, recAddress);
          // Update key-value length
          UnsafeAccess.putShort(addr, (short) keyLength);
          UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) EXTERNAL_VALUE);
        }
        // Set version, expire, op type and eviction (0)
        setRecordSeqId(addr, version);
        // Do not reset eviction field
        // setRecordEviction(addr, 0L);
        if (expire > 0) {
          setRecordExpire(addr, expire);
        }
        setRecordType(addr, Op.PUT);
      }
      return true;
    } finally {
      writeUnlock();
    }
  }
  
  final long search(byte[] key, int keyOffset, int keyLength, long version) {
    return search(key, keyOffset, keyLength, version, false);
  }
  /**
   * Search position of a first key which is greater or equals to a given key
   * @param key
   * @param keyOffset
   * @param keyLength
   * @param version
   * @param forPut
   * @return address to insert (or update)
   */
  final long search(byte[] key, int keyOffset, int keyLength, long version, boolean forPut) {
    long ptr = dataPtr;
    long stopAddress =0;
    int numRecords = getNumberOfRecords();
    int dataSize = getDataInBlockSize();
    long txId = BigSortedMap.getMostRecentActiveTxSeqId();
    stopAddress = dataPtr + dataSize;
    while (ptr < stopAddress) {
      int keylen = keyLength(ptr);     
      int vallen = blockValueLength(ptr);
      int res = Utils.compareTo(key, keyOffset, keyLength, keyAddress(ptr), keylen);
      if (res < 0 || (res == 0 && version == NO_VERSION)) {
        return ptr;
      } else if (res == 0) {
        // check versions
        long ver = getRecordSeqId(ptr);
        if (ver <= version) {
          return ptr;
        } else if (version > txId && forPut) {
          // We can not overwrite record with a higher version if there 
          return NOT_FOUND;
        }
      }
      keylen = blockKeyLength(ptr);
      ptr += keylen + vallen + RECORD_TOTAL_OVERHEAD;
    }
    // after the last record
    return stopAddress;
  }
  
  final long search(long keyPtr, int keyLength, long version) {
    return search(keyPtr, keyLength, version, false);
  }
  
  /**
   * Search position of a first key which is greater or equals to a given key
   * @param keyPtr
   * @param keyLength
   * @return address to insert (or update)
   */
  final long search(long keyPtr, int keyLength, long version, boolean forPut) {
    long ptr = dataPtr;
    long stopAddress =0;
    int numRecords = getNumberOfRecords();
    int dataSize = getDataInBlockSize();
    long txId = BigSortedMap.getMostRecentActiveTxSeqId();
    stopAddress = dataPtr + dataSize;
    while (ptr < stopAddress) {
      int keylen = keyLength(ptr);     
      int vallen = blockValueLength(ptr);
      int res = Utils.compareTo(keyPtr, keyLength, keyAddress(ptr), keylen);
      if (res < 0 || (res == 0 && version == NO_VERSION)) {
        return ptr;
      } else if (res == 0) {
        // check versions
        long ver = getRecordSeqId(ptr);
        if (ver <= version) {
          return ptr;
        } else if (version > txId && forPut) {
          return NOT_FOUND;
        }
      }
      keylen = blockKeyLength(ptr);
      ptr += keylen + vallen + RECORD_TOTAL_OVERHEAD;
    }
    // after the last record
    return stopAddress;
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
  final boolean isLargerThanMax(byte[] key, int keyOffset, int keyLength, long version) {
      long address = search(key, keyOffset, keyLength, version);
      return address == dataPtr + getDataInBlockSize();
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
  final boolean isLargerThanMax(long keyPtr, int keyLength, long version) {
    long address = search(keyPtr, keyLength, version);
    return address == dataPtr + getDataInBlockSize();
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

  final OpResult delete(long keyPtr, int keyLength, long version) throws RetryOperationException {

    if (isForbiddenKey(keyPtr, keyLength)) {
      // Return NOT FOUND TODO
      return OpResult.NOT_FOUND;
    }
    
    boolean insert = false;
    OpResult result = OpResult.NOT_FOUND;
    int valueLength = 0; // no value in delete op
    // Get the most recent active Tx Id or snapshot Id (scanner)
    long mostRecentActiveTxId = BigSortedMap.getMostRecentActiveTxSeqId();
    try {
      writeLock();
      int dataSize = getDataInBlockSize();
      int blockSize = getBlockSize();
      long foundSeqId;
      boolean firstKey;
      do {
        
        // We loop until we delete all eligible records with the same key
        // there can be multiple of them due to ongoing Tx and/or open snapshot scanners
        // TODO: 2 blocks can start (theoretically) with the same key!!!
        foundSeqId = -1;
        insert = false;
        long addr = search(keyPtr, keyLength, version);
        firstKey = addr == this.dataPtr;
        if (addr < dataPtr + dataSize) {
          int keylen = keyLength(addr);
          if (keylen == keyLength) {
            // Compare keys
            int res =
                Utils.compareTo(keyPtr , keyLength, keyAddress(addr), keylen);
            if (res == 0) {
              // Get seqId of existing record
              foundSeqId = getRecordSeqId(addr);
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
        AllocType type =  getRecordAllocationType(addr);
        boolean isExternalRecord = type != AllocType.EMBEDDED;

        if (type == AllocType.EXT_KEY_VALUE && insert) {
          keyLength = blockKeyLength(addr);
          valueLength = blockValueLength(addr); // 12 bytes
        }
        
        if (insert) {
          if (dataSize + keyLength + valueLength + RECORD_TOTAL_OVERHEAD > blockSize) {
            // try compact first (remove deleted, updated)
            compact(true);
            // Get data size again
            dataSize = getDataInBlockSize();
            if (dataSize + keyLength + valueLength + RECORD_TOTAL_OVERHEAD > blockSize) {
              // try to expand block
              boolean res = expand(dataSize + keyLength + valueLength + RECORD_TOTAL_OVERHEAD);
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
        
        if (getNumberOfRecords() > 1) {
          // If we have single record - no need to check parent index block
          // The block will be deleted
          if (firstKey && this.indexBlock.tryUpdateFirstKey(this) == false) {
            return OpResult.PARENT_SPLIT_REQUIRED;
          }
        }
        
        if (insert) {
          // TODO: test case for this code
          //TODO: key is must be allocated externally due to size case !!!
          
          // New K-V INSERT or we can't overwrite because of active Tx or snapshot
          // move from offset to offset + moveDist
          UnsafeAccess.copy(addr, addr + moveDist, dataPtr + dataSize - addr);
          if (type != AllocType.EXT_KEY_VALUE) {
            UnsafeAccess.copy(keyPtr, addr + RECORD_PREFIX_LENGTH, keyLength);
            // Update key length
            UnsafeAccess.putShort(addr, (short) keyLength);
            // 0
            UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) valueLength);
          } else {
            // REMEMBER: if record is Op.DELETE - do not deallocate any external allocations
            // b/c it refers to existing allocation
            long address = getExternalRecordAddress(addr);
            int kvSize = keyLength(addr) + valueLength(addr) + 2 *INT_SIZE;
            UnsafeAccess.putInt( addr + RECORD_TOTAL_OVERHEAD + 0 /*key length*/, kvSize);
            UnsafeAccess.putLong(addr + RECORD_TOTAL_OVERHEAD + INT_SIZE, address);
          }

          // Set version, expire, op type and eviction (0)
          setRecordSeqId(addr, version);
          setRecordEviction(addr, 0L);
          setRecordType(addr, Op.DELETE);

          incrNumberOfRecords((short) 1);
          incrDataSize((short) moveDist);
          incrNumberDeletedAndUpdatedRecords((short)1);
          
          result = OpResult.OK;
          
        } else {

          // delete existing record with the same key
          
          int keylen = blockKeyLength(addr);
          int vallen = blockValueLength(addr);
          moveDist = keylen + vallen + RECORD_TOTAL_OVERHEAD;

          // move from offset + moveDist to offset
          deallocateIfExternalRecord(addr);
          UnsafeAccess.copy(addr + moveDist, addr, dataPtr + dataSize - addr - moveDist);
          incrDataSize((short) -moveDist);
          incrNumberOfRecords((short) -1);
          // Update data size
          dataSize = getDataInBlockSize();
          result = OpResult.OK;
        }
        // We check that block is empty before
        // updating first key in a parent index block, because
        // this call can deallocate current block
        boolean isEmpty = isEmpty();
        if (firstKey && result == OpResult.OK) {
          this.indexBlock.updateFirstKey(this);
        }
        if (insert || isEmpty) {
          return result;
        }
      } while (foundSeqId >= 0);
      
      return result;
    } finally {
      writeUnlock();
    }
  }
  
  /**
   * TODO: check how this work with large K-V
   * Adds Delete tombstone to an empty block
   * @param key
   * @param keyOffset
   * @param keyLength
   * @param version
   */
  final void addDelete(byte[] key, int keyOffset, int keyLength, long version) {
    UnsafeAccess.putShort(dataPtr, (short) keyLength);
    UnsafeAccess.putShort(dataPtr + KEY_SIZE_LENGTH, (short) 0);
    UnsafeAccess.copy(key, keyOffset, dataPtr + RECORD_PREFIX_LENGTH, keyLength);
    setRecordSeqId(dataPtr, version);
    setRecordType(dataPtr, Op.DELETE);
  }

  /**
   * TODO: check how this work with large K-V
   * Adds Delete tombstone to an empty block
   * @param key
   * @param keyOffset
   * @param keyLength
   * @param version
   */
  final void addDelete(long keyPtr, int keyLength, long version) {
    UnsafeAccess.putShort(dataPtr, (short) keyLength);
    UnsafeAccess.putShort(dataPtr + KEY_SIZE_LENGTH, (short) 0);
    UnsafeAccess.copy(keyPtr, dataPtr + RECORD_PREFIX_LENGTH, keyLength);
    setRecordSeqId(dataPtr, version);
    setRecordType(dataPtr, Op.DELETE);
  }

  
  final boolean isEmpty() {
    return getNumberOfRecords() == 0;
  }
  /**
   * Delete operation 
   * TODO: update Index Block start key if we delete start key
   * TODO: handling deletion with a single k-v
   * @param key
   * @param keyPtr
   * @param keyLength
   * @param version
   * @return true if success, false otherwise
   * @throws RetryOperationException
   */
  final OpResult delete(byte[] key, int keyOffset, int keyLength, long version)
      throws RetryOperationException {
    if (isForbiddenKey(key, keyOffset, keyLength)) {
      // Return NOT FOUND TODO
      return OpResult.NOT_FOUND;
    }
    
    boolean insert = false;
    OpResult result = OpResult.NOT_FOUND;
    int valueLength = 0; // no value in delete op
    // Get the most recent active Tx Id or snapshot Id (scanner)
    long mostRecentActiveTxId = BigSortedMap.getMostRecentActiveTxSeqId();
    try {
      writeLock();
      int dataSize = getDataInBlockSize();
      int blockSize = getBlockSize();
      long foundSeqId;
      boolean firstKey;
      do {
        
        // We loop until we delete all eligible records with the same key
        // there can be multiple of them due to ongoing Tx and/or open snapshot scanners
        // TODO: 2 blocks can start (theoretically) with the same key!!!
        foundSeqId = -1;
        insert = false;
        long addr = search(key, keyOffset, keyLength, version);
        firstKey = addr == this.dataPtr;
        if (addr < dataPtr + dataSize) {
          int keylen = keyLength(addr);
          if (keylen == keyLength) {
            // Compare keys
            int res =
                Utils.compareTo(key , keyOffset, keyLength, keyAddress(addr), keylen);
            if (res == 0) {
              // Get seqId of existing record
              foundSeqId = getRecordSeqId(addr);
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
        AllocType type =  getRecordAllocationType(addr);
        boolean isExternalRecord = type != AllocType.EMBEDDED;

        if (type == AllocType.EXT_KEY_VALUE && insert) {
          keyLength = blockKeyLength(addr);
          valueLength = blockValueLength(addr); // 12 bytes
        }
        
        if (insert) {
          if (dataSize + keyLength + valueLength + RECORD_TOTAL_OVERHEAD > blockSize) {
            // try compact first (remove deleted, updated)
            compact(true);
            // Get data size again
            dataSize = getDataInBlockSize();
            if (dataSize + keyLength + valueLength + RECORD_TOTAL_OVERHEAD > blockSize) {
              // try to expand block
              boolean res = expand(dataSize + keyLength + valueLength + RECORD_TOTAL_OVERHEAD);
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
        
        if (getNumberOfRecords() > 1) {
          // If we have single record - no need to check parent index block
          // The block will be deleted
          if (firstKey && this.indexBlock.tryUpdateFirstKey(this) == false) {
            return OpResult.PARENT_SPLIT_REQUIRED;
          }
        }
        
        if (insert) {
          // TODO: test case for this code
          //TODO: key is must be allocated externally due to size case !!!
          
          // New K-V INSERT or we can't overwrite because of active Tx or snapshot
          // move from offset to offset + moveDist
          UnsafeAccess.copy(addr, addr + moveDist, dataPtr + dataSize - addr);
          if (type != AllocType.EXT_KEY_VALUE) {
            UnsafeAccess.copy(key, keyOffset, addr + RECORD_PREFIX_LENGTH, keyLength);
            // Update key length
            UnsafeAccess.putShort(addr, (short) keyLength);
            // 0
            UnsafeAccess.putShort(addr + KEY_SIZE_LENGTH, (short) valueLength);
          } else {
            // REMEMBER: if record is Op.DELETE - do not deallocate any external allocations
            // b/c it refers to existing allocation
            long address = getExternalRecordAddress(addr);
            int kvSize = keyLength(addr) + valueLength(addr) + 2 *INT_SIZE;
            UnsafeAccess.putInt( addr + RECORD_TOTAL_OVERHEAD + 0 /*key length*/, kvSize);
            UnsafeAccess.putLong(addr + RECORD_TOTAL_OVERHEAD + INT_SIZE, address);
          }

          // Set version, expire, op type and eviction (0)
          setRecordSeqId(addr, version);
          setRecordEviction(addr, 0L);
          setRecordType(addr, Op.DELETE);

          incrNumberOfRecords((short) 1);
          incrDataSize((short) moveDist);
          incrNumberDeletedAndUpdatedRecords((short)1);
          
          result = OpResult.OK;
          
        } else {

          // delete existing record with the same key
          
          int keylen = blockKeyLength(addr);//isExternalRecord? 0: keyLength;// must be equal to keyLength
          int vallen = blockValueLength(addr);//isExternalRecord?INT_SIZE + ADDRESS_SIZE: blockValueLength(addr);
          moveDist = keylen + vallen + RECORD_TOTAL_OVERHEAD;

          // move from offset + moveDist to offset
          deallocateIfExternalRecord(addr);
          UnsafeAccess.copy(addr + moveDist, addr, dataPtr + dataSize - addr - moveDist);
          incrDataSize((short) -moveDist);
          incrNumberOfRecords((short) -1);
          // Update data size
          dataSize = getDataInBlockSize();
          result = OpResult.OK;
        }
        // We check that block is empty before
        // updating first key in a parent index block, because
        // this call can deallocate current block
        boolean isEmpty = isEmpty();
        if (firstKey && result == OpResult.OK) {
          this.indexBlock.updateFirstKey(this);
        }
        if (insert || isEmpty) {
          return result;
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
  final boolean setExpire(byte[] key, int keyOffset, int keyLength, long expire, long version) {
    try {
      writeLock();
      long addr = search(key, keyOffset, keyLength, version);
      int dataSize = getDataInBlockSize();
      if (addr < dataPtr + dataSize) {
        int keylen = keyLength(addr);
        int res = Utils.compareTo(key, keyOffset, keyLength, keyAddress(addr), keylen);
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
  final boolean setExpire(long keyPtr, int keyLength, long expire, long version) {
    try {
      writeLock();
      long addr = search(keyPtr, keyLength, version);
      int dataSize = getDataInBlockSize();
      if (addr < dataPtr + dataSize) {
        int keylen = keyLength(addr);
        int res = Utils.compareTo(keyPtr, keyLength, keyAddress(addr), keylen);
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
  final long get(byte[] key, int keyOffset, int keyLength, long version)
      throws RetryOperationException {

    try {
      if (isForbiddenKey(key, keyOffset, keyLength)) {
        // Return NOT FOUND TODO
        return NOT_FOUND;
      }
      readLock();
      long addr = search(key, keyOffset, keyLength, version);
      int dataSize = getDataInBlockSize();
      if (addr < dataPtr + dataSize) {
        int keylen = keyLength(addr);
        int res = Utils.compareTo(key, keyOffset, keyLength, keyAddress(addr), keylen);
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
   * @return value length or NOT_FOUND if not found, make sure to check value length
   *         if value is larger than the buffer, no copy will be made,  clinet will need to provide
   *         larger buffer and repeat call 
   * @throws RetryOperationException
   */
  final long get(byte[] key, int keyOffset, int keyLength, byte[] valueBuf, int valOffset,
      long version) throws RetryOperationException {

    try {
      if (isForbiddenKey(key, keyOffset, keyLength)) {
        // Return NOT FOUND TODO
        return NOT_FOUND;
      }
      int maxValueLength = valueBuf.length - valOffset;
      readLock();
      long addr = search(key, keyOffset, keyLength, version);
      int dataSize = getDataInBlockSize();
      if (addr < dataPtr + dataSize) {
        int keylen = keyLength(addr);
        int vallen = valueLength(addr);
        int res = Utils.compareTo(key, keyOffset, keyLength, keyAddress(addr), keylen);
        if (res == 0) {
          // FOUND exact key
          Op type = getRecordType(addr);
          if (type == Op.PUT) {
            if (vallen <= maxValueLength) {
              // Copy value
              UnsafeAccess.copy(valueAddress(addr), valueBuf, valOffset, vallen);
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
  final long get(long keyPtr, int keyLength, long version) throws RetryOperationException {

    try {
      if (isForbiddenKey(keyPtr, keyLength)) {
        // Return NOT FOUND TODO
        return NOT_FOUND;
      }
      readLock();
      long addr = search(keyPtr, keyLength, version);
      int dataSize = getDataInBlockSize();
      if (addr < dataPtr + dataSize) {
        int keylen = keyLength(addr);
        int res = Utils.compareTo(keyPtr, keyLength, keyAddress(addr), keylen);
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
  final long get(long keyPtr, int keyLength, long valueBuf, int valueBufLength, long version)
      throws RetryOperationException {

    try {
      if (isForbiddenKey(keyPtr, keyLength)) {
        // Return NOT FOUND TODO
        return NOT_FOUND;
      }
      readLock();
      long addr = search(keyPtr, keyLength, version);
      int dataSize = getDataInBlockSize();
      if (addr < dataPtr + dataSize) {
        int keylen = keyLength(addr);
        int vallen = valueLength(addr);
        int res = Utils.compareTo(keyPtr, keyLength, keyAddress(addr), keylen);
        if (res == 0) {
          // FOUND exact key
          Op type = getRecordType(addr);
          if (type == Op.PUT) {
            if (vallen <= valueBufLength) {
              // Copy value
              UnsafeAccess.copy(valueAddress(addr), valueBuf, vallen);
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
  final int getMaxBlockSize() {
    return BigSortedMap.maxBlockSize;
  }

  /**
   * Compact block (remove deleted and collapse updates k-vs)
   * @param force - if true - force operation
   * @param seqId (TODO: do we need it?)
   * @throws RetryOperationException
   */

  final void compact(boolean force) throws RetryOperationException {
    long numRecords = getNumberOfRecords();
    long oldRecords = numRecords;

    long numDeletedRecords = getNumberOfDeletedAndUpdatedRecords();
    if (numRecords == 0 || numDeletedRecords == 0) {
      return;
    }
    int dataSize = getDataInBlockSize();
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
    short keylen = blockKeyLength(dataPtr);
    short vallen = blockValueLength(dataPtr);
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
      dataSize = getDataInBlockSize(); 
    }
    // get updated number of records
    numRecords = getNumberOfRecords();
    // Delete first record (deleted)
    if (isDeleted(dataPtr) && numRecords > 1) {
      long seqId = getRecordSeqId(dataPtr); 
      if (seqId > mostRecentTxId || seqId < leastRecentTxId) {
        deallocateIfExternalRecord(dataPtr);
        UnsafeAccess.copy(dataPtr + firstRecordLength, dataPtr, dataSize - firstRecordLength);
        incrNumberOfRecords((short)-1);
        incrDataSize((short)-firstRecordLength);
        incrNumberDeletedAndUpdatedRecords((short)-1);
      }
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
    short dataSize = getDataInBlockSize();
    short keylen = blockKeyLength(ptr);
    short vallen = blockValueLength(ptr);
    ptr += keylen + vallen + RECORD_TOTAL_OVERHEAD;
    //boolean canDelete = true;
    int count = 1;
    while (ptr < dataPtr + dataSize) {
      if (!keysEquals(startPtr, ptr)) {
        break;
      }
      // key is still the same
      keylen = blockKeyLength(ptr);
      vallen = blockValueLength(ptr);
      long version = getRecordSeqId(ptr);
      if (version >= minTxId && version <= maxTxId) {
        //canDelete = false;
        break;
      } else if (delPossible) {
        // Deallocate record
        deallocateIfExternalRecord(ptr);
      }
      count++;
      ptr += keylen + vallen + RECORD_TOTAL_OVERHEAD;
    }
    if (/*canDelete &&*/ delPossible) {
      short toDelete = (short) (ptr - startPtr);
      long len = dataPtr + dataSize - toDelete - startPtr;
      UnsafeAccess.copy(startPtr + toDelete, startPtr, len);
      incrNumberOfRecords((short) -count);
      incrDataSize((short) -toDelete);
      incrNumberDeletedAndUpdatedRecords((short) -(count - 1));
    }
    return ptr;
  }
  
  private void deallocateIfExternalRecord(long ptr) {
    AllocType type = getRecordAllocationType(ptr);
    if (type == AllocType.EMBEDDED) {
      return;
    }
    int size = keyLength(ptr) + valueLength(ptr) + 
        (type == AllocType.EXT_KEY_VALUE?(2 * INT_SIZE): INT_SIZE);
    UnsafeAccess.free(getExternalRecordAddress(ptr));
    largeKVs.decrementAndGet();
    BigSortedMap.totalDataSize.addAndGet(-size);
    BigSortedMap.totalAllocatedMemory.addAndGet(-size);
  }
  
  private boolean keysEquals (long address1, long address2) {
    int length1 = keyLength(address1);
    int length2 = keyLength(address2);
    if (length1 != length2) return false;
    return Utils.compareTo(keyAddress(address1), length1, keyAddress(address2), length2) == 0;
  }
  
  private long coalesceUpdates(long ptr, long minTxId, long maxTxId) {
    long startPtr = ptr;
    long ver = getRecordSeqId(ptr);
    boolean canDelete = ver < minTxId || ver > maxTxId;
    // the current record at startPtr is Delete
    short dataSize = getDataInBlockSize();
    short keylen = blockKeyLength(ptr);
    short vallen = blockValueLength(ptr);
    ptr += keylen + vallen + RECORD_TOTAL_OVERHEAD;
    boolean inBetween = false;// flag, we set it if one of the records has version
    // between minTxId and maxTxId
    while (ptr < dataPtr + dataSize) {
      if (!keysEquals(startPtr, ptr)) {
        break;
      }
      // key is still the same
      keylen = blockKeyLength(ptr);
      vallen = blockValueLength(ptr);
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
          // Update data size
          dataSize = getDataInBlockSize();
          deallocateIfExternalRecord(ptr);
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
  final boolean canSplit() {
    return getNumberOfRecords() > 1;
  }
  /**
   * TODO: split won't work if we have only 1 record
   * Must always compact before splitting block
   * @param forceCompact
   * @return new (right) block
   * @throws RetryOperationException
   */
  final DataBlock split(boolean forceCompact) throws RetryOperationException {

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
        int keylen = blockKeyLength(ptr + off);
        int vallen = blockValueLength(ptr + off);
        long old = off;
        off += keylen + vallen + RECORD_TOTAL_OVERHEAD;
        num++;
      }
      
      int oldDataSize = getDataInBlockSize();
      setDataSize((short) off);
      setNumberOfRecords((short)num);
      int rightDataSize = oldDataSize - off;
      int rightBlockSize = getMinSizeGreaterThan(getBlockSize(), rightDataSize);
      DataBlock right = new DataBlock((short)rightBlockSize);

      right.numRecords = (short)(oldNumRecords - num);
      right.numDeletedAndUpdatedRecords = (short)0;
      right.dataInBlockSize = (short)rightDataSize;
      right.seqNumberSplitOrMerge = 0;
      right.compressed = false;
      right.threadSafe = false;
      UnsafeAccess.copy(dataPtr + off, right.dataPtr, right.dataInBlockSize);
      // shrink current
      shrink();
      return right;
    } finally {
      writeUnlock();
    }
  }
  
  /**
   * TODO: which length? 
   * Get first key length
   * @return length
   */
  final int getFirstKeyLength() {
    return keyLength(dataPtr);
  }
  
  /**
   * TODO: which address
   * Get first key address
   * @return address
   */
  final long getFirstKeyAddress() {
    return keyAddress(dataPtr);
  }
  
  /**
   * Get first key version
   * @return version
   */
  final long getFirstKeyVersion() {
    int keylen = blockKeyLength(dataPtr);
    return UnsafeAccess.toLong(dataPtr +RECORD_PREFIX_LENGTH + keylen);
  }
  
  /**
   * Get first key type value
   * @return type 
   */
  final Op getFirstKeyType() {
    int keylen = blockKeyLength(dataPtr);
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
  final int compareTo(byte[] key, int off, int len, long version, Op type) {
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
  static int compareTo(long addr, byte[] key, int off, int len, long version, Op type) {
    int length = keyLength(addr);
    int res = Utils.compareTo(key, off, len, keyAddress(addr), length);
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
  
  final int compareTo(long key, int len, long version, Op type) {
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
  
  static int compareTo(long addr, long key, int len, long version, Op type) {
    int length = keyLength(addr);
    int res = Utils.compareTo(key, len, keyAddress(addr), length);
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
  
  final long splitPos(boolean forceCompact) {
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
      short keylen = blockKeyLength(ptr + off);
      short vallen = blockValueLength(ptr + off);
      off += keylen + vallen + RECORD_TOTAL_OVERHEAD;
      num++;
    }
    return dataPtr + off;
  }

  /**
   * Should compact this block
   * @return true, false
   */
  final boolean shouldCompact() {
    int numRecords = getNumberOfRecords();
    int numDeletedRecords = getNumberOfDeletedAndUpdatedRecords();
    if (numRecords == 0) return false;
    return (double) numDeletedRecords / numRecords > MIN_COMPACT_RATIO;
  }

  final boolean shouldMerge() {
    return ((double)getDataInBlockSize())/getBlockSize() < MIN_MERGE_FACTOR;
  }
  /**
   * Merge two adjacent blocks
   * @param right
   * @param forceCompact
   * @param forceMerge
   * @return true, if merge successful, false - otherwise
   * @throws RetryOperationException
   */
  final boolean merge(DataBlock right, boolean forceCompact)
      throws RetryOperationException {

    try {
      writeLock();
      right.writeLock();
      // Increment sequence numbers

      incrSeqNumberSplitOrMerge();
      right.incrSeqNumberSplitOrMerge();

      if (forceCompact) {
        compact(true);
        right.compact(true);
      }
      // Check total size
      int dataSize = getDataInBlockSize();
      int rightDataSize = right.getDataInBlockSize();
      int blockSize = getBlockSize();
      while (dataSize + rightDataSize >= blockSize) {
        boolean result = expand(dataSize + rightDataSize);
        if (result == false) {
          return result;
        }
        blockSize = getBlockSize();
      }
      UnsafeAccess.copy(right.dataPtr, this.dataPtr + dataSize, rightDataSize);

      incrNumberOfRecords(right.getNumberOfRecords());
      setNumberOfDeletedAndUpdatedRecords((short)0);
      int size = right.getDataInBlockSize();
      incrDataSize((short)size);
      // We need to decrement overall
      BigSortedMap.totalDataInDataBlocksSize.addAndGet(-size);
      BigSortedMap.totalDataSize.addAndGet(-size);     
      return true;
    } finally {
      right.writeUnlock();
      writeUnlock();
    }
  }

  /**
   * Get block address
   * @return address
   */
  final long getAddress() {
    return dataPtr;
  }

  
  public static AtomicLong largeKVs = new AtomicLong();
  
  /**
   * Free memory
   */
  final void free() {
    free(true);
  }

  
  /**
   * Free block memory and ext alloc (true/false) 
   * @param freeExternalAllocs
   */
  final void free(boolean freeExternalAllocs) {
    long ptr = dataPtr;
    int count = 0;
    int numRecords = getNumberOfRecords();
    int dataSize = getDataInBlockSize();
    int blockSize = getBlockSize();
    while (count++ < numRecords) {
      int keylen = blockKeyLength(ptr);
      int vallen = blockValueLength(ptr);
      AllocType type = getRecordAllocationType(ptr);
      if (freeExternalAllocs && getRecordAllocationType(ptr) != AllocType.EMBEDDED) {
        long addr = getExternalRecordAddress(ptr);
        long size = getExternalAllocationSize(addr, type);
        UnsafeAccess.free(addr);
        BigSortedMap.totalAllocatedMemory.addAndGet(-size);
        BigSortedMap.totalDataSize.addAndGet(-size);
        largeKVs.decrementAndGet();
      }
      ptr += keylen + vallen + RECORD_TOTAL_OVERHEAD;
    }
    UnsafeAccess.free(dataPtr);
    BigSortedMap.totalAllocatedMemory.addAndGet(-blockSize);
    BigSortedMap.totalBlockDataSize.addAndGet(-blockSize);
    BigSortedMap.totalDataInDataBlocksSize.addAndGet(-dataSize);
    valid = false;

  }

  
  /**
   * Used for testing only
   */
  final byte[] getFirstKey() {
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
