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
package org.bigbase.carrot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bigbase.carrot.compression.Codec;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.ops.IncrementDouble;
import org.bigbase.carrot.ops.IncrementFloat;
import org.bigbase.carrot.ops.IncrementInt;
import org.bigbase.carrot.ops.IncrementLong;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.ops.OperationFailedException;
import org.bigbase.carrot.redis.KeysLocker;
import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class BigSortedMap {
	
  Log LOG = LogFactory.getLog(BigSortedMap.class);
  
//TODO: configurable compression - default codec
  static Codec codec = CodecFactory.getInstance().getCodec(CodecType.NONE);
  
  /**
   * Is compression enabled
   * @return true, if - yes, false otherwise
   */
  
  public static boolean isCompressionEnabled() {
    return codec != null && codec.getType() != CodecType.NONE;
  }
  
  
  public static void setCompressionCodec(Codec codec) {
    BigSortedMap.codec = codec;
  }
  
  public static Codec getCompressionCodec() {
    return codec;
  }
  
  /*
   * Thread local storage for index blocks used as a key in a 
   * Map<IndexBlock,IndexBlock> operations
   */
  private static ThreadLocal<IndexBlock> keyBlock = new ThreadLocal<IndexBlock>(); 
  
  public final static String MAX_MEMORY_KEY = "map.max.memory";
  private ConcurrentSkipListMap<IndexBlock, IndexBlock> map = 
		  new ConcurrentSkipListMap<IndexBlock, IndexBlock>();
  
  static int maxBlockSize = DataBlock.MAX_BLOCK_SIZE;
  static int maxIndexBlockSize = IndexBlock.MAX_BLOCK_SIZE;
  public static AtomicLong totalAllocatedMemory = new AtomicLong(0);
  /*
   * This tracks total data blocks size (memory allocated for data blocks)
   */
  static AtomicLong totalBlockDataSize = new AtomicLong(0);
  /*
   * This tracks total data size in data blocks (data can be allocated outside
   * data blocks)
   */
  static AtomicLong totalDataInDataBlocksSize = new AtomicLong(0);

  /*
   * This tracks total compressed data size in data blocks (data can be allocated outside
   * data blocks)
   */
  static AtomicLong totalCompressedDataInDataBlocksSize = new AtomicLong(0);
  /*
   * This tracks total data size  allocated externally (not in data blocks)
   */
  static AtomicLong totalExternalDataSize = new AtomicLong(0);
  
  /*
   * This tracks total index blocks size (memory allocated for index blocks) 
   */
  static AtomicLong totalBlockIndexSize = new AtomicLong(0);
  
  /*
   * This tracks total data size in index blocks (data can be allocated outside
   * index blocks)
   */
  static AtomicLong totalDataInIndexBlocksSize = new AtomicLong(0);
  /*
   * This tracks total index size (total index size >= total data in index blocks  size)
   */
  
  static AtomicLong totalIndexSize = new AtomicLong(0);
  
  // For k-v versioning
  static AtomicLong sequenceID = new AtomicLong(0);

  //private ReentrantLock[] locks = new ReentrantLock[11113];
  /*
   * Read-Write Lock TODO: StampedLock (Java 8)
   */
  static ReentrantReadWriteLock[] locks = new ReentrantReadWriteLock[11113];
  static {
    for (int i = 0; i < locks.length; i++) {
      locks[i] = new ReentrantReadWriteLock();
    }
  }
  private long maxMemory; 
  /*
   * Keeps ordered list of active snapshots (future Tx)
   */
  static private ConcurrentSkipListMap<Long, Long> activeSnapshots = new ConcurrentSkipListMap<>();
  
  /*
   * We need this buffer to keep keys during execute update
   */
  static ThreadLocal<Long> keyBuffer1 = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(256);
    }
  };
  
  static ThreadLocal<Integer> keyBufferSize1 = new ThreadLocal<Integer>() {
    @Override
    protected Integer initialValue() {
      return 256;
    }    
  };
  
  static ThreadLocal<Long> keyBuffer2 = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(256);
    }
  };
  
  static ThreadLocal<Integer> keyBufferSize2 = new ThreadLocal<Integer>() {
    @Override
    protected Integer initialValue() {
      return 256;
    }    
  };
  
  static ThreadLocal<Long> incrBuffer = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(Utils.SIZEOF_LONG);
    }
  };
  
  public static long countRecords(BigSortedMap map) {
    BigSortedMapScanner scanner = map.getScanner(0, 0, 0, 0);
    long count = 0;
    if (scanner == null) {
      return 0;
    }
    try {
      while (scanner.hasNext()) {
        count++;
        scanner.next();
      }
    } catch (IOException e) {
      return -1;
    } finally {
      try {
        scanner.close();
      } catch (IOException e) {
      }
    }
    return count;
  }

  public static long dumpRecords(BigSortedMap map) {
    BigSortedMapScanner scanner = map.getScanner(0, 0, 0, 0);
    long count = 0;
    if (scanner == null) {
      return 0;
    }
    try {
      while (scanner.hasNext()) {
        count++;
        long ptr = scanner.keyAddress();
        int size = scanner.keySize();
        System.out.println(Bytes.toHex(ptr, size));
        scanner.next();
      }
    } catch (IOException e) {
      return -1;
    } finally {
      try {
        scanner.close();
      } catch (IOException e) {
      }
    }
    return count;
  }
  
  public static void memoryStats() {
    System.out.println("            Total : " + getTotalAllocatedMemory());
    System.out.println(" Data Blocks Size : " + getTotalBlockDataSize());
    System.out.println("Index Blocks Size : " + getTotalBlockIndexSize());
    System.out.println("       Index Size : " + getTotalIndexSize());
    System.out.println("        Data Size : " + getTotalDataSize());

  }
  
  static void checkKeyBuffer(ThreadLocal<Long> key, ThreadLocal<Integer> size, int required) {
    if (required <= size.get()) return;
    long ptr = UnsafeAccess.realloc(key.get(), required);
    key.set(ptr);
    size.set(required);
  }
  
  /**
   * To support Tx and snapshots
   * @return earliest active Tx/snapshot sequenceId or -1
   */
  public static long getMostRecentActiveTxSeqId() {
	  if(activeSnapshots.isEmpty()) {
	    return -1;
	  }
    Long id = activeSnapshots.lastKey();
	  if (id == null) {
	    return -1;
	  }
	  return id;
  }
  /**
   * To support Tx and snapshot based scanners
   * @return oldest active tx/snapshot id
   */
  public static long getMostOldestActiveTxSeqId() {
    if(activeSnapshots.isEmpty()) {
      return -1;
    }    
    Long id = activeSnapshots.firstKey();
    if (id == null) {
      return -1;
    }
    return id;
  }
  
  /**
   * Creates new snapshots and returns its id
   * @return id of a snapshot
   */
  public static long createSnapshot() {
    long id = sequenceID.incrementAndGet();
    activeSnapshots.put(id,  id);
    return id;
  }
  /**
   * Release snapshot by Id
   * @param id
   */
  public static void releaseSnapshot(long id) {
    activeSnapshots.remove(id);
  }
  
  /**
   * Gets max block size (data)
   * @return size
   */
  public static int getMaxBlockSize() {
	  return maxBlockSize;
  }
  
  /**
   * Sets maximum data block size
   * @param size
   */
  public static void setMaxBlockSize(int size) {
	  maxBlockSize = size;
  }
  
  /**
   * Gets maximum index block size
   * @return size
   */
  public static int getMaxIndexBlockSize() {
	  return maxIndexBlockSize;
  }
    
  /**
   * Constructor of a big sorted map
   * @param maxMemory - memory limit in bytes
   */
  public BigSortedMap(long maxMemory) {
    this.maxMemory = maxMemory;
    initNodes();
  }
  
  
  /**
   * Get total allocated memory
   * @return total allocated memory
   */
  public static long getTotalAllocatedMemory() {
    return totalAllocatedMemory.get();
  }
  /**
   * Get total memory allocated for data blocks
   * @return memory allocated for data blocks
   */
  public static long getTotalBlockDataSize() {
    return totalBlockDataSize.get();
  }
  
  /**
   * Get total memory which data occupies in data blocks
   * @return memory
   */
  public static long getDataInDataBlocksSize() {
    return totalDataInDataBlocksSize.get();
  }
  
  /**
   * Get memory allocated for index blocks
   * @return memory
   */
  public static long getTotalBlockIndexSize() {
	  return totalBlockIndexSize.get();
  }
  
  /**
   * Get total data size (uncompressed)
   * @return size
   */
  public static long getTotalDataSize() {
    return totalExternalDataSize.get() + totalDataInDataBlocksSize.get();
  }
  
  /**
   * Get total compressed data size
   * @return size
   */
  public static long getTotalCompressedDataSize() {
    return  totalCompressedDataInDataBlocksSize.get();
  }
  /**
   * Get total index size
   * @return size
   */
  public static long getTotalIndexSize() {
    return totalIndexSize.get();
  }
  /**
   * Get total memory which index occupies in index blocks
   * @return size
   */
  public static long getDataInIndexBlocksSize() {
    return totalDataInIndexBlocksSize.get();
  }
  
  public static void printMemoryAllocationStats() {
    System.out.println("\nCarrot memory allocation statistics:");
    System.out.println("Total memory               :" + getTotalAllocatedMemory());
    System.out.println("Total data blocks          :" + getTotalBlockDataSize());
    System.out.println("Total data size            :" + getTotalDataSize());
    System.out.println("Total data block usage     :" + ((double)getTotalDataSize())
      /getTotalBlockDataSize());
    System.out.println("Total index size           :" + getTotalBlockIndexSize());
    System.out.println("Total compressed data size :" + getTotalCompressedDataSize());
    System.out.println("Total external data size   :" + totalExternalDataSize.get());   
    System.out.println("Copmpression ratio         :" + ((double)getTotalDataSize()/ 
        getTotalAllocatedMemory())+"\n");
  }
  
  /** 
   * Gets maximum memory limit
   */
  public long getMaxMemory() {
    return maxMemory;
  }
  
  
  public void dumpStats() {
    long totalRows = 0;
    for(IndexBlock b: map.keySet()) {
      totalRows += b.getNumberOfDataBlock();
      b.dumpIndexBlockExt();
    }
    System.out.println("Total blocks="+ (totalRows) + " index blocks=" + map.size());
  }
  
  private void initNodes() {
    IndexBlock b = new IndexBlock(maxIndexBlockSize);
    b.setFirstIndexBlock();
    map.put(b, b);
  }
  
  private void ensureBlock() {
    if (keyBlock.get() != null) {
      return;
    } else {
      synchronized(keyBlock) {
        if (keyBlock.get() != null) {
          return;
        } else {
          IndexBlock b = new IndexBlock(maxIndexBlockSize);
          b.setThreadSafe(true);
          keyBlock.set(b);
        }
      }
    }
  }
  
  /**
   * Locking support. We have 2 - level locking: Level 1: BSM locks on index block to get correct
   * index block for read / write operations Level 2: Index block internal locking to prevent
   * concurrent access disaster
   * @param b index block
   */
  public void readLock(IndexBlock b) {
    int index = (b.hashCode() % locks.length);
    ReentrantReadWriteLock lock = locks[index];
    lock.readLock().lock();
  }

  /**
   * Read unlock
   */
  public void readUnlock(IndexBlock b) {

    int index = (b.hashCode() % locks.length);
    ReentrantReadWriteLock lock = locks[index];
    lock.readLock().unlock();
  }

  /**
   * Write lock
   * @throws RetryOperationException
   * @throws InterruptedException
   */
  public void writeLock(IndexBlock b) {
    int index = (b.hashCode() % locks.length);
    ReentrantReadWriteLock lock = locks[index];
    lock.writeLock().lock();
  }

  /**
   * Write unlock
   */
  public void writeUnlock(IndexBlock b) {
    int index = (b.hashCode() % locks.length);
    ReentrantReadWriteLock lock = locks[index];
    lock.writeLock().unlock();
  }
  
  private final IndexBlock getThreadLocalBlock () {
    IndexBlock kvBlock = keyBlock.get();
    if (kvBlock == null) {
      ensureBlock();
      kvBlock = keyBlock.get();
    }
    kvBlock.reset();
    return kvBlock;
  }
  
  /**
   * Returns native map
   * @return map
   */
  public ConcurrentSkipListMap<IndexBlock, IndexBlock> getMap() {
    return map;
  }
    
  private long getSequenceId() {
    return sequenceID.get();
  }
  

  /**
   * Put index block into map
   * @param b index block
   */
  private void putBlock(IndexBlock b) {
    while(true) {
       try {
         map.put(b, b);
         return;
       } catch (RetryOperationException e) {
         continue;
       }
    }
  }
  
  /**
   * Execute generic read - modify - write operation in a single update
   * If Update is in place or no updates - set Operation.setReadOnlyOrUpdateInPlace
   * @param op - update operation
   * @return true if operation succeeds, false otherwise
   */
  @SuppressWarnings("deprecation")  
  public boolean execute(Operation op) {
    long version = getSequenceId();
    IndexBlock kvBlock = getThreadLocalBlock();
    op.setVersion(version);
    kvBlock.putForSearch(op.getKeyAddress(), op.getKeySize(), version);
    IndexBlock b = null;
    boolean lowerKey = false;
    boolean readOnly = op.isReadOnlyOrUpdateInPlace();
    int seqNumber;
    while (true) {
      try {
        b = lowerKey == false? map.floorKey(kvBlock): map.lowerKey(b);
        if (b == null) {
          // TODO
          return false;
        }
        boolean firstBlock = b.isFirstIndexBlock();
        if (readOnly) {
          readLock(b);
        } else {
          writeLock(b);
        }
        if (!b.isValid()) {
          continue;
        }
        seqNumber = b.getSeqNumberSplitOrMerge();
        // TODO: optimize - last time split? what is the safest threshold? 100ms
        if(b.hasRecentUnsafeModification()) {
          IndexBlock bbb = lowerKey == false? map.floorKey(kvBlock): map.lowerKey(b);
          if (b != bbb) {
            continue;
          } else {
            int sn = b.getSeqNumberSplitOrMerge();
            if (sn != seqNumber) {
              seqNumber = sn;
              continue;
            }
          }
        } 
        
        // When map is empty, recordAddress is always 0
        // This call ONLY decompresses data
        long recordAddress = lowerKey == false? 
            b.get(op.getKeyAddress(), op.getKeySize(), version, op.isFloorKey()):
              b.lastRecordAddress();
        if (recordAddress < 0 && op.isFloorKey() && lowerKey == false && !firstBlock) {
          lowerKey = true;
          b.compressLastUsedDataBlock();
          continue;
        }
        op.setFoundRecordAddress(recordAddress);
        // Execute operation
        boolean result = op.execute();
        int updatesCount = op.getUpdatesCount();

        if (result == false || updatesCount == 0) {
          b.compressLastUsedDataBlock();
          return result;
        }         
        // updates count > 0
        // execute first update (update to existing key)
        long keyPtr = op.keys()[0];
        int keyLength     = op.keySizes()[0];
        long valuePtr = op.values()[0];
        int valueLength = op.valueSizes()[0];
        boolean updateType = op.updateTypes()[0];
        boolean reuseValue = op.reuseValues()[0];
        
        checkKeyBuffer(keyBuffer1, keyBufferSize1, keyLength);
        UnsafeAccess.copy(keyPtr, keyBuffer1.get(), keyLength);
        keyPtr = keyBuffer1.get();
        
        if (updatesCount > 1) {
          checkKeyBuffer(keyBuffer2, keyBufferSize2, op.keySizes()[1]);
          UnsafeAccess.copy(op.keys()[1], keyBuffer2.get(), op.keySizes()[1]);
        }
        // Compress again to preserve compressed data ptr
        b.compressLastUsedDataBlock();

        if (updateType == true) { // DELETE
          // This call compress data block in b
          OpResult res = b.delete(keyPtr, keyLength, version);
          if (res == OpResult.SPLIT_REQUIRED){
            // delete can fail if the key is the first one in the block
            // the next key is larger and index block does not have enough space
            boolean r = delete(keyPtr,keyLength);
            // MUST ALWAYS BE true
            assert(r);
          } else if (res != OpResult.OK){
            System.err.println("PANIC! Unexpected result of delete operation: " + res);
            Thread.dumpStack();
            System.exit(-1);
          }
          if (b.isEmpty() && !firstBlock) {
            map.remove(b);
            b.free();
            b.invalidate();
          }
          return true;
        } else { // PUT
          // This call compress data block in b
          result = b.put(keyPtr, keyLength, valuePtr, valueLength, version, op.getExpire(), reuseValue);
          if (!result && getTotalAllocatedMemory() < maxMemory) {
            result = put(keyPtr, keyLength, valuePtr, valueLength,  op.getExpire(), reuseValue);
          } else if (!result) {
            // MAP is FULL
            return false;
          }
          // block into
          if (updatesCount < 2) {
            return result;
          }
        }
        // updateCounts == 2 - second is insert new K-V
        // second key is larger than first one and if first was inserted into new split block
        // so the second one goes into it as well.
        keyPtr = keyBuffer2.get();
        keyLength     = op.keySizes()[1];
        valuePtr = op.values()[1];
        valueLength = op.valueSizes()[1];
        updateType = op.updateTypes()[1];
        reuseValue = op.reuseValues()[1];
        return put(keyPtr, keyLength, valuePtr, valueLength, op.getExpire(), reuseValue);
      } catch (RetryOperationException e) {
        continue;
      } finally {
        if (b != null) {
          if (readOnly) {
            readUnlock(b);
          } else {
            writeUnlock(b);
          }
        }
      }
    }
  }
  
  /**
   * 
   * @param key
   * @param off
   * @param len
   * @param value
   * @param valoff
   * @param vallen
   * @param expire
   * @return
   */
  public boolean put(byte[] key, int off, int len, byte[] value, int valoff, int vallen, long expire) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, off, len);
    long valuePtr = UnsafeAccess.allocAndCopy(value, valoff, vallen);
    int keySize = len;
    int valueSize = vallen;
    boolean result = put(keyPtr, keySize, valuePtr, valueSize, expire);
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(valuePtr);
    return result;
  }
  
  /**
   * Put key-value operation
   * @param keyPtr key address
   * @param keyLength key length
   * @param valuePtr value address
   * @param valueLength value length
   * @param expire expiration time
   * @return true, if success, false otherwise
   */ 
  public boolean put(long keyPtr, int keyLength, long valuePtr, int valueLength, 
      long expire) {
    return put(keyPtr, keyLength, valuePtr, valueLength, expire, false);
  }
  
  /**
   * Put key-value operation
   * @param keyPtr key address
   * @param keyLength key length
   * @param valuePtr value address
   * @param valueLength value length
   * @param expire expiration time
   * @param reuseValue reuse value allocation if possible, otherwise free it
   * @return true, if success, false otherwise
   */
  public boolean put(long keyPtr, int keyLength, long valuePtr, int valueLength, 
      long expire, boolean reuseValue) {

    long version = getSequenceId();
    IndexBlock kvBlock = getThreadLocalBlock();
    kvBlock.putForSearch(keyPtr, keyLength, version);

    while (true) {
      IndexBlock b = null;
      int seqNumber ;
      try {
        b = map.floorKey(kvBlock);
        // TODO: we do a lot of locking
        writeLock(b);
        seqNumber = b.getSeqNumberSplitOrMerge();
        // TODO: optimize - last time split? what is the safest threshold? 100ms
        if(b.hasRecentUnsafeModification()) {
          IndexBlock bbb = map.floorKey(kvBlock);
          if (b != bbb) {
            continue;
          } else {
            int sn = b.getSeqNumberSplitOrMerge();
            if (sn != seqNumber) {
              seqNumber = sn;
              continue;
            }
          }
        }
        boolean result =
            b.put(keyPtr, keyLength, valuePtr, valueLength, version, expire, reuseValue);
        if (!result && getTotalAllocatedMemory() < maxMemory) {
          IndexBlock bb = null;
          bb = b.split();
          // block into
          putBlock(bb);
          continue;
        } else if (!result) {
          // MAP is FULL
          return false;
        } else {
          return true;
        }
      } catch (RetryOperationException e) {
        continue;
      } finally {
        if (b != null) {
          writeUnlock(b);
        }
      }
    }
  }
  
  
  /**
   * To keep list of empty blocks
   */
  static ThreadLocal<List<IndexBlock>> emptyBlocks = new ThreadLocal<List<IndexBlock>>() {
    @Override
    protected List<IndexBlock> initialValue() {
      return new ArrayList<IndexBlock>();
    }
  };
  
  /**
   * 
   * TODO: How to safely delete empty index blocks?
   * 
   * TODO: TEST delete empty index blocks
   * Delete key range operation
   * @param startKeyPtr key address
   * @param startKeyLength key length
   * @return number of delted keys
   */
  public long deleteRange(long startKeyPtr, int startKeyLength, 
      long endKeyPtr, int endKeyLength) {
    IndexBlock kvBlock = getThreadLocalBlock();
    long version = getSequenceId();
    long deleted = 0;
    kvBlock.putForSearch(startKeyPtr, startKeyLength, version);
    IndexBlock b = null, prev = null;
    boolean firstBlock = true;
    int seqNumber;
    
    while (true) {
      try {
        long loopStartTime = System.currentTimeMillis();
        // We need it to avoid releasing write lock from a wrong block
        b = null;
        b = firstBlock? map.floorKey(kvBlock): map.higherKey(prev);
        if (b == null) {
          break;
        }    
        writeLock(b);
        
        if(!b.isValid()) {
          continue;
        }
        seqNumber = b.getSeqNumberSplitOrMerge();
 
        if (b.hasRecentUnsafeModification(loopStartTime)) {
          //TODO: what happens after index block split?
          IndexBlock bbb = firstBlock? map.floorKey(kvBlock): map.higherKey(prev);
          if (b != bbb) {
            continue;
          } else {
            int sn = b.getSeqNumberSplitOrMerge();
            if (sn != seqNumber) {
              seqNumber = sn;
              continue;
            }
          }
        }
        prev = b;        
        if (b.isEmpty()) {
          //TODO: fix this code
          continue;
        }      
        long del = b.deleteRange(startKeyPtr, startKeyLength, endKeyPtr, endKeyLength, version);
        if (b.isEmpty() && !b.isFirstIndexBlock()) {
          // TODO: what race conditions are possible? 
          // Do we need to lock index block?
          map.remove(b);
          b.invalidate();
        }
        if (del == 0 && !firstBlock) {
          break;
        } else if (del == 0){
          firstBlock = false;
          continue;
        }
        firstBlock = false;
        deleted += del; 
        // and continue loop
      } catch (RetryOperationException e) {
        continue;
      } finally {
        if (b != null) {
          writeUnlock(b);
        }
      }
    }     
    return deleted;
  }
  
  /**
   * Delete key operation
   * @param keyPtr key address
   * @param keyLength key length
   * @return true if success, false otherwise
   */
  public boolean delete(long keyPtr, int keyLength) {
    IndexBlock kvBlock = getThreadLocalBlock();
    long version = getSequenceId();
    kvBlock.putForSearch(keyPtr, keyLength, version);
    while (true) {
      IndexBlock b = null;
      int seqNumber;
      try {
        b = map.floorKey(kvBlock);
        writeLock(b);
        if (!b.isValid()) {
          continue;
        }
        seqNumber = b.getSeqNumberSplitOrMerge();
        if (b.hasRecentUnsafeModification()) {
          IndexBlock bbb = map.floorKey(kvBlock);
          if (b != bbb) {
            continue;
          } else {
            int sn = b.getSeqNumberSplitOrMerge();
            if (seqNumber != sn) {
              seqNumber = sn;
              continue;
            }
          }
        }
        // Now we get the correct index block - perform the operation
        OpResult result = b.delete(keyPtr, keyLength, version);
        if (result == OpResult.OK) {
          if (b.isEmpty()) {
            map.remove(b);
            b.free();
            b.invalidate();
          }
          return true;
        } else if (result == OpResult.NOT_FOUND) {
          return false;
        }
        // split is required
        IndexBlock bb = b.split();
        putBlock(bb);
        // and continue loop
      } catch (RetryOperationException e) {
        continue;
      } finally {
        if (b != null) {
          writeUnlock(b);
        }
      }
    }
  }
  
  
  /**
   * Get value by key 
   * @param keyPtr address to look for
   * @param keyLength key length
   * @param valueBuf value buffer address
   * @param valueBufLength value buffer length
   * @param version version
   * @return value length if found, or NOT_FOUND. if value length >  valueBufLength
   *          no copy will be made - one must repeat call with new value buffer
   */
  public long get(long keyPtr, int keyLength, long valueBuf, int valueBufLength, long version) {
    IndexBlock kvBlock = getThreadLocalBlock();
    kvBlock.putForSearch(keyPtr, keyLength, version);

    boolean locked = false;
    IndexBlock b = null;
    while (true) {
      try {
        b = map.floorKey(kvBlock);
        long result = b.get(keyPtr, keyLength, valueBuf, valueBufLength, version);
        if (result < 0 && b.hasRecentUnsafeModification()) {
          // check one more time with lock
          // - we caught split in flight
          IndexBlock bb = null;
          while (true) {
            b = map.floorKey(kvBlock);
            readLock(b);
            locked = true;
            bb = map.floorKey(kvBlock);
            if (bb != b) {
              readUnlock(b);
              locked = false;
              continue;
            } else {
              break;
            }
          }
          result =  b.get(keyPtr, keyLength, valueBuf, valueBufLength, version);
        }
        return result;
      } catch (RetryOperationException e) {
        continue;
      } finally {
        if (locked && b != null) {
          readUnlock(b);
        }
      }
    }
  }
  
  /**
   * Returns the greatest key, which is less or equals to a given key
   * @param keyPtr key
   * @param keyLength key length
   * @param buf key buffer
   * @param bufLength key buffer length
   * @return size of a key, -1 - does not exists
   */
  public long floorKey(long keyPtr, int keyLength, long buf, int bufLength) {
    IndexBlock kvBlock = getThreadLocalBlock();
    kvBlock.putForSearch(keyPtr, keyLength, 0);
    boolean locked = false;
    IndexBlock b = null;
    while (true) {
      try {
        b = map.floorKey(kvBlock);
        //TODO: b == null? possible?
        long result = b.floorKey(keyPtr, keyLength, buf, bufLength);
        if (result < 0 && b.hasRecentUnsafeModification()) {
          // check one more time with lock
          // - we caught split in flight
          IndexBlock bb = null;
          while (true) {
            b = map.floorKey(kvBlock);
            readLock(b);
            locked = true;
            bb = map.floorKey(kvBlock);
            if (bb != b) {
              readUnlock(b);
              locked = false;
              continue;
            } else {
              break;
            }
          }
          result =  b.floorKey(keyPtr, keyLength, buf, bufLength);
        }
        return result;
      } catch (RetryOperationException e) {
        continue;
      } finally {
        if (locked && b != null) {
          readUnlock(b);
        }
      }
    }
  }
  
  /**
   * TODO: test
   * TODO: update in place for compressed data
   * Increment value (Long)  by key 
   * @param keyPtr address to look for
   * @param keyLength key length
   * @return value after increment. 
   */
  public long incrementLong(long keyPtr, int keyLength, long version, long incr) {
    IndexBlock kvBlock = getThreadLocalBlock();
    kvBlock.putForSearch(keyPtr, keyLength, version);
    IndexBlock b = null;
    int seqNumber;
    while (true) {
      try {
        b = map.floorKey(kvBlock);
        writeLock(b);
        seqNumber = b.getSeqNumberSplitOrMerge();
        if (b.hasRecentUnsafeModification()) {
          IndexBlock bbb = map.floorKey(kvBlock);
          if (b != bbb) {
            continue;
          } else {
            int sn = b.getSeqNumberSplitOrMerge();
            if (seqNumber != sn) {
              seqNumber = sn;
              continue;
            }
          }
        }
        
        long valueBuf = incrBuffer.get();
        int valueSize = Utils.SIZEOF_LONG;
        long size = b.get(keyPtr, keyLength, valueBuf, valueSize,  version);
        
        if (size < 0) {
          // insert new
          UnsafeAccess.putLong(valueBuf, incr);
          put(keyPtr, keyLength, valueBuf, Utils.SIZEOF_LONG, 0);
          return incr;
        } else if (size != Utils.SIZEOF_LONG) {
          // TODO
          return Long.MIN_VALUE;
        }
        long value = UnsafeAccess.toLong(valueBuf);
        UnsafeAccess.putLong(valueBuf, value + incr);
        if (!b.put(keyPtr, keyLength, valueBuf, Utils.SIZEOF_LONG, 0, -1)) {
          put(keyPtr, keyLength, valueBuf, Utils.SIZEOF_LONG, 0);
        }
        return value + incr;
      } catch (RetryOperationException e) {
        continue;
      } finally {
        if (b != null) {
          writeUnlock(b);
        }
      }
    }
  }
  
  
  private static ThreadLocal<Key> key = new ThreadLocal<Key>() {
    @Override
    protected Key initialValue() {
      return new Key(0,0);
    }
  };
  
  private static Key getKey(long ptr, int size) {
    Key k = key.get();
    k.address = ptr;
    k.length = size;
    return k;
  }
  
  private static ThreadLocal<IncrementLong> incrLong = new ThreadLocal<IncrementLong>() {

    @Override
    protected IncrementLong initialValue() {
      return new IncrementLong();
    }
  };
  
  /**
   * Optimized for speed and multi-threading
   * @param keyPtr key address
   * @param keyLength key length
   * @param incr increment value
   * @return value after increment
   */
  public long incrementLongOp(long keyPtr, int keyLength, long incr) 
    throws OperationFailedException
  {
    Key k = getKey(keyPtr, keyLength);
    
    KeysLocker.writeLock(k);
    try {
      IncrementLong op = incrLong.get();
      op.reset();
      op.setKeyAddress(keyPtr);
      op.setKeySize(keyLength);
      op.setIncrement(incr);
      boolean result = execute(op);
      if (result) {
        return op.getValue();
      } else {
        throw new OperationFailedException();
      }
    } finally {
      KeysLocker.writeUnlock(k);
    }
  }
  
  private static ThreadLocal<IncrementInt> incrInt = new ThreadLocal<IncrementInt>() {
    @Override
    protected IncrementInt initialValue() {
      return new IncrementInt();
    }
  };
  /**
   * Optimized for speed and multi-threading
   * @param keyPtr key address
   * @param keyLength key length
   * @param incr increment value
   * @return value after increment
   */
  public int incrementIntOp(long keyPtr, int keyLength, int incr) 
      throws OperationFailedException
  {
    
    Key k = getKey(keyPtr, keyLength);
    KeysLocker.writeLock(k);
    try {
      IncrementInt op = incrInt.get();
      op.reset();
      op.setKeyAddress(keyPtr);
      op.setKeySize(keyLength);
      op.setIncrement(incr);
      boolean result = execute(op);
      if (result) {
        return op.getValue();
      } else {
        throw new OperationFailedException();
      }
    } finally {
      KeysLocker.writeUnlock(k);
    }
  }
  
  
  private static ThreadLocal<IncrementDouble> incrDouble = new ThreadLocal<IncrementDouble>() {
    @Override
    protected IncrementDouble initialValue() {
      return new IncrementDouble();
    }
  };
  /**
   * Optimized for speed and multi-threading
   * @param keyPtr key address
   * @param keyLength key length
   * @param incr increment value
   * @return value after increment
   */
  public double incrementDoubleOp(long keyPtr, int keyLength, double incr)
      throws OperationFailedException
  {
    
    Key k = getKey(keyPtr, keyLength);
    KeysLocker.writeLock(k);
    
    try {
      IncrementDouble op = incrDouble.get();
      op.reset();
      op.setKeyAddress(keyPtr);
      op.setKeySize(keyLength);
      op.setIncrement(incr);
      boolean result = execute(op);
      if (result) {
        return op.getValue();
      } else {
        throw new OperationFailedException();
      }
    } finally {
      KeysLocker.writeUnlock(k);
    }
  }
  
  private static ThreadLocal<IncrementFloat> incrFloat = new ThreadLocal<IncrementFloat>() {
    @Override
    protected IncrementFloat initialValue() {
      return new IncrementFloat();
    }
  };
  /**
   * Optimized for speed and multi-threading
   * @param keyPtr key address
   * @param keyLength key length
   * @param incr increment value
   * @return value after increment
   */
  public float incrementFloatOp(long keyPtr, int keyLength, float incr) 
    throws OperationFailedException
  {
    
    Key k = getKey(keyPtr, keyLength);
    KeysLocker.writeLock(k);
    try {
      IncrementFloat op = incrFloat.get();
      op.reset();
      op.setKeyAddress(keyPtr);
      op.setKeySize(keyLength);
      op.setIncrement(incr);
      boolean result = execute(op);
      if (result) {
        return op.getValue();
      } else {
        throw new OperationFailedException();
      }
    } finally {
      KeysLocker.writeUnlock(k);
    }
  }
  /**
   * TODO: test
   * TODO: update in place for compressed data
   * Increment value (Integer)  by key 
   * @param keyPtr address to look for
   * @param keyLength key length
   * @return value after increment. 
   */
  public int incrementInt(long keyPtr, int keyLength, long version, int incr) {
    IndexBlock kvBlock = getThreadLocalBlock();
    kvBlock.putForSearch(keyPtr, keyLength, version);
    IndexBlock b = null;
    int seqNumber;
    while (true) {
      try {
        b = map.floorKey(kvBlock);
        writeLock(b);
        seqNumber = b.getSeqNumberSplitOrMerge();
        if (b.hasRecentUnsafeModification()) {
          IndexBlock bbb = map.floorKey(kvBlock);
          if (b != bbb) {
            continue;
          } else {
            int sn = b.getSeqNumberSplitOrMerge();
            if (seqNumber != sn) {
              seqNumber = sn;
              continue;
            }
          }
        }
        long valueBuf = incrBuffer.get();
        int valueSize = Utils.SIZEOF_INT;
        long size = b.get(keyPtr, keyLength, valueBuf, valueSize,  version);        
        if (size < 0) {
          // insert new
          UnsafeAccess.putInt(valueBuf, incr);
          put(keyPtr, keyLength, valueBuf, Utils.SIZEOF_INT, 0);
          return incr;
        } else if (size != Utils.SIZEOF_INT) {
          // TODO
          return Integer.MIN_VALUE;
        }
        int value = UnsafeAccess.toInt(valueBuf);
        UnsafeAccess.putInt(valueBuf, value + incr);
        put(keyPtr, keyLength, valueBuf, Utils.SIZEOF_INT, 0);
        return value + incr;
      } catch (RetryOperationException e) {
        continue;
      } finally {
        if (b != null) {
          writeUnlock(b);
        }
      }
    }
  }
  
  /**
   * TODO: test
   * Increment value (Float)  by key 
   * @param keyPtr address to look for
   * @param keyLength key length
   * @return value after increment. 
   */
  public float incrementFloat(long keyPtr, int keyLength, long version, float incr) {
    IndexBlock kvBlock = getThreadLocalBlock();
    kvBlock.putForSearch(keyPtr, keyLength, version);
    IndexBlock b = null;
    int seqNumber;
    while (true) {
      try {
        b = map.floorKey(kvBlock);
        writeLock(b);
        seqNumber = b.getSeqNumberSplitOrMerge();
        if (b.hasRecentUnsafeModification()) {
          IndexBlock bbb = map.floorKey(kvBlock);
          if (b != bbb) {
            continue;
          } else {
            int sn = b.getSeqNumberSplitOrMerge();
            if (seqNumber != sn) {
              seqNumber = sn;
              continue;
            }
          }
        }
        long valueBuf = incrBuffer.get();
        int valueSize = Utils.SIZEOF_FLOAT;
        long size = b.get(keyPtr, keyLength, valueBuf, valueSize,  version);
        if (size < 0) {
          // insert new
          UnsafeAccess.putLong(valueBuf, Float.floatToIntBits(incr));
          put(keyPtr, keyLength, valueBuf, Utils.SIZEOF_FLOAT, 0);
          return incr;
        } else if (size != Utils.SIZEOF_FLOAT) {
          //TODO
          return Float.MIN_VALUE;
        }
        int value = UnsafeAccess.toInt(valueBuf);
        float val = Float.intBitsToFloat(value);
        UnsafeAccess.putInt(valueBuf, Float.floatToIntBits(val + incr));
        if (!b.put(keyPtr, keyLength, valueBuf, Utils.SIZEOF_FLOAT, 0, -1)){
          put(keyPtr, keyLength, valueBuf, Utils.SIZEOF_FLOAT, 0);
        }
        return val + incr;
      } catch (RetryOperationException e) {
        continue;
      } finally {
        if (b != null) {
          writeUnlock(b);
        }
      }
    }
  }
  
  /**
   * TODO: test
   * Increment value (Double)  by key 
   * @param keyPtr address to look for
   * @param keyLength key length
   * @return value after increment. 
   */
  public double incrementDouble(long keyPtr, int keyLength, long version, double incr) {
    IndexBlock kvBlock = getThreadLocalBlock();
    kvBlock.putForSearch(keyPtr, keyLength, version);
    IndexBlock b = null;
    int seqNumber;
    while (true) {
      try {
        b = map.floorKey(kvBlock);
        writeLock(b);
        seqNumber = b.getSeqNumberSplitOrMerge();
        if (b.hasRecentUnsafeModification()) {
          IndexBlock bbb = map.floorKey(kvBlock);
          if (b != bbb) {
            continue;
          } else {
            int sn = b.getSeqNumberSplitOrMerge();
            if (seqNumber != sn) {
              seqNumber = sn;
              continue;
            }
          }
        }
        long valueBuf = incrBuffer.get();
        int valueSize = Utils.SIZEOF_LONG;
        long size = b.get(keyPtr, keyLength, valueBuf, valueSize,  version);
        
        if (size < 0) {
          // insert new
          UnsafeAccess.putLong(valueBuf, Double.doubleToLongBits(incr));
          put(keyPtr, keyLength, valueBuf, Utils.SIZEOF_DOUBLE, 0);
          return incr;
        } else if (size != Utils.SIZEOF_DOUBLE) {
          //TODO
          return -Double.MAX_VALUE;
        }
        long value = UnsafeAccess.toLong(valueBuf);
        double val = Double.longBitsToDouble(value);
        UnsafeAccess.putLong(valueBuf, Double.doubleToLongBits(val + incr));
        // negative expire is ignored
        if (! b.put(keyPtr, keyLength, valueBuf, Utils.SIZEOF_DOUBLE, 0, -1)) {
          put(keyPtr, keyLength, valueBuf, Utils.SIZEOF_DOUBLE, 0);
        }
        return val + incr;
      } catch (RetryOperationException e) {
        continue;
      } finally {
        if (b != null) {
          writeUnlock(b);
        }
      }
    }
  }
  
  
  /**
   * Checks if key exists in a map
   * @param key key address
   * @param len key length
   * @return true if exists, false - false otherwise
   */
  public  boolean exists(long key, int len) {
    return get(key, len,  key, 0, Long.MAX_VALUE) > 0;

  }
  
  /**
   * TODO: this can have race conditions 
   * Get first key in a map
   * @return first key
   * @throws IOException 
   */
  public byte[] getFirstKey() throws IOException {
    IndexBlockScanner scanner = null;
    try {
      while (true) {
        try {
          IndexBlock b = null;
          while (true) {
            // TODO: race conditions?
            b = b == null ? map.firstKey() : map.higherKey(b);
            if (b == null) return null;
            scanner = IndexBlockScanner.getScanner(b, 0, 0, 0, 0, Long.MAX_VALUE);
            DataBlockScanner sc = null;
            while ((sc = scanner.nextBlockScanner()) != null) {
              if (!sc.hasNext()) {                
                continue;
              }
              int keySize = sc.keySize();
              byte[] key = new byte[keySize];
              sc.key(key, 0);
              return key;
            }
            scanner.close();
          }
        } catch (RetryOperationException e) {
          continue;
        }
      }
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }
  }

  
  /**
   * Get scanner (single instance per thread)
   * @param startRowPtr start row address
   * @param startRowLength start row length
   * @param stopRowPtr stop row address
   * @param stopRowLength stop row length
   * @return scanner
   */
  public BigSortedMapScanner getScanner(long startRowPtr, int startRowLength, 
      long stopRowPtr, int stopRowLength) {
    long snapshotId = getSequenceId();
    while(true) {
      try {
        return new BigSortedMapScanner(this, startRowPtr, startRowLength, 
          stopRowPtr, stopRowLength, snapshotId); 
      } catch (RetryOperationException e) {
        continue;
      } catch (IOException | IllegalArgumentException e) {
        // Legitimate exception
        return null;
      } 
    }
  }
  
  /**
   * Get scanner (single instance per thread)
   * @param startRowPtr start row address
   * @param startRowLength start row length
   * @param stopRowPtr stop row address
   * @param stopRowLength stop row length
   * @param reverse is reverse scanner
   * @return scanner
   */
  public BigSortedMapScanner getScanner(long startRowPtr, int startRowLength, 
      long stopRowPtr, int stopRowLength, boolean reverse) {
    long snapshotId = getSequenceId();
    while(true) {
      try {
        return new BigSortedMapScanner(this, startRowPtr, startRowLength, 
          stopRowPtr, stopRowLength, snapshotId, false, reverse); 
      } catch (RetryOperationException e) {
        continue;
      } catch (IOException | IllegalArgumentException ee) {
        return null;
      } 
    }
  }
  
  /**
   * Get safe scanner (multiple instances per thread)
   * @param startRowPtr start row address
   * @param startRowLength start row length
   * @param stopRowPtr stop row address
   * @param stopRowLength stop row length
   * @return scanner
   */
  public BigSortedMapScanner getSafeScanner(long startRowPtr, int startRowLength, 
      long stopRowPtr, int stopRowLength) {
    long snapshotId = getSequenceId();
    while(true) {
      try {
        return new BigSortedMapScanner(this, startRowPtr, startRowLength, 
          stopRowPtr, stopRowLength, snapshotId, true, false); 
      } catch (RetryOperationException e) {
        continue;
      } catch (IOException | IllegalArgumentException ee) {
        return null;
      }
    }
  }
  
  /**
   * Get safe scanner (multiple instances per thread)
   * @param startRowPtr start row address
   * @param startRowLength start row length
   * @param stopRowPtr stop row address
   * @param stopRowLength stop row length
   * @param reverse is reverse scanner
   * @return scanner
   */
  public BigSortedMapScanner getSafeScanner(long startRowPtr, int startRowLength, 
      long stopRowPtr, int stopRowLength, boolean reverse) {
    long snapshotId = getSequenceId();
    while(true) {
      try {
        return new BigSortedMapScanner(this, startRowPtr, startRowLength, 
          stopRowPtr, stopRowLength, snapshotId, true, reverse); 
      } catch (RetryOperationException e) {
        continue;
      } catch (IOException | IllegalArgumentException ee) {
        return null;
      }
    }
  }
  
  /**
   * Get prefix scanner (single instance per thread)
   * @param startRowPtr start row address
   * @param startRowLength stop row address
   * @return scanner
   */
  
  public BigSortedMapScanner getPrefixScanner(long startRowPtr, int startRowLength) {
    long endRowPtr = Utils.prefixKeyEnd(startRowPtr, startRowLength);
    if (endRowPtr == -1) {
      endRowPtr = 0;
    }
    int endRowLength = endRowPtr == 0? 0: startRowLength;
    BigSortedMapScanner scanner =
        getScanner(startRowPtr, startRowLength, endRowPtr, endRowLength);
    if (scanner == null && endRowPtr > 0) {
      UnsafeAccess.free(endRowPtr);
      return null;
    }
    scanner.setPrefixScanner(true);
    return scanner;
  }
  
  /**
   * Get prefix scanner (single instance per thread)
   * @param startRowPtr start row address
   * @param startRowLength stop row address
   * @param reverse is reverse scanner
   * @return scanner
   */
  
  public BigSortedMapScanner getPrefixScanner(long startRowPtr, int startRowLength, boolean reverse) {
    long endRowPtr = Utils.prefixKeyEnd(startRowPtr, startRowLength);
    if (endRowPtr == -1) {
      endRowPtr = 0;
    }
    int endRowLength = endRowPtr == 0? 0: startRowLength;

    BigSortedMapScanner scanner =
        getScanner(startRowPtr, startRowLength, endRowPtr, endRowLength, reverse);
    //TODO: is this right?
    if (scanner == null && endRowPtr > 0) {
      UnsafeAccess.free(endRowPtr);
      return null;
    }
    scanner.setPrefixScanner(true);
    return scanner;
  }
  
  
  /**
   * Get safe prefix scanner (multiple instances per thread)
   * @param startRowPtr start row address
   * @param startRowLength stop row address
   * @return scanner
   */
  
  public BigSortedMapScanner getSafePrefixScanner(long startRowPtr, int startRowLength) {
    long endRowPtr = Utils.prefixKeyEnd(startRowPtr, startRowLength);
    if (endRowPtr == -1) {
      return null;
    }
    
    return getSafeScanner(startRowPtr, startRowLength, endRowPtr, startRowLength);
  }
  
  /**
   * Get safe prefix scanner (multiple instances per thread)
   * @param startRowPtr start row address
   * @param startRowLength stop row address
   * @param reverse is reverse scanner
   * @return scanner
   */
  
  public BigSortedMapScanner getSafePrefixScanner(long startRowPtr, int startRowLength, 
      boolean reverse) {
    long endRowPtr = Utils.prefixKeyEnd(startRowPtr, startRowLength);
    if (endRowPtr == -1) {
      return null;
    }
    
    return getSafeScanner(startRowPtr, startRowLength, endRowPtr, startRowLength, reverse);
  }
  /**
   * Disposes map, deallocate all the memory
   */
  public void dispose() {
    for(IndexBlock b: map.keySet()) {
    	b.free();
    }
    map.clear();
    totalAllocatedMemory.set(0);
    totalBlockDataSize.set(0);
    totalBlockIndexSize.set(0);
    totalExternalDataSize.set(0);
    totalIndexSize.set(0);
    totalDataInDataBlocksSize.set(0);
    totalDataInIndexBlocksSize.set(0);
  }
}
