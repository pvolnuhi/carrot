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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Date;
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
import org.bigbase.carrot.redis.RedisConf;
import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.IOUtils;
import org.bigbase.carrot.util.Key;
import org.bigbase.carrot.util.KeysLocker;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class BigSortedMap {


  /**************** STATIC SECTION *******************************/
  
  
  static Log LOG = LogFactory.getLog(BigSortedMap.class);
  
  /**
   * Compression codec
   */
  static Codec codec = CodecFactory.getInstance().getCodec(CodecType.NONE);
  
  /**
   * Is compression enabled
   * @return true, if - yes, false otherwise
   */
  
  public static boolean isCompressionEnabled() {
    return codec != null && codec.getType() != CodecType.NONE;
  }
  
  /**
   * Sets compression codec
   * @param codec compression codec
   */
  public static void setCompressionCodec(Codec codec) {
    BigSortedMap.codec = codec;
  }
  
  /**
   * Get compression codec
   * @return compression codec
   */
  public static Codec getCompressionCodec() {
    return codec;
  }
  
  /*
   * Thread local storage for index blocks used as a key in a 
   * Map<IndexBlock,IndexBlock> operations
   */
  private static ThreadLocal<IndexBlock> keyBlock = new ThreadLocal<IndexBlock>(); 
    
  
  /**
   * Maximum data block size - default is 4096, should support at least 8K and 16K
   */
  static int maxBlockSize = DataBlock.MAX_BLOCK_SIZE;
  
  /**
   * Maximum index block size - default is 4096, should support at least 8K and 16K
   */
  
  static int maxIndexBlockSize = IndexBlock.MAX_BLOCK_SIZE;
  
  /**
   * This tracks globally allocated memory (overall)
   */
  private static AtomicLong globalAllocatedMemory = new AtomicLong(0);
  
  /*
   * This tracks global data blocks size (memory allocated for data blocks)
   */
  private static AtomicLong globalBlockDataSize = new AtomicLong(0);
  
  /*
   * This tracks global data size in data blocks (data can be allocated outside
   * data blocks)
   */
  private static AtomicLong globalDataInDataBlocksSize = new AtomicLong(0);

  /*
   * This tracks global compressed data size in data blocks (data can be allocated outside
   * data blocks)
   */
  
  private static AtomicLong globalCompressedDataInDataBlocksSize = new AtomicLong(0);
  
  /*
   * This tracks global data size  allocated externally (not in data blocks)
   */
  private static AtomicLong globalExternalDataSize = new AtomicLong(0);
  
  /*
   * This tracks global index blocks size (memory allocated for index blocks) 
   */
  private static AtomicLong globalBlockIndexSize = new AtomicLong(0);
  
  /*
   * This tracks global data size in index blocks (data can be allocated outside
   * index blocks)
   */
  private static AtomicLong globalDataInIndexBlocksSize = new AtomicLong(0);
  
  /*
   * This tracks global index size (total index size >= total data in index blocks  size)
   */
  private static AtomicLong globalIndexSize = new AtomicLong(0);
  
  /**
   *  For system logging versioning
   */
  private static AtomicLong sequenceID = new AtomicLong(0);

  /**
   * Global memory limit (Long.MAX_VALUE - no limit is default)
   */
  private static long globalMemoryLimit = Long.MAX_VALUE;
  
  
  /**
   * When true - no updates to a global and instance statistics is disabled
   * This is necessary during application startup when data is loading 
   * from a snapshot
   */
  private static boolean statsUpdateDisabled = false;
  
    
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
  
  /**
   * Set global memory limit
   * @param limit memory limit
   */
  public static void setGlobalMemoryLimit(long limit) {
    globalMemoryLimit = limit;
  }
  
  /**
   * Get global memory limit
   */
  public static long getGlobalMemoryLimit() {
    return globalMemoryLimit;
  }
  
  /**
   * Checks that key buffer is not less than 'require'
   * @param key key
   * @param size key size
   * @param required required size
   */
  static void checkKeyBuffer(ThreadLocal<Long> key, ThreadLocal<Integer> size, int required) {
    if (required <= size.get()) return;
    long ptr = UnsafeAccess.realloc(key.get(), required);
    key.set(ptr);
    size.set(required);
  }
  
  /**
   * TODO: remove this code
   * To support Tx and snapshots
   * @return earliest active Tx/snapshot sequenceId or -1
   */
  public static long getMostRecentActiveTxSeqId() {
	  return -1;
  }
  
  /**
   * TODO: remove this code
   * To support Tx and snapshot based scanners
   * @return oldest active tx/snapshot id
   */
  public static long getMostOldestActiveTxSeqId() {
    return -1;
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
   * Get global allocated memory
   * @return total allocated memory
   */
  public static long getGlobalAllocatedMemory() {
    return globalAllocatedMemory.get();
  }
  
  /**
   * Increment global allocated memory
   * @return global allocated memory after increment
   */
  public static long incrGlobalAllocatedMemory(long incr) {
    if (isStatsUpdatesDisabled()) return 0;
    return globalAllocatedMemory.addAndGet(incr);
  }
  
  /**
   * Get global memory allocated for data blocks
   * @return global memory allocated for data blocks
   */
  public static long getGlobalBlockDataSize() {
    return globalBlockDataSize.get();
  }
  
  /**
   * Increment global block data size - memory allocated for data blocks
   * @return global block data size after increment
   */
  public static long incrGlobalBlockDataSize(long incr) {
    if (isStatsUpdatesDisabled()) return 0;
    return globalBlockDataSize.addAndGet(incr);
  }
  
  /**
   * Get global memory which data occupies in data blocks
   * @return memory
   */
  public static long getGlobalDataInDataBlockSize() {
    return globalDataInDataBlocksSize.get();
  }
  
  /**
   * Increment global data in data block size 
   * @return global data in data block size after increment
   */
  public static long incrGlobalDataInDataBlockSize(long incr) {
    if (isStatsUpdatesDisabled()) return 0;
    return globalDataInDataBlocksSize.addAndGet(incr);
  }
  
  /**
   * Get global memory allocated for index blocks
   * @return memory
   */
  public static long getGlobalBlockIndexSize() {
    return globalBlockIndexSize.get();
  }
  
  /**
   * Increment global memory allocated for index blocks
   * @param incr increment value
   * @return memory after increment
   */
  public static long incrGlobalBlockIndexSize(long incr) {
    // We need updates to index during data loading
    // b/c index size will be lesser
    //if (isStatsUpdatesDisabled()) return 0;
    return globalBlockIndexSize.addAndGet(incr);
  }
  
  /**
   * Get global data size (uncompressed)
   * @return global data size
   */
  public static long getGlobalDataSize() {
    return globalExternalDataSize.get() + globalDataInDataBlocksSize.get();
  }
  
  /**
   * Get global compressed data size
   * @return global size of a compressed data
   */
  public static long getGlobalCompressedDataSize() {
    return  globalCompressedDataInDataBlocksSize.get();
  }
  
  /**
   * Increment global compressed data size
   * @param incr increment value
   * @return global size of a compressed data after an increment
   */
  public static long incrGlobalCompressedDataSize(long incr) {
    if (isStatsUpdatesDisabled()) return 0;
    return  globalCompressedDataInDataBlocksSize.addAndGet(incr);
  }
  
  /**
   * Get global external data size
   * @return global size of a compressed data
   */
  public static long getGlobalExternalDataSize() {
    return  globalExternalDataSize.get();
  }
  
  /**
   * Increment global external data size. This is external data + 
   * external index data
   * @param incr increment value
   * @return global size of a compressed data after an increment
   */
  public static long incrGlobalExternalDataSize(long incr) {
    if (isStatsUpdatesDisabled()) return 0;
    return  globalExternalDataSize.addAndGet(incr);
  }
  
  /**
   * Get global index size, which is globalBlockIndexSize + external index allocations (for large keys)
   * @return global index size
   */
  public static long getGlobalIndexSize() {
    return globalIndexSize.get();
  }
  
  /**
   * Increment global index size
   * @param incr increment value
   * @return global index size after an increment
   */
  public static long incrGlobalIndexSize(long incr) {
    //if (isStatsUpdatesDisabled()) return 0;
    return globalIndexSize.addAndGet(incr);
  }
  
  /**
   * Get global memory which index occupies in index blocks
   * @return global index size
   */
  public static long getGlobalDataInIndexBlocksSize() {
    return globalDataInIndexBlocksSize.get();
  }
  
  /**
   * Increment global memory which index occupies in index blocks
   * @param incr increment value
   * @return global index size after an increment
   */
  
  public static long incrGlobalDataInIndexBlocksSize(long incr) {
    //if (isStatsUpdatesDisabled()) return 0;
    return globalDataInIndexBlocksSize.addAndGet(incr);
  }
  
  /**
   * Prints global memory statistics
   */
  public static void printGlobalMemoryAllocationStats() {
    System.out.println("\nGLOBAL Carrot memory allocation statistics:");
    System.out.println("Total memory               :" + getGlobalAllocatedMemory());
    System.out.println("Total data blocks          :" + getGlobalBlockDataSize());
    System.out.println("Total data size            :" + getGlobalDataSize());
    System.out.println("Total data block usage     :" + ((double)getGlobalDataSize())
      /getGlobalBlockDataSize());
    System.out.println("Total block index size     :" + getGlobalBlockIndexSize());
    System.out.println("Total data index size      :" + getGlobalDataInIndexBlocksSize());
    System.out.println("Total index size           :" + getGlobalIndexSize());
    System.out.println("Total compressed data size :" + getGlobalCompressedDataSize());
    System.out.println("Total external data size   :" + getGlobalExternalDataSize());   
    System.out.println("Copmpression ratio         :" + ((double)getGlobalDataSize()/ 
        getGlobalAllocatedMemory())+"\n");
  }
  
  /**
   * Disable/ enable memory statistics updates
   * @param b
   */
  public static void setStatsUpdatesDisabled(boolean b) {
    statsUpdateDisabled = b;
    if (statsUpdateDisabled) {
      // Clear all counters;
      resetStats();
    }
  }
  
  /**
   * Reset all counters
   */
  public static void resetStats() {
    // Clear all counters;
    globalAllocatedMemory.set(0);
    globalBlockDataSize.set(0);
    globalBlockIndexSize.set(0);
    globalCompressedDataInDataBlocksSize.set(0);
    globalDataInDataBlocksSize.set(0);
    globalDataInIndexBlocksSize.set(0);
    globalExternalDataSize.set(0);
    globalIndexSize.set(0);
  }
  /**
   * Checks if stats updates disabled
   * @return true if - yes, false - otherwise
   */
  public static boolean isStatsUpdatesDisabled() {
    return statsUpdateDisabled;
  }
  
  /*****************************************************************/

  /**************** INSTANCE SECTION *******************************/
    
  /**
   * Major store data structure
   */
  private ConcurrentSkipListMap<IndexBlock, IndexBlock> map = 
      new ConcurrentSkipListMap<IndexBlock, IndexBlock>();
  /*
   * Read-Write Lock TODO: StampedLock (Java 8), decrease # of locks
   * too high
   */
  ReentrantReadWriteLock[] locks = new ReentrantReadWriteLock[11113];
  
  /*
   * Read-Write Lock for index blocks. TODO: StampedLock (Java 8)
   */
  ReentrantReadWriteLock[] indexLocks = new ReentrantReadWriteLock[11113];
  
  /**
   * This tracks instance allocated memory 
   */
  AtomicLong allocatedMemory = new AtomicLong(0);
  
  /*
   * This tracks instance data blocks size (memory allocated for data blocks)
   */
  AtomicLong blockDataSize = new AtomicLong(0);
  
  /*
   * This tracks instance data size in data blocks (data can be allocated outside
   * data blocks)
   */
  AtomicLong dataInDataBlocksSize = new AtomicLong(0);

  /*
   * This tracks instance compressed data size in data blocks (data can be allocated outside
   * data blocks)
   */
  
  AtomicLong compressedDataInDataBlocksSize = new AtomicLong(0);
  
  /*
   * This tracks instance data size allocated externally (not in data blocks)
   */
  AtomicLong externalDataSize = new AtomicLong(0);
  
  /*
   * This tracks instance index blocks size (memory allocated for index blocks) 
   */
  AtomicLong blockIndexSize = new AtomicLong(0);
  
  /*
   * This tracks instance data size in index blocks (data can be allocated outside
   * index blocks)
   */
  AtomicLong dataInIndexBlocksSize = new AtomicLong(0);
  
  /*
   * This tracks instance index size (total index size >= total data in index blocks  size)
   */
  AtomicLong indexSize = new AtomicLong(0);
  
  /**
   * Last snapshot time in ms
   */
  long lastSnapshotTimestamp;
  
  /**
   * Snapshot directory
   */
  String snapshotDir;
  
  /**
   * Little hack
   */
  private long indexBlockSizeBeforeSnapshot;
  
  /**
   * One more
   */
  private long dataBlockSizeBeforeSnapshot;
  /**
   * Legacy constructor of a big sorted map (single instance)
   * @param maxMemory - memory limit in bytes
   */
  public BigSortedMap(long maxMemory) {
    this(maxMemory, true);
  }
  
  /**
   * Legacy constructor of a big sorted map (single instance)
   * @param maxMemory - memory limit in bytes
   * @param init - initialize
   */
  public BigSortedMap(long maxMemory, boolean init) {
    this(init);
    setGlobalMemoryLimit(maxMemory);
  }
  
  /**
   * Constructor of a big sorted map (multiple instances)
   * @param init
   */
  public BigSortedMap(boolean init) {
    initLocks();
    if (init) initNodes();
  }
  
  /**
   * Constructor of a big sorted map
   */
  public BigSortedMap() {
    this(true);
  }
  
  /**
   * Initialize locks
   */
  private void initLocks() {
    for (int i = 0; i < locks.length; i++) {
      locks[i] = new ReentrantReadWriteLock();
      indexLocks[i] = new ReentrantReadWriteLock();
    }
  }
  
  /**
   * Get index locks
   * @return index lock array
   */
  ReentrantReadWriteLock[] getIndexLocks() {
    return this.indexLocks;
  }
  
  /**
   * Part of snapshot loading procedure
   */
  public void syncStatsToGlobal() {
    
    incrGlobalAllocatedMemory(getInstanceAllocatedMemory());
    incrGlobalBlockDataSize(getInstanceBlockDataSize());
    incrGlobalCompressedDataSize(getInstanceCompressedDataSize());
    incrGlobalDataInDataBlockSize(getInstanceDataInDataBlockSize());
    incrGlobalExternalDataSize(getInstanceExternalDataSize());
  }
  
  /**
   * Returns last snapshot time-stamp
   * @return last snapshot time-stamp
   */
  public long getLastSnapshotTimestamp() {
    return this.lastSnapshotTimestamp;
  }
  
  /**
   * Sets last snapshot time-stamp
   * @param timestamp
   */
  public void setLastSnapshotTimestamp(long timestamp) {
    this.lastSnapshotTimestamp = timestamp;
  }
    
  /**
   * Get snapshot directory for this store
   * @return directory
   */
  public String getSnapshotDir() {
    return this.snapshotDir;
  }
  
  /**
   * Set snapshot directory for the store
   * @param dir snapshot directory path
   */
  public void setSnapshotDir(String dir) {
    this.snapshotDir = dir;
  }
  
  /**
   * Prints memory allocation statistics for the store
   */
  public  void printMemoryAllocationStats() {
    System.out.println("\nCarrot memory allocation statistics [id=" + Thread.currentThread().getName() +"]:");
    System.out.println("Total allocated memory     :" + getInstanceAllocatedMemory());
    System.out.println("Total data block size      :" + getInstanceBlockDataSize());
    System.out.println("Total data size            :" + getInstanceDataSize());
    System.out.println("Total data block usage     :" + ((double)getInstanceDataSize())
      /getInstanceBlockDataSize());
    System.out.println("Total block index size     :" + getInstanceBlockIndexSize());
    System.out.println("Total data index size      :" + getInstanceDataInIndexBlocksSize());
    System.out.println("Total index size           :" + getInstanceIndexSize());
    System.out.println("Total compressed data size :" + getInstanceCompressedDataSize());
    System.out.println("Total external data size   :" + getInstanceExternalDataSize());   
    System.out.println("Copmpression ratio         :" + ((double)getInstanceDataSize()/ 
        getInstanceAllocatedMemory())+"\n");
  }
  
  /**
   * Get instance allocated memory
   * @return instance allocated memory
   */
  public long getInstanceAllocatedMemory() {
    return allocatedMemory.get();
  }
  
  /**
   * Increment instance allocated memory
   * @return allocated memory after increment
   */
  public long incrInstanceAllocatedMemory(long incr) {
    if (isStatsUpdatesDisabled()) return 0;
    // Increment global 
    incrGlobalAllocatedMemory(incr);
    // Increment instance
    return allocatedMemory.addAndGet(incr);
  }
  
  /**
   * Get instance memory allocated for data blocks
   * @return  memory allocated for data blocks
   */
  public long getInstanceBlockDataSize() {
    return blockDataSize.get();
  }
  
  /**
   * Increment instance block data size - memory allocated for data blocks
   * @return  block data size after increment
   */
  public long incrInstanceBlockDataSize(long incr) {
    //if (isStatsUpdatesDisabled()) return 0;
    // Increment global
    incrGlobalBlockDataSize(incr);
    // Increment instance
    return blockDataSize.addAndGet(incr);
  }
  
  /**
   * Get instance memory which data occupies in data blocks
   * @return memory
   */
  public long getInstanceDataInDataBlockSize() {
    return dataInDataBlocksSize.get();
  }
  
  /**
   * Increment instance data in data block size 
   * @return data size in data block size after increment
   */
  public long incrInstanceDataInDataBlockSize(long incr) {
    if (isStatsUpdatesDisabled()) return 0;
    // Increment global
    incrGlobalDataInDataBlockSize(incr);
    return dataInDataBlocksSize.addAndGet(incr);
  }
  
  /**
   * Get instance memory allocated for index blocks
   * @return memory
   */
  public long getInstanceBlockIndexSize() {
    return blockIndexSize.get();
  }
  
  /**
   * Increment instance memory allocated for index blocks
   * @param incr increment value
   * @return memory after increment
   */
  public long incrInstanceBlockIndexSize(long incr) {
    //if (isStatsUpdatesDisabled()) return 0;
    // Increment global
    incrGlobalBlockIndexSize(incr);
    // Increment instance
    return blockIndexSize.addAndGet(incr);
  }
  
  /**
   * Get instance data size (uncompressed)
   * @return  data size
   */
  public long getInstanceDataSize() {
    return externalDataSize.get() + dataInDataBlocksSize.get();
  }
  
  /**
   * Get instance compressed data size
   * @return  size of a compressed data
   */
  public long getInstanceCompressedDataSize() {
    return  compressedDataInDataBlocksSize.get();
  }
  
  /**
   * Increment instance compressed data size
   * @param incr increment value
   * @return size of a compressed data after an increment
   */
  public long incrInstanceCompressedDataSize(long incr) {
    if (isStatsUpdatesDisabled()) return 0;
    // Increment global
    incrGlobalCompressedDataSize(incr);
    // Increment instance
    return  compressedDataInDataBlocksSize.addAndGet(incr);
  }
  
  /**
   * Get instance external data size
   * @return  size of a compressed data
   */
  public long getInstanceExternalDataSize() {
    return  externalDataSize.get();
  }
  
  /**
   * Increment instance external data size
   * @param incr increment value
   * @return size of a external data after an increment
   */
  public long incrInstanceExternalDataSize(long incr) {
    if (isStatsUpdatesDisabled()) return 0;
    // Increment global
    incrGlobalExternalDataSize(incr);
    // Increment instance
    return  externalDataSize.addAndGet(incr);
  }
  
  
  /**
   * Get instance index size
   * @return global index size
   */
  public long getInstanceIndexSize() {
    return indexSize.get();
  }
  
  /**
   * Increment instance index size
   * @param incr increment value
   * @return index size after an increment
   */
  public long incrInstanceIndexSize(long incr) {
    //if (isStatsUpdatesDisabled()) return 0;
    // Increment global
    incrGlobalIndexSize(incr);
    // Increment instance
    return indexSize.addAndGet(incr);
  }
  
  /**
   * Get  memory which index occupies in index blocks
   * @return  real index size
   */
  public long getInstanceDataInIndexBlocksSize() {
    return dataInIndexBlocksSize.get();
  }
  
  /**
   * Increment instance memory which index occupies in index blocks
   * @param incr increment value
   * @return index size after an increment
   */
  
  public long incrInstanceDataInIndexBlocksSize(long incr) {
    //if (isStatsUpdatesDisabled()) return 0;
    // Increment global
    incrGlobalDataInIndexBlocksSize(incr);
    // Increment instance
    return dataInIndexBlocksSize.addAndGet(incr);
  }
  
  private void adjustCountersAfterLoad() {
    long loaded = blockIndexSize.get();
    long addj = indexBlockSizeBeforeSnapshot - loaded;
    allocatedMemory.addAndGet(-addj);
    loaded = blockDataSize.get();
    
    addj = dataBlockSizeBeforeSnapshot - loaded;
    allocatedMemory.addAndGet(-addj);
  }
  
  /**
   * For testing ONLY
   * @return dumps records in a HEX form and returns number of records
   */
  public long dumpRecords() {
    BigSortedMapScanner scanner = getScanner(0, 0, 0, 0);
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
  
  /**
   * Counts number of K-V records in a store
   * @return number of records
   */
  public long countRecords() {
    BigSortedMapScanner scanner = getScanner(0, 0, 0, 0);
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
    
  /**
   * Dumps some useful stats
   */
  public void dumpStats() {
    long totalRows = 0;
    for(IndexBlock b: map.keySet()) {
      totalRows += b.getNumberOfDataBlock();
      b.dumpIndexBlockExt();
    }
    System.out.println("Total blocks="+ (totalRows) + " index blocks=" + map.size());
  }
  
  private void initNodes() {
    IndexBlock b = new IndexBlock(this, maxIndexBlockSize);
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
          IndexBlock b = new IndexBlock(this, maxIndexBlockSize);
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
    boolean readOnly = op.isReadOnly();
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
        boolean updateInPlace = op.isUpdateInPlace();
        boolean compressionEnabled = isCompressionEnabled();
        int updatesCount = op.getUpdatesCount();

        if (result == false || (updatesCount == 0 && !compressionEnabled)) {
          b.compressLastUsedDataBlock();
          return result;
        }
        
        if (updateInPlace && compressionEnabled) {
          b.compressLastUsedDataBlockForced();
          return result;
        } else if (updatesCount == 0) {
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
          if (!result && getGlobalAllocatedMemory() < getGlobalMemoryLimit()) {
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
   * Put key-value (for testing only)
   * @param key key byte array
   * @param off key offset in a byte array
   * @param len key's length
   * @param value value byte array
   * @param valoff value offset
   * @param vallen value length
   * @param expire expire
   * @return true - success, false - otherwise (OOM)
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
        if (!result && getGlobalAllocatedMemory() < getGlobalMemoryLimit()) {
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
    int endRowLength = startRowLength;
    if (endRowPtr == 0) {
      endRowLength = 0;
    }
    
    return getSafeScanner(startRowPtr, startRowLength, endRowPtr, endRowLength);
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
    int endRowLength = startRowLength;
    if (endRowPtr == 0) {
      endRowLength = 0;;
    }
    
    return getSafeScanner(startRowPtr, startRowLength, endRowPtr, endRowLength, reverse);
  }
  /**
   * Disposes map, deallocate all the memory
   */
  public void dispose() {
    synchronized(map) {
      for(IndexBlock b: map.keySet()) {
        b.free();
      }
      map.clear();      
    }
  }
  
  public void flushAll() {
    long start = System.currentTimeMillis();
    dispose();
    initNodes();
    long end = System.currentTimeMillis();
    System.out.println("["+ Thread.currentThread().getName()+"] flushall took:"+ (end - start) + "ms");
  }
  
  public void flush (int db) {
    //TODO: flush DB by DB's id
    flushAll();
  }
  
  /**
   *  Memory compaction API. Compacts both: index and data blocks
   */
  
  public void compact() {
    // main loop over all index blocks
    IndexBlock ib = null, cur = null;
    int version = -1;
   
    while (true) {
      try {
        if (ib != null) {
          version = ib.getSeqNumberSplitOrMerge();
        }
        cur = nextIndexBlock(ib);
        if (cur == null) {
          break;
        } else if (cur.isValid() == false) {
          //TODO: is it safe?
          continue;
        }
        // Lock current index block
        cur.writeLock();
        if (ib != null && ib.hasRecentUnsafeModification()) {
          int v = ib.getSeqNumberSplitOrMerge();
          if (v != version) {
            // We caught IB split in fly
            ib = cur;
            continue;
          }
        }
        // Process index block    
        cur.compact();
        ib = cur;
      } catch (RetryOperationException e) {
        continue;
      } finally {
        if (cur != null) {
          cur.writeUnlock();
        }
      }
    }
  }
  
  /******************************************************************************************************
   * 
   * Persistence API - data store disk snapshot READ-WRITE
   * 
   */
  
  
  private static int BUFFER_SIZE = 256 * 1024;
  
  // WRITE DATA  
  public void snapshot() {
    // Check if dir exists
    // Check if snapshotDir is NULL - during tests
    if (snapshotDir == null) {
      snapshotDir = RedisConf.getInstance().getDataDir(0);
    }
    File dir = new File(snapshotDir);
    if (dir.exists() == false) {
      if (dir.mkdirs() == false) {
        System.err.println("Snapshot failed. Can not create directory: " + dir.getAbsolutePath());
        return;
      }
    }

    File snapshotFile = new File(dir, "snapshot.data_tmp");
    RandomAccessFile raf = null;
    FileChannel fc = null;
    try {
      raf = new RandomAccessFile(snapshotFile, "rw");
      fc = raf.getChannel();
      // Save store meta data
      saveStoreMeta(fc);
    } catch (IOException e) {
      System.err.println(
        "Snapshot failed. Can not create snapshot file: " + snapshotFile.getAbsolutePath());
      e.printStackTrace();
      return;
    }
    
    System.out.println("Snapshot file opened: " + snapshotFile.getAbsolutePath());
     
    // main loop over all index blocks
    IndexBlock ib = null, cur = null;
    int version = -1;
    ByteBuffer buf = ByteBuffer.allocateDirect(BUFFER_SIZE);//bb.get();
    buf.clear();
    boolean locked = false;
    while (true) {
      locked = false;
      try {
        if (ib != null) {
          version = ib.getSeqNumberSplitOrMerge();
        }
        cur = nextIndexBlock(ib);
        if (cur == null) {
          break;
        } else if (cur.isValid() == false) {
          //TODO: is it safe?
          continue;
        }
        // Lock current index block
        cur.readLock();
        locked = true;
        if (ib != null && ib.hasRecentUnsafeModification()) {
          int v = ib.getSeqNumberSplitOrMerge();
          if (v != version) {
            // We caught IB split in fly
            ib = cur;
            continue;
          }
        }
        // Process index block    
        cur.saveData(fc, buf);
        ib = cur;
      } catch (RetryOperationException e) {
        continue;
      } catch (IOException e) {
        System.err.println(
          "Snapshot failed. Can not create snapshot file: " + snapshotFile.getAbsolutePath());
        e.printStackTrace();
        return;
      } finally {
        if (cur != null && locked) {
          cur.readUnlock();
        }
      }
    }

    // Close file
    try {
      // Drain buffer
      IOUtils.drainBuffer(buf, fc);      
      // Save last snapshot time to a snapshot file
      long timestamp = System.currentTimeMillis();
      buf.putLong(timestamp);
      IOUtils.drainBuffer(buf, fc); 
      // Update store's last snapshot time
      setLastSnapshotTimestamp(timestamp);
      raf.close();
    } catch (IOException e) {
      System.err.println("WARNING! " + e.getMessage());
      e.printStackTrace();
      //TODO: what to do?
    }
        
    // Delete old snapshot
    File oldSnapshotFile = new File(dir, "snapshot.data");
    if (oldSnapshotFile.exists()) {
      boolean result = oldSnapshotFile.delete();
      if (!result) {
        System.err.println("ERROR! Can not delete old snapshot file.");
        return;
      }
    }
    boolean result = snapshotFile.renameTo(oldSnapshotFile);
    if (!result) {
      System.err.println("ERROR! Can not rename new snapshot file: "+ snapshotFile.getAbsolutePath() + 
        " to "+ oldSnapshotFile.getAbsolutePath());
    } else {
      System.out.println("Snapshot file created: " + oldSnapshotFile.getAbsolutePath());
    }
  }

  private void saveStoreMeta(FileChannel channel) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(Utils.SIZEOF_LONG * 7);
    // 1. maxMemory we load from configuration file
    buf.putLong(getGlobalMemoryLimit());
    buf.putLong(getInstanceAllocatedMemory());
    buf.putLong(getInstanceBlockDataSize());
    buf.putLong(getInstanceBlockIndexSize());
    buf.putLong(getInstanceCompressedDataSize());
    buf.putLong(getInstanceDataInDataBlockSize());
    buf.putLong(getInstanceExternalDataSize());
    buf.flip();
    while(buf.hasRemaining()) {
      channel.write(buf);
    }
  }

  final IndexBlock nextIndexBlock(IndexBlock ib) {
    return ib == null? map.firstKey(): map.higherKey(ib);
  }
  
  public static BigSortedMap loadStore(String server, int port) {
    RedisConf conf = RedisConf.getInstance();
    String snapshotDir = conf.getDataDirForNode(server, port);
    return loadStoreFromSnapshot(snapshotDir);
  }
  
  public static BigSortedMap loadStore(int storeId) {
    RedisConf conf = RedisConf.getInstance();
    String snapshotDir = conf.getDataDir(storeId);
    return loadStoreFromSnapshot(snapshotDir);
  }
  
  // READ DATA
  private static BigSortedMap loadStoreFromSnapshot(String snapshotDir) {
    BigSortedMap map = null;
    // Check if directory exists
    File dir = new File(snapshotDir);
    if (dir.exists() == false) {
      dir.mkdirs();
      System.err.println("Snapshot directory does not exists: " + dir.getAbsolutePath());
      return new BigSortedMap();
    }

    File snapshotFile = new File(dir, "snapshot.data");
    if (!snapshotFile.exists()) {
      System.err.println("Snapshot file does not exists: " + snapshotFile.getAbsolutePath());
      return new BigSortedMap();
    }
    
    RandomAccessFile raf = null;
    FileChannel fc = null;
    System.out.println(
      "Started loading store data from: " + snapshotFile.getAbsolutePath() + " at " + new Date());

    try {
      raf = new RandomAccessFile(snapshotFile, "r");
      fc = raf.getChannel();
      // Save store meta data
      map = loadStoreMeta(fc);
    } catch (IOException e) {
      System.err.println(
        "Loading store failed. Can not open snapshot file: " + snapshotFile.getAbsolutePath());
      return null;
    }

    ByteBuffer buf = ByteBuffer.allocateDirect(BUFFER_SIZE);//bb.get();
    buf.clear();

    try {
      // Read first chunk from file
      fc.read(buf);
      buf.flip();
      DataBlock block = null;
      boolean first = true;
      do {
        IndexBlock ib = new IndexBlock(map, maxIndexBlockSize);
        if (first) {
          ib.isFirst = true;
          first = false;
        }
        if (block != null) {
          //TODO: insert block into an empty index? Never tested
          ib.insertBlock(block);
          block.compressDataBlockIfNeeded();
        }
        block = ib.loadData(fc, buf);
        if (!ib.isEmpty()) {
          map.map.put(ib, ib);
        }
      } while (block != null);
      System.out
          .println("Loaded store from: " + snapshotFile.getAbsolutePath() + " at " + new Date());
      
      // Last read timestamp, buffer MUST have correct position
      if (buf.remaining() < Utils.SIZEOF_LONG) {
        IOUtils.ensureAvailable(fc, buf, Utils.SIZEOF_LONG);
      }
      long timestamp = buf.getLong();
      map.setLastSnapshotTimestamp(timestamp);
      map.setSnapshotDir(snapshotDir);
      map.adjustCountersAfterLoad();
      map.printMemoryAllocationStats();
      return map;
    } catch (IOException e) {
      System.err.println(
        "Loading store failed. Corrupted (?) snapshot file: " + snapshotFile.getAbsolutePath());
      e.printStackTrace();
    } finally {
      // Close file
      try {
        raf.close();
      } catch (IOException e) {
        System.err.println("WARNING! " + e.getMessage());
        e.printStackTrace();
      }
    }
    return null;
  }

  private static BigSortedMap loadStoreMeta(FileChannel fc) throws IOException {
    BigSortedMap map;
    int toRead = Utils.SIZEOF_LONG * 7;
    ByteBuffer buf = ByteBuffer.allocate(toRead);
    while(buf.remaining() > 0) {
      fc.read(buf);
    }
    buf.flip();
    map = new BigSortedMap(false);    
    long value = buf.getLong();
    // Set global memory limit TODO: this is mostly for testing
    if (BigSortedMap.getGlobalMemoryLimit() == 0) {
      BigSortedMap.setGlobalMemoryLimit(value);
    }
    
    value = buf.getLong();
    map.allocatedMemory.set(value);
    value = buf.getLong();
    //map.blockDataSize.set(value);
    map.dataBlockSizeBeforeSnapshot = value;
    value = buf.getLong();
    map.indexBlockSizeBeforeSnapshot = value;
    
    value = buf.getLong();
    map.compressedDataInDataBlocksSize.set(value);
    value = buf.getLong();
    map.dataInDataBlocksSize.set(value);
    value = buf.getLong();
    map.externalDataSize.set(value);
    
    return map;
  }
}


