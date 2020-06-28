package org.bigbase.carrot;

import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bigbase.carrot.updates.Update;

public class BigSortedMap {
	
  Log LOG = LogFactory.getLog(BigSortedMap.class);
  private static ThreadLocal<IndexBlock> keyBlock = new ThreadLocal<IndexBlock>();  
  public final static String MAX_MEMORY_KEY = "map.max.memory";
  private ConcurrentSkipListMap<IndexBlock, IndexBlock> map = 
		  new ConcurrentSkipListMap<IndexBlock, IndexBlock>();
  
  static int maxBlockSize = DataBlock.MAX_BLOCK_SIZE;
  static int maxIndexBlockSize = IndexBlock.MAX_BLOCK_SIZE;
  static AtomicLong totalAllocatedMemory = new AtomicLong(0);
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
   * This tracks total data size  (total data size >= total data in block size)
   */
  static AtomicLong totalDataSize = new AtomicLong(0);
  
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

  private ReentrantLock[] locks = new ReentrantLock[11113];
  private long maxMemory; 
  /*
   * Keeps ordered list of active snapshots (future Tx)
   */
  static private ConcurrentSkipListMap<Long, Long> activeSnapshots = new ConcurrentSkipListMap<>();
  
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
  
  public static void releaseSnapshot(long id) {
    activeSnapshots.remove(id);
  }
  
  public static int getMaxBlockSize() {
	  return maxBlockSize;
  }
  
  public static void setMaxBlockSize(int size) {
	  maxBlockSize = size;
  }
  
  public static int getMaxIndexBlockSize() {
	  return maxIndexBlockSize;
  }
    
  public BigSortedMap(long maxMemory) {
    this.maxMemory = maxMemory;
    initNodes();
  }
  
  public int getBlockSize() {
    return maxBlockSize;
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
   * Get total data size
   * @return size
   */
  public static long getTotalDataSize() {
    return totalDataSize.get();
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
  
  public long getMaxMemory() {
    return maxMemory;
  }
  
  public void dumpStats() {
    long totalRows = 0;
    for(IndexBlock b: map.keySet()) {
      totalRows += b.getNumberOfDataBlock();
    }
    System.out.println("Total blocks="+ (totalRows) + " index blocks=" + map.size());
  }
  
  private void initNodes() {
    IndexBlock b = new IndexBlock(maxIndexBlockSize);
    b.setFirstIndexBlock();
    map.put(b, b);
    for(int i =0; i < locks.length; i++) {
      locks[i] = new ReentrantLock();
    }
  }
  
  /**
   * TODO: do we need this locking?
   * @param b
   */
  private void lock(IndexBlock b) {
    int i = (int) (b.hashCode() % locks.length);
    ReentrantLock lock = locks[i];
    lock.lock();
  }
  
  private void unlock(IndexBlock b) {
    if (b == null) return;
    int i = (int) (b.hashCode() % locks.length);
    ReentrantLock lock = locks[i];
    if (lock.isHeldByCurrentThread()) {
      lock.unlock();
    } else {
    	System.out.println("Unexpected unlock attempt");
    	//TODO
    	Thread.dumpStack();
    	System.exit(-1);
    }
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
  
  private final IndexBlock getThreadLocalBlock () {
    IndexBlock kvBlock = keyBlock.get();
    if (kvBlock == null) {
      ensureBlock();
      kvBlock = keyBlock.get();
    }
    kvBlock.reset();
    return kvBlock;
  }
  
  ConcurrentSkipListMap<IndexBlock, IndexBlock> getMap() {
    return map;
  }
  
  private void verifyArgs(byte[] buf, int off, int len) 
  	throws NullPointerException, IllegalArgumentException
  {
	  if (buf == null) {
		  throw new NullPointerException("buffer is null");
	  }
	  if (off < 0 || off >= buf.length) {
		  throw new IllegalArgumentException("illegal offset: "+ off);
	  }
	  if (len <=0 || len > buf.length - off) {
		  throw new IllegalArgumentException("illegal length: "+ len);

	  }
  }
  
  private long getSequenceId() {
    return sequenceID.get();
  }
  
  private long incrementSequenceId() {
    return sequenceID.incrementAndGet();
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
   * @throws RetryOperationException 
   */
  public boolean put(byte[] key, int keyOffset, int keyLength, byte[] value, int valueOffset,
      int valueLength, long expire) {

    verifyArgs(key, keyOffset, keyLength);
    verifyArgs(value, valueOffset, valueLength);
    long version = getSequenceId();
    IndexBlock kvBlock = getThreadLocalBlock();
    kvBlock.putForSearch(key, keyOffset, keyLength, version);

    while (true) {
      IndexBlock b = null;
      try {
        b = map.floorKey(kvBlock);
        // TODO: we do a lot of locking
        lock(b); // to prevent locking from another thread
        // TODO: optimize - last time split? what is the safest threshold? 100ms
        if(b.hasRecentUnsafeModification()) {
          IndexBlock bbb = map.floorKey(kvBlock);
          if (b != bbb) {
            continue;
          }
        }
        boolean result =
            b.put(key, keyOffset, keyLength, value, valueOffset, valueLength, version, expire);
        if (!result && getTotalAllocatedMemory() < maxMemory) {
          // In sequential pattern of puts, we do not need to split
          // but need to add new block with a given K-V
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
        unlock(b);
      }
    }

  }
  
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
   * @param op - update operation
   * @return true or false
   */
  public boolean update(Update op) {
    long version = getSequenceId();
    IndexBlock kvBlock = getThreadLocalBlock();
    op.setVersion(version);
    kvBlock.putForSearch(op.getKeyAddress(), op.getKeySize(), version);
    while (true) {
      IndexBlock b = null;
      try {
        b = map.floorKey(kvBlock);
        lock(b); // to prevent locking from another thread
        // TODO: optimize - last time split? what is the safest threshold? 100ms
        if(b.hasRecentUnsafeModification()) {
          IndexBlock bbb = map.floorKey(kvBlock);
          if (b != bbb) {
            continue;
          }
        }        
        long recordAddress = b.get(op.getKeyAddress(), op.getKeySize(), version);
        if (recordAddress <=0) {
          return false;
        }
        op.setFoundRecordAddress(recordAddress);
        // Execute operation
        boolean result = op.execute();
        if (result == false ) {
          return result;
        } 
        int updatesCount = op.getUpdatesCount();
        if (updatesCount == 0) {
          // Update in place was done, so we can return now
          return result;
        }
        // updates count > 0
        // execute first update (update to existing key)
        long keyPtr = op.keys()[0];
        int keyLength     = op.keySizes()[0];
        long valuePtr = op.values()[0];
        int valueLength = op.valueSizes()[0];
        
        result = b.put(keyPtr, keyLength, valuePtr, valueLength, version, op.getExpire());
        IndexBlock bb = null;
        boolean isBB = false;
        if (!result && getTotalAllocatedMemory() < maxMemory) {
          bb = b.split();          
          if (!bb.isLessThanMin(keyPtr, keyLength, version)) {
            // Insert into new block
            result = bb.put(keyPtr, keyLength, valuePtr, valueLength, version, op.getExpire());
            // This should succeed?
            if (!result) {
              // TODO: We failed to insert into non-full index block
              return false;
            }
            isBB = true;
          } else {
            // try again into b
            result = b.put(keyPtr, keyLength, valuePtr, valueLength, version, op.getExpire());
            if (!result) {
              // TODO: We failed to insert into non-full index block
              return false;
            }
          }
        } else if (!result) {
          // MAP is FULL
          return false;
        }
        // block into
        if (updatesCount < 2) {
          if(bb != null) {
            putBlock(bb);
          }
          return true;
        }
        // updateCounts == 2 - second is insert new K-V
        // second key is larger than first one and if first was inserted into new split block
        // so the second one goes into it as well.
        IndexBlock block = isBB? bb: b;
        keyPtr = op.keys()[1];
        keyLength     = op.keySizes()[1];
        valuePtr = op.values()[1];
        valueLength = op.valueSizes()[1];
        
        result = block.put(keyPtr, keyLength, valuePtr, valueLength, version, op.getExpire());
        if (!result) {
          // We do not check allocated memory limit b/c it can break
          // the transaction
          IndexBlock ibb = block.split();          
          if (!ibb.isLessThanMin(keyPtr, keyLength, version)) {
            // Insert into new block
            result = ibb.put(keyPtr, keyLength, valuePtr, valueLength, version, op.getExpire());
            // This should succeed?
            if (!result) {
              // TODO: We failed to insert into non-full index block
              return false;
            }
          } else {
            // try again into block
            result = block.put(keyPtr, keyLength, valuePtr, valueLength, version, op.getExpire());
            if (!result) {
              // TODO: We failed to insert into non-full index block
              return false;
            }
          }
          putBlock(ibb);
        }
        // we put either ibb or bb - not both of them - mens
        // we do not do two index block splits in a single update operation
        if (block == bb) {
          putBlock(bb);
        }
        return true;
      } catch (RetryOperationException e) {
        continue;
      } finally {
        unlock(b);
      }
    }
  }

  /**
   * Put k-v operation
   * @param keyPtr
   * @param keyLength
   * @param valuePtr
   * @param valueLength
   * @return true, if success, false otherwise
   */
  public boolean put(long keyPtr, int keyLength, long valuePtr, int valueLength, long expire) {

    long version = getSequenceId();
    IndexBlock kvBlock = getThreadLocalBlock();
    kvBlock.putForSearch(keyPtr, keyLength, version);

    while (true) {
      IndexBlock b = null;
      try {
        b = map.floorKey(kvBlock);
        // TODO: we do a lot of locking
        lock(b); // to prevent locking from another thread
        // TODO: optimize - last time split? what is the safest threshold? 100ms
        if(b.hasRecentUnsafeModification()) {
          IndexBlock bbb = map.floorKey(kvBlock);
          if (b != bbb) {
            continue;
          }
        }
        boolean result =
            b.put(keyPtr, keyLength, valuePtr, valueLength, version, expire);
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
        unlock(b);
      }
    }
  }
  
  /**
   * Delete operation
   * TODO: compact on deletion
   * TODO: REmove IndxeNode if empty
   * TODO: check if IndexNode is valid after obtaining lock
   * 
   * @param key
   * @param keyOffset
   * @param keyLength
   * @return true, if success, false otherwise
   */
  
  public boolean delete(byte[] key, int keyOffset, int keyLength) {

    verifyArgs(key, keyOffset, keyLength);
    IndexBlock kvBlock = getThreadLocalBlock();
    long version = getSequenceId();
    kvBlock.putForSearch(key, keyOffset, keyLength, version);
    while (true) {
      IndexBlock b = null;
      try {
        b = map.floorKey(kvBlock);
        lock(b); // to prevent
        if (b.hasRecentUnsafeModification()) {
          IndexBlock bbb = map.floorKey(kvBlock);
          if (b != bbb) {
            continue;
          }
        }
        OpResult result = b.delete(key, keyOffset, keyLength, version);
        if (result == OpResult.OK) {
          if (b.isEmpty()) {
            IndexBlock removed = map.remove(b);
            if (removed == null) {
              IndexBlock fk = map.floorKey(b);
              IndexBlock ck = map.ceilingKey(b);
              boolean contains = map.containsKey(b);
              /* DEBUG */ System.out.println(
                "FATAL Removed IndexBlock " + removed + " firstKey=" + b.getFirstKey().length);
              System.out.println(
                "b=" + b.getAddress() + "ck=" + ck + "fk=" + fk + "contains =" + contains);
              System.exit(-1);
            }
            b.free();
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
        unlock(b);
      }
    }
  }
  
  /**
   * Delete operation
   * TODO: compact on deletion 
   * @param keyPtr
   * @param keyLength
   * @return true if success, false otherwise
   */
  public boolean delete(long keyPtr, int keyLength) {
    IndexBlock kvBlock = getThreadLocalBlock();
    long version = getSequenceId();
    kvBlock.putForSearch(keyPtr, keyLength, version);
    while (true) {
      IndexBlock b = null;
      try {
        b = map.floorKey(kvBlock);
        lock(b); // to prevent
        if (b.hasRecentUnsafeModification()) {
          IndexBlock bbb = map.floorKey(kvBlock);
          if (b != bbb) {
            continue;
          }
        }
        OpResult result = b.delete(keyPtr, keyLength, version);
        if (result == OpResult.OK) {
          if (b.isEmpty()) {
            IndexBlock removed = map.remove(b);
            if (removed == null) {
              IndexBlock fk = map.floorKey(b);
              IndexBlock ck = map.ceilingKey(b);
              boolean contains = map.containsKey(b);
              /* DEBUG */ System.out.println(
                "FATAL Removed IndexBlock " + removed + " firstKey=" + b.getFirstKey().length);
              System.out.println(
                "b=" + b.getAddress() + "ck=" + ck + "fk=" + fk + "contains =" + contains);
              System.exit(-1);
            }
            b.free();
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
        unlock(b);
      }
    }
  }
  
  /**
   * Get value by key in a block
   * @param key
   * @param keyOffset
   * @param keyLength
   * @return value length or NOT_FOUND if not found
   *         caller MUST verify that valueBuf.length > value length + valOffset   
   */
  public long get(byte[] key, int keyOffset, int keyLength, byte[] valueBuf, int valOffset,
      long version) {

    IndexBlock kvBlock = getThreadLocalBlock();
    kvBlock.putForSearch(key, keyOffset, keyLength, version);

    boolean locked = false;
    IndexBlock b = null;
    while (true) {
      try {
        b = map.floorKey(kvBlock);
        long result = b.get(key, keyOffset, keyLength, valueBuf, valOffset, version);
        if (result < 0 && b.hasRecentUnsafeModification()) {
          // check one more time with lock
          // - we caught split in flight
          IndexBlock bb = null;
          while (true) {
            b = map.floorKey(kvBlock);
            lock(b);
            locked = true;
            bb = map.floorKey(kvBlock);
            if (bb != b) {
              unlock(b);
              locked = false;
              continue;
            } else {
              break;
            }
          }
          result = b.get(key, keyOffset, keyLength, valueBuf, valOffset, version);
        }
        // TODO
        // check length
        return result;
      } catch (RetryOperationException e) {
        continue;
      } finally {
        if (locked) {
          unlock(b);
        }
      }
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
            lock(b);
            locked = true;
            bb = map.floorKey(kvBlock);
            if (bb != b) {
              unlock(b);
              locked = false;
              continue;
            } else {
              break;
            }
          }
          result =  b.get(keyPtr, keyLength, valueBuf, valueBufLength, version);
        }
        // TODO
        // check length
        return result;
      } catch (RetryOperationException e) {
        continue;
      } finally {
        if (locked) {
          unlock(b);
        }
      }
    }
  }
  
  /**
   * Exists
   * @param key
   * @param offset
   * @param len
   * @return true, false
   */
  public boolean exists(byte[] key, int offset, int len) {
    return get(key, offset, len, key, key.length -1, Long.MAX_VALUE) > 0;
  }
  
  /**
   * Exists API
   * @param key
   * @param len
   * @return true, false
   */
  public  boolean exists(long key, int len) {
    return get(key, len,  key, 0, Long.MAX_VALUE) > 0;

  }
  /**
   * Get first key
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
            scanner = IndexBlockScanner.getScanner(b, null, null, Long.MAX_VALUE);
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
   *  Get scanner
   *  @param start start row (inclusive)
   *  @param stop stop row (exclusive)
   */
  public BigSortedMapScanner getScanner(byte[] start, byte[] stop) {
    long snapshotId = getSequenceId();
    while(true) {
      try {
        return new BigSortedMapScanner(this, start, stop, snapshotId);
      }catch (RetryOperationException e) {
        continue;
      }
    }
  }
  
  /**
   * Get scanner (direct memory)
   */
  public BigSortedMapDirectMemoryScanner getScanner(long startRowPtr, int startRowLength, 
      long stopRowPtr, int stopRowLength) {
    long snapshotId = getSequenceId();
    while(true) {
      try {
        return new BigSortedMapDirectMemoryScanner(this, startRowPtr, startRowLength, 
          stopRowPtr, stopRowLength, snapshotId); 
      } catch (RetryOperationException e) {
        continue;
      }
    }
  }
  
  public void dispose() {
    for(IndexBlock b: map.keySet()) {
    	b.free();
    }
    map.clear();
    totalAllocatedMemory.set(0);
    totalBlockDataSize.set(0);
    totalBlockIndexSize.set(0);
    totalDataSize.set(0);
    totalIndexSize.set(0);
    totalDataInDataBlocksSize.set(0);
    totalDataInIndexBlocksSize.set(0);
  }
}
