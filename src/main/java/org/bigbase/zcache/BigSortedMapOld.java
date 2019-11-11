package org.bigbase.zcache;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.bigbase.carrot.RetryOperationException;

public class BigSortedMapOld {

  private static ThreadLocal<Block> keyBlock = new ThreadLocal<Block>();  
  public final static String MAX_MEMORY_KEY = "map.max.memory";
  private ConcurrentSkipListMap<Block, Block> map = new ConcurrentSkipListMap<Block, Block>();
  
  static int maxBlockSize = Block.DEFAULT_MAX_BLOCK_SIZE;
  
  static AtomicLong totalAllocatedMemory = new AtomicLong(0);
  static AtomicLong totalDataSize = new AtomicLong(0);
  static AtomicLong totalIndexSize = new AtomicLong(0);

  private ReentrantLock[] locks = new ReentrantLock[11113];
  private long maxMemory; 
  
  public static int getMaxBlockSize() {
	  return maxBlockSize;
  }
  
  public static void setMaxBlockSize(int size) {
	  maxBlockSize = size;
  }
  
  public BigSortedMapOld(long maxMemory) {
    this.maxMemory = maxMemory;
    initNodes();
  }
  
  public int getBlockSize() {
    return maxBlockSize;
  }
  
  public static long getMemoryAllocated() {
    return totalAllocatedMemory.get();
  }
  
  public static long getTotalDataSize() {
    return totalDataSize.get();
  }
  
  public static long getTotalIndexSize() {
	  return totalIndexSize.get();
  }
  
  public long getMaxMemory() {
    return maxMemory;
  }
  
  public void dumpStats() {
    long totalRows = 0;
    long deleted = 0;
    for(Block b: map.keySet()) {
      totalRows += b.getNumRecords();
      deleted += b.getNumDeletedRecords();
    }
    System.out.println("Total ="+ (totalRows - deleted) + " deleted=" + deleted);
  }
  
  private void initNodes() {
    Block b = new Block(maxBlockSize);
    byte[] key = new byte[] { (byte) 0};
    b.put(key, 0, key.length, key, 0, key.length);
    map.put(b, b);
    for(int i =0; i < locks.length; i++) {
      locks[i] = new ReentrantLock();
    }
  }
  
  
  private void lock(Block b) {
    int i = (int) (b.hashCode() % locks.length);
    ReentrantLock lock = locks[i];
    lock.lock();
  }
  
  private void unlock(Block b) {
    if (b == null) return;
    int i = (int) (b.hashCode() % locks.length);
    ReentrantLock lock = locks[i];
    if (lock.isHeldByCurrentThread()) {
      lock.unlock();
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
          Block b = new Block(maxBlockSize);
          b.setThreadSafe(true);
          keyBlock.set(b);
        }
      }
    }
  }
  
  
  private final Block getThreadLocalBlock () {
    Block kvBlock = keyBlock.get();
    if (kvBlock == null) {
      ensureBlock();
      kvBlock = keyBlock.get();
    }
    kvBlock.reset();
    return kvBlock;
  }
  
  ConcurrentSkipListMap<Block, Block> getMap() {
    return map;
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
  public  boolean put(byte[] key, int keyOffset, int keyLength, byte[] value, int valueOffset,
      int valueLength)  
  {
    
    Block kvBlock = getThreadLocalBlock() ;
    kvBlock.put(key, keyOffset, keyLength, value, valueOffset, valueLength);
    while(true) {
      Block b = null;
      boolean isSplit = false;
      try {
        b = map.floorKey(kvBlock);
        lock(b); // to prevent
        Block bbb = map.floorKey(kvBlock);
        if( b != bbb) {
          continue;
        }
        boolean result = b.put(key, keyOffset, keyLength, value, valueOffset, valueLength);
        if (!result && getMemoryAllocated() < maxMemory) {
          // In sequential pattern of puts, we do not need to split
          // but need to add new block with a given K-V
          Block bb = null;
          if (b.isLargerThanMax(key, keyOffset, keyLength)) {
            bb = new Block(b.getBlockSize());
            bb.put(key, keyOffset, keyLength, value, valueOffset, valueLength);
          } else {
            bb = b.split(true);
            isSplit = true;
          }
          // some records are missing until we put
          // block into 
          putBlock(bb);
          if (isSplit) {
            continue;
          } else {
            return true;
          }
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
  
  private void putBlock(Block b) {
    while(true) {
       try {
         Block bb = map.put(b, b);
         if(bb != null) {
           throw new RuntimeException("Unexpected put return value");
         }
         return;
       } catch (RetryOperationException e) {
         continue;
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
  public boolean put(long keyPtr, int keyLength, long valuePtr, int valueLength) {
    Block kvBlock = getThreadLocalBlock() ;
    kvBlock.put(keyPtr, keyLength, valuePtr,  valueLength);
    while(true) {
      Block b = null;
      boolean isSplit = false;

      try {
        b = map.floorKey(kvBlock);
        lock(b); // to prevent
        Block bbb = map.floorKey(kvBlock);
        if( b != bbb) {
          continue;
        }        
        boolean result = b.put(keyPtr, keyLength, valuePtr, valueLength);
        if (!result && getMemoryAllocated() < maxMemory) {
          // In sequential pattern of puts, we do not need to split
          // but need to add new block with a given K-V
          Block bb = null;
          if (b.isLargerThanMax(keyPtr, keyLength)) {
            bb = new Block(b.getBlockSize());
            bb.put(keyPtr, keyLength, valuePtr, valueLength);
          } else {
            bb = b.split(true);
            isSplit = true;
          }
          putBlock(bb);
          if (isSplit) {
            continue;
          } else {
            return true;
          }
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
   * @param key
   * @param keyOffset
   * @param keyLength
   * @return true, if success, false otherwise
   */
  
  public boolean delete(byte[] key, int keyOffset, int keyLength) {
    Block kvBlock = getThreadLocalBlock() ;
    kvBlock.put(key, keyOffset, keyLength, key, keyOffset, keyLength);
    while(true) {
      Block b = null;
      try {
        b = map.floorKey(kvBlock);
        lock(b); // to prevent
        Block bbb = map.floorKey(kvBlock);
        if( b.getAddress() != bbb.getAddress()) {
          continue;
        }        
        boolean result = b.delete(key, keyOffset, keyLength);
        return result;
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
    Block kvBlock = getThreadLocalBlock();
    kvBlock.put(keyPtr, keyLength, keyPtr, keyLength);
    while (true) {
      Block b = null;
      try {
        b = map.floorKey(kvBlock);
        lock(b); // to prevent
        Block bbb = map.floorKey(kvBlock);
        if (b.getAddress() != bbb.getAddress()) {
          continue;
        }
        boolean result = b.delete(keyPtr, keyLength);
        return result;
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
   */
  public long get(byte[] key, int keyOffset, int keyLength, byte[] valueBuf, int valOffset) {
  
    Block kvBlock = getThreadLocalBlock() ;
    kvBlock.put(key, keyOffset, keyLength, key, keyOffset, keyLength);
    boolean locked = false;
    Block b = null;
    while(true) {
      try {
        b = map.floorKey(kvBlock);
        long  result = b.get(key, keyOffset, keyLength, valueBuf, valOffset);
        if (result < 0) {
        	// check one more time with lock
        	// it is possible that we caught split in flight
        	Block bb = null;
        	while (true) {
        		b = map.floorKey(kvBlock);
        		lock(b);
        		locked = true;
        		bb = map.floorKey(kvBlock);
        		if (bb.getAddress() != b.getAddress()) {
        			unlock(b);
        		} else {
        			break;
        		}
        	}
        	result = b.get(key, keyOffset, keyLength, valueBuf, valOffset);
//        	/*DEBUG*/
        	if (result < 0) {
        		System.out.println("FAILED to GET "+ new String(key, keyOffset, keyLength));
        		Block next = map.higherKey(b);
        		System.out.println("BLOCK DUMP");
        		BlockScannerOld bs = BlockScannerOld.getScanner(b);
        		while(bs.hasNext()) {
        			byte[] buf = new byte[bs.keySize()];
        			bs.key(buf, 0);
        			System.out.println(new String(buf));
        			bs.next();
        		}
           		System.out.println("BLOCK DUMP NEXT first");
        		bs = BlockScannerOld.getScanner(next);
        		
        		byte[] bbuf = new byte[bs.keySize()];
        		bs.key(bbuf, 0);
        		System.out.println(new String(bbuf));
        		
        	}
        }
        //TODO
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
  public long get(long keyPtr, int keyLength, long valueBuf, int valueBufLength) {
    Block kvBlock = getThreadLocalBlock() ;
    kvBlock.put(keyPtr, keyLength, keyPtr, keyLength);
    Block b = null;
    boolean locked = false;
    while(true) {
      try {
        b = map.floorKey(kvBlock);
        long  result = b.get(keyPtr, keyLength, valueBuf, valueBufLength);
        if (result < 0) {
        	// check one more time with lock
        	// it is possible that we caught split in flight
        	Block bb = null;
        	while (true) {
        		b = map.floorKey(kvBlock);
        		lock(b);
        		locked = true;
        		bb = map.floorKey(kvBlock);
        		if (bb.getAddress() != b.getAddress()) {
        			unlock(b);
        		} else {
        			break;
        		}
        	}
        	result = b.get(keyPtr, keyLength, valueBuf, valueBufLength);

        }
        //TODO
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
    return get(key, offset, len, key, key.length -1) > 0;
  }
  
  /**
   * Get first key
   * @return first key
   */
  public byte[] getFirstKey() {

    while (true) {
      try {
        Block b = null;
        while (true) {
          boolean skipFirst = b == null;
          b = b == null ? map.firstKey() : map.ceilingKey(b);
          if (b == null) return null;
          BlockScannerOld scanner = BlockScannerOld.getScanner(b);
          if (skipFirst) {
            scanner.next(); // skip {0}
          }
          if (scanner.hasNext()) {
            int keySize = scanner.keySize();
            byte[] key = new byte[keySize];
            scanner.key(key, 0);
            return key;
          } else {
            continue;
          }
        }
      } catch (RetryOperationException e) {
        continue;
      }
    }
  }
  
  /**
   * Get last key
   * @return last key
   */
  public byte[] getLastKey() {

    while (true) {
      try {
        Block b = null;
        while (true) {
          b = b == null ? map.lastKey() : map.floorKey(b);
          if (b == null) return null;
          byte[] key = b.getLastKey(true);
          if (key == null) {
            continue;
          } else {
            return key;
          }
        }
      } catch (RetryOperationException e) {
        continue;
      }
    }
  }
  
  /**
   *  Get scanner
   *  @param start start row (inclusive)
   *  @param stop stop row (exclusive)
   */
  public BigSortedMapScannerOld getScanner(byte[] start, byte[] stop) {
    return new BigSortedMapScannerOld(this, start, stop);
  }
  
  public void dispose() {
    for(Block b: map.keySet()) {
      b.free();
    }
    totalAllocatedMemory.set(0);
    totalDataSize.set(0);
    totalIndexSize.set(0);
  }
}
