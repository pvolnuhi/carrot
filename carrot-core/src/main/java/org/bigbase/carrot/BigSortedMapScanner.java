package org.bigbase.carrot;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.bigbase.carrot.util.Utils;

/**
 * Scanner implementation
 * @author jenium65
 *
 */
public class BigSortedMapScanner implements Closeable{

  private BigSortedMap map;
  private byte[] startRow;
  private byte[] stopRow;
  private byte[] nextBlockFirstKey;
  private DataBlockScanner blockScanner;
  private IndexBlockScanner indexScanner;
  private IndexBlock currentIndexBlock;
  private long snapshotId;
  private boolean isMultiSafe = false;
  /**
   * Multi - instance SAFE (can be used in multiple instances in context of a one thread)
   */
  static ThreadLocal<IndexBlock> tlKey = new ThreadLocal<IndexBlock>() {
    @Override
    protected IndexBlock initialValue() {
      return new IndexBlock(IndexBlock.MAX_BLOCK_SIZE);
    }
  };
  
  BigSortedMapScanner(BigSortedMap map, byte[] start, byte[] stop, long snapshotId) {
    this(map, start, stop, snapshotId, false);
  }
  
  BigSortedMapScanner(BigSortedMap map, byte[] start, byte[] stop, long snapshotId,
    boolean isMultiSafe) {
    checkArgs(startRow, stopRow);
    this.map = map;
    this.startRow = start;
    this.stopRow = stop;
    this.snapshotId = snapshotId;
    this.isMultiSafe = isMultiSafe;
    init();
  }
  
  private void checkArgs(byte[] startRow, byte[] stopRow) {
    if (startRow != null && stopRow != null) {
      if (Utils.compareTo(startRow, 0, startRow.length, stopRow, 0, stopRow.length) > 0) {
        throw new IllegalArgumentException("start row is greater than stop row");
      }
    }
  }

  private void init() {
    
    ConcurrentSkipListMap<IndexBlock, IndexBlock> cmap = map.getMap();
    IndexBlock key = null;
    if (startRow != null) {
      key = tlKey.get();   
      key.reset();
      key.putForSearch(startRow, 0, startRow.length, snapshotId);
    }
    while(true) {
      try {
        currentIndexBlock = key != null? cmap.floorKey(key): cmap.firstKey();
        if (isMultiSafe) {
          indexScanner = IndexBlockScanner.getScanner(currentIndexBlock, this.startRow, 
            this.stopRow, snapshotId, indexScanner);
        } else {
          indexScanner = IndexBlockScanner.getScanner(currentIndexBlock, this.startRow, 
            this.stopRow, snapshotId);
        }
        blockScanner = indexScanner.nextBlockScanner(); 
        updateNextFirstKey();
        break;
      } catch(RetryOperationException e) {
        if(indexScanner != null) {
          try {
            indexScanner.close();
          } catch (IOException e1) {
            // TODO Auto-generated catch block
          }
        }
        continue;
      }
    } 
  }
  //TODO: is it safe?
  private void updateNextFirstKey() {
    IndexBlock next = map.getMap().higherKey(this.currentIndexBlock);
    if(next != null) {
      this.nextBlockFirstKey = next.getFirstKey();
    }
  }
  public boolean hasNext() throws IOException {
    if (blockScanner == null) {
      return false;
    }
    boolean result = blockScanner.hasNext();
    if (!result) {    
      result = nextBlockAndScanner();
      if (!result) {
        return false;
      }
    }
    return this.blockScanner.hasNext();
  }
    
  public boolean next() {
    // Block scanner can be NULL
    if (blockScanner == null) {
      return false;
    }
    return blockScanner.next();
  }
  
  
  /**
   * Scanner can duplicate rows
   * if split happens 
   * and skip some rows when merge happens
   * @return
   * @throws IOException 
   */
  private boolean nextBlockAndScanner() throws IOException {
    
    this.blockScanner = indexScanner.nextBlockScanner();
    
    if (this.blockScanner != null) {
      return true;
    }
    
    ConcurrentSkipListMap<IndexBlock, IndexBlock> cmap = map.getMap();
    if (this.indexScanner != null) {
      this.indexScanner.close();
      this.indexScanner = null;
    }
    while (true) {
      IndexBlock tmp = null;
      try {
        tmp = cmap.higherKey(currentIndexBlock);
        if (tmp == null) {
          return false;
        }
        // set startRow to nextBlockFirstKey, because it is out of range of a IndexBlockScanner
        try {
          tmp.readLock();
          // We need this lock to get current first key,
          // because previous one could have been deleted
          byte[] firstKey = tmp.getFirstKey();
          if (Utils.compareTo(firstKey, 0, firstKey.length, 
            nextBlockFirstKey, 0, nextBlockFirstKey.length) > 0) {
            nextBlockFirstKey = firstKey;
          }
          
          //this.indexScanner = IndexBlockScanner.getScanner(tmp, nextBlockFirstKey, stopRow, snapshotId);
          if (isMultiSafe) {
            indexScanner = IndexBlockScanner.getScanner(tmp, nextBlockFirstKey, 
              this.stopRow, snapshotId, indexScanner);
          } else {
            indexScanner = IndexBlockScanner.getScanner(tmp, nextBlockFirstKey, 
              this.stopRow, snapshotId);
          }
          if (this.indexScanner == null) {
            return false;
          }
        } finally {
          tmp.readUnlock();
        }
        this.currentIndexBlock = tmp;
        this.blockScanner = this.indexScanner.nextBlockScanner();
        updateNextFirstKey();
        return true;
      } catch (RetryOperationException e) {
        if (this.indexScanner != null) {
          this.indexScanner.close();
        }
        continue;
      }
    }
  }

  /**
   * Get current key size. Make sure, that hasNext() returned true
   * @return current key size (-1 if invalid)
   */
  public int keySize() {
    return blockScanner.keySize();
  }
  
  /**
   * Returns key address
   * @return key address
   */
  public long keyAddress() {
    return blockScanner.keyAddress();
  }
  
  public long keyVersion() {
    return blockScanner.keyVersion();
  }
  
  public Op keyOpType() {
    return blockScanner.keyOpType();
  }
  
  /**
   * Get current value size. Make sure, that hasNext() returned true
   * @return value size (-1 if scanner is invalid)
   */
  public int valueSize() {
    return blockScanner.valueSize();
  }
  
  /**
   * Returns value address
   * @return value address
   */
  public long valueAddress() {
    return blockScanner.valueAddress();
  }
  /**
   * Get key into buffer. 
   * @param buffer buffer where to store 
   * @param offset offset in this buffer
   * @return size of a key or -1. Make sure you have enough space in a buffer
   */
  public int key(byte[] buffer, int offset) {
    return blockScanner.key(buffer, offset);
  }
  
  /**
   * Get key into buffer
   * @param addr buffer address
   * @param len available space
   * @return size of a key or -1. Make sure you have enough space in a buffer
   */
  
  public int key(long addr, int len) {
    return blockScanner.key(addr, len);
  }
  
  /**
   * Get value into buffer
   * @param buffer
   * @param offset
   * @return size of a value or -1. Make sure you have enough space in a buffer
   */
  public int value(byte[] buffer, int offset) {
    return blockScanner.value(buffer, offset);
  }
  
  /**
   * Get value into buffer
   * @param addr
   * @param len available space
   * @return size of a value or -1. Make sure you have enough space in a buffer
   */
  public int value(long addr, int len) {
    return blockScanner.value(addr, len);
  }

  /**
   * Get current key - value into buffer
   * @param buffer buffer
   * @param offset offset
   * @return combined size of a key+value or -1. Make sure you have enough space 
   * and know sizes of a key and value
   */
  public int keyValue (byte[] buffer, int offset)
  {
    return blockScanner.keyValue(buffer, offset);
  }
  
  /**
   * Get current key-value into buffer
   * @param addr address
   * @param len available space
   * @return combined size of a key+value or -1. Make sure you have enough space 
   * and know sizes of a key and value
   *    
   */
  
  public int keyValue(long addr, int len) {
    return blockScanner.keyValue(addr, len);
  }
  
 
  @Override
  public void close() throws IOException {
    // do nothing yet
    if (this.indexScanner != null) {
      this.indexScanner.close();
    }
    //checkOrphanLocks();
  }

  @SuppressWarnings("unused")
  private void checkOrphanLocks() {
    // TODO Auto-generated method stub
    for (ReentrantReadWriteLock lock: IndexBlock.locks) {
      if(lock.isWriteLockedByCurrentThread()) {
        System.out.println("Orphan Write Lock found "+ lock);
        Thread.dumpStack();
      }
      if (lock.getReadHoldCount() > 0) {
        System.out.println("Orphan Read Lock found "+ lock + " count=" + lock.getReadLockCount() + " indexScanner=" +
            this.indexScanner);
        Thread.dumpStack();
      }
    }
  }
 
  
}
