package org.bigbase.carrot;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListMap;

import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Scanner implementation
 * @author jenium65
 *
 */
public class BigSortedMapDirectMemoryScanner implements Closeable{

  private BigSortedMap map;
  private long startRowPtr;
  private int startRowLength;
  private long stopRowPtr;
  private int stopRowLength;
  private long nextBlockFirstKey;
  private int nextBlockFirstKeySize;
  private long toFree;
  private DataBlockDirectMemoryScanner blockScanner;
  private IndexBlockDirectMemoryScanner indexScanner;
  private IndexBlock currentIndexBlock;
  private long snapshotId;
  
  static ThreadLocal<IndexBlock> tlKey = new ThreadLocal<IndexBlock>() {
    @Override
    protected IndexBlock initialValue() {
      return new IndexBlock(IndexBlock.MAX_BLOCK_SIZE);
    }
  };
  
  BigSortedMapDirectMemoryScanner(BigSortedMap map, long startRowPtr, 
    int startRowLength, long stopRowPtr, int stopRowLength, long snapshotId) {
    checkArgs(startRowPtr, startRowLength, stopRowPtr, stopRowLength);
    this.map = map;
    this.startRowPtr = startRowPtr;
    this.startRowLength = startRowLength;
    this.stopRowPtr = stopRowPtr;
    this.stopRowLength = stopRowLength;
    this.snapshotId = snapshotId;
    init();
  }
  
  private void checkArgs(long startRowPtr, int startRowLength, long stopRowPtr, int stopRowLength) {
    if (startRowPtr != 0 && stopRowPtr != 0) {
      if (Utils.compareTo(startRowPtr, startRowLength, stopRowPtr, stopRowLength) > 0) {
        throw new IllegalArgumentException("start row is greater than stop row");
      }
    }
  }

  private void init() {
    
    ConcurrentSkipListMap<IndexBlock, IndexBlock> cmap = map.getMap();
    IndexBlock key = null;
    if (startRowPtr != 0) {
      key = tlKey.get();   
      key.reset();
      key.putForSearch(startRowPtr, startRowLength, snapshotId);
    }
    while(true) {
      try {
        currentIndexBlock = key != null? cmap.floorKey(key): cmap.firstKey();
        indexScanner = IndexBlockDirectMemoryScanner.getScanner(currentIndexBlock, this.startRowPtr, 
          this.startRowLength, this.stopRowPtr, this.stopRowLength, snapshotId);
        blockScanner = indexScanner.nextBlockScanner(); 
        updateNextFirstKey();
        break;
        //TODO null
      } catch(RetryOperationException e) {
        if (this.indexScanner != null) {
          try {
            this.indexScanner.close();
          } catch (IOException e1) {
          }
        }
        continue;
      }
    } 
  }
  
  //TODO : is it safe?
  private void updateNextFirstKey() {
    if (this.toFree > 0) {
      UnsafeAccess.free(toFree);
    }
    this.toFree = this.nextBlockFirstKey;
    IndexBlock next = map.getMap().higherKey(this.currentIndexBlock);
    if (next != null) {
      byte[] firstKey = next.getFirstKey();
      this.nextBlockFirstKey = UnsafeAccess.allocAndCopy(firstKey, 0, firstKey.length);
      this.nextBlockFirstKeySize = firstKey.length;
    }    
  }
  
  public boolean hasNext() throws IOException {
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
            nextBlockFirstKey, nextBlockFirstKeySize) > 0) {
            // set new next block first key
            UnsafeAccess.free(nextBlockFirstKey);
            nextBlockFirstKey = UnsafeAccess.allocAndCopy(firstKey, 0, firstKey.length);
            nextBlockFirstKeySize = firstKey.length;
          }
          // set startRow to null, because it is out of range of a IndexBlockScanner
          this.indexScanner = IndexBlockDirectMemoryScanner.getScanner(tmp, nextBlockFirstKey, 
            nextBlockFirstKeySize, stopRowPtr, 
            stopRowLength, snapshotId);
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
        if(this.indexScanner != null) {
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
    if (this.nextBlockFirstKey > 0) {
      UnsafeAccess.free(this.nextBlockFirstKey);
    }
    if (this.toFree > 0 && this.toFree != this.nextBlockFirstKey) {
      UnsafeAccess.free(this.toFree);
    }
  }
  
}
