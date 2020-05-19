package org.bigbase.carrot;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Scanner implementation
 * @author jenium65
 *
 */
public class BigSortedMapScanner implements Closeable{

  private BigSortedMap map;
  private byte[] startRow;
  private byte[] stopRow;
  private DataBlockScanner blockScanner;
  private IndexBlockScanner indexScanner;
  private IndexBlock currentIndexBlock;
  private long snapshotId;
  
  static ThreadLocal<IndexBlock> tlKey = new ThreadLocal<IndexBlock>() {
    @Override
    protected IndexBlock initialValue() {
      return new IndexBlock(IndexBlock.MAX_BLOCK_SIZE);
    }
  };
  
  BigSortedMapScanner(BigSortedMap map, byte[] start, byte[] stop, long snapshotId) {
    this.map = map;
    this.startRow = start;
    this.stopRow = stop;
    this.snapshotId = snapshotId;
    init();
  }
  
  private void init() {
    
    ConcurrentSkipListMap<IndexBlock, IndexBlock> cmap = map.getMap();
    IndexBlock key = null;
    if (startRow != null) {
      key = tlKey.get();   
      key.putForSearch(startRow, 0, startRow.length, snapshotId);
    }
    while(true) {
      try {
        currentIndexBlock = key != null? cmap.floorKey(key): cmap.firstKey();
        indexScanner = IndexBlockScanner.getScanner(currentIndexBlock, this.startRow, this.stopRow, snapshotId);
        blockScanner = indexScanner.nextBlockScanner(); 
        break;
        // TODO null?
      } catch(RetryOperationException e) {
        continue;
      }
    }    
  }
  
  
  public boolean hasNext() {
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
   */
  private boolean nextBlockAndScanner() {
    
    this.blockScanner = indexScanner.nextBlockScanner();
    
    if (this.blockScanner != null) {
      return true;
    }
    
    ConcurrentSkipListMap<IndexBlock, IndexBlock> cmap = map.getMap();
    while (true) {
      IndexBlock tmp = null;
      try {
        tmp = cmap.higherKey(currentIndexBlock);
        if (tmp == null) {
          return false;
        }
        this.indexScanner = IndexBlockScanner.getScanner(tmp, startRow, stopRow, snapshotId);
        if (this.indexScanner == null) {
          return false;
        }
        this.currentIndexBlock = tmp;
        this.blockScanner = this.indexScanner.nextBlockScanner();
        return true;
      } catch (RetryOperationException e) {
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
   * Get current value size. Make sure, that hasNext() returned true
   * @return value size (-1 if scanner is invalid)
   */
  public int valueSize() {
    return blockScanner.valueSize();
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
    this.indexScanner.close();
  }
  
}
