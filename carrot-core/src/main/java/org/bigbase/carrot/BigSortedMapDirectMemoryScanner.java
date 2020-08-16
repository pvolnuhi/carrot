package org.bigbase.carrot;

import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListMap;

import org.bigbase.carrot.util.BidirectionalScanner;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Scanner implementation
 * TODO Scanner w/o thread locals
 * WARNING: we can not create multiple scanners in a single thread
 * @author jenium65
 *
 */
public class BigSortedMapDirectMemoryScanner implements BidirectionalScanner{

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
  private boolean isMultiSafe = false;
  private boolean reverse = false;
  
  /**
   * Multi - instance SAFE (can be used in multiple instances in context of a one thread)
   */
  static ThreadLocal<IndexBlock> tlKey = new ThreadLocal<IndexBlock>() {
    @Override
    protected IndexBlock initialValue() {
      return new IndexBlock(IndexBlock.MAX_BLOCK_SIZE);
    }
  };
  
  
  /**
   * Constructor in non safe mode
   * @param map ordered map
   * @param startRowPtr start row address
   * @param startRowLength start row length
   * @param stopRowPtr stop row address
   * @param stopRowLength stop row length
   * @param snapshotId snapshot id
   * @throws IOException 
   */
  BigSortedMapDirectMemoryScanner(BigSortedMap map, long startRowPtr, 
    int startRowLength, long stopRowPtr, int stopRowLength, long snapshotId) throws IOException {
    this(map, startRowPtr, startRowLength, stopRowPtr, stopRowLength, snapshotId, false, false);
  }
  /**
   * Constructor in a safe mode
   * @param map ordered map
   * @param startRowPtr start row address
   * @param startRowLength start row length
   * @param stopRowPtr stop row address
   * @param stopRowLength stop row length
   * @param snapshotId snapshot id
   * @param isMultiSafe true - safe for multiple instances
   * @param reverse - is reverse scanner
   * @throws IOException 
   */
  BigSortedMapDirectMemoryScanner(BigSortedMap map, long startRowPtr, 
    int startRowLength, long stopRowPtr, int stopRowLength, long snapshotId,
    boolean isMultiSafe, boolean reverse) throws IOException {
    checkArgs(startRowPtr, startRowLength, stopRowPtr, stopRowLength);
    this.map = map;
    this.startRowPtr = startRowPtr;
    this.startRowLength = startRowLength;
    this.stopRowPtr = stopRowPtr;
    this.stopRowLength = stopRowLength;
    this.snapshotId = snapshotId;
    this.isMultiSafe = isMultiSafe;
    this.reverse = reverse;
    init();
  }
  
  private void checkArgs(long startRowPtr, int startRowLength, long stopRowPtr, int stopRowLength) {
    if (startRowPtr != 0 && stopRowPtr != 0) {
      if (Utils.compareTo(startRowPtr, startRowLength, stopRowPtr, stopRowLength) > 0) {
        throw new IllegalArgumentException("start row is greater than stop row");
      }
    }
  }

  private void init() throws IOException {
    
    ConcurrentSkipListMap<IndexBlock, IndexBlock> cmap = map.getMap();
    IndexBlock key = null;
    long ptr = 0;
    int length = 0;
    if (reverse) {
      ptr = stopRowPtr;
      length = stopRowLength;
    } else {
      ptr = startRowPtr;
      length = startRowLength;
    }
    
    if (ptr != 0) {
      key = tlKey.get();   
      key.reset();
      key.putForSearch(ptr, length, snapshotId);
    }
    while(true) {
      try {
        currentIndexBlock = key != null? reverse? 
            cmap.lowerKey(key):cmap.floorKey(key):reverse? cmap.lastKey(): cmap.firstKey();
        if (reverse) {
          currentIndexBlock.readLock();
          if(currentIndexBlock.hasRecentUnsafeModification()) {
            IndexBlock tmp =  key != null? reverse? cmap.lowerKey(key):
              cmap.floorKey(key):cmap.lastKey();
            
              if (tmp != currentIndexBlock) {
              continue;
            }
          }
        }
        
        if (!isMultiSafe) {
          indexScanner = IndexBlockDirectMemoryScanner.getScanner(currentIndexBlock, this.startRowPtr, 
            this.startRowLength, this.stopRowPtr, this.stopRowLength, snapshotId, reverse);
        } else {
          indexScanner = IndexBlockDirectMemoryScanner.getScanner(currentIndexBlock, this.startRowPtr, 
            this.startRowLength, this.stopRowPtr, this.stopRowLength, snapshotId, indexScanner, reverse);
        }
        if (indexScanner != null) {
          blockScanner = reverse? indexScanner.previousBlockScanner(): indexScanner.nextBlockScanner(); 
          updateNextFirstKey();
        }
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
      } finally {
        if (reverse) {
          currentIndexBlock.readUnlock();
        }
      }
    } 
    if (blockScanner == null) {
      close();
      throw new IOException("empty scanner");
    }
  }
  
  //TODO : is it safe?
  private void updateNextFirstKey() {
    if (reverse) {
      return;
    }
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
    if (reverse) {
      throw new UnsupportedOperationException("hasNext");
    }
    if (blockScanner == null) {
      return false;
    }
    boolean result = blockScanner.hasNext();
    if (result) {
      return result;
    } 
    result = nextBlockAndScanner();
    if (!result) {
      return false;
    }
    return this.blockScanner.hasNext();
  }
    
  public boolean next() {
    if (reverse) {
      throw new UnsupportedOperationException("next");
    }  
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
          if (tmp.hasRecentUnsafeModification()) {
            IndexBlock temp = cmap.higherKey(currentIndexBlock);
            if (temp != tmp) {
              continue;
            }
          }
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
          if (!isMultiSafe) {
            this.indexScanner = IndexBlockDirectMemoryScanner.getScanner(tmp, nextBlockFirstKey, 
              nextBlockFirstKeySize, stopRowPtr, 
              stopRowLength, snapshotId);
          } else {
            this.indexScanner = IndexBlockDirectMemoryScanner.getScanner(tmp, nextBlockFirstKey, 
              nextBlockFirstKeySize, stopRowPtr, 
              stopRowLength, snapshotId, indexScanner);
          }
          
          if (this.indexScanner == null) {
            return false;
          }
        } finally {
          tmp.readUnlock();
        }
        this.currentIndexBlock = tmp;
        this.blockScanner = this.indexScanner.nextBlockScanner();
        if (this.blockScanner == null) {
          return false;
        }
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
   * For reverse scanner we move backwards
   * @return scanner or null
   * @throws IOException 
   */
  private boolean previousBlockAndScanner() throws IOException {
    this.blockScanner = indexScanner.previousBlockScanner();
    
    if (this.blockScanner != null) {
      return true;
    }
    //*DEBUG*/ System.out.println("PREVIOUS INDEX");

    ConcurrentSkipListMap<IndexBlock, IndexBlock> cmap = map.getMap();
    if (this.indexScanner != null) {
      this.indexScanner.close();
      this.indexScanner = null;
    }
    while (true) {
      IndexBlock tmp = null;
      try {
        tmp = cmap.lowerKey(currentIndexBlock);
        if (tmp == null) {
          return false;
        }
        // set startRow to nextBlockFirstKey, because it is out of range of a IndexBlockScanner
        try {
          tmp.readLock();
          if (tmp.hasRecentUnsafeModification()) {
            IndexBlock temp = cmap.lowerKey(currentIndexBlock);
            if (temp != tmp) {
              continue;
            }
          }
          // Check startRow and index block
          if (!isMultiSafe) {
            this.indexScanner = IndexBlockDirectMemoryScanner.getScanner(tmp, startRowPtr, 
              startRowLength, stopRowPtr, 
              stopRowLength, snapshotId, reverse);
          } else {
            this.indexScanner = IndexBlockDirectMemoryScanner.getScanner(tmp, startRowPtr, 
              startRowLength, stopRowPtr, 
              stopRowLength, snapshotId, indexScanner, reverse);
          }
          
          if (this.indexScanner == null) {
            return false;
          }
        } finally {
          tmp.readUnlock();
        }
        this.currentIndexBlock = tmp;
        this.blockScanner = this.indexScanner.previousBlockScanner();
        if (this.blockScanner == null) {
          return false;
        }
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
    close(false);
  }
  
  public void close(boolean full) throws IOException {
    if (this.indexScanner != null) {
      this.indexScanner.close();
    }
    if (this.nextBlockFirstKey > 0) {
      UnsafeAccess.free(this.nextBlockFirstKey);
    }
    if (this.toFree > 0 && this.toFree != this.nextBlockFirstKey) {
      UnsafeAccess.free(this.toFree);
    }
    if (full) {
      if (startRowPtr > 0) {
        UnsafeAccess.free(startRowPtr);
      }
      if (stopRowPtr > 0) {
        UnsafeAccess.free(stopRowPtr);
      }
    }
  }
  
  @Override
  public boolean first() {
    throw new UnsupportedOperationException("first");
  }
  @Override
  public boolean last() {
    throw new UnsupportedOperationException("last");
  }
  
  /**
   * For reverse scanner, patter hasPrevious() , previous()
   * is expensive. The better approach is to use:
   * do{
   * 
   * } while(scanner.previous());
   * @throws IOException 
   */
  
  @Override
  public boolean previous() throws IOException {
    if (!reverse) {
      throw new UnsupportedOperationException("previous");
    }
    boolean result = blockScanner.previous();
    if (result) {
      return result;
    }
    if (!result) {    
      result = previousBlockAndScanner();
      if (!result) {
        return false;
      }
    }
    return true;
  }
  
  @Override
  public boolean hasPrevious() throws IOException {
    if (!reverse) {
      throw new UnsupportedOperationException("previous");
    }    

    boolean result = blockScanner.hasPrevious();
    if (result) {
      return result;
    }
    if (!result) {    
      result = previousBlockAndScanner();
      if (!result) {
        return false;
      }
    }
    return this.blockScanner.hasPrevious();
  }

}
