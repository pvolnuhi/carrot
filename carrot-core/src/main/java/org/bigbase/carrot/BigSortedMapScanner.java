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
import java.util.concurrent.ConcurrentSkipListMap;

import org.bigbase.carrot.util.Scanner;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Scanner implementation
 * TODO Scanner w/o thread locals
 * WARNING: we can not create multiple scanners in a single thread
 * @author jenium65
 *
 */
public class BigSortedMapScanner extends Scanner{

  private BigSortedMap map;
  private long startRowPtr;
  private int startRowLength;
  private long stopRowPtr;
  private int stopRowLength;
  private long nextBlockFirstKey;
  private int nextBlockFirstKeySize;
  private long toFree;
  private DataBlockScanner blockScanner;
  private IndexBlockScanner indexScanner;
  private IndexBlock currentIndexBlock;
  private long snapshotId;
  private boolean isMultiSafe = false;
  private boolean reverse = false;
  private boolean isPrefixScanner = false;
  
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
  BigSortedMapScanner(BigSortedMap map, long startRowPtr, 
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
  BigSortedMapScanner(BigSortedMap map, long startRowPtr, 
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
  
  public DataBlockScanner getBlockScanner()
  {
    return this.blockScanner;
  }
  
  public boolean isPrefixScanner() {
    return this.isPrefixScanner;
  }
  
  public void setPrefixScanner(boolean v) {
    this.isPrefixScanner = v;
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
    while (true) {
      try {
        currentIndexBlock = key != null ? reverse ? cmap.lowerKey(key) : cmap.floorKey(key)
            : reverse ? cmap.lastKey() : cmap.firstKey();
        
        currentIndexBlock.readLock();
        // TODO: Fix the code
        if (currentIndexBlock.hasRecentUnsafeModification()) {
          IndexBlock tmp =
              key != null ? reverse ? cmap.lowerKey(key) : cmap.floorKey(key) : cmap.lastKey();
          if (tmp != currentIndexBlock) {
            continue;
          }
        }

        if (!isMultiSafe) {
          indexScanner =
              IndexBlockScanner.getScanner(currentIndexBlock, this.startRowPtr,
                this.startRowLength, this.stopRowPtr, this.stopRowLength, snapshotId, reverse);
        } else {
          indexScanner = IndexBlockScanner.getScanner(currentIndexBlock,
            this.startRowPtr, this.startRowLength, this.stopRowPtr, this.stopRowLength, snapshotId,
            indexScanner, reverse);
        }
        if (indexScanner != null) {
          blockScanner =
              reverse ? indexScanner.previousBlockScanner() : indexScanner.nextBlockScanner();
//          updateNextFirstKey();
        }
        break;
        // TODO null
      } catch (RetryOperationException e) {
        if (this.indexScanner != null) {
          try {
            this.indexScanner.close();
            this.indexScanner = null;
          } catch (IOException e1) {
          }
        }
        continue;
      } finally {
        currentIndexBlock.readUnlock();
      }
    }
    if (blockScanner == null) {
      close();
      throw new IOException("empty scanner");
    }
  }
  
  /**
   * Get start row pointer
   * @return start row pointer
   */
  public long getStartRowPtr () {
    return this.startRowPtr;
  }
  
  /**
   * Get start row length
   * @return start row length
   */
  public long getStartRowLength () {
    return this.startRowLength;
  }
  
  /**
   * Get stop row pointer
   * @return stop row pointer
   */
  public long getStopRowPtr () {
    return this.stopRowPtr;
  }
  
  /**
   * Get stop row length
   * @return stop row pointer
   */
  public long getStopRowLength () {
    return this.stopRowLength;
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
//    long address = this.currentIndexBlock.lastRecordAddress();
//    long ptr = DataBlock.keyAddress(address);
//    int size = DataBlock.keyLength(address);
//    this.nextBlockFirstKey = UnsafeAccess.allocAndCopy(ptr, size);
//    this.nextBlockFirstKeySize = size;
  }
  
  public boolean hasNext() throws IOException {
    /*if (reverse) {
      throw new UnsupportedOperationException("hasNext");
    }*/
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
    /*if (reverse) {
      throw new UnsupportedOperationException("next");
    } */ 
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
    
    // Now currentIndexBlock is totally unlocked!!!
    // Block can be deleted (invalidated) or split !!!
    // 
    IndexBlock current = this.currentIndexBlock;
    
    int version = current.getSeqNumberSplitOrMerge();
    
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
          int ver = currentIndexBlock.getSeqNumberSplitOrMerge();
          if (ver != version) {
            this.currentIndexBlock  = tmp;
            version = tmp.getSeqNumberSplitOrMerge();
            continue;
          }
//          if (tmp.hasRecentUnsafeModification()) {
//            IndexBlock temp = cmap.higherKey(currentIndexBlock);
//            if (temp != tmp) {
//              continue;
//            }
//          }
          // We need this lock to get current first key,
          // because previous one could have been deleted
//          byte[] firstKey = tmp.getFirstKey();
//          int res = Utils.compareTo(firstKey, 0, firstKey.length, 
//            nextBlockFirstKey, nextBlockFirstKeySize);
//          if ( res > 0) {
//            // set new next block first key
//            //UnsafeAccess.free(nextBlockFirstKey);
//            //nextBlockFirstKey = UnsafeAccess.allocAndCopy(firstKey, 0, firstKey.length);
//            //nextBlockFirstKeySize = firstKey.length;
//          } else if (res < 0) {
//            /*DEBUG*/ System.err.println("index block split on-the-fly");
//            this.currentIndexBlock  = tmp;
//            continue;
//          }
          // set startRow to null, because it is out of range of a IndexBlockScanner
          if (!isMultiSafe) {
            this.indexScanner = IndexBlockScanner.getScanner(tmp, 0/*nextBlockFirstKey*/, 
              /*nextBlockFirstKeySize*/0, stopRowPtr, 
              stopRowLength, snapshotId);
          } else {
            this.indexScanner = IndexBlockScanner.getScanner(tmp, 0/*nextBlockFirstKey*/, 
              /*nextBlockFirstKeySize*/0, stopRowPtr, 
              stopRowLength, snapshotId, indexScanner);
          }
          
          if (this.indexScanner == null) {
            return false;
          }
        } finally {
          tmp.readUnlock();
        }
        // We set new index block and later ...
        this.currentIndexBlock = tmp;
        this.blockScanner = this.indexScanner.nextBlockScanner();
        if (this.blockScanner == null) {
          return false;
        }
        // Here we can fail with RetryOperationException
        //updateNextFirstKey();
        return true;
      } catch (RetryOperationException e) {
        if(this.indexScanner != null) {
            this.indexScanner.close();
        }
        // Set currentIndexBlock to be current again
        this.currentIndexBlock = current;
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
            this.indexScanner = IndexBlockScanner.getScanner(tmp, startRowPtr, 
              startRowLength, stopRowPtr, 
              stopRowLength, snapshotId, reverse);
          } else {
            this.indexScanner = IndexBlockScanner.getScanner(tmp, startRowPtr, 
              startRowLength, stopRowPtr, 
              stopRowLength, snapshotId, indexScanner, reverse);
          }
          
          if (this.indexScanner == null) {
            return false;
          }
        } finally {
          tmp.readUnlock();
        }
        this.blockScanner = this.indexScanner.previousBlockScanner();
        if (this.blockScanner == null) {
          return false;
        }
        this.currentIndexBlock = tmp;
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
    } else if (isPrefixScanner) {
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
   /* if (!reverse) {
      throw new UnsupportedOperationException("previous");
    }*/
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
   /* if (!reverse) {
      throw new UnsupportedOperationException("previous");
    }*/    

    boolean result = blockScanner.hasPrevious();
    if (result) {
      return result;
    } else {
      return previousBlockAndScanner();  
//      result = previousBlockAndScanner();
//      if (!result) {
//        return false;
//      }
    }
//    return this.blockScanner.hasPrevious();
  }

}
