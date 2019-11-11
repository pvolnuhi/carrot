package org.bigbase.zcache;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListMap;

import org.bigbase.carrot.RetryOperationException;
import org.bigbase.util.Utils;


public class BigSortedMapScannerOld implements Closeable{

  BigSortedMapOld map;
  byte[] startRow;
  byte[] stopRow;
  Block currentBlock;
  BlockScannerOld blockScanner; 
  
  static ThreadLocal<byte[]> tmpBuf = new ThreadLocal<byte[]>();
  static ThreadLocal<byte[]> tmpBuf2 = new ThreadLocal<byte[]>();
  
  BigSortedMapScannerOld(BigSortedMapOld map, byte[] start, byte[] stop) {
    this.map = map;
    this.startRow = start;
    this.stopRow = stop;
    init();
  }
  
  private void init() {
    
    ConcurrentSkipListMap<Block, Block> cmap = map.getMap();
    Block key = null;
    if (startRow != null) {
      key = new Block(map.getBlockSize());   
      key.put(startRow, 0, startRow.length,startRow, 0, startRow.length );
    }
    while(true) {
      try {
        currentBlock = key != null? cmap.floorKey(key): cmap.firstKey();
        blockScanner = BlockScannerOld.getScanner(currentBlock);
        if (startRow != null) {
          blockScanner.search(startRow, 0, startRow.length);
        } else {
          // skip first key [0]
          blockScanner.next();
        }
        initTLBuffers();
        break;
        // TODO null?
      } catch(RetryOperationException e) {
        continue;
      }
    }    
  }
  
  
  private void initTLBuffers() {
    if (tmpBuf.get() == null) {
      tmpBuf.set(new byte[map.getBlockSize()]);
    }
    if (tmpBuf2.get() == null) {
      tmpBuf2.set(new byte[map.getBlockSize()]);
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
    if (stopRow == null) {
      return true;
    }
    // Now check current key
    int keySize = blockScanner.keySize();
    byte[] buf = tmpBuf.get();
    blockScanner.key(buf, 0);
    int res = Utils.compareTo(buf, 0, keySize, stopRow, 0, stopRow.length);
    if(res < 0) {
      return true;
    } else {
      return false;
    }    
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
    
    ConcurrentSkipListMap<Block, Block> cmap = map.getMap();
    while (true) {
      Block tmp = null;
      try {
        tmp = cmap.higherKey(currentBlock);
        
        if (tmp == null) {
          return false;
        }
        
        byte[] buf = tmpBuf.get();
        int size = currentBlock.getLastKey(buf, 0, true);
        blockScanner = BlockScannerOld.getScanner(tmp);
        // To mitigate possible block split
        //TODO MERGE
        blockScanner.search(buf, 0, size);
        blockScanner.skipDeletedRecords();
        
        currentBlock = tmp;
        return true;
      } catch (RetryOperationException e) {
        continue;
      }
    }
  }

  /**
   * Key size
   * @return current key size
   */
  public int keySize() {
    return blockScanner.keySize();
  }
  
  /**
   * Value size
   * @return value size
   */
  public int valueSize() {
    return blockScanner.valueSize();
  }
  
  
  /**
   * Get key into buffer
   * @param buffer
   * @param offset
   */
  public void key(byte[] buffer, int offset) {
    blockScanner.key(buffer, offset);
  }
  
  /**
   * Get key into buffer
   * @param addr buffer address
   */
  public void key(long addr) {
    blockScanner.key(addr);
  }
  
  /**
   * Get value into buffer
   * @param buffer
   * @param offset
   */
  public void value(byte[] buffer, int offset) {
    blockScanner.value(buffer, offset);
  }
  
  /**
   * Get value into buffer
   * @param addr
   */
  public void value(long addr) {
    blockScanner.value(addr);
  }

  /**
   * Get current key - value
   * @param keyBuffer
   * @param keyOffset
   * @param valueBuffer
   * @param valueOffset
   */
  public void keyValue (byte[] keyBuffer, int keyOffset, byte[] valueBuffer, int valueOffset)
  {
    blockScanner.keyValue(keyBuffer, keyOffset, valueBuffer, valueOffset);
  }
  
  /**
   * Get current key-value into buffer
   * @param addr address
   */
  public void keyValue(long addr) {
    blockScanner.keyValue(addr);
  }
  
 
  @Override
  public void close() throws IOException {
    // do nothing yet
  }
  
}
