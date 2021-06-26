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

import java.io.Closeable;
import java.io.IOException;

import org.bigbase.carrot.util.Utils;


/**
 * Thread unsafe implementation
 * Index scanner scans block scanners
 * Precautions should be taken on each next()
 * to make sure that index block is still valid
 * TODO: stopRow logic
 */
public final class IndexBlockScanner implements Closeable{

  /*
   * Start Row pointer
   */
  long startRowPtr = 0; // INCLUSIVE  
  
  /*
   * Start row length
   */
  int startRowLength = 0;
  /*
   * Stop Row pointer
   */
  long stopRowPtr = 0; // EXCLUSIVE  
  /*
   * Stop row length
   */
  int stopRowLength = 0;
  /*
   * Index block to scan: readLock index block
   */
  private IndexBlock indexBlock;
  
  /*
   * Current block scanner
   */
  private DataBlockScanner curDataBlockScanner;
  
  
  /*
   * Snapshot Id
   */
  private long snapshotId;
  
  /*
   * Current data block 
   */
  private DataBlock currentDataBlock;
  
  /*
   * Is closed
   */
  private boolean closed = false;
  
  /*
   * Is safe to use in multiple instances inside one thread?
   */
  private boolean isMultiSafe = false;
  
  /*
   * Is reverse scanner?
   */
  private boolean reverse = false;
  /*
   * Thread local for scanner instance.
   * Multiple instances UNSAFE (can not be used in multiple 
   * instances in context of a one thread)
   */
  static ThreadLocal<IndexBlockScanner> scanner = 
      new ThreadLocal<IndexBlockScanner>() {
    @Override
    protected IndexBlockScanner initialValue() {
      return new IndexBlockScanner();
    }    
  };
  
  /**
   * Get or create new scanner
   * @param b data block
   * @param snapshotId snapshot Id
   * @return scanner 
   * @throws RetryOperationException
   */
  private static IndexBlockScanner getScanner(IndexBlock b, long snapshotId) 
      throws RetryOperationException {
    IndexBlockScanner bs = scanner.get();
    bs.reset();
    bs.setBlock(b);
    bs.snapshotId = snapshotId;
    return bs;
  }

  /**
   * Get scanner instance
   * @param b data block
   * @param startRowPtr start row address
   * @param startRowLength start row length
   * @param stopRowPtr stop row address
   * @param stopRowLength stop row length
   * @param snapshotId snapshot id
   * @return scanner
   * @throws RetryOperationException
   */
  public static IndexBlockScanner getScanner(IndexBlock b, long startRowPtr,
      int startRowLength, long stopRowPtr, int stopRowLength, long snapshotId)
      throws RetryOperationException {
    return getScanner(b, startRowPtr, startRowLength, stopRowPtr, stopRowLength, snapshotId, false);
  }

  /**
   * Get scanner instance
   * @param b data block
   * @param startRowPtr start row address
   * @param startRowLength start row length
   * @param stopRowPtr stop row address
   * @param stopRowLength stop row length
   * @param snapshotId snapshot id
   * @param reverse is reverse scanner
   * @return scanner
   * @throws RetryOperationException
   */
  public static IndexBlockScanner getScanner(IndexBlock b, long startRowPtr,
      int startRowLength, long stopRowPtr, int stopRowLength, long snapshotId, boolean reverse)
      throws RetryOperationException {
    //FIXME: move this code under lock
    if (stopRowPtr != 0) {
      byte[] firstKey = b.getFirstKey();
      int res = Utils.compareTo(firstKey, 0, firstKey.length, stopRowPtr, stopRowLength);
      if (res > 0) {
        return null; // out of range
      }
    }
    try {
      // Lock index block
      b.readLock();
      IndexBlockScanner bs = getScanner(b, snapshotId);
      bs.setReverse(reverse);
      bs.setStartStopRows(startRowPtr, startRowLength, stopRowPtr, stopRowLength);
      return bs;
    } catch (RetryOperationException e) {
      b.readUnlock();
      throw e;
    } /*catch (RuntimeException ee) {
      b.readUnlock();
      return null;
    }*/
  }
  /**
   * Get new scanner instance
   * @param b data block
   * @param startRowPtr start row address
   * @param startRowLength start row length
   * @param stopRowPtr stop row address
   * @param stopRowLength stop row length
   * @param snapshotId snapshot id
   * @return scanner
   * @throws RetryOperationException
   */
  public static IndexBlockScanner getScanner(IndexBlock b, long startRowPtr,
      int startRowLength, long stopRowPtr, int stopRowLength, long snapshotId,
      IndexBlockScanner bs, boolean reverse)
      throws RetryOperationException {
    if (stopRowPtr != 0) {
      byte[] firstKey = b.getFirstKey();
      int res = Utils.compareTo(firstKey, 0, firstKey.length, stopRowPtr, stopRowLength);
      if (res > 0) {
        return null; // out of range
      }
    }
    try {
      // Lock index block
      b.readLock();
      if(bs == null) {
        bs = new IndexBlockScanner();
      }
      bs.setMultiInstanceSafe(true);
      bs.setReverse(reverse);
      bs.setBlock(b);
      bs.snapshotId = snapshotId;
      bs.setStartStopRows(startRowPtr, startRowLength, stopRowPtr, stopRowLength);
      return bs;
    } catch (RetryOperationException e) {
      b.readUnlock();
      throw e;
    }
  }
  
  /**
   * Get new scanner instance
   * @param b data block
   * @param startRowPtr start row address
   * @param startRowLength start row length
   * @param stopRowPtr stop row address
   * @param stopRowLength stop row length
   * @param snapshotId snapshot id
   * @param reverse is reverse scanner
   * @return scanner
   * @throws RetryOperationException
   */
  public static IndexBlockScanner getScanner(IndexBlock b, long startRowPtr,
      int startRowLength, long stopRowPtr, int stopRowLength, long snapshotId,
      IndexBlockScanner bs) {
    return getScanner(b, startRowPtr, startRowLength, stopRowPtr, stopRowLength, snapshotId, bs, false);
  }
  /** 
   * Private ctor
   */
  private IndexBlockScanner() {
  }
  
  private void reset() {
    this.startRowPtr = 0;
    this.startRowLength = 0;
    this.stopRowPtr = 0;
    this.stopRowLength = 0;
    this.indexBlock = null;
    
    this.curDataBlockScanner = null;
    this.currentDataBlock = null;
    this.snapshotId = 0;
    this.closed = false;
    this.isMultiSafe = false;
    this.reverse = true;
  }
  
  private void setStartStopRows(long start, int startLength, long stop, int stopLength) {
    this.startRowPtr = start;
    this.startRowLength = startLength;
    this.stopRowPtr = stop;
    this.stopRowLength = stopLength;
    // Returns IndexBlock thread local if isMutiSafe = false
    DataBlock b = null;
    long ptr = reverse? stopRowPtr: startRowPtr;
    int len = reverse? stopRowLength: startRowLength;
    if (!isMultiSafe) {
      b = ptr != 0? reverse? indexBlock.searchLowerBlock(ptr, len, snapshotId, Op.DELETE): 
        indexBlock.searchBlock(ptr, len, snapshotId, Op.PUT)
        : reverse? indexBlock.lastBlock(): indexBlock.firstBlock();
        
    } else {
      b = new DataBlock();
      b = ptr != 0? reverse? indexBlock.searchLowerBlock(ptr, len, snapshotId, Op.DELETE, b):
        indexBlock.searchBlock(ptr, len, snapshotId, Op.PUT, b)
          : reverse? indexBlock.lastBlock(b):indexBlock.firstBlock(b);
    }
    
    if (b == null) {
      //TODO: is it normal?
      ///*DEBUG*/ System.err.println(Thread.currentThread().getName() +": BLOCK is null in IndexBlockDirectMemoryScanner.setStartStopRows");
      try {
        close();
      } catch (IOException e) {
      }
      return;
    }
    
    this.currentDataBlock = b;
    if (this.currentDataBlock != null) {
      this.currentDataBlock.decompressDataBlockIfNeeded();
    }
  }
 
  /**
   * Is this scanner safe to use in multiple instances in the context
   * of a single thread?
   * @return
   */
  boolean isMultiInstanceSafe() {
    return this.isMultiSafe;
  }
  
  /**
   * Set multiple instances safe
   * @param b
   */
  void setMultiInstanceSafe(boolean b) {
    this.isMultiSafe = b;
  }
  
  /**
   * Is reverse scanner
   * @return true, if - yes
   */
  public final boolean isReverse() {
    return this.reverse;
  }
  /**
   * Set reverse scanner
   * @param b
   */
  public void setReverse(boolean b) {
    this.reverse = b;
  }
  
  /**
   * Set scanner with new block
   * @param b block
   * @throws RetryOperationException 
   */
  private void setBlock(IndexBlock b) throws RetryOperationException {
    this.indexBlock = b;
  }
  

  
  /**
   * Advance scanner by one data block
   * @return data block scanner or null
   */
  public final DataBlockScanner nextBlockScanner() {

    if (isClosed()) {
      return null;
    }
    if (this.currentDataBlock != null && this.curDataBlockScanner != null) {
      this.currentDataBlock.compressDataBlockIfNeeded();
    }
    DataBlock b = this.currentDataBlock;
    if (this.curDataBlockScanner != null) {
      b =  this.indexBlock.nextBlock(this.currentDataBlock, isMultiSafe);
    }
    return setBlockAndReturnScanner(b);
  }

  
  private DataBlockScanner setBlockAndReturnScanner(DataBlock b) {
    
    this.currentDataBlock = b;
    if (this.currentDataBlock != null) {
      this.currentDataBlock.decompressDataBlockIfNeeded();
    }
    
    if (this.curDataBlockScanner != null) {
      try {
        this.curDataBlockScanner.close();
        this.curDataBlockScanner = null;
      } catch (IOException e) {
      }

      if (this.currentDataBlock == null) {
        return null;
      } 
      
      if(stopRowPtr != 0) {
        if (this.currentDataBlock.compareTo(stopRowPtr, stopRowLength, 0, Op.DELETE) < 0) {
          this.currentDataBlock.compressDataBlockIfNeeded();
          this.currentDataBlock = null;
          return null;
        }
      }
      if(startRowPtr != 0) {
        long lastRecord = this.currentDataBlock.last();
        if (DataBlock.compareTo(lastRecord, startRowPtr, startRowLength, 0, Op.DELETE) > 0) {
          this.currentDataBlock.compressDataBlockIfNeeded();
          this.currentDataBlock = null;
          return null;
        }
      }

      if (!isMultiSafe) {
        this.curDataBlockScanner =
          DataBlockScanner.getScanner(this.currentDataBlock, this.startRowPtr, 
            this.startRowLength, this.stopRowPtr, this.stopRowLength, snapshotId);
      } else {
        this.curDataBlockScanner =
            DataBlockScanner.getScanner(this.currentDataBlock, this.startRowPtr, 
              this.startRowLength, this.stopRowPtr, this.stopRowLength, snapshotId,
              this.curDataBlockScanner);
      }
    } else if(!closed){
      if (!isMultiSafe) {
        this.curDataBlockScanner =
          DataBlockScanner.getScanner(this.currentDataBlock, this.startRowPtr, 
            this.startRowLength, this.stopRowPtr, this.stopRowLength, snapshotId);
      } else {
        this.curDataBlockScanner =
            DataBlockScanner.getScanner(this.currentDataBlock, this.startRowPtr, 
              this.startRowLength, this.stopRowPtr, this.stopRowLength, snapshotId,
              this.curDataBlockScanner);
      }
      if (this.indexBlock.isFirstIndexBlock() && this.startRowPtr == 0 &&
          this.curDataBlockScanner != null && !reverse) {
        // skip system first entry : {0}{0}
        this.curDataBlockScanner.next();
      }
    } else {
      return null;
    }
    if (reverse && this.curDataBlockScanner != null) {
      boolean result = this.curDataBlockScanner.last();
      if (!result) {
        return null;
      }
    }
    return this.curDataBlockScanner;    
  }
  /**
   * Get last block scanner
   * @return last block scanner
   */
  DataBlockScanner lastBlockScanner() {

    if (isClosed()) {
      return null;
    }
    if (this.currentDataBlock != null && this.curDataBlockScanner != null) {
      this.currentDataBlock.compressDataBlockIfNeeded();
    }
    DataBlock b = this.currentDataBlock;
    if (this.curDataBlockScanner != null) {    
      b = this.indexBlock.lastBlock(this.currentDataBlock, stopRowPtr, stopRowLength);
    }
    DataBlockScanner scanner =  setBlockAndReturnScanner(b);
    if (scanner != null) {
      return scanner;
    }
    return previousBlockScanner();
  }
  
  /**
   * Gets first block scanner
   * @return first block scanner
   */
  DataBlockScanner firstBlockScanner() {
    // No checks are required
    if (isClosed()) {
      return null;
    }
    if (this.currentDataBlock != null && this.curDataBlockScanner != null) {
      this.currentDataBlock.compressDataBlockIfNeeded();
    }
    DataBlock b = this.currentDataBlock;
    if (this.curDataBlockScanner != null) {
      b =  this.indexBlock.firstBlock(this.currentDataBlock, startRowPtr, startRowLength);
    }
    return setBlockAndReturnScanner(b);
  }
  
  /**
   * Gets previous block scanner
   * @return previous block scanner
   */
  DataBlockScanner previousBlockScanner() {
    if (isClosed()) {
      return null;
    }
    if (this.currentDataBlock != null && this.curDataBlockScanner != null) {
      this.currentDataBlock.compressDataBlockIfNeeded();
    }
    DataBlock b = this.currentDataBlock;
    if (this.curDataBlockScanner != null) {
      b =  this.indexBlock.previousBlock(this.currentDataBlock);
    }
    return setBlockAndReturnScanner(b);
  }
  
  public DataBlockScanner getBlockScanner() {
    return this.curDataBlockScanner;
  }
  
  public boolean isClosed() {
    return this.closed;
  }
  
  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    if(this.curDataBlockScanner != null) {
      try {
        this.curDataBlockScanner.close();
      } catch (IOException e) {
        
      }
    }
    if (this.currentDataBlock != null) {
      this.currentDataBlock.compressDataBlockIfNeeded();
    }
    // Unlock index block
    if (indexBlock != null) {
      indexBlock.readUnlock();
    } 
  }
 
}
