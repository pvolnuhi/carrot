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
public final class IndexBlockDirectMemoryScanner implements Closeable{

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
  private DataBlockDirectMemoryScanner curDataBlockScanner;
  
  
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
   * Thread local for scanner instance.
   * Multiple instances UNSAFE (can not be used in multiple 
   * instances in context of a one thread)
   */
  static ThreadLocal<IndexBlockDirectMemoryScanner> scanner = 
      new ThreadLocal<IndexBlockDirectMemoryScanner>() {
    @Override
    protected IndexBlockDirectMemoryScanner initialValue() {
      return new IndexBlockDirectMemoryScanner();
    }    
  };
  
  /**
   * Get or create new scanner
   * @param b data block
   * @param snapshotId snapshot Id
   * @return scanner 
   * @throws RetryOperationException
   */
  private static IndexBlockDirectMemoryScanner getScanner(IndexBlock b, long snapshotId) 
      throws RetryOperationException {
    IndexBlockDirectMemoryScanner bs = scanner.get();
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
  public static IndexBlockDirectMemoryScanner getScanner(IndexBlock b, long startRowPtr,
      int startRowLength, long stopRowPtr, int stopRowLength, long snapshotId)
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
      IndexBlockDirectMemoryScanner bs = getScanner(b, snapshotId);
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
   * @return scanner
   * @throws RetryOperationException
   */
  public static IndexBlockDirectMemoryScanner getScanner(IndexBlock b, long startRowPtr,
      int startRowLength, long stopRowPtr, int stopRowLength, long snapshotId,
      IndexBlockDirectMemoryScanner bs)
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
        bs = new IndexBlockDirectMemoryScanner();
      }
      bs.setMultiInstanceSafe(true);
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
   * Private ctor
   */
  private IndexBlockDirectMemoryScanner() {
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
  }
  
  private void setStartStopRows(long start, int startLength, long stop, int stopLength) {
    this.startRowPtr = start;
    this.startRowLength = startLength;
    this.stopRowPtr = stop;
    this.stopRowLength = stopLength;
    // Returns IndexBlock thread local if isMutiSafe = false
    DataBlock b = null;
    if (!isMultiSafe) {
      b = this.startRowPtr != 0
        ? indexBlock.searchBlock( this.startRowPtr,  this.startRowLength,
          snapshotId, Op.DELETE)
        : indexBlock.firstBlock();
        
    } else {
      b = new DataBlock();
      b = this.startRowPtr != 0
          ? indexBlock.searchBlock( this.startRowPtr,  this.startRowLength,
            snapshotId, Op.DELETE, b)
          : indexBlock.firstBlock(b);
    }
    this.currentDataBlock = b;

    if (b == null) {
      // FATAL
      throw new RuntimeException("Index block scanner");
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
  public final DataBlockDirectMemoryScanner nextBlockScanner() {
    // No checks are required
    if (isClosed()) {
      return null;
    }
    if (this.curDataBlockScanner != null) {
      try {
        this.curDataBlockScanner.close();
        this.curDataBlockScanner = null;
      } catch (IOException e) {
      }
      this.currentDataBlock = this.indexBlock.nextBlock(this.currentDataBlock, !isMultiSafe);

      if (this.currentDataBlock == null) {
        this.closed = true;
        return null;
      }
      
      //TODO: check stopRow, version and op
      if(stopRowPtr != 0) {
        if (this.currentDataBlock.compareTo(stopRowPtr, stopRowLength, 0, Op.DELETE) < 0) {
          this.currentDataBlock = null;
          this.closed = true;
          return null;
        }
      }
      if (!isMultiSafe) {
        this.curDataBlockScanner =
          DataBlockDirectMemoryScanner.getScanner(this.currentDataBlock, this.startRowPtr, 
            this.startRowLength, this.stopRowPtr, this.stopRowLength, snapshotId);
      } else {
        this.curDataBlockScanner =
            DataBlockDirectMemoryScanner.getScanner(this.currentDataBlock, this.startRowPtr, 
              this.startRowLength, this.stopRowPtr, this.stopRowLength, snapshotId,
              this.curDataBlockScanner);
      }
      return this.curDataBlockScanner;
    } else if(!closed){
      if (!isMultiSafe) {
        this.curDataBlockScanner =
          DataBlockDirectMemoryScanner.getScanner(this.currentDataBlock, this.startRowPtr, 
            this.startRowLength, this.stopRowPtr, this.stopRowLength, snapshotId);
      } else {
        this.curDataBlockScanner =
            DataBlockDirectMemoryScanner.getScanner(this.currentDataBlock, this.startRowPtr, 
              this.startRowLength, this.stopRowPtr, this.stopRowLength, snapshotId,
              this.curDataBlockScanner);
      }
      if (this.indexBlock.isFirstIndexBlock() && this.startRowPtr == 0 &&
          this.curDataBlockScanner != null) {
        // skip system first entry : {0}{0}
        this.curDataBlockScanner.next();
      }
      return this.curDataBlockScanner;
    } else {
      return null;
    }
  }
    
  public boolean isClosed() {
    return this.closed;
  }
  @Override
  public void close() throws IOException {
    // do nothing yet
    closed = true;
    if(this.curDataBlockScanner != null) {
      try {
        this.curDataBlockScanner.close();
      } catch (IOException e) {
        
      }
    }
    // Unlock index block
    if (indexBlock != null) {
      indexBlock.readUnlock();
    } 
  }
  /**
   * Delete current Key
   * @return true if success, false - otherwise
   */
  public boolean delete() {
    //TODO
    return false;
  }
}
