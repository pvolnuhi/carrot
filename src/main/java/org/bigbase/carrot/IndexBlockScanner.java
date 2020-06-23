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
   * Start Row
   */
  byte[] startRow; // INCLUSIVE  
  /*
   * Stop Row
   */
  byte[] stopRow; // EXCLUSIVE  
  
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
  
  private boolean closed = false;
  
  /*
   * Thread local for scanner instance
   */
  static ThreadLocal<IndexBlockScanner> scanner = new ThreadLocal<IndexBlockScanner>() {
    @Override
    protected IndexBlockScanner initialValue() {
      return new IndexBlockScanner();
    }    
  };
  
  
  private static IndexBlockScanner getScanner(IndexBlock b, long snapshotId) 
      throws RetryOperationException {
    IndexBlockScanner bs = scanner.get();
    bs.reset();
    bs.setBlock(b);
    bs.snapshotId = snapshotId;
    return bs;
  }
  
  public static IndexBlockScanner getScanner(IndexBlock b, byte[] startRow, byte[] stopRow, long snapshotId) 
      throws RetryOperationException {
    if (stopRow != null) {
      byte[] firstKey = b.getFirstKey();
      int res = Utils.compareTo(firstKey, 0, firstKey.length, stopRow, 0, stopRow.length);
      if (res > 0) {
        return null; // out of range
      }
    }    
    try {     
      // Lock index block   
      b.readLock();
      IndexBlockScanner bs = getScanner(b, snapshotId);
      bs.setStartStopRows(startRow, stopRow);
      return bs;
    } catch(RetryOperationException e) {
      b.readUnlock();
      throw e;
    }
  }
  /** 
   * Private ctor
   */
  private IndexBlockScanner() {
  }
  
  private void reset() {
    this.startRow = null;
    this.stopRow = null;
    this.indexBlock = null;
    
    this.curDataBlockScanner = null;
    this.currentDataBlock = null;
    this.snapshotId = 0;
    this.closed = false;
  }
  
  private void setStartStopRows(byte[] start, byte[] stop) {
    this.startRow = start;
    this.stopRow = stop;
    // Returns IndexBlock thread local
    DataBlock b = this.startRow != null
        ? indexBlock.searchBlock( this.startRow, 0, this.startRow.length,
          snapshotId, Op.DELETE)
        : indexBlock.firstBlock();
        
    // It can not be null
    this.currentDataBlock = b;
    if (b == null) {
      // FATAL
      throw new RuntimeException("Index block scanner");
    }

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
      this.currentDataBlock = this.indexBlock.nextBlock(this.currentDataBlock);

      if (this.currentDataBlock == null) {
        this.closed = true;
        return null;
      }
      
      //TODO: check stopRow, version and op
      if(stopRow != null) {
        if (this.currentDataBlock.compareTo(stopRow, 0, stopRow.length, 0, Op.DELETE) < 0) {
          this.currentDataBlock = null;
          this.closed = true;
          return null;
        }
      }
      this.curDataBlockScanner =
          DataBlockScanner.getScanner(this.currentDataBlock, this.startRow, this.stopRow, snapshotId);
      return this.curDataBlockScanner;
    } else if(!closed){
      this.curDataBlockScanner =
          DataBlockScanner.getScanner(this.currentDataBlock, this.startRow, this.stopRow, snapshotId);
      if (this.indexBlock.isFirstIndexBlock() && this.startRow == null &&
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
}
