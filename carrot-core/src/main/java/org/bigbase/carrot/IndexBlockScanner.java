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
   * Is safe to use in multiple instances inside one thread?
   */
  private boolean isMultiSafe = false;
 
  /*
   * Thread local for scanner instance.
   * Multiple instances UNSAFE (can not be used in multiple 
   * instances in context of a one thread)
   */
  static ThreadLocal<IndexBlockScanner> scanner = new ThreadLocal<IndexBlockScanner>() {
    @Override
    protected IndexBlockScanner initialValue() {
      return new IndexBlockScanner();
    }    
  };
  
  /**
   * Get or create new instance of a scanner
   * @param b data block
   * @param snapshotId snapshot id
   * @param create create new if true, otherwise use thread local
   * @return new scanner instance
   * @throws RetryOperationException
   */
  private static IndexBlockScanner getScanner(IndexBlock b, long snapshotId) 
      throws RetryOperationException {
    IndexBlockScanner bs = scanner.get();
    bs.reset();
    bs.setIndexBlock(b);
    bs.snapshotId = snapshotId;
    return bs;
  }
  /**
   * Get  instance of a scanner
   * @param b data block
   * @param snapshotId snapshot id
   * @param create create new if true, otherwise use thread local
   * @return new scanner instance
   * @throws RetryOperationException
   */
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
   * Get new instance of a scanner (multiple instance safe)
   * @param b index block
   * @param snapshotId snapshot id
   * @param bs scanner to reuse
   * @return new scanner instance
   * @throws RetryOperationException
   */
  public static IndexBlockScanner getScanner(IndexBlock b, byte[] startRow, byte[] stopRow, long snapshotId,
      IndexBlockScanner bs) 
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
      if(bs == null) {
        bs = new IndexBlockScanner();
      };
      bs.reset();
      bs.setMultiInstanceSafe(true);
      bs.setIndexBlock(b);
      bs.snapshotId = snapshotId;
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
    this.isMultiSafe = false;
  }
  
  private void setStartStopRows(byte[] start, byte[] stop) {
    this.startRow = start;
    this.stopRow = stop;
    // Returns IndexBlock thread local if isMutiSafe = false
    DataBlock b = null;
    if (!isMultiSafe) {
      b = this.startRow != null
        ? indexBlock.searchBlock( this.startRow, 0, this.startRow.length,
          snapshotId, Op.DELETE)
        : indexBlock.firstBlock();
        
    } else {
      b = new DataBlock();
      b = this.startRow != null
          ? indexBlock.searchBlock( this.startRow, 0, this.startRow.length,
            snapshotId, Op.DELETE, b)
          : indexBlock.firstBlock(b);
    }
    // It can not be null
    this.currentDataBlock = b;
    if (b == null) {
      // FATAL
      throw new RuntimeException("Index block scanner");
    }
    this.currentDataBlock.decompressDataBlockIfNeeded();
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
  private void setIndexBlock(IndexBlock b) throws RetryOperationException {
    this.indexBlock = b;
  }

  /**
   * Advance scanner by one data block
   * @return data block scanner or null
   */
  int count = 1;
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
      // Compress current block if needed
      this.currentDataBlock.compressDataBlockIfNeeded();
      this.currentDataBlock = this.indexBlock.nextBlock(this.currentDataBlock, isMultiSafe);

      if (this.currentDataBlock == null) {
        this.closed = true;
        return null;
      } else {
        this.currentDataBlock.decompressDataBlockIfNeeded();
      }
      
      //TODO: check stopRow, version and op
      if(stopRow != null) {
        if (this.currentDataBlock.compareTo(stopRow, 0, stopRow.length, 0, Op.DELETE) < 0) {
          this.currentDataBlock.compressDataBlockIfNeeded();
          this.currentDataBlock = null;
          this.closed = true;
          return null;
        }
      }
      if (!isMultiSafe) {
        this.curDataBlockScanner =
          DataBlockScanner.getScanner(this.currentDataBlock, this.startRow, this.stopRow, snapshotId);
      } else {
        this.curDataBlockScanner =
            DataBlockScanner.getScanner(this.currentDataBlock, this.startRow, this.stopRow, snapshotId,
              this.curDataBlockScanner );
      }
      return this.curDataBlockScanner;
    } else if(!closed){
      
      if (!isMultiSafe) {
        this.curDataBlockScanner =
          DataBlockScanner.getScanner(this.currentDataBlock, this.startRow, this.stopRow, snapshotId);
      } else {
        this.curDataBlockScanner =
            DataBlockScanner.getScanner(this.currentDataBlock, this.startRow, this.stopRow, snapshotId,
              this.curDataBlockScanner );
      }
      
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
    if (this.currentDataBlock != null) {
      this.currentDataBlock.compressDataBlockIfNeeded();
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
