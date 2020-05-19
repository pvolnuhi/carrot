package org.bigbase.carrot;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
   * Index block to scan
   */
  private IndexBlock indexBlock;
  
  /*
   * Current block scanner
   */
  private DataBlockScanner curDataBlockScanner;
  
  /*
   * Start index in a list of a data blocks
   */
  int blockIndex = 0;
  
  /*
   * Stop index in a list of data blocks (if any)
   */
  int stopIndex = -1;
  
  /*
   * Snapshot Id
   */
  private long snapshotId;
  
  /*
   * Number of data blocks in the index block
   */
  private int numBlocks = 0;
  
  /*
   * Thread local for scanner instance
   */
  static ThreadLocal<IndexBlockScanner> scanner = new ThreadLocal<IndexBlockScanner>() {
    @Override
    protected IndexBlockScanner initialValue() {
      return new IndexBlockScanner();
    }    
  };
  
  static ThreadLocal<List<DataBlock>> blockList = new ThreadLocal<List<DataBlock>>() {

    @Override
    protected List<DataBlock> initialValue() {
      // TODO Auto-generated method stub
      return new ArrayList<DataBlock>();
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
    IndexBlockScanner bs = getScanner(b, snapshotId);
    bs.setStartStopRows(startRow, stopRow);
    return bs;
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
    this.snapshotId = 0;
  }
  
  //TODO - change
  private void setStartStopRows(byte[] start, byte[] stop) {
    this.startRow = start;
    this.stopRow = stop;
    DataBlock b = this.startRow != null
        ? indexBlock.searchBlock(blockList.get(), this.startRow, 0, this.startRow.length,
          snapshotId, Op.DELETE)
        : blockList.get().get(0);
    if (b == null) {
      // FATAL
      throw new RuntimeException("Index block scanner");
    }
    // Set start index
    this.blockIndex = blockList.get().indexOf(b);
    this.curDataBlockScanner =
        DataBlockScanner.getScanner(b, this.startRow, this.stopRow, snapshotId);
    // Set stop index
    if (this.stopRow != null) {
      DataBlock bb = indexBlock.searchBlock(blockList.get(), this.stopRow, 0, this.stopRow.length, snapshotId, 
        Op.DELETE);
      if (bb == null) {
        // FATAL
        throw new RuntimeException("Index block scanner");
      }
      this.stopIndex = blockList.get().indexOf(bb);
    }
  }
 
  /**
   * Set scanner with new block
   * @param b block
   * @throws RetryOperationException 
   */
  private void setBlock(IndexBlock b) throws RetryOperationException {
    this.indexBlock = b;
    List<DataBlock> list = blockList.get();
    this.numBlocks = b.getDataBlocks(list);
    blockList.set(list);
  }
  
  /**
   * Check if has next
   * @return true, false
   */
  public final boolean hasNextBlockScanner() {
	  if (this.blockIndex == this.numBlocks -1) {
	    return false;
	  }
	  
	  if (this.stopIndex >=0) {
	    return this.blockIndex < this.stopIndex;
	  } else {
	    return true;
	  }
  }
  
  /**
   * Advance scanner by one data block
   * @return data block scanner or null
   */
  public final DataBlockScanner nextBlockScanner() {
    if (this.blockIndex == this.numBlocks - 1) {
      return null;
    }
    if (this.stopIndex >=0) {
      if (this.blockIndex < this.stopIndex) {
        return nextBlockScannerInternal();
      } else {
        return null;
      }
    } else {
      return nextBlockScannerInternal();
    }
  }
  
    
  private DataBlockScanner nextBlockScannerInternal() {
    // No checks are required
    if(this.curDataBlockScanner != null) {
      try {
        this.curDataBlockScanner.close();
      } catch (IOException e) {
        //TODO Auto-generated catch block
        //e.printStackTrace();
      }
    }
    this.blockIndex++;
    DataBlock b = blockList.get().get(this.blockIndex);
    this.curDataBlockScanner =
        DataBlockScanner.getScanner(b, this.startRow, this.stopRow, snapshotId);
    return this.curDataBlockScanner;
  }

  @Override
  public void close() throws IOException {
    // do nothing yet
    if(this.curDataBlockScanner != null) {
      try {
        this.curDataBlockScanner.close();
      } catch (IOException e) {
        //TODO Auto-generated catch block
        //e.printStackTrace();
      }
    }
  }
}
