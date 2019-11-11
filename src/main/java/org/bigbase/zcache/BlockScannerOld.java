package org.bigbase.zcache;

import java.io.Closeable;
import java.io.IOException;

import org.bigbase.carrot.RetryOperationException;
import org.bigbase.util.UnsafeAccess;
import org.bigbase.util.Utils;

/**
 * Thread unsafe implementation
 * TODO: stopRow logic
 */
public final class BlockScannerOld implements Closeable{

  /*
   * Start Row
   */
  byte[] startRow; // INCLUSIVE  
  /*
   * Stop Row
   */
  byte[] stopRow; // EXCLUSIVE   
  /*
   * Pointer to memory base
   */
  long ptr;
  /*
   * Current pointer
   */
  long curPtr;
  /*
   * Block size (max)
   */
  int blockSize;
  /*
   * Data size in this block
   */
  int dataSize;
  /*
   * Number of k-v's in this block
   */
  int numRecords;
  /*
   * Number of deleted records
   */
  int numDeletedRecords;
  /*
   * Thread local for memory buffer
   */
  static ThreadLocal<Long> memory = new ThreadLocal<Long>(); 
  /*
   * Thread local for scanner instance
   */
  static ThreadLocal<BlockScannerOld> scanner = new ThreadLocal<BlockScannerOld>() {
    @Override
    protected BlockScannerOld initialValue() {
      return new BlockScannerOld();
    }    
  };
      
  public static BlockScannerOld getScanner(Block b) 
      throws RetryOperationException {
    BlockScannerOld bs = scanner.get();
    bs.reset();
    bs.setBlock(b);
    return bs;
  }
  
  public static BlockScannerOld getScanner(Block b, byte[] startRow, byte[] stopRow) 
      throws RetryOperationException {
    BlockScannerOld bs = scanner.get();
    bs.reset();
    bs.setBlock(b);
    bs.setStartRow(startRow);
    bs.setStopRow(stopRow);
    return bs;
  }
  /** 
   * Private ctor
   */
  private BlockScannerOld() {
  }
  
  private void reset() {
    this.startRow = null;
    this.stopRow = null;
    this.ptr = 0;
    this.curPtr = 0;
    this.blockSize = 0;
    this.dataSize = 0;
    this.numRecords = 0;
    this.numDeletedRecords = 0;
  }
  
  private void setStartRow(byte[] row) {
    this.startRow = row;
    if (startRow != null) {
      search(startRow, 0, startRow.length);
      skipDeletedRecords();// One more time
    }
  }
  
  void search(byte[] key, int keyOffset, int keyLength) {
    long ptr = this.ptr;
    int count = 0;
    while (count++ < numRecords) {
      short keylen = UnsafeAccess.toShort(ptr);
      keylen = normalize(keylen);
      short vallen = UnsafeAccess.toShort(ptr + 2);
      int res =
          Utils.compareTo(key, keyOffset, keyLength, ptr + Block.RECORD_PREFIX_LENGTH, keylen);
      if (res <= 0) {
        this.curPtr = ptr;
        return;
      }
      ptr += keylen + vallen + Block.RECORD_PREFIX_LENGTH;
    }
    // after the last record
    this.curPtr = ptr + dataSize;
  }

  private void setStopRow(byte[] row) {
    this.stopRow = row;
  }
  
  private short normalize(short len) {
    short mask = 0x7fff;
    return (short) (mask & len);
  }
  /**
   * Set scanner with new block
   * @param b block
   * @throws RetryOperationException 
   */
  private void setBlock(Block b) throws RetryOperationException {
    
	try {

      b.readLock();
      this.blockSize = b.getMaxBlockSize();
      this.dataSize = b.getDataSize();
      this.numRecords = b.getNumRecords();
      this.numDeletedRecords = b.getNumDeletedRecords();

      if (memory.get() == null) {
        this.ptr = UnsafeAccess.malloc(this.blockSize);
        // TODO handle allocation failure
        memory.set(ptr);
      } else {
        this.ptr = memory.get();
      }
      this.curPtr = this.ptr;
      UnsafeAccess.copy(b.getAddress(), this.ptr, this.dataSize);
    } finally {
      b.readUnlock();
    }
    // Skip deleted records in the beginning of a block
    skipDeletedRecords();
  }
  
  /**
   * Check if has next
   * @return true, false
   */
  public final boolean hasNext() {
    skipDeletedRecords();
    return curPtr - ptr < dataSize;
  }
  
  /**
   * Advance scanner by one record
   * @return true, false
   */
  public final boolean next() {
    //skipDeletedRecords();
    if (curPtr - ptr < dataSize) {
      int keylen = UnsafeAccess.toShort(curPtr);
      int vallen = UnsafeAccess.toShort(curPtr + 2);
      curPtr += keylen + vallen + Block.RECORD_PREFIX_LENGTH;
      return true;
    } else {
      return false;
    }
  }

  /**
   * Get current address of a k-v in a scanner
   * @return address of a record
   */
  public long address() {
    return curPtr;
  }
  
  /**
   *  Get current key size (in bytes)
   * @return key size
   */
  public int keySize() {
    if (hasNext()) {
      // Can be negative?
      return UnsafeAccess.toShort(curPtr);     
    }
    return -1;
  }
  
  /**
   * Get current value size (in bytes)
   * @return value size
   */
  public int valueSize() {
    if (hasNext()) {
      return UnsafeAccess.toShort(curPtr + 2);     
    }
    return -1;
  }  
  
  /**
   * Skips deleted records
   * TODO" fix hang
   */
  final void skipDeletedRecords() {
    while (curPtr - ptr < dataSize) {
      short keylen = UnsafeAccess.toShort(curPtr);
      if (keylen > 0) {
        return;
      }
      keylen = normalize(keylen);
      short vallen = UnsafeAccess.toShort(curPtr + 2);
      curPtr += keylen + vallen + 4;      
    }
  }
  
  /**
   * Get key into buffer
   * @param buffer
   * @param offset
   */
  public void key(byte[] buffer, int offset) {
    int keylen = UnsafeAccess.toShort(curPtr);
    UnsafeAccess.copy( curPtr + 4 , buffer, offset, keylen);
  }
  
  /**
   * Get key into buffer
   * @param addr buffer address
   */
  public void key(long addr) {
    int keylen = UnsafeAccess.toShort(curPtr);
    UnsafeAccess.copy( curPtr + 4, addr, keylen);
  }
  
  /**
   * Get value into buffer
   * @param buffer
   * @param offset
   */
  public void value(byte[] buffer, int offset) {
    int keylen = UnsafeAccess.toShort(curPtr);
    int vallen = UnsafeAccess.toShort(curPtr + 2);
    UnsafeAccess.copy( curPtr + 4 + keylen, buffer, offset, vallen);
  }
  
  /**
   * Get value into buffer
   * @param addr
   */
  public void value(long addr) {
    int keylen = UnsafeAccess.toShort(curPtr);
    int vallen = UnsafeAccess.toShort(curPtr + 2);
    UnsafeAccess.copy( curPtr + 4 + keylen, addr, vallen);
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
    int keylen = UnsafeAccess.toShort(curPtr);
    int vallen = UnsafeAccess.toShort(curPtr + 2);
    UnsafeAccess.copy( curPtr + 4, keyBuffer,  keyOffset, keylen);
    UnsafeAccess.copy( curPtr + 4 + keylen, valueBuffer,  valueOffset, vallen);
  }
  
  /**
   * Get current key-value into buffer
   * @param addr address
   */
  public void keyValue(long addr) {
    int keylen = UnsafeAccess.toShort(curPtr);
    int vallen = UnsafeAccess.toShort(curPtr + 2);
    UnsafeAccess.copy( curPtr + 4, addr, keylen + vallen);
  }
  
  @Override
  public void close() throws IOException {
    // do nothing yet
  }
}
