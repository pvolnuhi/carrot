package org.bigbase.carrot;

import java.io.Closeable;
import java.io.IOException;

import org.bigbase.util.UnsafeAccess;
import org.bigbase.util.Utils;

/**
 * Thread unsafe implementation
 * TODO: stopRow logic
 */
public final class DataBlockScanner implements Closeable{

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
   * DataBlock size (max)
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
   * Maximum sequenceId (snapshotId)
   * We consider only records with sequenceId < snapshotId
   */
  long snapshotId;
  /*
   * Thread local for memory buffer
   */
  static ThreadLocal<Long> memory = new ThreadLocal<Long>(); 
  /*
   * Thread local for scanner instance
   */
  static ThreadLocal<DataBlockScanner> scanner = new ThreadLocal<DataBlockScanner>() {
    @Override
    protected DataBlockScanner initialValue() {
      return new DataBlockScanner();
    }    
  };
      
  public static DataBlockScanner getScanner(DataBlock b, long snapshotId) 
      throws RetryOperationException {
    DataBlockScanner bs = scanner.get();
    bs.reset();
    bs.setBlock(b);
    bs.setSnapshotId(snapshotId);
    return bs;
  }
  
  public static DataBlockScanner getScanner(DataBlock b, byte[] startRow, byte[] stopRow,
      long snapshotId) throws RetryOperationException {

    // TODO valid block
    try {
      DataBlockScanner bs = scanner.get();
      bs.reset();
      b.readLock();
      if (!b.isValid()) {
        // Return null for now
        return null;
      }
      bs.setBlock(b);
      bs.setSnapshotId(snapshotId);
      bs.setStartRow(startRow);
      bs.setStopRow(stopRow);
      return bs;
    } finally {
      b.readUnlock();
    }
  }
  /** 
   * Private ctor
   */
  private DataBlockScanner() {
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
    this.snapshotId = Long.MAX_VALUE;
  }
  
  private void setStartRow(byte[] row) {
    this.startRow = row;
    if (startRow != null) {
      search(startRow, 0, startRow.length, snapshotId, Op.DELETE);
      skipDeletedAndIrrelevantRecords();// One more time
    } else {
      this.curPtr = this.ptr;
    }
  }
  
  /**
   * Search first record, which is greater or equal to a given key
   * @param key key array
   * @param keyOffset offset in a key array
   * @param keyLength  length of a key in bytes
   * @param snapshotId snapshot Id of a scanner
   * @param type op type 
   */
  void search(byte[] key, int keyOffset, int keyLength, long snapshotId, Op type) {
    long ptr = this.ptr;
    int count = 0;
    while (count++ < numRecords) {
      short keylen = UnsafeAccess.toShort(ptr);
      short vallen = UnsafeAccess.toShort(ptr + DataBlock.KEY_SIZE_LENGTH);
      int res =
          Utils.compareTo(key, keyOffset, keyLength, ptr + DataBlock.RECORD_PREFIX_LENGTH, keylen);
      if (res < 0) {
        this.curPtr = ptr;
        return;
      } else if (res == 0) {
        // check version
        long version = DataBlock.version(ptr);
        if (version < this.snapshotId) {
          this.curPtr = ptr;
          return;
        } else if (version == this.snapshotId) {
          Op type_ = DataBlock.type(ptr);
          if (type.ordinal() <= type_.ordinal()) {
            this.curPtr = ptr;
            return;
          }
        }
      }
      ptr += keylen + vallen + DataBlock.RECORD_TOTAL_OVERHEAD;
    }
    // after the last record
    this.curPtr = this.ptr + dataSize;
  }

  private void setStopRow(byte[] row) {
    this.stopRow = row;
  }
  
  /**
   * Set scanner with new block
   * @param b block
   * @throws RetryOperationException 
   */
  private void setBlock(DataBlock b) throws RetryOperationException {
    
	try {

      b.readLock();
      this.blockSize = BigSortedMap.maxBlockSize;
      this.dataSize = b.getDataSize();
      this.numRecords = b.getNumberOfRecords();
      this.numDeletedRecords = b.getNumberOfDeletedAndUpdatedRecords();

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

  }
  
  protected void setSnapshotId(long snapshotId) {
    this.snapshotId = snapshotId;
  }
  
  protected long getSnapshotId() {
    return this.snapshotId;
  }
  
  /**
   * Check if has next
   * @return true, false
   */
  public final boolean hasNext() {
    skipDeletedAndIrrelevantRecords();
    if (this.curPtr - this.ptr >= this.dataSize) {
      return false;
    } else if (stopRow != null) {
      int res = DataBlock.compareTo(this.curPtr, stopRow, 0, stopRow.length, Long.MAX_VALUE, Op.DELETE);
      if (res >= 0) {
        return true;
      } else {
        return false;
      }
    } else {
      return true;
    }
  }
  public int getOffset() {
    return (int) (this.curPtr - this.ptr);
  }
  /**
   * Advance scanner by one record
   * @return true, false
   */
  public final boolean next() {
    skipDeletedAndIrrelevantRecords();
    if (this.curPtr - this.ptr < this.dataSize) {
      int keylen = DataBlock.keyLength(this.curPtr);
      int vallen = DataBlock.valueLength(this.curPtr);
      this.curPtr += keylen + vallen + DataBlock.RECORD_TOTAL_OVERHEAD;
      if (stopRow != null) {
        int res = DataBlock.compareTo(this.curPtr, stopRow, 0, stopRow.length, Long.MAX_VALUE, Op.DELETE);
        if (res >= 0) {
          return true;
        } else {
          return false;
        }     
      } else {
        return true;
      }
    } else {
      return false;
    }
  }

  /**
   * Get current address of a k-v in a scanner
   * @return address of a record
   */
  public final long address() {
    return this.curPtr;
  }
  
  /**
   *  Get current key size (in bytes)
   * @return key size
   */
  public final int keySize() {
    if (this.curPtr - this.ptr >= this.dataSize) {
      return -1;
    }
    return DataBlock.keyLength(this.curPtr);
  }
  
  /**
   * Get current value size (in bytes)
   * @return value size
   */
  public final int valueSize() {
    if (this.curPtr - this.ptr >= this.dataSize) {
      return -1;
    }
    return DataBlock.valueLength(this.curPtr);
  }  
  
  /**
   * Skips deleted records
   * TODO" fix hang
   */
  final void skipDeletedAndIrrelevantRecords() {
    while (this.curPtr - this.ptr < this.dataSize) {
      long version = DataBlock.version(this.curPtr);
      Op type = DataBlock.type(this.curPtr);
      short keylen = DataBlock.keyLength(this.curPtr);
      short vallen = DataBlock.valueLength(this.curPtr);

      if (version > this.snapshotId) {
        this.curPtr += keylen + vallen + DataBlock.RECORD_TOTAL_OVERHEAD;
      } else if (type == Op.DELETE) {
        //skip all deleted records - the same key
        long keyAddress = DataBlock.keyAddress(this.curPtr);
        this.curPtr += keylen + vallen + DataBlock.RECORD_TOTAL_OVERHEAD;
        while (this.curPtr - this.ptr < this.dataSize) {
          long kaddr = DataBlock.keyAddress(this.curPtr);
          short klen = DataBlock.keyLength(this.curPtr);
          if( Utils.compareTo(kaddr, klen, keyAddress, keylen) != 0) {
            break;
          } else {
            short vlen = DataBlock.valueLength(this.curPtr);
            this.curPtr += klen + vlen + DataBlock.RECORD_TOTAL_OVERHEAD;
          }
        }
      } else {
        break;
      }
    }
  }
  
  /**
   * Get key into buffer
   * @param buffer
   * @param offset
   * @return key length if was success or not enough room size, -1 if scanner is done
   */
  public int key(byte[] buffer, int offset) {
    if (this.curPtr - this.ptr >= this.dataSize) {
      return -1;
    }
    int keylen = DataBlock.keyLength(this.curPtr);
    if (keylen > buffer.length - offset) {
      return keylen;
    }
    UnsafeAccess.copy( DataBlock.keyAddress(this.curPtr) , buffer, offset, keylen);
    return keylen;
  }
  
  /**
   * Get key into buffer
   * @param addr buffer address
   * @param len  length of a buffer
   * @return key length if was success or not enough room size, -1 if scanner is done
   */
  public int key(long addr, int len) {
    if (this.curPtr - this.ptr >= this.dataSize) {
      return -1;
    }
    int keylen = DataBlock.keyLength(this.curPtr);
    if (keylen > len) {
      return keylen;
    }
    UnsafeAccess.copy(DataBlock.keyAddress(this.curPtr), addr, keylen);
    return keylen;
  }
  
  /**
   * Get value into buffer
   * @param buffer
   * @param offset
   * @return value length if was success or not enough room size, -1 if scanner is done
   */
  public int value(byte[] buffer, int offset) {
    if (this.curPtr - this.ptr >= this.dataSize) {
      return -1;
    }
    int vallen = DataBlock.valueLength(this.curPtr);
    if (vallen > buffer.length - offset) {
      return vallen;
    }
    long address = DataBlock.valueAddress(this.curPtr);
    UnsafeAccess.copy( address, buffer, offset, vallen);
    return vallen;
  }
  
  /**
   * Get value into buffer
   * @param addr buffer address
   * @param len length of a buffer
   * @return value length if was success or not enough room size, -1 if scanner is done
   */
  public int value(long addr, int len) {
    if (this.curPtr - this.ptr >= this.dataSize) {
      return -1;
    }
    int vallen = DataBlock.valueLength(this.curPtr);
    if (vallen > len) {
      return vallen;
    }
    long address = DataBlock.valueAddress(this.curPtr);
    UnsafeAccess.copy( address, addr, vallen);
    return vallen;
  }

  /**
   * Get current key - value
   * @param keyBuffer
   * @param keyOffset
   * @param valueBuffer
   * @param valueOffset
   * @return value + key length if was success or not enough room size, -1 if scanner is done
   */
  public int keyValue (byte[] buffer, int offset)
  {
    if (this.curPtr - this.ptr >= this.dataSize) {
      return -1;
    }
    int keylen = DataBlock.keyLength(curPtr);
    int vallen = DataBlock.valueLength(curPtr);
    if (keylen + vallen > buffer.length - offset) {
      return keylen + vallen;
    }
    UnsafeAccess.copy( DataBlock.keyAddress(this.curPtr), buffer,  offset, keylen);
    UnsafeAccess.copy( DataBlock.valueAddress(this.curPtr), buffer,  offset + keylen, vallen);
    return keylen + vallen;
  }
  
  /**
   * Get current key-value into buffer
   * @param addr buffer address
   * @param len buffer length in bytes
   * @return value + key length if was success or not enough room size, -1 if scanner is done
   */
  public int keyValue(long addr, int len) {
    if (this.curPtr - this.ptr >= this.dataSize) {
      return -1;
    }
    int keylen = DataBlock.keyLength(curPtr);
    int vallen = DataBlock.valueLength(curPtr);
    if (keylen + vallen > len) {
      return keylen + vallen;
    }
    UnsafeAccess.copy( DataBlock.keyAddress(this.curPtr), addr, keylen);
    UnsafeAccess.copy( DataBlock.valueAddress(this.curPtr), addr + keylen, vallen);
    return keylen + vallen;
  }
  
  @Override
  public void close() throws IOException {
    // do nothing yet
  }
}
