package org.bigbase.carrot;

import java.io.Closeable;
import java.io.IOException;

import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Thread unsafe implementation
 */
public final class DataBlockDirectMemoryScanner implements Closeable{

  /*
   * Start Row pointer
   */
  long startRowPtr = 0; // INCLUSIVE
  
  /*
   * Start row length
   */
  int startRowLength;
  /*
   * Stop Row pointer
   */
  long stopRowPtr = 0; // EXCLUSIVE
  
  /*
   *  Stop row length
   */
  int stopRowLength;
  
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
   * Thread local for scanner instance.
   * Multiple instances UNSAFE (can not be used in multiple 
   * instances in context of a one thread)
   */
  static ThreadLocal<DataBlockDirectMemoryScanner> scanner = 
      new ThreadLocal<DataBlockDirectMemoryScanner>() {
    @Override
    protected DataBlockDirectMemoryScanner initialValue() {
      return new DataBlockDirectMemoryScanner();
    }    
  };
      
  /**
   * Call this method when single instance is expected inside one thread operation
   * @param b data block
   * @param startRowPtr start row address
   * @param startRowLength start row length
   * @param stopRowPtr stop row address 
   * @param stopRowLength stop row length
   * @param snapshotId snapshot id
   * @return new instance of a scanner
   * @throws RetryOperationException
   */
  public static DataBlockDirectMemoryScanner getScanner(DataBlock b, long startRowPtr,
      int startRowLength, long stopRowPtr, int stopRowLength, long snapshotId)
      throws RetryOperationException {

    DataBlockDirectMemoryScanner bs = scanner.get();
    bs.reset();
    if (!b.isValid()) {
      // Return null for now
      return null;
    }
    bs.setBlock(b);
    bs.setSnapshotId(snapshotId);
    bs.setStartRow(startRowPtr, startRowLength);
    bs.setStopRow(stopRowPtr, stopRowLength);
    return bs;
  }
  
  /**
   * Call this method when multiple instances are expected inside 
   * one thread operation
   * @param b data block
   * @param startRowPtr start row address
   * @param startRowLength start row length
   * @param stopRowPtr stop row address 
   * @param stopRowLength stop row length
   * @param snapshotId snapshot id
   * @param scanner scanner to reuse
   * @return new instance of a scanner
   * @throws RetryOperationException
   */
  public static DataBlockDirectMemoryScanner getScanner(DataBlock b, long startRowPtr,
      int startRowLength, long stopRowPtr, int stopRowLength, long snapshotId, 
      DataBlockDirectMemoryScanner bs)
      throws RetryOperationException {

    if (bs == null) { 
      bs = new DataBlockDirectMemoryScanner();
    }
    if (!b.isValid()) {
      // Return null for now
      return null;
    }
    bs.setBlock(b);
    bs.setSnapshotId(snapshotId);
    bs.setStartRow(startRowPtr, startRowLength);
    bs.setStopRow(stopRowPtr, stopRowLength);
    return bs;
  }
  /** 
   * Private ctor
   */
  private DataBlockDirectMemoryScanner() {
  }
  
  private void reset() {
    this.startRowPtr = 0;
    this.startRowLength = 0;
    this.stopRowPtr = 0;
    this.stopRowLength = 0;
    this.ptr = 0;
    this.curPtr = 0;
    this.blockSize = 0;
    this.dataSize = 0;
    this.numRecords = 0;
    this.numDeletedRecords = 0;
    this.snapshotId = Long.MAX_VALUE;
  }
  
  private void setStartRow(long ptr, int len) {
    this.startRowPtr = ptr;
    this.startRowLength = len;
    if (startRowPtr != 0) {
      search(startRowPtr, startRowLength, snapshotId, Op.DELETE);
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
  void search(long key, int keyLength, long snapshotId, Op type) {
    long ptr = this.ptr;
    int count = 0;
    while (count++ < numRecords) {
      int keylen = DataBlock.keyLength(ptr);
      int vallen = DataBlock.valueLength(ptr);
      int res =
          Utils.compareTo(key, keyLength, DataBlock.keyAddress(ptr), keylen);
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
          Op type_ = DataBlock.getRecordType(ptr);
          if (type.ordinal() <= type_.ordinal()) {
            this.curPtr = ptr;
            return;
          }
        }
      }
      keylen = DataBlock.blockKeyLength(ptr);
      vallen = DataBlock.blockValueLength(ptr);
      ptr += keylen + vallen + DataBlock.RECORD_TOTAL_OVERHEAD;

    }
    // after the last record
    this.curPtr = this.ptr + dataSize;
  }

  /**
   * Search last record, which is less or equals to a given key
   * @param key key array
   * @param keyOffset offset in a key array
   * @param keyLength  length of a key in bytes
   * @param snapshotId snapshot Id of a scanner
   * @param type op type 
   */
  void searchBefore(long key, int keyLength, long snapshotId, Op type) {
    long ptr = this.ptr;
    long prevPtr = ptr;
    int count = 0;
    while (count++ < numRecords) {
      int keylen = DataBlock.keyLength(ptr);
      int vallen = DataBlock.valueLength(ptr);
      int res =
          Utils.compareTo(key, keyLength, DataBlock.keyAddress(ptr), keylen);
      if (res < 0) {
        this.curPtr = prevPtr;
        return;
      } else if (res == 0) {
        // check version
        long version = DataBlock.version(ptr);
        if (version < this.snapshotId) {
          this.curPtr = ptr;
          return;
        } else if (version == this.snapshotId) {
          Op type_ = DataBlock.getRecordType(ptr);
          if (type.ordinal() <= type_.ordinal()) {
            this.curPtr = ptr;
            return;
          }
        }
      }
      keylen = DataBlock.blockKeyLength(ptr);
      vallen = DataBlock.blockValueLength(ptr);
      prevPtr = ptr;
      ptr += keylen + vallen + DataBlock.RECORD_TOTAL_OVERHEAD;

    }
    // after the last record
    this.curPtr = this.ptr + dataSize;
  }
  
  private void setStopRow(long ptr, int len) {
    this.stopRowPtr = ptr;
    this.stopRowLength = len;
  }
  
  /**
   * TODO: deep copy of data block including external allocations
   * Or keep read lock until block is released
   * Set scanner with new block
   * @param b block
   * @throws RetryOperationException 
   */
  private void setBlock(DataBlock b) throws RetryOperationException {
    
      this.blockSize = BigSortedMap.maxBlockSize;
      this.dataSize = b.getDataInBlockSize();
      this.numRecords = b.getNumberOfRecords();
      this.numDeletedRecords = b.getNumberOfDeletedAndUpdatedRecords();
      this.ptr = b.getAddress(); 
      this.curPtr = this.ptr;
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
    } else if (stopRowPtr != 0) {
      int res = DataBlock.compareTo(this.curPtr, stopRowPtr, stopRowLength, 0, Op.DELETE);
      if (res > 0) {
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
      int keylen = DataBlock.blockKeyLength(this.curPtr);
      int vallen = DataBlock.blockValueLength(this.curPtr);
      this.curPtr += keylen + vallen + DataBlock.RECORD_TOTAL_OVERHEAD;
      if(this.curPtr - this.ptr >= this.dataSize) {
        return false;
      }
      if (stopRowPtr != 0) {
        int res = DataBlock.compareTo(this.curPtr, stopRowPtr, stopRowLength, 0, Op.DELETE);
        if (res > 0) {
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
   * Advance backwards by one from a a given key if 
   * not equals to this key
   * @param keyPtr key address
   * @param keySize key size
   */
  public final void previous(long keyPtr, int keySize) {
    long kPtr = keyAddress();
    int  kSize = keySize();
    if (Utils.compareTo(kPtr, kSize, keyPtr, keySize) <= 0) {
      return ;
    }
    searchBefore(keyPtr, keySize, snapshotId, Op.DELETE);
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
   * TODO: fix this code
   */
  final void skipDeletedAndIrrelevantRecords() {
    while (this.curPtr - this.ptr < this.dataSize) {
      long version = DataBlock.version(this.curPtr);
      Op type = DataBlock.getRecordType(this.curPtr);
      short keylen = DataBlock.blockKeyLength(this.curPtr);
      short vallen = DataBlock.blockValueLength(this.curPtr);

      if (version > this.snapshotId) {
        this.curPtr += keylen + vallen + DataBlock.RECORD_TOTAL_OVERHEAD;
      } else if (type == Op.DELETE) {
        //skip all deleted records - the same key
        long keyAddress = DataBlock.keyAddress(this.curPtr);
        int keyLen = DataBlock.keyLength(this.curPtr);

        this.curPtr += keylen + vallen + DataBlock.RECORD_TOTAL_OVERHEAD;
        while (this.curPtr - this.ptr < this.dataSize) {
          long kaddr = DataBlock.keyAddress(this.curPtr);
          int klen = DataBlock.keyLength(this.curPtr);
          
          if( Utils.compareTo(kaddr, klen, keyAddress, keyLen) != 0) {
            break;
          } else {
            this.curPtr = advanceByOneRecord(this.curPtr);
          }
        }
      } else {
        break;
      }
    }
  }
  
  private long advanceByOneRecord(long ptr) {
    short vlen = DataBlock.blockValueLength(ptr);
    int klen = DataBlock.blockKeyLength(ptr);
    return ptr + klen + vlen + DataBlock.RECORD_TOTAL_OVERHEAD;
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
  }
  /**
   *  Get current key address
   * @return key address
   */
  public final long valueAddress() {
    if (this.curPtr - this.ptr >= this.dataSize) {
      return -1;
    }
    return DataBlock.valueAddress(this.curPtr);
  }

  /**
   *  Get current key address
   * @return key address
   */
  public final long keyAddress() {
    if (this.curPtr - this.ptr >= this.dataSize) {
      return -1;
    }
    return DataBlock.keyAddress(this.curPtr);
  }
  
  public long keyVersion() {
    // TODO Auto-generated method stub
    return DataBlock.version(curPtr);
  }
  
  public Op keyOpType() {
    return DataBlock.getRecordType(curPtr);
  }
}
