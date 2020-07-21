package org.bigbase.carrot;

import java.io.IOException;

import org.bigbase.carrot.util.BidirectionalScanner;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Thread unsafe implementation
 * TODO: stopRow logic
 */
public final class DataBlockScanner implements BidirectionalScanner{

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
   * Is first block
   */
  boolean isFirst;
  /*
   * Thread local for scanner instance.
   * Multiple instances UNSAFE (can not be used in multiple 
   * instances in context of a one thread)
   */
  static ThreadLocal<DataBlockScanner> scanner = new ThreadLocal<DataBlockScanner>() {
    @Override
    protected DataBlockScanner initialValue() {
      return new DataBlockScanner();
    }    
  };
      
  /**
   * This method is to call when single instance of DataBlockScanner is 
   * expected inside one thread operation
   * @param b data block
   * @param startRow start row
   * @param stopRow stop row
   * @param snapshotId snapshotID
   * @return scanner instance
   * @throws RetryOperationException
   */
  public static DataBlockScanner getScanner(DataBlock b, byte[] startRow, byte[] stopRow,
      long snapshotId) throws RetryOperationException {

    DataBlockScanner bs = scanner.get();
    bs.reset();
    if (!b.isValid() || b.isEmpty()) {
      // Return null for now
      return null;
    }
    if (startRow != null && stopRow != null) {
      if (Utils.compareTo(startRow, 0, startRow.length, stopRow, 0, stopRow.length) >=0) {
        return null;
      }
    }
    bs.setBlock(b);
    bs.setSnapshotId(snapshotId);
    bs.setStartRow(startRow);
    bs.setStopRow(stopRow);
    return bs;
  }
  
  /**
   * This method is to call when multiple instances of DataBlockScanners are 
   * expected inside one thread operation
   * @param b data block
   * @param startRow start row
   * @param stopRow stop row
   * @param snapshotId snapshotID
   * @param bs scanner to reuse
   * @return scanner instance
   * @throws RetryOperationException
   */
  public static DataBlockScanner getScanner(DataBlock b, byte[] startRow, byte[] stopRow,
      long snapshotId, DataBlockScanner bs) throws RetryOperationException {

    if (bs == null) {
      bs = new DataBlockScanner();
    }
    if (!b.isValid() || b.isEmpty()) {
      // Return null for now
      return null;
    }
    if (startRow != null && stopRow != null) {
      if (Utils.compareTo(startRow, 0, startRow.length, stopRow, 0, stopRow.length) >=0) {
        return null;
      }
    }
    bs.reset();
    bs.setBlock(b);
    bs.setSnapshotId(snapshotId);
    bs.setStartRow(startRow);
    bs.setStopRow(stopRow);
    return bs;
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
    this.isFirst = false;
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
      int keylen = DataBlock.keyLength(ptr);
      int vallen = DataBlock.valueLength(ptr);
      int res =
          Utils.compareTo(key, keyOffset, keyLength, DataBlock.keyAddress(ptr), keylen);
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
  void searchBefore(byte[] key, int keyOffset, int keyLength, long snapshotId, Op type) {
    long ptr = this.ptr;
    long prevPtr = ptr;
    int count = 0;
    while (count++ < numRecords) {
      int keylen = DataBlock.keyLength(ptr);
      int vallen = DataBlock.valueLength(ptr);
      int res =
          Utils.compareTo(key, keyOffset, keyLength, DataBlock.keyAddress(ptr), keylen);
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
  
  
  private void setStopRow(byte[] row) {
    this.stopRow = row;
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
    this.isFirst = b.isFirstBlock();
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
      int res = DataBlock.compareTo(this.curPtr, stopRow, 0, stopRow.length, 0, Op.DELETE);
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
      if (stopRow != null) {
        int res = DataBlock.compareTo(this.curPtr, stopRow, 0, stopRow.length, 0, Op.DELETE);
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
   *  Get current key address
   * @return key address
   */
  public final long keyAddress() {
    if (this.curPtr - this.ptr >= this.dataSize) {
      return -1;
    }
    return DataBlock.keyAddress(this.curPtr);
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
  
  private final long advanceByOneRecord(long ptr) {
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
  
  public long keyVersion() {
    // TODO Auto-generated method stub
    return DataBlock.version(curPtr);
  }
  
  public Op keyOpType() {
    return DataBlock.getRecordType(curPtr);
  }

  @Override
  public boolean first() {
    this.curPtr = this.ptr;
    if (this.isFirst) { 
      int keylen = DataBlock.blockKeyLength(this.ptr);
      int vallen = DataBlock.blockValueLength(this.ptr);
      this.curPtr += keylen + vallen + DataBlock.RECORD_TOTAL_OVERHEAD;
    }
    if (startRow != null) {
      int res = DataBlock.compareTo(this.curPtr, startRow, 0, startRow.length, 0, Op.PUT);
      if (res <= 0) {
        return true;
      }
    } else {
      return true;
    }
    // OK now we should repeat scan with checking startRow

    this.curPtr = this.ptr;
    while(this.curPtr < this.ptr + dataSize) {
      if (startRow != null) {
        int res = DataBlock.compareTo(this.curPtr, this.startRow, 0, this.startRow.length, 0, Op.PUT);
        if (res <= 0) {
          break;
        }
      }
      int keylen = DataBlock.blockKeyLength(this.curPtr);
      int vallen = DataBlock.blockValueLength(this.curPtr);
      this.curPtr += keylen + vallen + DataBlock.RECORD_TOTAL_OVERHEAD;
    }
    return true;
  }

  /**
   * Set to the last record
   */
  @Override
  public boolean last() {
    
    long prev = 0;
    this.curPtr = isFirst? this.ptr +DataBlock.RECORD_TOTAL_OVERHEAD + 2: this.ptr;
    while(this.curPtr < this.ptr + dataSize) {
      prev = curPtr;
      int keylen = DataBlock.blockKeyLength(this.curPtr);
      int vallen = DataBlock.blockValueLength(this.curPtr);
      this.curPtr += keylen + vallen + DataBlock.RECORD_TOTAL_OVERHEAD;
    }
    this.curPtr = prev;
    
    if (stopRow != null) {
      int res = DataBlock.compareTo(this.curPtr, this.stopRow, 0, this.stopRow.length, 0, Op.PUT);
      if (res > 0) {
        this.curPtr = prev;
        return true;
      }
    } else {
      this.curPtr = prev;
      return true;
    } 
    prev = 0;
    // OK now we should repeat scan with checking stopRow
    this.curPtr = isFirst? this.ptr +DataBlock.RECORD_TOTAL_OVERHEAD + 2: this.ptr;
    while(this.curPtr < this.ptr + dataSize) {
      if (stopRow != null) {
        int res = DataBlock.compareTo(this.curPtr, this.stopRow, 0, this.stopRow.length, 0, Op.PUT);
        if (res <= 0) {
          break;
        }
      }
      prev = curPtr;
      int keylen = DataBlock.blockKeyLength(this.curPtr);
      int vallen = DataBlock.blockValueLength(this.curPtr);
      this.curPtr += keylen + vallen + DataBlock.RECORD_TOTAL_OVERHEAD;
    }
    if (prev == 0) {
      return false;
    }
    this.curPtr = prev;
    return true;
  }

  @Override
  public boolean previous() {
    long limit = isFirst ? this.ptr + DataBlock.RECORD_TOTAL_OVERHEAD + 2 : this.ptr;
    long pptr = limit;
    if (pptr == this.curPtr) {
      return false;
    }
    while (pptr < this.curPtr) {
      int keylen = DataBlock.blockKeyLength(pptr);
      int vallen = DataBlock.blockValueLength(pptr);
      if (pptr + keylen + vallen + DataBlock.RECORD_TOTAL_OVERHEAD == this.curPtr) {
        break;
      } else {
        pptr += keylen + vallen + DataBlock.RECORD_TOTAL_OVERHEAD;
      }
    }

    if (startRow != null) {
      int res = DataBlock.compareTo(pptr, startRow, 0, startRow.length, 0, Op.PUT);
      if (res > 0) {
        return false;
      }
    }

    this.curPtr = pptr;

    return true;
  }

  @Override
  public boolean hasPrevious() {
    long limit = isFirst ? this.ptr + DataBlock.RECORD_TOTAL_OVERHEAD + 2 : this.ptr;
    long pptr = limit;
    if (pptr == this.curPtr) {
      return false;
    }
    while (pptr < this.curPtr) {
      int keylen = DataBlock.blockKeyLength(pptr);
      int vallen = DataBlock.blockValueLength(pptr);
      if (pptr + keylen + vallen + DataBlock.RECORD_TOTAL_OVERHEAD == this.curPtr) {
        break;
      } else {
        pptr += keylen + vallen + DataBlock.RECORD_TOTAL_OVERHEAD;
      }
    }

    if (startRow != null) {
      int res = DataBlock.compareTo(pptr, startRow, 0, startRow.length, 0, Op.PUT);
      if (res > 0) {
        return false;
      }
    }
    return true;
  }
}
