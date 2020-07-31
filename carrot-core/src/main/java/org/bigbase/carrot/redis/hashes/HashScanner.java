package org.bigbase.carrot.redis.hashes;

import static org.bigbase.carrot.redis.Commons.NUM_ELEM_SIZE;

import java.io.IOException;

import org.bigbase.carrot.BigSortedMapDirectMemoryScanner;
import org.bigbase.carrot.redis.OperationFailedException;
import org.bigbase.carrot.util.BidirectionalScanner;
import org.bigbase.carrot.util.Utils;
/**
 * 
 * Scanner for hash field values.
 * TODO: pattern matching support (native C)
 * @author Vladimir Rodionov
 *
 */
public class HashScanner implements BidirectionalScanner{
  
  /**
   * Base scanner
   */
  private BigSortedMapDirectMemoryScanner mapScanner;
  
  /**
   * Start row
   */
  long startFieldPtr; // Inclusive
  
  /**
   * Start row size
   */
  int startFieldSize;
  
  /**
   * Stop row
   */
  
  long stopFieldPtr; // Exclusive
  
  /**
   * Stop row size
   */
  int stopFieldSize;
  
  /**
   * Current BigSortedMap value address
   */
  long valueAddress;
  /**
   * Current BigSortedMap value size
   */
  int valueSize;
  
  /**
   * Current offset in BSM value, which is <= valueSize
   */
  int offset;
  /**
   * Current hash field address
   */
  long fieldAddress;
  /**
   * Current hash value address
   */
  long fieldValueAddress;
  /**
   * Current hash field size
   */
  int fieldSize;
  /**
   * Current hash value size
   */
  int fieldValueSize;
  
  /**
   * Free keys on close
   */
  boolean disposeKeysOnClose = false;
  
  /**
   * Reverse scanner
   */
  boolean reverse;
  
  /**
   * Constructor
   * @param scanner base BSM scanner
   * @param keyPtr address of a start key
   * @throws OperationFailedException 
   */
  public HashScanner(BigSortedMapDirectMemoryScanner scanner) throws OperationFailedException {
    this(scanner, false);
  }
  
  /**
   * Constructor
   * @param scanner base BSM scanner
   * @param keyPtr address of a start key
   * @throws OperationFailedException 
   */
  public HashScanner(BigSortedMapDirectMemoryScanner scanner, boolean reverse) throws OperationFailedException {
    this(scanner, 0, 0, 0, 0, reverse);
  }
  
  public HashScanner(BigSortedMapDirectMemoryScanner scanner, long startFieldPtr, int startFieldSize, 
      long stopFieldPtr, int stopFieldSize, boolean reverse) throws OperationFailedException {
    this.mapScanner = scanner;
    this.reverse = reverse;
    setStartStopFields(startFieldPtr, startFieldSize, stopFieldPtr, stopFieldSize);
  }
  
  public void setDisposeKeysOnClose(boolean b) {
    this.disposeKeysOnClose = b;
  }
  

  
  /**
   * Sets start and stop field for the scanner
   * @param startPtr start field address
   * @param startSize start field size
   * @param stopPtr stop field address
   * @param stopSize stop field size
   * @throws OperationFailedException 
   */
  private void setStartStopFields(long startPtr, int startSize, long stopPtr, int stopSize) 
      throws OperationFailedException {
    this.valueAddress = mapScanner.valueAddress();
    this.valueSize = mapScanner.valueSize();
    this.offset = NUM_ELEM_SIZE;
    this.startFieldPtr = startPtr;
    this.startFieldSize = startSize;
    this.stopFieldPtr = stopPtr;
    this.stopFieldSize = stopSize;
    boolean result = false;
    if (!reverse && this.startFieldPtr > 0) {
      result = searchFirstMember();
    } else if (reverse) {
      result = searchLastMember();
    }
    if (!result) {
      try {
        close();
      } catch (IOException e) {
      }
      throw new OperationFailedException();
    }
    updateFields();
  }
  
  private boolean searchFirstMember() {
    if (this.startFieldPtr == 0) {
      this.offset = NUM_ELEM_SIZE;
      return true;
    }
    return seek(this.startFieldPtr, this.startFieldSize, false);
  }
  
  private boolean searchLastMember() {
    return seekLower(this.stopFieldPtr, this.stopFieldSize);
  }
  /**
   * Updates all current fields, after scanner advances by one field-value
   */
  private void updateFields() {
    this.valueAddress = mapScanner.valueAddress();
    this.valueSize = mapScanner.valueSize();
    this.fieldSize = Utils.readUVInt(valueAddress + offset);
    int fSizeSize = Utils.sizeUVInt(this.fieldSize);
    this.fieldValueSize = Utils.readUVInt(valueAddress + offset + fSizeSize);
    int vSizeSize = Utils.sizeUVInt(this.fieldValueSize);
    this.fieldAddress = valueAddress + offset + fSizeSize + vSizeSize;
    this.fieldValueAddress = this.fieldAddress + this.fieldSize;
  }
  /**
   * Returns total current field-value pair size
   * @return size of a field-value pair
   */
  
  private int fvSize() {
    return fieldSize + fieldValueSize + Utils.sizeUVInt(fieldSize) + Utils.sizeUVInt(fieldValueSize);
  }
  /**
   * Has next field-value
   * @return true, if yes, false - otherwise
   * @throws IOException
   */
  public boolean hasNext() throws IOException {
    if (reverse) {
      throw new UnsupportedOperationException("hasNext");
    }
    if (offset < valueSize && offset >= NUM_ELEM_SIZE) {
      if (this.stopFieldPtr > 0) {
        if (Utils.compareTo(this.stopFieldPtr, this.stopFieldSize, fieldAddress, fieldSize) <= 0) {
          return false;
        } else {
          return true;
        }
      } else {
        return true;
      }
    } else {
      return false;
    }
  }
  
  /**
   * Advances scanner by one field-value
   * @return true if success, false if end of scanner reached
   * @throws IOException
   */
  public boolean next() throws IOException {
    if (reverse) {
      throw new UnsupportedOperationException("next");
    }
    if (offset < valueSize) {     
      offset += fvSize();
      if (offset < valueSize) {
        updateFields();
        if (this.stopFieldPtr > 0) {
          if (Utils.compareTo(this.fieldAddress, this.fieldSize, this.stopFieldPtr, this.stopFieldSize) >=0) {
            this.offset = 0;
            return false;
          }
        }
        return true;
      }
    }
    // TODO next K-V can not have 0 elements - it must be deleted
    // but first element can, so we has to check what next() call return
    // the best way is to use do
    while(mapScanner.hasNext()) {
      mapScanner.next();
      this.valueAddress = mapScanner.valueAddress();
      this.valueSize = mapScanner.valueSize();
      // check if it it is not empty
      this.offset = NUM_ELEM_SIZE;
      if (valueSize <= NUM_ELEM_SIZE) {
        // empty set in K-V? Must be deleted
        continue;
      } else {
        updateFields();
        if (this.stopFieldPtr > 0) {
          if (Utils.compareTo(this.fieldAddress, this.fieldSize, this.stopFieldPtr, this.stopFieldSize) >=0) {
            this.offset = 0;
            return false;
          } else {
            return true;
          }
        }
        return true;
      }
    }
    this.offset = 0;
    return false;
  }
  
  /**
   * Seeks a position which is larger or equals to a given field
   * @param fieldPtr field address
   * @param fieldSize field's size
   * @param next if true, exactly next field is required
   * @return true if success, false if end of scanner reached
   */
  public boolean seek(long fieldPtr, int fieldSize, boolean next) {
    while (this.offset < this.valueSize) {
      int fSize = Utils.readUVInt(this.valueAddress + this.offset);
      int fSizeSize = Utils.sizeUVInt(fSize);
      int vSize = Utils.readUVInt(this.valueAddress + this.offset + fSizeSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      long fPtr = this.valueAddress + this.offset + fSizeSize + vSizeSize;

      if (fieldPtr > 0) {
        int res = Utils.compareTo(fPtr, fSize, fieldPtr, fieldSize);
        if (next) {
          if (res > 0) return true;
        } else {
          if (res >= 0) return true;
        }
      }
      if (this.offset + fSize + vSize + fSizeSize + vSizeSize == this.valueSize) {
        break;
      }
      this.offset += fSize + vSize + fSizeSize + vSizeSize;
    }

    return false;
  }
  
  /**
   * Seeks a maximum position which is strictly less than a given field
   * @param fieldPtr field address
   * @param fieldSize field's size
   * @return true if success, false if end of scanner reached
   */
  public boolean seekLower(long fieldPtr, int fieldSize) {
    int prevOff = -1;
    while (this.offset < this.valueSize) {
      int fSize = Utils.readUVInt(this.valueAddress + this.offset);
      int fSizeSize = Utils.sizeUVInt(fSize);
      int vSize = Utils.readUVInt(this.valueAddress + this.offset + fSizeSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      long fPtr = this.valueAddress + this.offset + fSizeSize + vSizeSize;

      if (fieldPtr > 0) {
        int res = Utils.compareTo(fPtr, fSize, fieldPtr, fieldSize);
        if (res >= 0) {
          if (prevOff < 0) {
            return false;
          } else {
            this.offset = prevOff;
            return true;
          }
        }
      }
      if (this.offset + fSize + vSize + fSizeSize + vSizeSize == this.valueSize) {
        break;
      }
      prevOff = this.offset;
      this.offset += fSize + vSize + fSizeSize + vSizeSize;
    }
    if (prevOff < 0) {
      return false;
    }
    this.offset = prevOff;
    return true;
  }
  
  /**
   * Returns current field's address
   * @return field address
   */
  public final long fieldAddress() {
    return this.fieldAddress;
  }
  
  /**
   * Returns field's size
   * @return field size
   */
  
  public final int fieldSize() {
    return this.fieldSize;
  }
  
  /**
   * Returns current value's address
   * @return value's address
   */

  public final long valueAddress() {
    return this.fieldValueAddress;
  }
  
  /**
   * Returns current value's size 
   * @return value's size
   */  
  public final int valueSize() {
    return this.fieldValueSize;
  }
  
  
  @Override
  public void close() throws IOException {
    mapScanner.close(disposeKeysOnClose);
  }
  /**
   * Delete current field-value
   * @return true if success, false - otherwise
   */
  public boolean delete() {
    //TODO
    return false;
  }
  
  /**
   * Delete all fields in this scanner
   * @return true on success, false?
   */
  public boolean deleteAll() {
    return false;
  }

  @Override
  public boolean first() throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean last() throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean previous() throws IOException {
    if (!reverse) {
      throw new UnsupportedOperationException("previous");
    }
    if (this.offset > NUM_ELEM_SIZE) {
      int off = NUM_ELEM_SIZE;
      while (off < this.offset) {
        int fSize = Utils.readUVInt(this.valueAddress + off);
        int fSizeSize = Utils.sizeUVInt(fSize);
        int vSize = Utils.readUVInt(this.valueAddress + off + fSizeSize);
        int vSizeSize = Utils.sizeUVInt(vSize);
        long fAddress = this.valueAddress + off + fSizeSize + vSizeSize;
        if (off + fSize + vSize + fSizeSize + vSizeSize == this.offset) {
          if (this.startFieldPtr > 0) {
            if (Utils.compareTo(this.startFieldPtr, this.startFieldSize, fAddress, fSize) > 0) {
              this.offset = 0;
              return false;
            }
          }
          this.offset = off;
          updateFields();
          return true;
        }
        off += fSize + vSize + fSizeSize + vSizeSize;
      }
    }
    while(mapScanner.hasPrevious()) {
      mapScanner.previous();
      updateFields();
      if (this.valueSize  <= NUM_ELEM_SIZE) {
        // empty
        continue;
      }
      if (!searchLastMember()) {
        this.offset = 0;
        return false;
      }
      updateFields();
      // Check startField
      if (this.startFieldPtr > 0) {
        if (Utils.compareTo(this.startFieldPtr, this.startFieldSize, this.fieldAddress, this.fieldSize) > 0) {
          this.offset = 0;
          return false;
        }
      }
      return true;
    }
    this.offset = 0;
    return false;  
  }

  /**
   * Using both hasPrevious and previous is overkill
   */
  @Override
  public boolean hasPrevious() throws IOException {
    if (!reverse) {
      throw new UnsupportedOperationException("hasPrevious");
    }
    if (this.offset >= NUM_ELEM_SIZE && this.offset < this.valueSize) {
      if (this.startFieldPtr > 0) {
        if (Utils.compareTo(this.startFieldPtr, this.startFieldSize, this.fieldAddress, this.fieldSize) > 0) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }
}