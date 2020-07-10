package org.bigbase.carrot.redis.hashes;

import static org.bigbase.carrot.redis.Commons.NUM_ELEM_SIZE;

import java.io.Closeable;
import java.io.IOException;

import org.bigbase.carrot.BigSortedMapDirectMemoryScanner;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
/**
 * 
 * Scanner for hash field values.
 * TODO: pattern matching support (native C)
 * @author Vladimir Rodionov
 *
 */
public class HashScanner implements Closeable{
  
  /**
   * Start key address (must be deallocated on close)
   */
  private long keyPtr;
  /**
   * Base scanner
   */
  private BigSortedMapDirectMemoryScanner mapScanner;
  
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
   * Constructor
   * @param scanner base BSN scanner
   * @param keyPtr address of a start key
   */
  public HashScanner(BigSortedMapDirectMemoryScanner scanner, long keyPtr) {
    this.keyPtr = keyPtr;
    this.mapScanner = scanner;
    init();
  }
  
  private void init() {
    this.valueAddress = mapScanner.valueAddress();
    this.valueSize = mapScanner.valueSize();
    this.offset = NUM_ELEM_SIZE;
    if (this.valueSize > NUM_ELEM_SIZE) {
      updateFields();
    } 
  }
  
  /**
   * Updates all current fields, after scanner advances by one field-value
   */
  private void updateFields() {
    this.fieldSize = Utils.readUVInt(valueAddress + offset);
    int fSizeSize = Utils.sizeUVInt(this.fieldSize);
    this.fieldAddress = valueAddress + offset + fSizeSize;
    this.fieldValueSize = Utils.readUVInt(this.fieldAddress + this.fieldSize);
    this.fieldValueAddress = this.fieldAddress + this.fieldSize + Utils.sizeUVInt(this.fieldValueSize);
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
    if (offset < valueSize) {
      return offset + fvSize() < valueSize;
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
        return true;
      }
    }
    return false;
  }
  
  /**
   * Advances scanner by one field-value
   * @return true if success, false if end of scanner reached
   * @throws IOException
   */
  public boolean next() throws IOException {
    if (offset < valueSize) {     
      offset += fvSize();
      if (offset < valueSize) {
        updateFields();
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
        return true;
      }
    }
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
    try {
      while(hasNext()) {
        long fPtr = fieldAddress();
        int fSize = fieldSize();
        int res = Utils.compareTo(fPtr, fSize, fieldPtr, fieldSize);
        if (next) {
          if (res > 0) return true;
        } else {
          if (res >= 0) return true;
        }
        next();
      }
    } catch (IOException e) {
      // should never be thrown
    }
    return false;
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
    if (keyPtr != 0) {
      UnsafeAccess.free(keyPtr);
    }
    mapScanner.close();
  }
  
}