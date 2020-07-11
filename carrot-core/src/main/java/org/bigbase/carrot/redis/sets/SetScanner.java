package org.bigbase.carrot.redis.sets;

import static org.bigbase.carrot.redis.Commons.NUM_ELEM_SIZE;

import java.io.Closeable;
import java.io.IOException;

import org.bigbase.carrot.BigSortedMapDirectMemoryScanner;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Scanner to iterate through set members
 * @author Vladimir Rodionov
 *
 */
public class SetScanner implements Closeable{
  
  private long keyPtr;
  private BigSortedMapDirectMemoryScanner mapScanner;
  long valueAddress;
  int valueSize;
  int offset;
  int elementSize;
  long elementAddress;
  long pos = 1;
  /**
   * Constructor
   * @param scanner base scanner
   * @param keyPtr key address to free on close
   */
  public SetScanner(BigSortedMapDirectMemoryScanner scanner, long keyPtr) {
    this.keyPtr = keyPtr;
    this.mapScanner = scanner;
    init();
  }
  
  private void init() {
    // TODO Auto-generated method stub
    this.valueAddress = mapScanner.valueAddress();
    this.valueSize = mapScanner.valueSize();
    this.offset = NUM_ELEM_SIZE;
    if (this.valueSize > NUM_ELEM_SIZE) {
      this.elementSize = Utils.readUVInt(valueAddress + offset);
      this.elementAddress = valueAddress + offset + Utils.sizeUVInt(this.elementSize);
    } else {
      this.elementAddress = this.valueAddress + this.offset;
      this.elementSize = 0;
    }
    
  }

  /**
   * Checks if scanner has next element
   * @return true, false
   * @throws IOException
   */
  public boolean hasNext() throws IOException {
    if (offset < valueSize) {
      return elementAddress + elementSize + Utils.sizeUVInt(elementSize) 
        < valueAddress + valueSize;
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
   * Advance scanner by one element
   * @return true if operation succeeded , false - end of scanner
   * @throws IOException
   */
  public boolean next() throws IOException {
    if (offset < valueSize) {
      int elSize = Utils.readUVInt(valueAddress + offset);
      int elSizeSize =Utils.sizeUVInt(elSize);
      offset += elSize + elSizeSize;
      if (offset < valueSize) {
        this.elementSize = Utils.readUVInt(valueAddress + offset);
        this.elementAddress = valueAddress + offset + Utils.sizeUVInt(this.elementSize);
        pos++;
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
        this.elementSize = Utils.readUVInt(valueAddress + offset);
        this.elementAddress = valueAddress + offset + Utils.sizeUVInt(this.elementSize);
        pos++;
        return true;
      }
    }
    return false;
  }
  
  /**
   * Get current element address
   * @return element address
   */
  public long elementAddress() {
    return this.elementAddress;
  }
  
  /**
   * Gets current element size
   * @return element size
   */
  public int elementSize() {
    return this.elementSize;
  }
  
  public long getPosition() {
    return this.pos;
  }
  
  /**
   * Skips to position
   * @param pos position to skip
   * @return current position (can be less than pos)
   */
  public long skipTo(long pos) {
    if (pos <= this.pos) return this.pos;
    while(this.pos < pos) {
      try {
        boolean res = next();
        if (!res) break;
      } catch (IOException e) {
        // Should not throw
      }
    }
    return this.pos;
  }
  
  @Override
  public void close() throws IOException {
    UnsafeAccess.free(keyPtr);
    mapScanner.close();
  }
  /**
   * Delete current Element
   * @return true if success, false - otherwise
   */
  public boolean delete() {
    //TODO
    return false;
  }
  
  /**
   * Delete all Elements in this scanner
   * @return true on success, false?
   */
  public boolean deleteAll() {
    return false;
  }
}