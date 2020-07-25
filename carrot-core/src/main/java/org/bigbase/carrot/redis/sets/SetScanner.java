package org.bigbase.carrot.redis.sets;

import static org.bigbase.carrot.redis.Commons.NUM_ELEM_SIZE;

import java.io.Closeable;
import java.io.IOException;

import org.bigbase.carrot.BigSortedMapDirectMemoryScanner;
import org.bigbase.carrot.util.Utils;

/**
 * Scanner to iterate through set members
 * @author Vladimir Rodionov
 *
 */
public class SetScanner implements Closeable{
  
  /*
   * Base Map scanner
   */
  private BigSortedMapDirectMemoryScanner mapScanner;
  /*
   * Minimum member (inclusive)
   */
  long startMemberPtr;
  /*
   * Minimum member size
   */
  int startMemberSize;
  /*
   * Maximum member (exclusive) 
   */
  long stopMemberPtr;
  /*
   * Maximum member size
   */
  int stopMemberSize;
  
  /*
   * Current value address
   */
  long valueAddress;
  /*
   * Current value size
   */
  int valueSize;
  /*
   * Current offset in the value
   */
  int offset;
  
  /*
   * Current member size
   */
  int memberSize;
  /*
   * Current member address
   */
  long memberAddress;
  /*
   * Current position (index)
   */
  long pos = 1;
  /**
   * Constructor
   * @param scanner base scanner
   * @param keyPtr key address to free on close
   */
  public SetScanner(BigSortedMapDirectMemoryScanner scanner) {
    this.mapScanner = scanner;
    init();
  }
  
  
  public SetScanner(BigSortedMapDirectMemoryScanner scanner, long start, int startSize, 
      long stop, int stopSize) {
    this.mapScanner = scanner;
    this.startMemberPtr = start;
    this.startMemberSize = startSize;
    this.stopMemberPtr = stop;
    this.stopMemberSize = stopSize;
    init();
  }
  
  private void init() {
    // TODO Auto-generated method stub
    this.valueAddress = mapScanner.valueAddress();
    this.valueSize = mapScanner.valueSize();
    searchFirstMember();
  }
  
  private void searchFirstMember() {
    if (this.startMemberPtr == 0) {
      this.offset = NUM_ELEM_SIZE;
    } else {
      try {
        while(hasNext()) {
          int size = Utils.readUVInt(valueAddress + offset);
          long ptr = valueAddress + offset + Utils.sizeUVInt(size);
          int res = Utils.compareTo(ptr, size, this.startMemberPtr, this.startMemberSize);
          if (res >= 0) {
            return;
          }
          next();
        }
      } catch (IOException e) {
        // should never be thrown
      }
    }  
  }
  /**
   * Checks if scanner has next element
   * @return true, false
   * @throws IOException
   */
  public boolean hasNext() throws IOException {
    if (offset < valueSize) {
      long nextPtr =  memberAddress + memberSize + Utils.sizeUVInt(memberSize);
      if (nextPtr < valueAddress + valueSize) {
        if (stopMemberPtr == 0) {
          return true;
        } else {
          int nextSize =  Utils.readUVInt(nextPtr);
          nextPtr += Utils.sizeUVInt(nextSize);
          if (Utils.compareTo(nextPtr, nextSize, this.stopMemberPtr, this.stopMemberSize) >=0) {
            return false;
          } else {
            return true;
          }
        }
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
      } else if (this.stopMemberPtr > 0) {
        int nextSize = Utils.readUVInt(valueAddress + offset);
        long nextPtr = valueAddress + offset + Utils.sizeUVInt(memberSize);
        if (Utils.compareTo(nextPtr, nextSize, this.stopMemberPtr, this.stopMemberSize) >=0) {
          return false;
        } else {
          return true;
        }
      } else {
        return true;
      }
    }
    return false;
  }
  
  /**
   * Advance scanner by one element 
   * MUST BE USED IN COMBINATION with hasNext()
   * @return true if operation succeeded , false - end of scanner
   * @throws IOException
   */
  public boolean next() throws IOException {
    if (offset < valueSize) {
      int elSize = Utils.readUVInt(valueAddress + offset);
      int elSizeSize =Utils.sizeUVInt(elSize);
      offset += elSize + elSizeSize;
      if (offset < valueSize) {
        this.memberSize = Utils.readUVInt(valueAddress + offset);
        this.memberAddress = valueAddress + offset + Utils.sizeUVInt(this.memberSize);
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
  public long memberAddress() {
    return this.memberAddress;
  }
  
  /**
   * Gets current element size
   * @return element size
   */
  public int memberSize() {
    return this.memberSize;
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
    mapScanner.close(true);
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