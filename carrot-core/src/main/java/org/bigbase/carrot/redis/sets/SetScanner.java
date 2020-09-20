package org.bigbase.carrot.redis.sets;

import static org.bigbase.carrot.redis.Commons.NUM_ELEM_SIZE;

import java.io.IOException;

import org.bigbase.carrot.BigSortedMapDirectMemoryScanner;
import org.bigbase.carrot.util.BiScanner;
import org.bigbase.carrot.util.Utils;

/**
 * Scanner to iterate through set members
 * @author Vladimir Rodionov
 *
 */
public class SetScanner extends BiScanner{
  
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
  
  /*
   * Reverse scanner
   */
  boolean reverse;
  
  /**
   * Constructor
   * @param scanner base scanner
   */
  public SetScanner(BigSortedMapDirectMemoryScanner scanner) {
    this(scanner, 0, 0, 0, 0);
  }
  
  
  /**
   * Constructor
   * @param scanner base scanner
   * @param reverse true, if reverse scanner, false - otherwise
   */
  public SetScanner(BigSortedMapDirectMemoryScanner scanner, boolean reverse) {
    this(scanner, 0, 0, 0, 0, reverse);
  }
  
  /**
   * Constructor for a range scanner
   * @param scanner base scanner
   * @param start start member address 
   * @param startSize start member size
   * @param stop stop member address
   * @param stopSize stop member size
   */
  public SetScanner(BigSortedMapDirectMemoryScanner scanner, long start, int startSize, 
      long stop, int stopSize) {
    this.mapScanner = scanner;
    this.startMemberPtr = start;
    this.startMemberSize = startSize;
    this.stopMemberPtr = stop;
    this.stopMemberSize = stopSize;
    init();
  }
  
  /**
   * Constructor for a range scanner
   * @param scanner base scanner
   * @param start start member address 
   * @param startSize start member size
   * @param stop stop member address
   * @param stopSize stop member size
   * @param reverse reverse scanner
   */
  public SetScanner(BigSortedMapDirectMemoryScanner scanner, long start, int startSize, 
      long stop, int stopSize, boolean reverse) {
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
    if (reverse) {
      searchLastMember();
    } else {
      searchFirstMember();
    }
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
  
  private boolean searchLastMember() {
    this.valueAddress = mapScanner.valueAddress();
    this.valueSize = mapScanner.valueSize();
    // check if it it is not empty
    this.offset = NUM_ELEM_SIZE;
    int prevOffset = 0;
    while (this.offset < this.valueAddress + this.valueSize) {
      int size = Utils.readUVInt(valueAddress + offset);
      long ptr = valueAddress + offset + Utils.sizeUVInt(size);
      if (stopMemberPtr > 0) {
        int res = Utils.compareTo(ptr, size, this.stopMemberPtr, this.stopMemberSize);
        if (res >= 0) {
          break;
        }
      }
      prevOffset = offset;
      offset += size + Utils.sizeUVInt(size);
    }
    this.offset = prevOffset;
    if (this.offset == 0) {
      return false;
    }
    
    if (startMemberPtr > 0) {
      int size = Utils.readUVInt(valueAddress + offset);
      long ptr = valueAddress + offset + Utils.sizeUVInt(size);
      int res = Utils.compareTo(ptr, size, this.startMemberPtr, this.startMemberSize);
      if (res < 0) {
        this.offset = 0;
        return false;
      }
    }
    this.memberSize = Utils.readUVInt(valueAddress + offset);
    this.memberAddress = valueAddress + offset + Utils.sizeUVInt(this.memberSize);
    return true;
  }
  /**
   * Checks if scanner has next element
   * @return true, false
   * @throws IOException
   */
  public boolean hasNext() throws IOException {
    if (reverse) {
      throw new UnsupportedOperationException("hasNext");
    }
    if (offset < valueSize && offset >= NUM_ELEM_SIZE) {
      if (stopMemberPtr == 0) {
        return true;
      } else {
        if (Utils.compareTo(this.memberAddress, this.memberSize, this.stopMemberPtr,
          this.stopMemberSize) >= 0) {
          return false;
        } else {
          return true;
        }
      }
    } else {
      return false;
    }
  }
  
  /**
   * Advance scanner by one element 
   * MUST BE USED IN COMBINATION with hasNext()
   * @return true if operation succeeded , false - end of scanner
   * @throws IOException
   */
  public boolean next() throws IOException {
    if (reverse) {
      throw new UnsupportedOperationException("next");
    }
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
      } 
      
      this.memberSize = Utils.readUVInt(valueAddress + offset);
      this.memberAddress = valueAddress + offset + Utils.sizeUVInt(this.memberSize);
      
      if (this.stopMemberPtr > 0) {
        
        if (Utils.compareTo(this.memberAddress, this.memberSize, 
          this.stopMemberPtr, this.stopMemberSize) >=0) {
          this.offset = 0;
          return false;
        } else {
          pos++;
          return true;
        }
      } else {
        pos++;
        return true;
      }
    }
    this.offset = 0;
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
    if (this.offset > NUM_ELEM_SIZE && this.offset < this.valueSize) {
      int off = NUM_ELEM_SIZE;
      while (off < this.offset) {
        int mSize = Utils.readUVInt(this.valueAddress + off);
        int mSizeSize = Utils.sizeUVInt(mSize);
        if (off + mSize + mSizeSize >= this.offset) {
          break;
        }
        off += mSize + mSizeSize;
      }
      this.offset = off;
      this.memberSize = Utils.readUVInt(valueAddress + offset);
      this.memberAddress = valueAddress + offset + Utils.sizeUVInt(this.memberSize);
      return true;
    }
    if (mapScanner.hasPrevious()) {
      mapScanner.previous();
      this.valueAddress = mapScanner.valueAddress();
      this.valueSize = mapScanner.valueSize();
      this.offset = NUM_ELEM_SIZE;
    } else {
      this.offset = 0;
      return false;
    }
    return searchLastMember();
  }


  @Override
  public boolean hasPrevious() throws IOException {
    if (!reverse) {
      throw new UnsupportedOperationException("hasPrevious");
    } 
    if (this.offset >= NUM_ELEM_SIZE && this.offset < this.valueSize) {
      // TODO check startMemberPtr
      if (this.startMemberPtr > 0) {
        if (Utils.compareTo(this.memberAddress, this.memberSize, this.startMemberPtr, this.startMemberSize) < 0) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }     
  }
}