package org.bigbase.carrot.redis.sets;

import static org.bigbase.carrot.redis.Commons.NUM_ELEM_SIZE;

import java.io.IOException;

import org.bigbase.carrot.BigSortedMapDirectMemoryScanner;
import org.bigbase.carrot.redis.Commons;
import org.bigbase.carrot.util.Scanner;
import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Scanner to iterate through set members
 * @author Vladimir Rodionov
 */
public class SetScanner extends Scanner {

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

  boolean disposeKeysOnClose;

  /**
   * Constructor
   * @param scanner base scanner
   * @throws IOException 
   */
  public SetScanner(BigSortedMapDirectMemoryScanner scanner) throws IOException {
    this(scanner, 0, 0, 0, 0);
  }

  /**
   * Constructor
   * @param scanner base scanner
   * @param reverse true, if reverse scanner, false - otherwise
   * @throws IOException 
   */
  public SetScanner(BigSortedMapDirectMemoryScanner scanner, boolean reverse) throws IOException {
    this(scanner, 0, 0, 0, 0, reverse);
  }

  /**
   * Constructor for a range scanner
   * @param scanner base scanner
   * @param start start member address
   * @param startSize start member size
   * @param stop stop member address
   * @param stopSize stop member size
   * @throws IOException 
   */
  public SetScanner(BigSortedMapDirectMemoryScanner scanner, long start, int startSize, long stop,
      int stopSize) throws IOException {
   this(scanner, start, startSize, stop, stopSize, false);
  }

  /**
   * Constructor for a range scanner
   * @param scanner base scanner
   * @param start start member address
   * @param startSize start member size
   * @param stop stop member address
   * @param stopSize stop member size
   * @param reverse reverse scanner
   * @throws IOException 
   */
  public SetScanner(BigSortedMapDirectMemoryScanner scanner, long start, int startSize, long stop,
      int stopSize, boolean reverse) throws IOException {
    this.mapScanner = scanner;
    this.startMemberPtr = start;
    this.startMemberSize = startSize;
    this.stopMemberPtr = stop;
    this.stopMemberSize = stopSize;
    this.reverse = reverse;
    init();
  }

  @SuppressWarnings("unused")
  private void dumpLimits() {
    System.out.println(
      "start=" + (startMemberPtr > 0 ? Bytes.toHex(startMemberPtr, startMemberSize) : "0") + " end="
          + (stopMemberPtr > 0 ? Bytes.toHex(stopMemberPtr, stopMemberSize) : "0"));
  }

  @SuppressWarnings("unused")
  private void dumpKey() {
    long ptr = this.mapScanner.keyAddress();
    int size = this.mapScanner.keySize();
    System.out.println("KEY=" + Bytes.toHex(ptr, size));
  }

  /**
   * Get set member address from a set key
   * @param ptr set key
   * @return address
   */
  private long getMemberAddress(long ptr) {
    int keySize = UnsafeAccess.toInt(ptr + Utils.SIZEOF_BYTE);
    return ptr + keySize + Utils.SIZEOF_BYTE + Commons.KEY_SIZE;
  }

  /**
   * Get set member size
   * @param ptr set key 
   * @param size set key size
   * @return size of a member
   */
  private int getMemberSize(long ptr, int size) {
    return (int) (ptr + size - getMemberAddress(ptr));
  }

  /**
   * Dispose range keys on close()
   * @param b
   */
  public void setDisposeKeysOnClose(boolean b) {
    this.disposeKeysOnClose = b;
  }

  private void init() throws IOException {
    this.valueAddress = mapScanner.valueAddress();
    this.valueSize = mapScanner.valueSize();
    if (this.valueAddress == -1) {
      // Hack, rewind block scanner by one record back
      // This hack works, b/c when scanner seeks first record
      // which is *always* greater or equals to a startRow, but
      // we need the previous one, which is the largest row which is less
      // to a startRow.
      // TODO: Reverse scanner?
      mapScanner.getBlockScanner().prev();
      this.valueAddress = mapScanner.valueAddress();
      this.valueSize = mapScanner.valueSize();

    } else if (this.startMemberPtr > 0 && !reverse) {
      // Check if current key in a mapScanner is equals to start
      // This is a hack for direct scanner
      long ptr = mapScanner.keyAddress();
      int size = mapScanner.keySize();
      size = getMemberSize(ptr, size);
      ptr = getMemberAddress(ptr);
      if (Utils.compareTo(this.startMemberPtr, this.startMemberSize, ptr, size) != 0) {
        mapScanner.getBlockScanner().prev();
        this.valueAddress = mapScanner.valueAddress();
        this.valueSize = mapScanner.valueSize();
      }
    }
    if (reverse) {
      if (!searchLastMember()) {
        boolean result = previous();
        if (!result) {
          throw new IOException("Empty scanner");
        }
      }
    } else {
      searchFirstMember();
    }
  }

  private boolean searchFirstMember() {

    if (this.valueAddress <= 0) {
      return false;
    }
    this.offset = NUM_ELEM_SIZE;
    this.memberSize = Utils.readUVInt(this.valueAddress + this.offset);
    this.memberAddress = this.valueAddress + this.offset + Utils.sizeUVInt(this.memberSize);
    try {
      if (this.startMemberPtr != 0) {
        while (hasNext()) {
          int size = Utils.readUVInt(this.valueAddress + this.offset);
          long ptr = this.valueAddress + this.offset + Utils.sizeUVInt(size);
          int res = Utils.compareTo(ptr, size, this.startMemberPtr, this.startMemberSize);
          if (res >= 0) {
            return true;
          }
          next();
        }
      } else if (this.valueSize == NUM_ELEM_SIZE) { // skip possible empty K-V (first one)
        next();
      }
    } catch (IOException e) {
      // should never be thrown
    }
    return false;
  }

  private boolean searchLastMember() {
    
    if (this.valueAddress <= 0) {
      return false;
    }  
    this.valueAddress = mapScanner.valueAddress();
    this.valueSize = mapScanner.valueSize();
    // check if it it is not empty
    this.offset = NUM_ELEM_SIZE;
    int prevOffset = 0;
    while (this.offset <  this.valueSize) {
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

    // TODO: what is it for?
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
    if (this.valueAddress <= 0) {
      return false;
    }
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
      // First K-V in a set can be empty, we need to scan to the next one
      mapScanner.next();
      if (mapScanner.hasNext()) {
        this.valueAddress = mapScanner.valueAddress();
        this.valueSize = mapScanner.valueSize();
        this.offset = NUM_ELEM_SIZE;
        this.memberSize = Utils.readUVInt(valueAddress + offset);
        this.memberAddress = valueAddress + offset + Utils.sizeUVInt(this.memberSize);
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * Advance scanner by one element MUST BE USED IN COMBINATION with hasNext()
   * @return true if operation succeeded , false - end of scanner
   * @throws IOException
   */
  public boolean next() throws IOException {
    if (reverse) {
      throw new UnsupportedOperationException("next");
    }
    if (offset < valueSize) {
      int elSize = Utils.readUVInt(valueAddress + offset);
      int elSizeSize = Utils.sizeUVInt(elSize);
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

    // TODO: fix previous, hashScanner.next previous

    mapScanner.next();

    while (mapScanner.hasNext()) {
      this.valueAddress = mapScanner.valueAddress();
      this.valueSize = mapScanner.valueSize();
      // check if it it is not empty
      this.offset = NUM_ELEM_SIZE;
      if (valueSize <= NUM_ELEM_SIZE) {
        // empty set in K-V? Must be deleted
        mapScanner.next();
        continue;
      }

      this.memberSize = Utils.readUVInt(valueAddress + offset);
      this.memberAddress = valueAddress + offset + Utils.sizeUVInt(this.memberSize);

      if (this.stopMemberPtr > 0) {

        if (Utils.compareTo(this.memberAddress, this.memberSize, this.stopMemberPtr,
          this.stopMemberSize) >= 0) {
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
    while (this.pos < pos) {
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
    mapScanner.close(disposeKeysOnClose);
  }

  /**
   * Delete current Element
   * @return true if success, false - otherwise
   */
  public boolean delete() {
    // TODO
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

  /**
   * To iterate reverse scan use the following pattern
   * do {} while(scanner.previous())
   *  
   */
  @Override
  public boolean previous() throws IOException {
    if (!reverse) {
      throw new UnsupportedOperationException("previous");
    }
    if (this.offset > NUM_ELEM_SIZE) {
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
      if (this.startMemberPtr > 0) {
        int result = Utils.compareTo(this.memberAddress, this.memberSize, 
          this.startMemberPtr, this.startMemberSize);
        if (result < 0) {
          return false;
        }
      }
      return true;
    }
    
    boolean result = mapScanner.previous();
    if (result) {
      return searchLastMember();
    } else {
      return false;
    }
  }
  
  /**
   * Do not use it. 
   */
  @Override
  public boolean hasPrevious() throws IOException {
    throw new UnsupportedOperationException("hasPrevious");

  }
}