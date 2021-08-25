/**
 *    Copyright (C) 2021-present Carrot, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 */
package org.bigbase.carrot.redis.sets;

import static org.bigbase.carrot.redis.util.Commons.NUM_ELEM_SIZE;

import java.io.IOException;

import org.bigbase.carrot.BigSortedMapScanner;
import org.bigbase.carrot.redis.util.Commons;
import org.bigbase.carrot.util.Scanner;
import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Scanner to iterate through set members
 * 
 */
public class SetScanner extends Scanner {

  /*
   * This is used to speed up reverse scanner.
   * during initialization process of a next Value blob,
   * when we seek last element, we calculate all offsets along the way
   * from the beginning of a Value blob till the last element 
   */
  static ThreadLocal<Long> offsetBuffer = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      long ptr = UnsafeAccess.malloc(4096); // More than enough to keep reverse scanner offsets 
      return ptr;
    }
  };
  
  /*
   * Keeps index for offsetBuffer
   */
  int offsetIndex = 0;
  /*
   * Base Map scanner
   */
  private BigSortedMapScanner mapScanner;
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
   * Number of members in a current value 
   */
  int valueNumber;
  /*
   * Current offset in the value (bytes)
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
   * Current global position (index)
   */
  long position = 0;
  /*
   * Position at current value 
   */
  int pos = 0;
  /*
   * Reverse scanner
   */
  boolean reverse;
  /*
   * Free memory for keys on close
   */
  boolean disposeKeysOnClose;

  /**
   * Constructor
   * @param scanner base scanner
   * @throws IOException 
   */
  public SetScanner(BigSortedMapScanner scanner) throws IOException {
    this(scanner, 0, 0, 0, 0);
  }

  /**
   * Constructor
   * @param scanner base scanner
   * @param reverse true, if reverse scanner, false - otherwise
   * @throws IOException 
   */
  public SetScanner(BigSortedMapScanner scanner, boolean reverse) throws IOException {
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
  public SetScanner(BigSortedMapScanner scanner, long start, int startSize, long stop,
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
  public SetScanner(BigSortedMapScanner scanner, long start, int startSize, long stop,
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
  @SuppressWarnings("unused")
  private int getMemberSize(long ptr, int size) {
    return (int) (ptr + size - getMemberAddress(ptr));
  }

  /**
   * Dispose range keys on close()
   * @param b true - dispose, false - don't
   */
  public void setDisposeKeysOnClose(boolean b) {
    this.disposeKeysOnClose = b;
  }

  
  /**
   * Get value number (number elements in a current value)
   * @return value number
   */
  public int getValueNumber() {
    return this.valueNumber;
  }
  
  /**
   *  Main initialization routine
   */
  private void init() throws IOException {
    this.valueAddress = mapScanner.valueAddress();
    this.valueSize = mapScanner.valueSize();
    if (this.valueAddress == -1) {
      throw new IOException("Empty scanner");
    }

    this.valueNumber = Commons.numElementsInValue(this.valueAddress);

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

  private final void setOffsetIndexValue(int index, short value) {
    long ptr = offsetBuffer.get();
    UnsafeAccess.putShort(ptr + Utils.SIZEOF_SHORT * index, value);
  }
  
  private final int getOffsetByIndex(int index) {
    long ptr = offsetBuffer.get();
    return UnsafeAccess.toShort(ptr + Utils.SIZEOF_SHORT * index);
  }
  
  private boolean searchLastMember() {
    
    if (this.valueAddress <= 0) {
      return false;
    }  
    this.valueAddress = mapScanner.valueAddress();
    this.valueSize = mapScanner.valueSize();
    this.valueNumber = Commons.numElementsInValue(this.valueAddress);

    // check if it it is not empty
    this.offset = NUM_ELEM_SIZE;
    this.offsetIndex = -1;    
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
      setOffsetIndexValue(++this.offsetIndex, (short)prevOffset);
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
        this.valueNumber = Commons.numElementsInValue(this.valueAddress);

        this.offset = NUM_ELEM_SIZE;
        this.memberSize = Utils.readUVInt(valueAddress + offset);
        this.memberAddress = valueAddress + offset + Utils.sizeUVInt(this.memberSize);
        position++; pos = 0;
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
        position++; pos++;
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
      this.valueNumber = Commons.numElementsInValue(this.valueAddress);

      this.memberSize = Utils.readUVInt(valueAddress + offset);
      this.memberAddress = valueAddress + offset + Utils.sizeUVInt(this.memberSize);
      if (this.stopMemberPtr > 0) {
        if (Utils.compareTo(this.memberAddress, this.memberSize, this.stopMemberPtr,
          this.stopMemberSize) >= 0) {
          this.offset = 0;
          return false;
        } else {
          position++; pos = 0;
          return true;
        }
      } else {
        position++; pos = 0;
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
  /** 
   * Get global position
   * @return global position
   */
  public long getPosition() {
    return this.position;
  }
  /**
   * Get local (in current value) position
   * @return position
   */
  public int getPos() {
    return this.pos;
  }
  /**
   * Skips to position - works only forward
   * @param pos position to skip (always less than cardinality)
   * @return current position (can be less than pos)
   */
  public long skipTo(long pos) {
    if (reverse) {
      throw new UnsupportedOperationException("skipTo");
    }
    if (pos <= this.position) {
      return this.position;
    }

    while (this.position < pos) {
      int left = this.valueNumber - this.pos;
      if (left > pos - this.position) {
        skipLocal((int)(this.pos + pos - this.position));
        return this.position;
      } else {
        this.position += left;
        try {
          mapScanner.next();
          if (mapScanner.hasNext()) {
            this.valueAddress = mapScanner.valueAddress();
            this.valueSize = mapScanner.valueSize();
            this.valueNumber  = Commons.numElementsInValue(this.valueAddress);
            this.pos = 0;
            this.offset = NUM_ELEM_SIZE;
          } else {
            break;
          }
        } catch (IOException e) {
        }
      } 
    }
    this.memberSize = Utils.readUVInt(valueAddress + offset);
    this.memberAddress = valueAddress + offset + Utils.sizeUVInt(this.memberSize);
    return this.position;
  }
  
  /**
   * We do not check stop limit to improve performance
   * because:
   * 1. We now in advance cardinality of the set
   * 2. We use this API only in direct scanner w/o start and stop
   * @param pos position to search
   * @return number of skipped elements
   */
  public int skipLocal(int pos) {
    if (pos >= this.valueNumber || pos <= this.pos) {
      // do nothing - return current
      return this.pos;
    }
    while (this.pos < pos) {
      int elSize = Utils.readUVInt(valueAddress + offset);
      int elSizeSize = Utils.sizeUVInt(elSize);
      this.offset += elSize + elSizeSize;
      this.position++; 
      this.pos++;
      
    }
    this.memberSize = Utils.readUVInt(valueAddress + offset);
    this.memberAddress = valueAddress + offset + Utils.sizeUVInt(this.memberSize);
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
    if (this.offset > NUM_ELEM_SIZE && this.offsetIndex > 0) {
      this.offsetIndex--;
      this.offset = getOffsetByIndex(this.offsetIndex);
      //this.offset = off;
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