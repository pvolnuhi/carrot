package org.bigbase.carrot.redis.lists;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Returns the element at index in the list stored at key. The index is zero-based, so 0 means 
 * the first element, 1 the second element and so on. Negative indices can be used to designate elements 
 * starting at the tail of the list. Here, -1 means the last element, -2 means the penultimate and so forth.
 * When the value at key is not a list, an error is returned.
 * 
 * Return value
 * Bulk string reply: the requested element, or nil when index is out of range.
 * 
 * This is fully fenced, atomic implementation
 */ 
public class ListsLindex extends Operation {
  private static ThreadLocal<Segment> segment = new ThreadLocal<Segment>() {
    @Override
    protected Segment initialValue() {
      return new Segment();
    } 
  };
  /*
   * Buffer to load element to
   */
  private long buffer;
  /*
   * Buffer size
   */
  private int bufferSize;
  /*
   * Length of an element
   */
  private int length = -1;
  
  /*
   * Index of an element in a list
   */
  private long index = -1;
  
  public ListsLindex() {
    setReadOnlyOrUpdateInPlace(true);
  }
  
  @Override
  public boolean execute() {
    // This is a read operation
    this.updatesCount = 0;
    if (foundRecordAddress <= 0) {
      return false;
    }
    // We found the list
    long valuePtr = DataBlock.valueAddress(foundRecordAddress);
    // Address of a first segment after total number of elements in this list (INT value)
    long ptr = UnsafeAccess.toLong(valuePtr + Utils.SIZEOF_INT);
    if (ptr <= 0) {
      return true;
    }
    // TOTAL number of elements
    int listSize = UnsafeAccess.toInt(valuePtr);
    if (index < 0) {
      index += listSize;
      if (index < 0) {
        return false; 
      }
    }
    if (index >= listSize) {
      return false;
    }
    Segment s = segment.get();
    s.setDataPointer(ptr);
    int off = Lists.findSegmentForIndex(s, index);
    if (off < 0) {
      return false; // Index is too big
    }
    this.length = s.get(off, buffer, bufferSize);
    return true;
  }
  /** 
   * Set buffer to read data to
   * @param buffer buffer address
   * @param bufferSize buffer size
   */
  public void setBuffer(long buffer, int bufferSize) {
    this.buffer = buffer;
    this.bufferSize = bufferSize;
  }
  
  /**
   * Set index
   * @param index
   */
  public void setIndex(long index) {
    this.index = index;
  }
  
  /** 
   * Get length of an element or -1
   * @return length
   */
  public int getLength() {
    return this.length;
  }
  
  @Override
  public void reset() {
    super.reset();
    this.buffer = 0;
    this.bufferSize = 0;
    this.index = -1;
    this.length = -1;
    setReadOnlyOrUpdateInPlace(true);
  }
}
