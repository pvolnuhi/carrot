package org.bigbase.carrot.redis.lists;

import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Linked list segment (memory area). Each segment has:
 * 
 * 1. Previous segment address (8 bytes)
 * 2. Next segment address (8 bytes) - 0 means NULL
 * 3. Segment size in bytes - 2 bytes
 * 4. Segment data size in bytes - 2 bytes
 * 5. Number of elements in the segment - 2 bytes
 * 6. Compression type (codec) - 1 byte 
 * @author Vladimir Rodionov
 *
 */

public final class Segment {
  
  public final static int ADDRESS_SIZE = Utils.SIZEOF_LONG;
  public final static int SEGMENT_SIZE = Utils.SIZEOF_SHORT; // size of a segment
  public final static int SEGMENT_DATA_SIZE = Utils.SIZEOF_SHORT; // size of a segment  
  public final static int ELEMENT_NUMBER_SIZE = Utils.SIZEOF_SHORT;
  public final static int COMPRESSION_TYPE_SIZE = Utils.SIZEOF_BYTE;
  public final static int SEGMENT_OVERHEAD = 2 * ADDRESS_SIZE + SEGMENT_SIZE + 
      + SEGMENT_DATA_SIZE + COMPRESSION_TYPE_SIZE + ELEMENT_NUMBER_SIZE;
  public final static int MAXIMUM_SEGMENT_SIZE = 4096;
  public final static int EXTERNAL_ALLOC_THRESHOLD = 256;
  
  static ThreadLocal<Segment> segment = new ThreadLocal<Segment>() {
    @Override
    protected Segment initialValue() {
      return new Segment();
    }   
  };
  
  /*
   * TODO: make this configurable
   * TODO: Optimal block ratios (check jemalloc sizes)
   * 128-256 - step 64
   * 256-4096 with step 256 - this is jemalloc specific
   * sizes of allocation 
   * 256 * 2, 3, 4, ... 16
   * The minimum allocation is 128, maximum - 4096
   */
  static int[] BASE_SIZES = new int[] { 64, 64, 256, 256, 256, 256, 256, 256, 256, 
                                        256, 256, 256, 256, 256, 256, 256, 256, 256
  };
  static int[] BASE_MULTIPLIERS = new int[] {2, 3, 1, 2, 3, 4, 5, 6, 7, 8, 
                                             9, 10, 11, 12, 13, 14, 15, 16};
  
  /**
   * Get min size greater than current
   * @param max - max size
   * @param current current size
   * @return min size or -1;
   */
  static int getMinSizeGreaterThan(int current) {
    for (int i = 0; i < BASE_MULTIPLIERS.length; i++) {
      int size = BASE_SIZES[i] * BASE_MULTIPLIERS[i];
      if (size > current) return size;
    }
    return -1;
  }

  /**
   * Get min size greater than current
   * @param max - max size
   * @param current current size
   * @return min size or -1;
   */
  static int getMinSizeGreaterOrEqualsThan(int current) {
    for (int i = 0; i < BASE_MULTIPLIERS.length; i++) {
      int size = BASE_SIZES[i] * BASE_MULTIPLIERS[i];
      if (size >= current) return size;
    }
    return -1;
  }
  
  /**
   * Segment data address
   */
  private long dataPtr;
  
  Segment() {
  }
  
  private Segment(long ptr, int size) {
    this.dataPtr = ptr;
    setSize (size);
  }
  
  public static Segment allocateNew() {
    int size = BASE_SIZES[0] * BASE_MULTIPLIERS[0];
    return allocateNew(size);
  }
  
  public static Segment allocateNew(Segment s) {
    int size = BASE_SIZES[0] * BASE_MULTIPLIERS[0];
    return allocateNew(s, size);
  }
  
  public static Segment allocateNew(int sizeRequired) {
    int size = getMinSizeGreaterOrEqualsThan(sizeRequired);
    if (size < 0) return null;
    long ptr = UnsafeAccess.mallocZeroed(size);
    return new Segment(ptr, size);
  }
  
  public static Segment allocateNew(Segment s, int sizeRequired) {
    int size = getMinSizeGreaterOrEqualsThan(sizeRequired);
    if (size < 0) return null;
    long ptr = UnsafeAccess.mallocZeroed(size);
    s.setDataPointer(ptr);
    s.setSize(size);
    return s;
  }
  
  /**
   * Reset instance for reuse
   */
  public void reset() {
    this.dataPtr = 0;
  }
  /**
   * Sets data pointer
   * @param ptr
   */
  public void setDataPointer(long ptr) {
    this.dataPtr = ptr;
  }
  
  /**
   * Gets data pointer
   * @return
   */
  public long getDataPtr() {
    return this.dataPtr;
  }
  
  
  /**
   * Sets segment size
   * @param ptr segment data pointer
   * @param size size
   */
  public static void setSegmentSize(long ptr, int size) {
    UnsafeAccess.putShort(ptr + 2 * ADDRESS_SIZE, (short) size);
  }
  
  /**
   * Gets segment size
   * @return segment size
   */
  public static int getSegmentSize(long ptr) {
    return UnsafeAccess.toShort(ptr + 2 * ADDRESS_SIZE);
  }
  
  /**
   * Sets segment data size
   * @param ptr segment data pointer
   * @param size size
   */
  
  public static void setSegmentDataSize(long ptr, int size) {
    UnsafeAccess.putShort(ptr + 2 * ADDRESS_SIZE + SEGMENT_SIZE, (short) size);
  }
  
  /**
   * Gets segment data size
   * @param ptr segment data pointer
   * @return segment data size
   */
  
  public static int getSegmentDataSize(long ptr) {
    return UnsafeAccess.toShort(ptr + 2 * ADDRESS_SIZE + SEGMENT_SIZE);
  }
  
  /**
   * Sets previous segment address
   * @param ptr segment data pointer
   * @param address previous segment address
   */
  public static void setPreviousSegmentAddress(long ptr, long address) {
    UnsafeAccess.putLong(ptr, address);
  }
  
  /**
   * Gets previous segment address
   * @return previous segment address
   */
  public static long getPreviousSegmentAddress(long ptr) {
    return UnsafeAccess.toLong(ptr);
  }
  
  /**
   * Sets next segment address
   * @param ptr segment data pointer
   * @param address next segment address
   */
  public static void setNextSegmentAddress(long ptr, long address) {
    UnsafeAccess.putLong(ptr + ADDRESS_SIZE, address);
  }
  
  /**
   * Gets next segment address
   * @return next segment address
   */
  public static long getNextSegmentAddress(long ptr) {
    return UnsafeAccess.toLong(ptr + ADDRESS_SIZE);
  }
  
  /**
   * Sets compression type
   * @param ptr segment data pointer
   * @param type compression type
   */
  public static void setSegmentCompressionType(long ptr, CodecType type) {
    UnsafeAccess.putByte(ptr + 2 * ADDRESS_SIZE + SEGMENT_SIZE + SEGMENT_DATA_SIZE + ELEMENT_NUMBER_SIZE, 
      (byte)type.ordinal());
  }
  
  /**
   * Gets compression type
   * @param ptr segment data pointer
   * @return compression type
   */
  public static CodecType getSegmentCompressionType(long ptr) {
    int ordinal = UnsafeAccess.toByte(ptr + 2 * ADDRESS_SIZE + SEGMENT_SIZE + SEGMENT_DATA_SIZE 
      + ELEMENT_NUMBER_SIZE);
    return CodecType.values()[ordinal];
  }
  
  /**
   * Gets number of elements in this segment
   * @param ptr segment data pointer
   * @return
   */
  public static int getNumberOfElements(long ptr) {
    return UnsafeAccess.toShort(ptr + 2 * ADDRESS_SIZE + SEGMENT_SIZE + SEGMENT_DATA_SIZE);
  }
  
  /**
   * Sets number of elements
   * @param ptr segment data pointer
   * @param n number of elements
   */
  public static void setNumberOfElements(long ptr, int n) {
    UnsafeAccess.putShort(ptr + 2 * ADDRESS_SIZE + SEGMENT_SIZE + SEGMENT_DATA_SIZE, (short) n);
  }
  
  
  
  /**
   * Sets segment size
   * @param size size
   */
  public void setSize(int size) {
    setSegmentSize(this.dataPtr, size);
  }
  
  /**
   * Get segment size
   * @return segment size
   */
  public int getSize() {
    return getSegmentSize(this.dataPtr);
  }
  
  /**
   * Sets data size
   * @param size data size
   */
  
  public void setDataSize(int size) {
    setSegmentDataSize(this.dataPtr, size);
  }
  
  /**
   * Gets data size
   * @return data size
   */
  public int getDataSize() {
    return getSegmentDataSize(this.dataPtr);
  }
  
  
  /**
   * Increment data size
   * @param incr value
   * @return new data size
   */
  public int incrementDataSize(int incr) {
    int v = getDataSize();
    setDataSize(v + incr);
    return v + incr;
  }
  
  /**
   * Increment number of elements
   * @param incr value
   * @return new number of elements
   */
  public int incrementNumberOfElements(int incr) {
    int v = getNumberOfElements();
    setNumberOfElements(v + incr);
    return v + incr;
  }
  
  /**
   * Sets previous segment address
   * @param address previous segment address
   */
  public void setPreviousAddress(long address) {
    setPreviousSegmentAddress(this.dataPtr, address);
  }
  
  /**
   * Gets previous segment address
   * @return previous segment address
   */
  public long getPreviousAddress() {
    return getPreviousSegmentAddress(this.dataPtr);
  }
  
  /**
   * Sets next segment address
   * @param address next segment address
   */
  public void setNextAddress(long address) {
    setNextSegmentAddress(this.dataPtr, address);
  }
  
  /**
   * Gets next segment address
   * @return next segment address
   */
  public long getNextAddress() {
    return getNextSegmentAddress(this.dataPtr);
  }
  
  /**
   * Sets number of elements
   * @param n number of elements
   */
  public void setNumberOfElements(int n) {
    setNumberOfElements(this.dataPtr, n);
  }
  
  /**
   * Gets number of elements
   * @return number of elements
   */
  public int getNumberOfElements() {
    return getNumberOfElements(this.dataPtr);
  }
  
  /**
   * Sets compression type
   * @param type compression type
   */
  public void setCompressionType(CodecType type) {
    setSegmentCompressionType(this.dataPtr, type);
  }
  
  /**
   * Gets compression type
   * @return compression type
   */
  public CodecType getCompressionType() {
    return getSegmentCompressionType(this.dataPtr);
  }
  
  /**
   * Shrink the segment
   */
  public void shrink() {
    int dataSize = getDataSize();
    int newSize = getMinSizeGreaterThan(dataSize + SEGMENT_OVERHEAD);
    if (newSize == getSize()) return;
    long ptr = UnsafeAccess.mallocZeroed(newSize);
    UnsafeAccess.copy(this.dataPtr, ptr, SEGMENT_OVERHEAD + dataSize);
    UnsafeAccess.free(this.dataPtr);
    setDataPointer(ptr);
    setSegmentSize(ptr, newSize);
    updateNextPrevious();
  }
  
  /**
   * Expand the segment
   * @param requiredSize (exclude overhead)
   * @return true, if success, false - otherwise
   */
  public boolean expand(int requiredSize) {
    int dataSize = getDataSize();
    int newSize = getMinSizeGreaterThan(requiredSize + SEGMENT_OVERHEAD);
    if (newSize < 0) return false;
    
    long ptr = UnsafeAccess.mallocZeroed(newSize);
    UnsafeAccess.copy(this.dataPtr, ptr, SEGMENT_OVERHEAD + dataSize);
    UnsafeAccess.free(this.dataPtr);
    setDataPointer(ptr);
    setSegmentSize(ptr, newSize);
    updateNextPrevious();
    return true;
  }
  
  private void updateNextPrevious() {
    long prevSegment = getPreviousAddress();
    if (prevSegment > 0) {
      setNextSegmentAddress(prevSegment, this.dataPtr);
    }
    long nextSegment = getNextAddress();
    if (nextSegment > 0) {
      setPreviousSegmentAddress(nextSegment, this.dataPtr);
    }
  }
  /**
   * Get element by index (0- based)
   * @param index index of element
   * @param buffer buffer for the result
   * @param bufferSize buffer size
   * @return size of the element, or -1
   */
  public int get(int index, long buffer, int bufferSize) {
    long ptr = this.dataPtr + SEGMENT_OVERHEAD;
    long max = this.dataPtr + SEGMENT_OVERHEAD + getDataSize();
    int count = 0;
    int size, ssize;
    while(ptr < max && count < index) {
      size = elementBlockSize(ptr);
      ssize = Utils.sizeUVInt(size);
      ptr += size + ssize;
      count++;
    }
    if (ptr == max) {
      return -1;
    }
    long addr = elementAddress(ptr);
    size = elementSize(ptr);
    if (size <= bufferSize) {
      UnsafeAccess.copy(addr,  buffer, size);
    }
    return size;
  }
  
  /**
   * Search position for insert
   * @param elemPtr element to search pointer
   * @param elemSize element to search size
   * @param after true - position after
   * @return address (position) or -1
   */
  public long search(long elemPtr, int elemSize, boolean after) {
    long ptr = this.dataPtr + SEGMENT_OVERHEAD;
    long max = ptr + getDataSize();
    
    while(ptr < max) {
      long ePtr = elementAddress(ptr);
      int eSize = elementSize(ptr);
      // eSize == 0 if external allocation
      int eBlockSize = elementBlockSize(ptr);
      if (Utils.compareTo(ePtr, eSize, elemPtr, elemSize) == 0) {
        return after? ptr + eBlockSize + Utils.sizeUVInt(eSize): ptr;
      }
      ptr += eBlockSize + Utils.sizeUVInt(eSize);
    }
    return -1;
  }
  /**
   * Insert element at the specified position
   * @param offset offset from beginning to insert
   * @param elemPtr element pointer
   * @param elemSize element size
   * @return address of this segment (it can change)
   */
  public long insert(int offset, long elemPtr, int elemSize) {
    int dataSize = getDataSize();
    int segmentSize = getSize();
    int eSizeSize = Utils.sizeUVInt(elemSize);
    boolean extAlloc = elemSize > EXTERNAL_ALLOC_THRESHOLD;
    int toMove = extAlloc? Utils.SIZEOF_INT + Utils.SIZEOF_LONG + Utils.sizeUVInt(0) :
      elemSize + eSizeSize;

    int requiredSize = extAlloc? dataSize + SEGMENT_OVERHEAD + Utils.SIZEOF_INT +
        Utils.SIZEOF_LONG + Utils.sizeUVInt(0): dataSize + SEGMENT_OVERHEAD + elemSize + eSizeSize;
    if (requiredSize <= segmentSize || expand(requiredSize)) {      
      dataSize = getDataSize();
      segmentSize = getSize();
      UnsafeAccess.copy(this.dataPtr + offset, this.dataPtr + offset + toMove, 
        dataSize + SEGMENT_OVERHEAD - offset);
      if (extAlloc) {
        long ptr = UnsafeAccess.allocAndCopy(elemPtr, elemSize);
        int zeroSize = Utils.sizeUVInt(0);
        Utils.writeUVInt(this.dataPtr + offset, 0);
        UnsafeAccess.putInt(this.dataPtr + zeroSize, elemSize);
        UnsafeAccess.putLong(this.dataPtr + offset + zeroSize + Utils.SIZEOF_INT, ptr);
      } else {
        Utils.writeUVInt(this.dataPtr + offset, elemSize);
        UnsafeAccess.copy(elemPtr, this.dataPtr + offset + eSizeSize, elemSize);
      }
      incrementNumberOfElements(1);
      incrementDataSize(toMove);
      return this.dataPtr;
    }
    
    // do split
    split();
    
    dataSize = getDataSize();
    if (offset <= dataSize + SEGMENT_OVERHEAD) {
      //  repeat call
      return insert(offset, elemPtr, elemSize);
    } else {
      // insert into next segment
      long nextPtr = getNextAddress();
      // Make sure that we do not do recursive calls anymore
      Segment s = segment.get();
      s.setDataPointer(nextPtr);
      s.insert(offset - dataSize, elemPtr, elemSize);
      // Even if we insert into next segment, we return address of a current
      // segment
      return this.dataPtr;
    }
    
  }
  
  /**
   * Insert element at the beginning
   * @param ptr element pointer
   * @param size element size
   * @return pointer to this segment after this operation
   */
  public long prepend(long ptr, int size) {
    return insert(SEGMENT_OVERHEAD, ptr, size);
  }
  /**
   * Append element at the end
   * @param ptr element pointer
   * @param size element size
   * @return pointer to this segment after this operation
   */
  public long append(long ptr, int size) {
    return insert(SEGMENT_OVERHEAD + getDataSize(), ptr, size);
  }
  
  /**
   * Insert element before or after a given element
   * @param elemPtr given element pointer
   * @param elemSize given element size
   * @param ptr element pointer
   * @param size element size
   * @param after true, then insert after
   * @return this segment adat pointer after the operation
   */
  public long insert(long elemPtr, int elemSize, long ptr, int size, boolean after) {
    long addr = search(elemPtr, elemSize, after);
    if (addr < 0) return addr; // not found
    return insert((int)(addr - this.dataPtr), ptr, size);
  }
  
  /**
   * Is first segment in the linked list
   * @return true if - yes, false - otherwise
   */
  public boolean isFirst() {
    return getPreviousAddress() == 0;
  }
  
  /**
   * Is last segment in the linked list
   * @return true, if - yes, false -otherwise
   */
  public boolean isLast() {
    return getNextAddress() == 0;
  }
  
  /**
   * Is first segment
   * @param ptr address of the segment
   * @return true, if - yes, false - otherwise
   */
  public static boolean isFirst(long ptr) {
    if (ptr == 0) return false;
    return getPreviousSegmentAddress(ptr) == 0;
  }
  
  /**
   * Is last segment in the linked list
   * @param ptr segment address
   * @return true, if - yes, false - otherwise
   */
  public static boolean isLast(long ptr) {
    if (ptr == 0) return false;
    return getNextSegmentAddress(ptr) == 0;
  }
  
  
  /**
   * Splits the segment
   */
  private void split() {
    int dataSize = getDataSize();
    int off = 0;
    int count = 0;
    while(off < dataSize/2) {
      int size = elementBlockSize(this.dataPtr + off + SEGMENT_OVERHEAD);
      off += size + Utils.sizeUVInt(size);
      count++;
    }
    int elementNumber = getNumberOfElements();
    int newDataSize = off;
    int newElementNumber = count;
    int rightDataSize = dataSize - off;
    int rightElementNumber = elementNumber - count;
    int newSize = getMinSizeGreaterOrEqualsThan(rightDataSize);
    
    long ptr = UnsafeAccess.mallocZeroed(newSize);
    UnsafeAccess.copy(this.dataPtr + off + SEGMENT_OVERHEAD, ptr + SEGMENT_OVERHEAD, rightDataSize);
    // Update new segment
    setNextSegmentAddress(ptr, getNextAddress());
    setSegmentSize(ptr, newSize);
    setSegmentDataSize(ptr, rightDataSize);
    setNumberOfElements(ptr, rightElementNumber);
    setSegmentCompressionType(ptr, getCompressionType());
    // Update current segment
    setNextAddress(ptr);
    setDataSize(newDataSize);
    setNumberOfElements(newElementNumber);
    shrink();
    // Do this after shrink
    setPreviousSegmentAddress(ptr, this.dataPtr);
    
  }

  /**
   * Gets element address
   * @param ptr current pointer in a segment
   * @return address of an element
   */
  public static long elementAddress(long ptr) {
    int size = Utils.readUVInt(ptr);
    int ssize = Utils.sizeUVInt(size);
    if (size == 0) {
      return UnsafeAccess.toLong(ptr + ssize + Utils.SIZEOF_INT); // it 
    } else {
      return ptr + ssize;
    }
  }
  
  /**
   * Gets element address
   * @param ptr current pointer in a segment
   * @return address of an element
   */
  public static int elementSize(long ptr) {
    int size = Utils.readUVInt(ptr);
    if (size == 0) {
      int ssize = Utils.sizeUVInt(size);
      return UnsafeAccess.toInt(ptr + ssize); // it 
    } else {
      return size;
    }
  }
  
  public static int elementBlockSize(long ptr) {
    int size = Utils.readUVInt(ptr);
    if (size == 0) {
      return Utils.SIZEOF_INT + Utils.SIZEOF_LONG;
    }
    return size;
  }
  
  /**
   * Has next segment
   * @return true, if - yes
   */
  public boolean hasNext() {
    return getNextAddress() > 0;
  }
  
  /**
   * Has previous segment
   * @return true, if - yes
   */
  public boolean hasPrevious() {
    return getPreviousAddress() > 0;
  }
  
  /**
   * Get next segment
   * @param s segment to reuse (this)
   * @return next segment or null
   */
  public Segment next(Segment s) {
    long ptr = getNextAddress();
    if (ptr <=0) return null;
    s.setDataPointer(ptr);
    return s;
  }
  
  /**
   * Get previous segment
   * @param s segment to reuse (this)
   * @return previous segment or null
   */
  public Segment previous(Segment s) {
    long ptr = getPreviousAddress();
    if (ptr <=0) return null;
    s.setDataPointer(ptr);
    return s;
  }
  
  /**
   * Gets last segment starting from a given segment
   * @param s segment to start from
   * @return last segment in a linked list
   */
  public Segment last(Segment s) {
    while (s.next(s) != null) {}
    return s;
  }
  
}
