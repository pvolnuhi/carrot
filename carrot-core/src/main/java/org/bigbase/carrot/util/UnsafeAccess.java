package org.bigbase.carrot.util;


import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.util.RangeTree.Range;

import sun.misc.Unsafe;
//import sun.nio.ch.DirectBuffer;

public final class UnsafeAccess {
  public static boolean debug = false; 
  public static class MallocStats {
    public AtomicLong allocEvents= new AtomicLong();
    public AtomicLong freeEvents = new AtomicLong();
    public AtomicLong allocated = new AtomicLong();
    public AtomicLong freed = new AtomicLong();
    private RangeTree allocMap = new RangeTree();
        
    
    public void allocEvent(long address, long alloced) {
      if (!UnsafeAccess.debug) return;
      //*DEBUG*/ System.out.println("malloc " + address + " size=" + alloced);
      //Thread.dumpStack();
      allocEvents.incrementAndGet();
      allocated.addAndGet(alloced);
      allocMap.delete(address);
      Range r = allocMap.add(new Range(address, (int)alloced));
      if (r != null) {
        System.out.println("Released ["+ r.start +"," + r.size);
      }
    }
    
    public void reallocEvent(long address, long alloced) {
      if (!UnsafeAccess.debug) return;
      //*DEBUG*/ System.out.println("remalloc " + address + " size=" + alloced);
      //Thread.dumpStack();
      //allocEvents.incrementAndGet();
      Range r = allocMap.delete(address);
      allocMap.add(new Range(address, (int)alloced));
      allocated.addAndGet(alloced - r.size);
    }
    
    public void freeEvent(long address) {
      if (!UnsafeAccess.debug) return;
      //*DEBUG*/ System.out.println("free " + address);

      Range mem = allocMap.delete(address);
      if (mem == null) {
        System.out.println("FATAL: not found address "+ address);
        Thread.dumpStack();
        System.exit(-1);
      }
      
      //Thread.dumpStack();

      freed.addAndGet(mem.size);      
      freeEvents.incrementAndGet();
    }
    
    public void checkAllocation(long address, int size) {
      if (!UnsafeAccess.debug) return;

      if (!allocMap.inside(address, size)) {
        System.out.println("Memory corruption: address="+ address +" size="+size);
        Thread.dumpStack();
        System.exit(-1);
      }
    }
    
    public void printStats() {
      if (!UnsafeAccess.debug) return;

      System.out.println("allocations        ="+ allocEvents.get());
      System.out.println("allocated memory   ="+ allocated.get());
      System.out.println("deallocations      ="+ freeEvents.get());
      System.out.println("deallocated memory ="+ freed.get());
      System.out.println("leaked             ="+ (allocated.get() - freed.get()));
      if (allocMap.size() > 0) {
        System.out.println("Orphaned allocation sizes:");
        for(Map.Entry<Range, Range> entry: allocMap.entrySet()) {
          System.out.println(entry.getKey().start +" size="+ entry.getValue().size);
        }
      }
    }
  }
  
  public static MallocStats mallocStats = new MallocStats();
  
  private static final Log LOG = LogFactory.getLog(UnsafeAccess.class);
  public static final Unsafe theUnsafe;
  
  public final static long MALLOC_FAILED = -1;
  
  /** The offset to the first element in a byte array. */
  public static final long BYTE_ARRAY_BASE_OFFSET;

  static final boolean littleEndian = ByteOrder.nativeOrder()
      .equals(ByteOrder.LITTLE_ENDIAN);

  // This number limits the number of bytes to copy per call to Unsafe's
  // copyMemory method. A limit is imposed to allow for safepoint polling
  // during a large copy
  static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;
  static {
    theUnsafe = (Unsafe) AccessController.doPrivileged(new PrivilegedAction<Object>() {
      @Override
      public Object run() {
        try {
          Field f = Unsafe.class.getDeclaredField("theUnsafe");
          f.setAccessible(true);
          return f.get(null);
        } catch (Throwable e) {
          LOG.warn("sun.misc.Unsafe is not accessible", e);
        }
        return null;
      }
    });

    if (theUnsafe != null) {
      BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);
    } else{
      BYTE_ARRAY_BASE_OFFSET = -1;
    }
  }

  private UnsafeAccess(){}

  // APIs to read primitive data from a byte[] using Unsafe way
  /**
   * Converts a byte array to a short value considering it was written in big-endian format.
   * @param bytes byte array
   * @param offset offset into array
   * @return the short value
   */
  public static short toShort(byte[] bytes, int offset) {
    if (littleEndian) {
      return Short.reverseBytes(theUnsafe.getShort(bytes, offset + BYTE_ARRAY_BASE_OFFSET));
    } else {
      return theUnsafe.getShort(bytes, offset + BYTE_ARRAY_BASE_OFFSET);
    }
  }
  
  public static short toShort(long addr) {
    mallocStats.checkAllocation(addr, 2);
    if (littleEndian) {
      return Short.reverseBytes(theUnsafe.getShort(addr));
    } else {
      return theUnsafe.getShort(addr);
    }
  }
  
  /**
   * Bit manipulation routines
   */
  
  /**
   * Get offset of a first bit set in a long value (8 bytes)
   * @param addr address to read value from
   * @return offset, or -1 if no bits set 
   */
  public static int firstBitSetLong(long addr) {
    
    long value = theUnsafe.getLong(addr);
    if (value == 0) return -1;
    if (littleEndian) {
      value = Long.reverseBytes(value);
    }
    return Long.numberOfTrailingZeros(value) +1;
  }
  
  /**
   * Get offset of a first bit unset in a long value (8 bytes)
   * @param addr address to read value from
   * @return offset, or -1 if no bits unset 
   */
  public static int firstBitUnSetLong(long addr) {
    
    long value = theUnsafe.getLong(addr);
    if (value == 0xffffffffffffffffL) return -1;
    if (littleEndian) {
      value = Long.reverseBytes(value);
    }
    value = ~value;
    return Long.numberOfTrailingZeros(value) +1;
  }
  
  /**
   * Get offset of a first bit set in a integer value (4 bytes)
   * @param addr address to read value from
   * @return offset, or -1 if 
   */
  public static int firstBitSetInt(long addr) {
    
    int value = theUnsafe.getInt(addr);
    if (value == 0) return -1;
    if (littleEndian) {
      value = Integer.reverseBytes(value);
    }
    return Integer.numberOfTrailingZeros(value) +1;
  }
  
  /**
   * Get offset of a first bit unset in a integer value (4 bytes)
   * @param addr address to read value from
   * @return offset of first '0', or -1 if not found 
   */
  public static int firstBitUnSetInt(long addr) {
    
    int value = theUnsafe.getInt(addr);
    if (value == 0xffffffff) return -1;
    if (littleEndian) {
      value = Integer.reverseBytes(value);
    }
    value = ~value;
    return Integer.numberOfTrailingZeros(value) +1;
  }
  
  /**
   * Get offset of a first bit set in a byte value (1 byte)
   * @param addr address to read value from
   * @return offset of first '1', or -1 if not found 
   */
  public static int firstBitSetByte(long addr) {
    byte value= theUnsafe.getByte(addr);
    if (value == 0) return -1;
    return Integer.numberOfLeadingZeros(Byte.toUnsignedInt(value)) - 24;
  }
  
  
  /**
   * Get offset of a first bit unset in a byte value (1 byte)
   * @param addr address to read value from
   * @return offset of first '0', or -1 if not found 
   */
  public static int firstBitUnSetByte(long addr) {
    byte value= theUnsafe.getByte(addr);
    if (value == (byte) 0xff) return -1;
    // TODO: test it
    value =(byte) ~value;
    return Integer.numberOfLeadingZeros(Byte.toUnsignedInt(value)) - 24;
  }
  
  /**
   * Get offset of a first bit set in a short value (2 bytes)
   * @param addr address to read value from
   * @return offset of first '1', or -1 if not found 
   */
  public static int firstBitSetShort(long addr) {
    short value= theUnsafe.getShort(addr);
    if (value == 0) return -1;
    if (littleEndian) {
      value = Short.reverseBytes(value);
    }
    return Integer.numberOfLeadingZeros(Short.toUnsignedInt(value)) - 16;
  }
  
  
  /**
   * Get offset of a first bit unset in a short value (2 bytes)
   * @param addr address to read value from
   * @return offset of first '0', or -1 if not found 
   */
  public static int firstBitUnSetShort(long addr) {
    short value= theUnsafe.getShort(addr);
    if (value == (short)0xffff) return -1;
    if (littleEndian) {
      value = Short.reverseBytes(value);
    }
    value = (short)~value;
    return Integer.numberOfLeadingZeros(Short.toUnsignedInt(value)) - 16;
  }
  
  /**
   * Converts a byte array to an int value considering it was written in big-endian format.
   * @param bytes byte array
   * @param offset offset into array
   * @return the int value
   */
  public static int toInt(byte[] bytes, int offset) {
    if (littleEndian) {
      return Integer.reverseBytes(theUnsafe.getInt(bytes, offset + BYTE_ARRAY_BASE_OFFSET));
    } else {
      return theUnsafe.getInt(bytes, offset + BYTE_ARRAY_BASE_OFFSET);
    }
  }

  
  /**
   * Converts a byte array to a long value considering it was written in big-endian format.
   * @param bytes byte array
   * @param offset offset into array
   * @return the long value
   */
  public static long toLong(byte[] bytes, int offset) {
    if (littleEndian) {
      return Long.reverseBytes(theUnsafe.getLong(bytes, offset + BYTE_ARRAY_BASE_OFFSET));
    } else {
      return theUnsafe.getLong(bytes, offset + BYTE_ARRAY_BASE_OFFSET);
    }
  }

  // APIs to write primitive data to a byte[] using Unsafe way
  /**
   * Put a short value out to the specified byte array position in big-endian format.
   * @param bytes the byte array
   * @param offset position in the array
   * @param val short to write out
   * @return incremented offset
   */
  public static int putShort(byte[] bytes, int offset, short val) {
    if (littleEndian) {
      val = Short.reverseBytes(val);
    }
    theUnsafe.putShort(bytes, offset + BYTE_ARRAY_BASE_OFFSET, val);
    return offset + Bytes.SIZEOF_SHORT;
  }

  public static void putShort(long addr, short val) {
    mallocStats.checkAllocation(addr, 2);

    if (littleEndian) {
      val = Short.reverseBytes(val);
    }
    theUnsafe.putShort(addr,  val);
  }
  
  public static int putByte(byte[] bytes, int offset, byte val) {
    theUnsafe.putByte(bytes, offset + BYTE_ARRAY_BASE_OFFSET, val);
    return offset + Bytes.SIZEOF_BYTE;
  }

  public static void putByte(long addr, byte val) {
    mallocStats.checkAllocation(addr, 1);

    theUnsafe.putByte(addr,  val);
  }
  /**
   * Put an int value out to the specified byte array position in big-endian format.
   * @param bytes the byte array
   * @param offset position in the array
   * @param val int to write out
   * @return incremented offset
   */
  public static int putInt(byte[] bytes, int offset, int val) {
    if (littleEndian) {
      val = Integer.reverseBytes(val);
    }
    theUnsafe.putInt(bytes, offset + BYTE_ARRAY_BASE_OFFSET, val);
    return offset + Bytes.SIZEOF_INT;
  }

  public static void putInt(long addr, int val) {
    mallocStats.checkAllocation(addr, 4);

    if (littleEndian) {
      val = Integer.reverseBytes(val);
    }
    theUnsafe.putInt(addr,  val);
  }  
  /**
   * Put a long value out to the specified byte array position in big-endian format.
   * @param bytes the byte array
   * @param offset position in the array
   * @param val long to write out
   * @return incremented offset
   */
  public static int putLong(byte[] bytes, int offset, long val) {
    if (littleEndian) {
      val = Long.reverseBytes(val);
    }
    theUnsafe.putLong(bytes, offset + BYTE_ARRAY_BASE_OFFSET, val);
    return offset + Bytes.SIZEOF_LONG;
  }

  /**
   * Put long direct
   * @param addr
   * @param val
   */
  public static void putLong(long addr, long val) {
    mallocStats.checkAllocation(addr, 8);

    if (littleEndian) {
      val = Long.reverseBytes(val);
    }
    theUnsafe.putLong(addr,  val);
  }
  
  

  // APIs to read primitive data from a ByteBuffer using Unsafe way
  /**
   * Reads a short value at the given buffer's offset considering it was written in big-endian
   * format.
   *
   * @param buf
   * @param offset
   * @return short value at offset
   */
//  public static short toShort(ByteBuffer buf, int offset) {
//    if (littleEndian) {
//      return Short.reverseBytes(getAsShort(buf, offset));
//    }
//    return getAsShort(buf, offset);
//  }

  /**
   * Reads a short value at the given Object's offset considering it was written in big-endian
   * format.
   * @param ref
   * @param offset
   * @return short value at offset
   */
  public static short toShort(Object ref, long offset) {
    if (littleEndian) {
      return Short.reverseBytes(theUnsafe.getShort(ref, offset));
    }
    return theUnsafe.getShort(ref, offset);
  }

  /**
   * Reads bytes at the given offset as a short value.
   * @param buf
   * @param offset
   * @return short value at offset
   */
//  static short getAsShort(ByteBuffer buf, int offset) {
//    if (buf.isDirect()) {
//      return theUnsafe.getShort(((DirectBuffer) buf).address() + offset);
//    }
//    return theUnsafe.getShort(buf.array(), BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset);
//  }

  /**
   * Reads an int value at the given buffer's offset considering it was written in big-endian
   * format.
   *
   * @param buf
   * @param offset
   * @return int value at offset
   */
//  public static int toInt(ByteBuffer buf, int offset) {
//    if (littleEndian) {
//      return Integer.reverseBytes(getAsInt(buf, offset));
//    }
//    return getAsInt(buf, offset);
//  }

  /**
   * Reads a int value at the given Object's offset considering it was written in big-endian
   * format.
   * @param ref
   * @param offset
   * @return int value at offset
   */
  public static int toInt(Object ref, long offset) {
    if (littleEndian) {
      return Integer.reverseBytes(theUnsafe.getInt(ref, offset));
    }
    return theUnsafe.getInt(ref, offset);
  }

  public static int toInt(long addr) {
    mallocStats.checkAllocation(addr, 4);

    if (littleEndian) {
      return Integer.reverseBytes(theUnsafe.getInt(addr));
    } else {
      return theUnsafe.getInt(addr);
    }
  }  
  /**
   * Reads bytes at the given offset as an int value.
   * @param buf
   * @param offset
   * @return int value at offset
   */
//  static int getAsInt(ByteBuffer buf, int offset) {
//    if (buf.isDirect()) {
//      return theUnsafe.getInt(((DirectBuffer) buf).address() + offset);
//    }
//    return theUnsafe.getInt(buf.array(), BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset);
//  }

  /**
   * Reads a long value at the given buffer's offset considering it was written in big-endian
   * format.
   *
   * @param buf
   * @param offset
   * @return long value at offset
   */
//  public static long toLong(ByteBuffer buf, int offset) {
//    if (littleEndian) {
//      return Long.reverseBytes(getAsLong(buf, offset));
//    }
//    return getAsLong(buf, offset);
//  }

  /**
   * Reads a long value at the given Object's offset considering it was written in big-endian
   * format.
   * @param ref
   * @param offset
   * @return long value at offset
   */
  public static long toLong(Object ref, long offset) {
    if (littleEndian) {
      return Long.reverseBytes(theUnsafe.getLong(ref, offset));
    }
    return theUnsafe.getLong(ref, offset);
  }

  public static long toLong(long addr) {
    mallocStats.checkAllocation(addr, 8);

    if (littleEndian) {
      return Long.reverseBytes(theUnsafe.getLong(addr));
    } else {
      return theUnsafe.getLong(addr);
    }
  }
  
  /**
   * Reads bytes at the given offset as a long value.
   * @param buf
   * @param offset
   * @return long value at offset
   */
//  static long getAsLong(ByteBuffer buf, int offset) {
//    if (buf.isDirect()) {
//      return theUnsafe.getLong(((DirectBuffer) buf).address() + offset);
//    }
//    return theUnsafe.getLong(buf.array(), BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset);
//  }

  /**
   * Put an int value out to the specified ByteBuffer offset in big-endian format.
   * @param buf the ByteBuffer to write to
   * @param offset offset in the ByteBuffer
   * @param val int to write out
   * @return incremented offset
   */
//  public static int putInt(ByteBuffer buf, int offset, int val) {
//    if (littleEndian) {
//      val = Integer.reverseBytes(val);
//    }
//    if (buf.isDirect()) {
//      theUnsafe.putInt(((DirectBuffer) buf).address() + offset, val);
//    } else {
//      theUnsafe.putInt(buf.array(), offset + buf.arrayOffset() + BYTE_ARRAY_BASE_OFFSET, val);
//    }
//    return offset + Bytes.SIZEOF_INT;
//  }

  // APIs to copy data. This will be direct memory location copy and will be much faster
  /**
   * Copies the bytes from given array's offset to length part into the given buffer.
   * @param src
   * @param srcOffset
   * @param dest
   * @param destOffset
   * @param length
   */
//  public static void copy(byte[] src, int srcOffset, ByteBuffer dest, int destOffset, int length) {
//    long destAddress = destOffset;
//    Object destBase = null;
//    if (dest.isDirect()) {
//      destAddress = destAddress + ((DirectBuffer) dest).address();
//    } else {
//      destAddress = destAddress + BYTE_ARRAY_BASE_OFFSET + dest.arrayOffset();
//      destBase = dest.array();
//    }
//    long srcAddress = srcOffset + BYTE_ARRAY_BASE_OFFSET;
//    unsafeCopy(src, srcAddress, destBase, destAddress, length);
//  }

  public static void copy(byte[] src, int srcOffset, long address, int length) {
    mallocStats.checkAllocation(address, length);

    Object destBase = null;
    long srcAddress = srcOffset + BYTE_ARRAY_BASE_OFFSET;
    unsafeCopy(src, srcAddress, destBase, address, length);
  }


  public static void copy(byte[] src, int srcOffset, byte[] dst, int dstOffset, int length) {
    long srcAddress = srcOffset + BYTE_ARRAY_BASE_OFFSET;
    long dstAddress = dstOffset + BYTE_ARRAY_BASE_OFFSET;
    unsafeCopy(src, srcAddress, dst, dstAddress, length);
  }

  public static void copy(long src, byte[] dest, int off, int length) {
    Object srcBase = null;
    long dstOffset = off + BYTE_ARRAY_BASE_OFFSET;
    unsafeCopy(srcBase, src, dest, dstOffset, length);
  }

  public static void copy(long src, long dst, long len) {
    mallocStats.checkAllocation(src, (int)len);
    mallocStats.checkAllocation(dst, (int)len);
    

    while (len > 0) {
      long size = (len > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : len;
      theUnsafe.copyMemory(src, dst, size);
      len -= size;
      src += size;
      dst += size;
    }
  }

  private static void unsafeCopy(Object src, long srcAddr, Object dst, long destAddr, long len) {
    while (len > 0) {
      long size = (len > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : len;
      theUnsafe.copyMemory(src, srcAddr, dst, destAddr, size);
      len -= size;
      srcAddr += size;
      destAddr += size;
    }
  }
  /**
   * Allocate native memory and copy array
   * @param arr array to copy
   * @param off offset
   * @param len length
   * @return address
   */
  public static long allocAndCopy(byte[] arr, int off, int len) {
    long ptr = malloc(len);
    copy(arr, 0, ptr, len);
    return ptr;
  }
  
  /**
   * Allocate native memory and copy source 
   * @param src source data
   * @param size size of a source data
   * @return address
   */
  public static long allocAndCopy(long src, int size) {
    long ptr = malloc(size);
    copy(src, ptr, size);
    return ptr;
  }
  
  /**
   * Copies specified number of bytes from given offset of {@code src} ByteBuffer to the
   * {@code dest} array.
   *
   * @param src
   * @param srcOffset
   * @param dest
   * @param destOffset
   * @param length
   */
//  public static void copy(ByteBuffer src, int srcOffset, byte[] dest, int destOffset,
//      int length) {
//    long srcAddress = srcOffset;
//    Object srcBase = null;
//    if (src.isDirect()) {
//      srcAddress = srcAddress + ((DirectBuffer) src).address();
//    } else {
//      srcAddress = srcAddress + BYTE_ARRAY_BASE_OFFSET + src.arrayOffset();
//      srcBase = src.array();
//    }
//    long destAddress = destOffset + BYTE_ARRAY_BASE_OFFSET;
//    unsafeCopy(srcBase, srcAddress, dest, destAddress, length);
//  }

  /**
   * Copies specified number of bytes from given offset of {@code src} buffer into the {@code dest}
   * buffer.
   *
   * @param src
   * @param srcOffset
   * @param dest
   * @param destOffset
   * @param length
   */
//  public static void copy(ByteBuffer src, int srcOffset, ByteBuffer dest, int destOffset,
//      int length) {
//    long srcAddress, destAddress;
//    Object srcBase = null, destBase = null;
//    if (src.isDirect()) {
//      srcAddress = srcOffset + ((DirectBuffer) src).address();
//    } else {
//      srcAddress = srcOffset +  src.arrayOffset() + BYTE_ARRAY_BASE_OFFSET;
//      srcBase = src.array();
//    }
//    if (dest.isDirect()) {
//      destAddress = destOffset + ((DirectBuffer) dest).address();
//    } else {
//      destAddress = destOffset + BYTE_ARRAY_BASE_OFFSET + dest.arrayOffset();
//      destBase = dest.array();
//    }
//    unsafeCopy(srcBase, srcAddress, destBase, destAddress, length);
//  }

  // APIs to add primitives to BBs
  /**
   * Put a short value out to the specified BB position in big-endian format.
   * @param buf the byte buffer
   * @param offset position in the buffer
   * @param val short to write out
   * @return incremented offset
   */
//  public static int putShort(ByteBuffer buf, int offset, short val) {
//    if (littleEndian) {
//      val = Short.reverseBytes(val);
//    }
//    if (buf.isDirect()) {
//      theUnsafe.putShort(((DirectBuffer) buf).address() + offset, val);
//    } else {
//      theUnsafe.putShort(buf.array(), BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset, val);
//    }
//    return offset + Bytes.SIZEOF_SHORT;
//  }

  /**
   * Put a long value out to the specified BB position in big-endian format.
   * @param buf the byte buffer
   * @param offset position in the buffer
   * @param val long to write out
   * @return incremented offset
   */
//  public static int putLong(ByteBuffer buf, int offset, long val) {
//    if (littleEndian) {
//      val = Long.reverseBytes(val);
//    }
//    if (buf.isDirect()) {
//      theUnsafe.putLong(((DirectBuffer) buf).address() + offset, val);
//    } else {
//      theUnsafe.putLong(buf.array(), BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset, val);
//    }
//    return offset + Bytes.SIZEOF_LONG;
//  }
  /**
   * Put a byte value out to the specified BB position in big-endian format.
   * @param buf the byte buffer
   * @param offset position in the buffer
   * @param b byte to write out
   * @return incremented offset
   */
//  public static int putByte(ByteBuffer buf, int offset, byte b) {
//    if (buf.isDirect()) {
//      theUnsafe.putByte(((DirectBuffer) buf).address() + offset, b);
//    } else {
//      theUnsafe.putByte(buf.array(),
//          BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset, b);
//    }
//    return offset + 1;
//  }

  /**
   * Returns the byte at the given offset
   * @param buf the buffer to read
   * @param offset the offset at which the byte has to be read
   * @return the byte at the given offset
   */
//  public static byte toByte(ByteBuffer buf, int offset) {
//    if (buf.isDirect()) {
//      return theUnsafe.getByte(((DirectBuffer) buf).address() + offset);
//    } else {
//      return theUnsafe.getByte(buf.array(), BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset);
//    }
//  }

  /**
   * Returns the byte at the given offset of the object
   * @param ref
   * @param offset
   * @return the byte at the given offset
   */
  public static byte toByte(Object ref, long offset) {
    return theUnsafe.getByte(ref, offset);
  }
  
  public static byte toByte(long addr) {
    mallocStats.checkAllocation(addr, 1);

    return theUnsafe.getByte(addr);
  }
  /**
   * Malloc
   * @param size
   * @return memory pointer
   */
  public static long malloc (long size) {
    long address = theUnsafe.allocateMemory(size);
    mallocStats.allocEvent(address, size);
    return address;
  }
  
  /**
   * Malloc zeroed
   * @param size
   * @return memory pointer
   */
  public static long mallocZeroed (long size) {
    long address = theUnsafe.allocateMemory(size);
    theUnsafe.setMemory(address, size, (byte)0);
    mallocStats.allocEvent(address, size);
    return address;
  }
  
  public static void mallocStats() {
    mallocStats.printStats();
  }
  
  /**
   * Reallocate memory
   */
  
  public static long realloc(long ptr, long newSize) {
    long pptr = theUnsafe.reallocateMemory(ptr, newSize);
    if(pptr != ptr) {
      mallocStats.freeEvent(ptr);
      mallocStats.allocEvent(pptr, newSize);
    } else {
      mallocStats.reallocEvent(pptr, newSize);
    }
    return pptr;

  }

  /**
   * Reallocate memory zeroed
   */
  
  public static long reallocZeroed(long ptr, long oldSize, long newSize) {
    long addr = theUnsafe.reallocateMemory(ptr, newSize);
    theUnsafe.setMemory(addr + oldSize, newSize - oldSize, (byte) 0);
    if(addr != ptr) {
      mallocStats.freeEvent(ptr);
      mallocStats.allocEvent(addr, newSize);
    } else {
      mallocStats.reallocEvent(addr, newSize);
    }

    return addr;
  }

  public static void setMemory(long ptr, long size, byte v) {
    theUnsafe.setMemory(ptr, size, v);
  }
  /**
   * Free memory
   * @param ptr memory pointer
   */
  public static void free(long ptr) {
    mallocStats.freeEvent(ptr);
    theUnsafe.freeMemory(ptr);
  }
  
  /**
   * Load fence command
   */
  public static void loadFence() {
	  theUnsafe.loadFence();
  }
  
  /**
   * Store fence command
   */
  public static void storeFence() {
	  theUnsafe.storeFence();
  }
  
  /**
   * Full fence command
   */
  
  public static void fullFence() {
	  theUnsafe.fullFence();
  }
  
}
