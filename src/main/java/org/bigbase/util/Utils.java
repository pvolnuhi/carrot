package org.bigbase.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import sun.misc.Unsafe;

public class Utils {

  final static int SIZEOF_LONG = 8;
  final static int SIZEOF_INT = 4;
  final static int SIZEOF_SHORT = 2;
  final static int SIZEOF_BYTE = 1;
  

  /**
   * Returns true if x1 is less than x2, when both values are treated as unsigned long. Both values
   * are passed as is read by Unsafe. When platform is Little Endian, have to convert to
   * corresponding Big Endian value and then do compare. We do all writes in Big Endian format.
   */
  static boolean lessThanUnsignedLong(long x1, long x2) {
    if (UnsafeAccess.littleEndian) {
      x1 = Long.reverseBytes(x1);
      x2 = Long.reverseBytes(x2);
    }
    return (x1 + Long.MIN_VALUE) < (x2 + Long.MIN_VALUE);
  }

  /**
   * Returns true if x1 is less than x2, when both values are treated as unsigned int. Both values
   * are passed as is read by Unsafe. When platform is Little Endian, have to convert to
   * corresponding Big Endian value and then do compare. We do all writes in Big Endian format.
   */
  static boolean lessThanUnsignedInt(int x1, int x2) {
    if (UnsafeAccess.littleEndian) {
      x1 = Integer.reverseBytes(x1);
      x2 = Integer.reverseBytes(x2);
    }
    return (x1 & 0xffffffffL) < (x2 & 0xffffffffL);
  }

  /**
   * Returns true if x1 is less than x2, when both values are treated as unsigned short. Both values
   * are passed as is read by Unsafe. When platform is Little Endian, have to convert to
   * corresponding Big Endian value and then do compare. We do all writes in Big Endian format.
   */
  static boolean lessThanUnsignedShort(short x1, short x2) {
    if (UnsafeAccess.littleEndian) {
      x1 = Short.reverseBytes(x1);
      x2 = Short.reverseBytes(x2);
    }
    return (x1 & 0xffff) < (x2 & 0xffff);
  }

  /**
   * Lexicographically compare two arrays.
   * @param buffer1 left operand
   * @param buffer2 right operand
   * @param offset1 Where to start comparing in the left buffer
   * @param offset2 Where to start comparing in the right buffer
   * @param length1 How much to compare from the left buffer
   * @param length2 How much to compare from the right buffer
   * @return 0 if equal, < 0 if left is less than right, etc.
   */
  public static int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2,
      int offset2, int length2) {

    Unsafe theUnsafe = UnsafeAccess.theUnsafe;
    // Short circuit equal case
    if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
      return 0;
    }
    final int minLength = Math.min(length1, length2);
    final int minWords = minLength / SIZEOF_LONG;
    final long offset1Adj = offset1 + UnsafeAccess.BYTE_ARRAY_BASE_OFFSET;
    final long offset2Adj = offset2 + UnsafeAccess.BYTE_ARRAY_BASE_OFFSET;

    /*
     * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a time is no slower than
     * comparing 4 bytes at a time even on 32-bit. On the other hand, it is substantially faster on
     * 64-bit.
     */
    // This is the end offset of long parts.
    int j = minWords << 3; // Same as minWords * SIZEOF_LONG
    for (int i = 0; i < j; i += SIZEOF_LONG) {
      long lw = theUnsafe.getLong(buffer1, offset1Adj + (long) i);
      long rw = theUnsafe.getLong(buffer2, offset2Adj + (long) i);
      long diff = lw ^ rw;
      if (diff != 0) {
        return lessThanUnsignedLong(lw, rw) ? -1 : 1;
      }
    }
    int offset = j;

    if (minLength - offset >= SIZEOF_INT) {
      int il = theUnsafe.getInt(buffer1, offset1Adj + offset);
      int ir = theUnsafe.getInt(buffer2, offset2Adj + offset);
      if (il != ir) {
        return lessThanUnsignedInt(il, ir) ? -1 : 1;
      }
      offset += SIZEOF_INT;
    }
    if (minLength - offset >= SIZEOF_SHORT) {
      short sl = theUnsafe.getShort(buffer1, offset1Adj + offset);
      short sr = theUnsafe.getShort(buffer2, offset2Adj + offset);
      if (sl != sr) {
        return lessThanUnsignedShort(sl, sr) ? -1 : 1;
      }
      offset += SIZEOF_SHORT;
    }
    if (minLength - offset == 1) {
      int a = (buffer1[(int) (offset1 + offset)] & 0xff);
      int b = (buffer2[(int) (offset2 + offset)] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return length1 - length2;
  }

  /**
   * Lexicographically compare array and native memory.
   * @param buffer1 left operand
   * @param address right operand - native
   * @param offset1 Where to start comparing in the left buffer
   * @param length1 How much to compare from the left buffer
   * @param length2 How much to compare from the right buffer
   * @return 0 if equal, < 0 if left is less than right, etc.
   */
  public static int compareTo(byte[] buffer1, int offset1, int length1, long address,
       int length2) {

    Unsafe theUnsafe = UnsafeAccess.theUnsafe;
 
    final int minLength = Math.min(length1, length2);
    final int minWords = minLength / SIZEOF_LONG;
    final long offset1Adj = offset1 + UnsafeAccess.BYTE_ARRAY_BASE_OFFSET;

    /*
     * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a time is no slower than
     * comparing 4 bytes at a time even on 32-bit. On the other hand, it is substantially faster on
     * 64-bit.
     */
    // This is the end offset of long parts.
    int j = minWords << 3; // Same as minWords * SIZEOF_LONG
    for (int i = 0; i < j; i += SIZEOF_LONG) {
      long lw = theUnsafe.getLong(buffer1, offset1Adj + (long) i);
      long rw = theUnsafe.getLong(address + (long) i);
      long diff = lw ^ rw;
      if (diff != 0) {
        return lessThanUnsignedLong(lw, rw) ? -1 : 1;
      }
    }
    int offset = j;

    if (minLength - offset >= SIZEOF_INT) {
      int il = theUnsafe.getInt(buffer1, offset1Adj + offset);
      int ir = theUnsafe.getInt(address + offset);
      if (il != ir) {
        return lessThanUnsignedInt(il, ir) ? -1 : 1;
      }
      offset += SIZEOF_INT;
    }
    if (minLength - offset >= SIZEOF_SHORT) {
      short sl = theUnsafe.getShort(buffer1, offset1Adj + offset);
      short sr = theUnsafe.getShort(address + offset);
      if (sl != sr) {
        return lessThanUnsignedShort(sl, sr) ? -1 : 1;
      }
      offset += SIZEOF_SHORT;
    }
    if (minLength - offset == 1) {
      int a = (buffer1[(int) (offset1 + offset)] & 0xff);
      int b = theUnsafe.getByte(address + offset) & 0xff;
      if (a != b) {
        return a - b;
      }
    }
    return length1 - length2;
  }

  /**
   * Lexicographically compare two native memory pointers.
   * @param buffer1 left operand
   * @param address right operand - native
   * @param offset1 Where to start comparing in the left buffer
   * @param length1 How much to compare from the left buffer
   * @param length2 How much to compare from the right buffer
   * @return 0 if equal, < 0 if left is less than right, etc.
   */
  public static int compareTo(long address1, int length1, long address2,
       int length2) {

    Unsafe theUnsafe = UnsafeAccess.theUnsafe;
 
    final int minLength = Math.min(length1, length2);
    final int minWords = minLength / SIZEOF_LONG;

    /*
     * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a time is no slower than
     * comparing 4 bytes at a time even on 32-bit. On the other hand, it is substantially faster on
     * 64-bit.
     */
    // This is the end offset of long parts.
    int j = minWords << 3; // Same as minWords * SIZEOF_LONG
    for (int i = 0; i < j; i += SIZEOF_LONG) {
      long lw = theUnsafe.getLong(address1 + (long) i);
      long rw = theUnsafe.getLong(address2 + (long) i);
      long diff = lw ^ rw;
      if (diff != 0) {
        return lessThanUnsignedLong(lw, rw) ? -1 : 1;
      }
    }
    int offset = j;

    if (minLength - offset >= SIZEOF_INT) {
      int il = theUnsafe.getInt(address1 + offset);
      int ir = theUnsafe.getInt(address2 + offset);
      if (il != ir) {
        return lessThanUnsignedInt(il, ir) ? -1 : 1;
      }
      offset += SIZEOF_INT;
    }
    if (minLength - offset >= SIZEOF_SHORT) {
      short sl = theUnsafe.getShort(address1 + offset);
      short sr = theUnsafe.getShort(address2 + offset);
      if (sl != sr) {
        return lessThanUnsignedShort(sl, sr) ? -1 : 1;
      }
      offset += SIZEOF_SHORT;
    }
    if (minLength - offset == 1) {
      int a = theUnsafe.getByte(address1 + offset) & 0xff;
      int b = theUnsafe.getByte(address2 + offset) & 0xff;
      if (a != b) {
        return a - b;
      }
    }
    return length1 - length2;
  }
 
  /**
   * Calculates common prefix (number of bytes) of two byte arrays.
   * @param buffer1 left operand
   * @param buffer2 right operand
   * @param offset1 Where to start comparing in the left buffer
   * @param offset2 Where to start comparing in the right buffer
   * @param length1 How much to compare from the left buffer
   * @param length2 How much to compare from the right buffer
   * @return length of common prefix in bytes.
   */
  public static int prefix(byte[] buffer1, int offset1, int length1, byte[] buffer2,
      int offset2, int length2) {

    Unsafe theUnsafe = UnsafeAccess.theUnsafe;
    // Short circuit equal case
    if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
      return length1;
    }
    final int minLength = Math.min(length1, length2);
    int minWords = minLength / SIZEOF_LONG;
    final long offset1Adj = offset1 + UnsafeAccess.BYTE_ARRAY_BASE_OFFSET;
    final long offset2Adj = offset2 + UnsafeAccess.BYTE_ARRAY_BASE_OFFSET;

    /*
     * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a time is no slower than
     * comparing 4 bytes at a time even on 32-bit. On the other hand, it is substantially faster on
     * 64-bit.
     */
    // This is the end offset of long parts.
    int j = minWords << 3; // Same as minWords * SIZEOF_LONG
    int i = 0;
    for (; i < j; i += SIZEOF_LONG) {
      long lw = theUnsafe.getLong(buffer1, offset1Adj + (long) i);
      long rw = theUnsafe.getLong(buffer2, offset2Adj + (long) i);
      if (lw != rw) break;
    }
    
    j = (minLength / SIZEOF_INT) * SIZEOF_INT;
        
    for (; i < j; i += SIZEOF_INT) {
      int lw = theUnsafe.getInt(buffer1, offset1Adj + (long) i);
      int rw = theUnsafe.getInt(buffer2, offset2Adj + (long) i);
      if (lw != rw) break;
    }
    
    j = (minLength / SIZEOF_SHORT) * SIZEOF_SHORT;
 
    for (; i < j; i += SIZEOF_SHORT) {
      short lw = theUnsafe.getShort(buffer1, offset1Adj + (long) i);
      short rw = theUnsafe.getShort(buffer2, offset2Adj + (long) i);
      if (lw != rw) break;
    }
    
    j = minLength;
    
    for (; i < j; i += SIZEOF_BYTE) {
      byte lw = theUnsafe.getByte(buffer1, offset1Adj + (long) i);
      byte rw = theUnsafe.getByte(buffer2, offset2Adj + (long) i);
      if (lw != rw) break;
    }    
    return i;
  }

  /**
   * Calculates common prefix (number of bytes) of two byte arrays.
   * @param buffer1 left operand
   * @param address right operand - native
   * @param offset1 Where to start comparing in the left buffer
   * @param length1 How much to compare from the left buffer
   * @param length2 How much to compare from the right buffer
   * @return length of common prefix in bytes.
   */
  public static int prefix(byte[] buffer1, int offset1, int length1, long address,
       int length2) {

    Unsafe theUnsafe = UnsafeAccess.theUnsafe;
    
    final int minLength = Math.min(length1, length2);
    int minWords = minLength / SIZEOF_LONG;
    final long offset1Adj = offset1 + UnsafeAccess.BYTE_ARRAY_BASE_OFFSET;

    /*
     * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a time is no slower than
     * comparing 4 bytes at a time even on 32-bit. On the other hand, it is substantially faster on
     * 64-bit.
     */
    // This is the end offset of long parts.
    int j = minWords << 3; // Same as minWords * SIZEOF_LONG
    int i = 0;
    for (; i < j; i += SIZEOF_LONG) {
      long lw = theUnsafe.getLong(buffer1, offset1Adj + (long) i);
      long rw = theUnsafe.getLong(address + (long) i);
      if (lw != rw) break;
    }
    
    j = (minLength / SIZEOF_INT) * SIZEOF_INT;
        
    for (; i < j; i += SIZEOF_INT) {
      int lw = theUnsafe.getInt(buffer1, offset1Adj + (long) i);
      int rw = theUnsafe.getInt(address + (long) i);
      if (lw != rw) break;
    }
    
    j = (minLength / SIZEOF_SHORT) * SIZEOF_SHORT;
 
    for (; i < j; i += SIZEOF_SHORT) {
      short lw = theUnsafe.getShort(buffer1, offset1Adj + (long) i);
      short rw = theUnsafe.getShort(address + (long) i);
      if (lw != rw) break;
    }
    
    j = minLength;
    
    for (; i < j; i += SIZEOF_BYTE) {
      byte lw = theUnsafe.getByte(buffer1, offset1Adj + (long) i);
      byte rw = theUnsafe.getByte(address + (long) i);
      if (lw != rw) break;
    }    
    return i;
  }

  /**
   * Calculates common prefix (number of bytes) of two byte arrays
   * @param buffer1 left operand
   * @param address right operand - native
   * @param offset1 Where to start comparing in the left buffer
   * @param length1 How much to compare from the left buffer
   * @param length2 How much to compare from the right buffer
   * @return length of common prefix in bytes.
   */
  public static int prefix(long address1, int length1, long address2,
       int length2) {

    Unsafe theUnsafe = UnsafeAccess.theUnsafe;
    
    final int minLength = Math.min(length1, length2);
    int minWords = minLength / SIZEOF_LONG;

    /*
     * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a time is no slower than
     * comparing 4 bytes at a time even on 32-bit. On the other hand, it is substantially faster on
     * 64-bit.
     */
    // This is the end offset of long parts.
    int j = minWords << 3; // Same as minWords * SIZEOF_LONG
    int i = 0;
    for (; i < j; i += SIZEOF_LONG) {
      long lw = theUnsafe.getLong(address1 + (long) i);
      long rw = theUnsafe.getLong(address2 + (long) i);
      if (lw != rw) break;
    }
    
    j = (minLength / SIZEOF_INT) * SIZEOF_INT;
        
    for (; i < j; i += SIZEOF_INT) {
      int lw = theUnsafe.getInt(address1 + (long) i);
      int rw = theUnsafe.getInt(address2 + (long) i);
      if (lw != rw) break;
    }
    
    j = (minLength / SIZEOF_SHORT) * SIZEOF_SHORT;
 
    for (; i < j; i += SIZEOF_SHORT) {
      short lw = theUnsafe.getShort(address1 + (long) i);
      short rw = theUnsafe.getShort(address2 + (long) i);
      if (lw != rw) break;
    }
    
    j = minLength;
    
    for (; i < j; i += SIZEOF_BYTE) {
      byte lw = theUnsafe.getByte(address1 + (long) i);
      byte rw = theUnsafe.getByte(address2 + (long) i);
      if (lw != rw) break;
    }    
    return i;
  }
  
  public static void sort (List<byte[]> list) {
    Collections.sort(list, new Comparator<byte[]> () {
      @Override
      public int compare(byte[] left, byte[] right) {
        return Bytes.compareTo(left, right);
      }
    });
  } 
}
