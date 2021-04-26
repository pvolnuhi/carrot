package org.bigbase.carrot.util;

import static org.bigbase.carrot.util.UnsafeAccess.firstBitSetByte;
import static org.bigbase.carrot.util.UnsafeAccess.firstBitSetInt;
import static org.bigbase.carrot.util.UnsafeAccess.firstBitSetLong;
import static org.bigbase.carrot.util.UnsafeAccess.firstBitSetShort;
import static org.bigbase.carrot.util.UnsafeAccess.firstBitUnSetByte;
import static org.bigbase.carrot.util.UnsafeAccess.firstBitUnSetInt;
import static org.bigbase.carrot.util.UnsafeAccess.firstBitUnSetLong;
import static org.bigbase.carrot.util.UnsafeAccess.firstBitUnSetShort;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.Key;
import org.bigbase.carrot.KeyValue;
import org.bigbase.carrot.redis.sets.SetScanner;

import sun.misc.Unsafe;

public class Utils {

  public final static int SIZEOF_LONG = 8;
  public final static int SIZEOF_DOUBLE = 8;
  
  public final static int SIZEOF_INT = 4;
  public final static int SIZEOF_FLOAT = 4;

  public final static int SIZEOF_SHORT = 2;
  public final static int SIZEOF_BYTE = 1;
  
  public final static int BITS_PER_BYTE = 8;
  
  private static Random rnd = new Random();
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

    UnsafeAccess.mallocStats.checkAllocation(address, length2);
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
    UnsafeAccess.mallocStats.checkAllocation(address1, length1);
    UnsafeAccess.mallocStats.checkAllocation(address2, length2);

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
  
  public static void sortKeys(List<? extends Key> list) {
    Collections.sort(list, new Comparator<Key> () {
      @Override
      public int compare(Key k1, Key k2) {
        return Utils.compareTo(k1.address, k1.length, k2.address, k2.length);
      }
    });
  }
  
  public static void sortKeyValues(List<? extends KeyValue> list) {
    Collections.sort(list, new Comparator<KeyValue> () {
      @Override
      public int compare(KeyValue k1, KeyValue k2) {
        return Utils.compareTo(k1.keyPtr, k1.keySize, k2.keyPtr, k2.keySize);
      }
    });
  }
  
  /**
   * TODO: handle all 0xff key
   * Calculates end key for prefix scanner
   * @param start start key address
   * @param startSize start key size
   * @return end key address if success, or -1
   */
  public static long prefixKeyEnd(long start, int startSize) {
    long end = UnsafeAccess.malloc(startSize);
    UnsafeAccess.copy(start, end, startSize);
    for( int i = startSize - 1; i >=0; i--) {
      int v = UnsafeAccess.toByte(end + i) & 0xff; 
      if (v == 0xff) {
        continue;
      } else {
        UnsafeAccess.putByte(end + i, (byte)(v+1));
        return end;
      }
    }
    return -1;
  }
  
  /**
   * TODO: handle all 0xff key
   * Calculates end key for prefix scanner
   * @param start start key address
   * @param startSize start key size
   * @return end key address if success, or -1
   */
  public static long prefixKeyEndNoAlloc(long start, int startSize) {
 
    for( int i = startSize - 1; i >=0; i--) {
      int v = UnsafeAccess.toByte(start + i) & 0xff; 
      if (v == 0xff) {
        continue;
      } else {
        UnsafeAccess.putByte(start + i, (byte)(v+1));
        return start;
      }
    }
    return -1;
  }
  /**
   * 
   * TODO: THIS METHOD IS UNSAFE??? CHECK IT
   * Read unsigned VarInt
   * @param ptr address to read from
   * @return int value
   */
  public static int readUVInt(long ptr) {
    int v1 = UnsafeAccess.toByte(ptr) & 0xff;
    
    int cont = v1 >>> 7; // either 0 or 1
    ptr += cont;
    v1 &= 0x7f; // set 8th bit 0
    int v2 = (byte) (UnsafeAccess.toByte(ptr) * cont) & 0xff;
    cont = v2 >>> 7;
    ptr += cont;
    v2 &= 0x7f;
    int v3 = (byte)(UnsafeAccess.toByte(ptr) * cont) & 0xff;
    cont = v3 >>> 7;
    ptr += cont;
    v3 &= 0x7f;
    int v4 = (byte)(UnsafeAccess.toByte(ptr) * cont) & 0xff;
    v4 &= 0x7f;
    return v1 + (v2 << 7) + (v3 << 14) + (v4 << 21);
  }
  
  /**
   * Returns size of unsigned variable integer in bytes
   * @param value
   * @return size in bytes
   */
  public static int sizeUVInt(int value) {
    if (value < v1) {
      return 1;
    } else if (value < v2) {
      return 2;
    } else if (value < v3) {
      return 3;
    } else if (value < v4){
      return 4;
    }   
    return 0;
  }
  
  final static int v1 = 1 << 7;
  final static int v2 = 1 << 14;
  final static int v3 = 1 << 21;
  final static int v4 = 1 << 28;
  /**
   * Writes unsigned variable integer
   * @param ptr address to write to
   * @param value 
   * @return number of bytes written
   */
  public static int writeUVInt(long ptr, int value) {
    
    
    if (value < v1) {
      UnsafeAccess.putByte(ptr, (byte) value);
      return 1;
    } else if (value < v2) {
      UnsafeAccess.putByte(ptr, (byte) ((value & 0xff) | 0x80));
      UnsafeAccess.putByte(ptr + 1, (byte)(value >>> 7));
      return 2;
    } else if (value < v3) {
      UnsafeAccess.putByte(ptr, (byte) ((value & 0xff) | 0x80));
      UnsafeAccess.putByte(ptr + 1, (byte)((value >>> 7) | 0x80));
      UnsafeAccess.putByte(ptr + 2, (byte)(value >>> 14));
      return 3;
    } else if (value < v4){
      UnsafeAccess.putByte(ptr, (byte) ((value & 0xff) | 0x80));
      UnsafeAccess.putByte(ptr + 1, (byte)((value >>> 7) | 0x80));
      UnsafeAccess.putByte(ptr + 2, (byte)((value >>> 14) | 0x80));
      UnsafeAccess.putByte(ptr + 3, (byte)(value >>> 21));
      return 4;
    }   
    return 0;
  }
  
  /**
   * Counts set bits in a byte value
   * @param b value
   * @return number of set bits
   */
  public static int bitCount(byte b) {
    final byte[] table = new byte[] {0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4};
    return table[b & 0xf] + table[b >>> 4];
  }
  /**
   * Count bits in a short value
   * @param s short value
   * @return number of bits set
   */
  public static int bitCount(short s) {
    return bitCount( (byte)(s & 0xff)) + bitCount((byte)(s>>>8));
  }
  
  
  
  /**
   * Murmur3hash implementation with native pointer.
   * @param ptr the address of memory
   * @param len the length of memory
   * @param seed the seed 
   * @return hash value
   */
  public static int murmurHash(long ptr,  int len, int seed) {
    Unsafe unsafe = UnsafeAccess.theUnsafe;

    final int m = 0x5bd1e995;
    final int r = 24;
    final int length = len;
    int h = seed ^ length;

    final int len_4 = length >> 2;
    for (int i = 0; i < len_4; i++) {
      int i_4 = i << 2;
      int k = unsafe.getByte(ptr + i_4 + 3) & 0xff;
      k = k << 8;
      k = k | (unsafe.getByte(ptr + i_4 + 2) & 0xff);
      k = k << 8;
      k = k | (unsafe.getByte(ptr + i_4 + 1) & 0xff);
      k = k << 8;
      k = k | (unsafe.getByte(ptr + i_4) & 0xff);
      k *= m;
      k ^= k >>> r;
      k *= m;
      h *= m;
      h ^= k;
    }
    // avoid calculating modulo
    int len_m = len_4 << 2;
    int left = length - len_m;

    if (left != 0) {
      if (left >= 3) {
        h ^= (unsafe.getByte(ptr + length - 3) & 0xff) << 16;
      }
      if (left >= 2) {
        h ^= (unsafe.getByte(ptr + length - 2) & 0xff) << 8;
      }
      if (left >= 1) {
        h ^= (unsafe.getByte(ptr + length - 1) & 0xff);
      }

      h *= m;
    }

    h ^= h >>> 13;
    h *= m;
    h ^= h >>> 15;

    // This is a stupid thinh I have ever stuck upon
    if (h == Integer.MIN_VALUE) h = -(Integer.MIN_VALUE + 1);
    return h;

  }
  
  /**
   * Conversion string-number utility methods
   */
  
  /**
   * All possible chars for representing a number as a String
   */
  final static byte[] digits = {
      '0' , '1' , '2' , '3' , '4' , '5' ,
      '6' , '7' , '8' , '9' , 'a' , 'b' ,
      'c' , 'd' , 'e' , 'f' , 'g' , 'h' ,
      'i' , 'j' , 'k' , 'l' , 'm' , 'n' ,
      'o' , 'p' , 'q' , 'r' , 's' , 't' ,
      'u' , 'v' , 'w' , 'x' , 'y' , 'z'
  };
  final static byte [] DigitTens = {
      '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
      '1', '1', '1', '1', '1', '1', '1', '1', '1', '1',
      '2', '2', '2', '2', '2', '2', '2', '2', '2', '2',
      '3', '3', '3', '3', '3', '3', '3', '3', '3', '3',
      '4', '4', '4', '4', '4', '4', '4', '4', '4', '4',
      '5', '5', '5', '5', '5', '5', '5', '5', '5', '5',
      '6', '6', '6', '6', '6', '6', '6', '6', '6', '6',
      '7', '7', '7', '7', '7', '7', '7', '7', '7', '7',
      '8', '8', '8', '8', '8', '8', '8', '8', '8', '8',
      '9', '9', '9', '9', '9', '9', '9', '9', '9', '9',
      } ;

  final static byte [] DigitOnes = {
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      } ;
  /**
   * Convert string representation to a long value
   * @param ptr address, where string starts
   * @param size size of a string
   * @return
   */
  public static long strToLong(long ptr, int size) throws NumberFormatException {

    long result = 0;
    boolean negative = false;
    int i = 0, len = size;
    long limit = -Long.MAX_VALUE;
    long multmin;
    int digit;

    if (len > 0) {
      byte firstChar = UnsafeAccess.toByte(ptr + i);
      if (firstChar < '0') { // Possible leading "+" or "-"
        if (firstChar == '-') {
          negative = true;
          limit = Long.MIN_VALUE;
        } else if (firstChar != '+') throw new NumberFormatException();

        if (len == 1) // Cannot have lone "+" or "-"
          throw new NumberFormatException();
        i++;
      }
      multmin = limit / 10;
      while (i < len) {
        // Accumulating negatively avoids surprises near MAX_VALUE
        digit = UnsafeAccess.toByte(ptr + i++) - (byte)'0';
        if (digit < 0) {
          throw new NumberFormatException();
        }
        if (result < multmin) {
          throw new NumberFormatException();
        }
        result *= 10;
        if (result < limit + digit) {
          throw new NumberFormatException();
        }
        result -= digit;
      }
    } else {
      throw new NumberFormatException();
    }
    return negative ? result : -result;
  }
  /**
   * Converts long to string representation and stores it in memory
   * @param v long value
   * @param ptr memory address
   * @param size memory size
   * @return size of a string, if it il's larger than 'size', call fails
   */
  public static int longToStr(long i, long ptr, int size) {
    if (i == Long.MIN_VALUE) {
      byte[] buf = "-9223372036854775808".getBytes();
      if (buf.length > size) {
        return buf.length;
      }
      UnsafeAccess.copy(buf, 0, ptr, buf.length);
      return buf.length;
    }
    int s = (i < 0) ? stringSize(-i) + 1 : stringSize(i);
    if (s > size) {
      return s;
    }
    int sign = 0;
    if (i < 0) {
      sign = -1;
      i = -i;
    }
    long value = i;
    int index = s - 1;
    if (value == 0) {
      UnsafeAccess.putByte(ptr, (byte)'0');
    } else {
      while (value > 0) {
        long old = value;
        value /= 10; 
        long rem = old - 10 * value;
        UnsafeAccess.putByte( ptr + index--, (byte)(rem + '0'));
      }
    }
    
    if (sign != 0) {
      UnsafeAccess.putByte(ptr, (byte)'-');
    }
    return s;
  }
  
  
  
  /**
   * Places characters representing the integer i into the
   * character array buf. The characters are placed into
   * the buffer backwards starting with the least significant
   * digit at the specified index (exclusive), and working
   * backwards from there.
   *
   * Will fail if i == Long.MIN_VALUE
   */
  static void getChars(long i, long buf, int size) {
      long q;
      int r;
      int charPos = size;
      byte sign = 0;

      if (i < 0) {
          sign = '-';
          i = -i;
      }

      // Get 2 digits/iteration using longs until quotient fits into an int
      while (i > Integer.MAX_VALUE) {
          q = i / 100;
          // really: r = i - (q * 100);
          r = (int)(i - ((q << 6) + (q << 5) + (q << 2)));
          i = q;
          UnsafeAccess.putByte(buf + (--charPos), DigitOnes[r]);
          UnsafeAccess.putByte(buf + (--charPos), DigitTens[r]);
      }

      // Get 2 digits/iteration using ints
      int q2;
      int i2 = (int)i;
      while (i2 >= 65536) {
          q2 = i2 / 100;
          // really: r = i2 - (q * 100);
          r = i2 - ((q2 << 6) + (q2 << 5) + (q2 << 2));
          i2 = q2;
          UnsafeAccess.putByte(buf + (--charPos), DigitOnes[r]);
          UnsafeAccess.putByte(buf + (--charPos), DigitTens[r]);
      }

      // Fall thru to fast mode for smaller numbers
      // assert(i2 <= 65536, i2);
      for (;;) {
          q2 = (i2 * 52429) >>> (16+3);
          r = i2 - ((q2 << 3) + (q2 << 1));  // r = i2-(q2*10) ...
          UnsafeAccess.putByte(buf + (--charPos), digits[r]);
          i2 = q2;
          if (i2 == 0) break;
      }
      if (sign != 0) {
        UnsafeAccess.putByte(buf + (--charPos), sign);
      }
  }

  
  // Requires positive x
  static int stringSize(long x) {
      long p = 10;
      for (int i=1; i<19; i++) {
          if (x < p)
              return i;
          p = 10*p;
      }
      return 19;
  }
  
  /**
   * String - Double conversion
   *
   */
  
  static ThreadLocal<byte[]> buffer = new ThreadLocal<byte[]>() {

    @Override
    protected byte[] initialValue() {
      return new byte[64];
    }
  };
  
  /**
   * Converts string representation to double
   * @param ptr address of a string 
   * @param size size of a string
   * @return double
   */
  
  public static final double strToDouble (long ptr, int size) {
    if (size > 64 || size <= 0) {
      throw new NumberFormatException();
    }
    byte[] buf = buffer.get();
    UnsafeAccess.copy(ptr, buf, 0, size);
    // Yep, we create new string instance
    String s = new String(buf, 0, size);
    return Double.parseDouble(s);
  }
  
  /**
   * Converts double into string and stores it at memory address 'ptr'
   * @param d double value
   * @param ptr memory address
   * @param size memory size
   * @return size of a string representation, if size is greater than memory buffer
   *        size, call must be repeated
   */

  public final static int doubleToStr(double d, long ptr, int size) {
    String s = Double.toString(d);
    int len = s.length();
    if (len > size) {
      return len;
    }
    for(int i =0; i < len; i++) {
      UnsafeAccess.putByte(ptr + i, (byte) s.charAt(i));
    }
    return len;
  }
  /**
   * Generated random array (with possible repeats)
   * @param max max value
   * @param count total random elements count
   * @return random  long array sorted
   */
  public static long[] randomArray(long max, int count) {
     long[] ret = new long[count];
     for (int i = 0; i < count; i++) {
       long v = Math.abs(rnd.nextLong()) % max;
       ret[i] = v;
     }
     Arrays.sort(ret);
     return ret;
  }
  
  /**
   * TODO: test
   * Generated random array (with no possible repeats)
   * @param max max value
   * @param count total random elements count
   * @return random  distinct (array) sorted
   */
  public static long[] randomDistinctArray(long max, int count) {
     boolean reverseBuild = count > max/2;
     if (reverseBuild) {
       count = (int)(max - count); // Hey , this is not kosher
     }
     long[] ret = new long[count];
     Arrays.fill(ret, -1);    
     for (int i = 0; i < count; i++) {
       while (true) {
         long v = Math.abs(rnd.nextLong()) % max;
         if (!contains(ret, v)) {
           ret[i] = v;
           break;
         }
       }
     }
     
     Arrays.sort(ret);
     
     if (!reverseBuild) {
       return ret;
     } else {
       long[] arr = new long[(int)(max - count)];
       int k = 0;
       for (int i=0; i <= ret.length; i++) {
         long start = i == 0? 0: ret[i-1] + 1;
         long end = i == ret.length? max: ret[i];
         for(long n = start; n < end; n++, k++) {
           arr[k] = n;
         }
       }
       // already sorted
       return arr;
     }     
  }
  /**
   * Checks if array contains the element
   * @param arr integer array
   * @param v element
   * @return true, if - yes, false - otherwise
   */
  private static boolean contains(final long[] arr, final long v) {
    for (int i = 0; i < arr.length; i++) {
      if (arr[i] == v) {
        return true;
      } else if (arr[i] < 0) {
        break;
      }
    }  
    return false;
  }
  
  /**
   * Bit count in a memory block
   * @param valuePtr start address
   * @param valueSize length of a block
   * @return
   */
  public static long bitcount(long valuePtr, int valueSize) {
    
    int num8 = valueSize / Utils.SIZEOF_LONG;
    int rem8 = valueSize - Utils.SIZEOF_LONG * num8;
    long c = 0;
    long ptr = valuePtr;
    for(int i=0; i < num8; i++) {
      long v = UnsafeAccess.toLong(ptr);
      c += Long.bitCount(v);
      ptr += Utils.SIZEOF_LONG;
    }
    int num4 = rem8 / Utils.SIZEOF_INT;
    int rem4 = rem8 - Utils.SIZEOF_INT * num4;
    for(int i=0; i < num4; i++) {
      int v = UnsafeAccess.toInt(ptr);
      c += Integer.bitCount(v);
      ptr += Utils.SIZEOF_INT;
    }
    
    int num2 = rem4 / Utils.SIZEOF_SHORT;
    int rem2 = rem4 - Utils.SIZEOF_SHORT * num2;
    
    for(int i=0; i < num2; i++) {
      short v = UnsafeAccess.toShort(ptr);
      c += Utils.bitCount(v);
      ptr += Utils.SIZEOF_SHORT;
    }
    if (rem2 == 1) {
      byte v = UnsafeAccess.toByte(ptr);
      c += Utils.bitCount(v);
    }
    return c;
  }
  /** 
   * Returns first position of the set bit ('1')
   * @param valuePtr memory address start
   * @param valueSize memory block length
   * @return position of a first set ('1') bit or -1 if no set bits
   */
  public static long bitposSet(long valuePtr, int valueSize) {

    int num8 = valueSize / Utils.SIZEOF_LONG;
    int rem8 = valueSize - Utils.SIZEOF_LONG * num8;
    long ptr = valuePtr;
    int pos = 0;
    for(int i=0; i < num8; i++) {
      pos = firstBitSetLong(ptr);
      if (pos >= 0) {
        return (ptr - valuePtr) * Utils.SIZEOF_BYTE + pos;
      }
      ptr += Utils.SIZEOF_LONG;
    }
    int num4 = rem8 / Utils.SIZEOF_INT;
    int rem4 = rem8 - Utils.SIZEOF_INT * num4;
    for(int i=0; i < num4; i++) {
      pos = firstBitSetInt(ptr);
      if (pos >= 0) {
        return (ptr - valuePtr) * Utils.SIZEOF_BYTE + pos;
      }
      ptr += Utils.SIZEOF_INT;
    }
    
    int num2 = rem4 / Utils.SIZEOF_SHORT;
    int rem2 = rem4 - Utils.SIZEOF_SHORT * num2;
    
    for(int i=0; i < num2; i++) {
      pos = firstBitSetShort(ptr);
      if (pos >= 0) {
        return (ptr - valuePtr) * Utils.SIZEOF_BYTE + pos;
      }
      ptr += Utils.SIZEOF_SHORT;
    }
    
    if (rem2 == 1) {
      pos = firstBitSetByte(ptr);
      if (pos >= 0) {
        return (ptr - valuePtr) * Utils.SIZEOF_BYTE + pos;
      }
    }
    return -1;
  }

  /** 
   * Returns first position of a unset bit
   * @param valuePtr memory address start
   * @param valueSize memory block length
   * @return position of a first unset ('0') bit or -1
   */
  public static long bitposUnset(long valuePtr, int valueSize) {
    int num8 = valueSize / Utils.SIZEOF_LONG;
    int rem8 = valueSize - Utils.SIZEOF_LONG * num8;
    long ptr = valuePtr;
    int pos = 0;
    for(int i=0; i < num8; i++) {
      pos = firstBitUnSetLong(ptr);
      if (pos >= 0) {
        return (ptr - valuePtr) * Utils.SIZEOF_BYTE + pos;
      }
      ptr += Utils.SIZEOF_LONG;
    }
    int num4 = rem8 / Utils.SIZEOF_INT;
    int rem4 = rem8 - Utils.SIZEOF_INT * num4;
    for(int i=0; i < num4; i++) {
      pos = firstBitUnSetInt(ptr);
      if (pos >= 0) {
        return (ptr - valuePtr) * Utils.SIZEOF_BYTE + pos;
      }
      ptr += Utils.SIZEOF_INT;
    }
    
    int num2 = rem4 / Utils.SIZEOF_SHORT;
    int rem2 = rem4 - Utils.SIZEOF_SHORT * num2;
    
    for(int i=0; i < num2; i++) {
      pos = firstBitUnSetShort(ptr);
      if (pos >= 0) {
        return (ptr - valuePtr) * Utils.SIZEOF_BYTE + pos;
      }
      ptr += Utils.SIZEOF_SHORT;
    }
    
    if (rem2 == 1) {
      pos = firstBitUnSetByte(ptr);
      if (pos >= 0) {
        return (ptr - valuePtr) * Utils.SIZEOF_BYTE + pos;
      }
    }    
    return -1;
  }
  
  static final long FRACTION_MASK = 0x000fffffffffffffL; // 52 lower bits
  static final long MAX_FRACTION = FRACTION_MASK;
  static final long EXP_MASK = 0x7ff0000000000000L;
  static final long MAX_EXP = EXP_MASK;
  /**
   * Convert double to lexicographically sortable sequence of bytes,
   * which preserves double value order
   * @param ptr address of a buffer
   * @param v double value
   */
  public static void doubleToLex(long ptr, double v) {
    v = -v;
    long lv = Double.doubleToLongBits(v);
    // check first bit
    if ((lv >>> 63) == 1) {
      UnsafeAccess.putLong(ptr, lv);
    } else {
      // negative
      long exp = lv & EXP_MASK;
      exp = MAX_EXP - exp;
      long fraction = lv & FRACTION_MASK;
      fraction = MAX_FRACTION - fraction;
      lv = exp | fraction;
      UnsafeAccess.putLong(ptr, lv);
    }
  }
  
  /**
   * Reads double from a lexicographical stream of bytes
   * @param ptr address
   * @return double value
   */
  public static double lexToDouble(long ptr) {
    long lv = UnsafeAccess.toLong(ptr);
    if ((lv >>> 63) == 1) {
      return -Double.longBitsToDouble(lv);
    } else {
      long fraction = lv & FRACTION_MASK;
      fraction = MAX_FRACTION - fraction;
      long exp = lv & EXP_MASK;
      exp = MAX_EXP - exp;
      lv = exp | fraction;
      return -Double.longBitsToDouble(lv);
    }
  }
  
  /**
   * Gets overall allocated memory size 
   * for a list of objects
   * @param list
   * @return total size
   */
  public static long size(List<KeyValue> list) {
    long size = 0;
    for (KeyValue kv :list) {
      size += kv.keySize + kv.valueSize;
    }
    return size;
  }
  
  /**
   * Free memory
   * @param keys
   */
  public static void freeKeys(List<Key> keys) {
    for (Key k: keys) {
      UnsafeAccess.free(k.address);
    }
  }
  
  /**
   * Free memory
   * @param kvs
   */
  public static void freeKeyValues(List<KeyValue> kvs) {
    for (KeyValue kv: kvs) {
      UnsafeAccess.free(kv.keyPtr);
      UnsafeAccess.free(kv.valuePtr);
    }
  }
  
  /**
   * Reads memory as a string
   * @param ptr address
   * @param size size of a memory
   * @return string
   */
  public static String toString(long ptr, int size) {
    byte[] buf = new byte[size];
    UnsafeAccess.copy(ptr, buf, 0, size);
    return new String(buf);
  }
  
  /**
   * TODO: optimize for speed
   * TODO: Regex flavors? What flavor does Java support?
   * Checks if a memory blob specified by address and size
   * matches regular expression
   * @param ptr address
   * @param size size
   * @param pattern pattern to match
   * @return true - yes, false - otherwise
   */
  public static boolean matches(long ptr, int size, String pattern) {
    String s = toString(ptr, size);
    return s.matches(pattern);
  }
  
  /**
   * Read memory as byte array
   * @param ptr address
   * @param size size of a memory 
   * @return byte array
   */
  public static byte[] toBytes(long ptr, int size) {
    byte[] buf = new byte[size];
    UnsafeAccess.copy(ptr, buf, 0, size);
    return buf;
  }
  
  /**
   * Converts string representation of a number 
   * to a byte array
   * @param value string number
   * @return number as a byte array
   */
  public static byte[] numericStrToBytes(String value) {
    // value is numeric 
    long v = Long.parseLong(value);
    if (v < Byte.MAX_VALUE && v > Byte.MIN_VALUE) {
      return Bytes.toBytes((byte) v);
    } else if ( v < Short.MAX_VALUE && v > Short.MIN_VALUE) {
      return Bytes.toBytes((short) v);
    } else if ( v < Integer.MAX_VALUE && v > Integer.MIN_VALUE) {
      return Bytes.toBytes((int) v);
    } else {
      return Bytes.toBytes(v);
    }
  }
  
  /**
   * Generates random alphanumeric string
   * @param r random generator
   * @param size size of a string to generate
   * @return string
   */
  public static String getRandomStr(Random r, int size) {
    int start = 'A';
    int stop = 'Z';
    StringBuffer sb = new StringBuffer(size);
    for (int i = 0; i < size; i++) {
      int v = r.nextInt(stop - start) + start;
      sb.append((char)v);
    }
    return sb.toString();
  }
  
  /**
   * Counts elements in a reverse scanner
   * @param s scanner
   * @return total number of elements
   * @throws IOException
   */
  public static int countReverse (Scanner s) throws IOException {
    if (s == null) return 0;
    int total = 0;
    do {
      total++;
    } while(s.previous());
    return total;
  }
  /**
   * Counts elements in a direct scanner
   * @param s scanner
   * @return total number of elements
   * @throws IOException
   */
  public static int count (Scanner s) throws IOException {
    if (s == null) return 0;
    int total = 0;
    while(s.hasNext()) {
      total++;
      s.next();
    };
    return total;
  }
  
  /**
   * Checks if list has all unique members. List must be sorted.
   * @param list
   * @return true/false
   */
  public static <T extends Comparable<? super T>> boolean unique(List<T> list) {
    if (list.size() <= 1) return true;
    Collections.sort(list);
    for(int i = 1; i < list.size(); i++) {
      if (list.get(i-1).equals(list.get(i))) {
        return false;
      }
    }
    return true; 
  }
  
  public static void main(String[] args) {
    int count =0;
    int num = 100000000;
    long ptr = UnsafeAccess.malloc(num);
    while( count++ < 1000) {
      encode(ptr, num);
      
      long t1 = System.currentTimeMillis();
      long total = decode(ptr, num);
      long t2 = System.currentTimeMillis();
      
      System.out.println("total="+ total +" time="+(t2-t1)+"ms");
    }
  }
  
  private static long encode (long ptr, int num) {
    Random r = new Random();
    for (int i=0; i < num; i++) {
      writeUVInt(ptr + i, r.nextInt(128));
    }
    return ptr;
  }
  
  private static long decode(long ptr, int size) {
    int  off = 0;
    long totalSize = 0;
    while (off < size) {
      totalSize+=readUVInt(ptr + off);
      off++;
    }
    return totalSize;
  }
  
}
