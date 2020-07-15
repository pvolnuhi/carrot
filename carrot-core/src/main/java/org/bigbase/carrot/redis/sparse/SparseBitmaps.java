package org.bigbase.carrot.redis.sparse;

import static org.bigbase.carrot.redis.Commons.KEY_SIZE;

import java.io.IOException;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.BigSortedMapDirectMemoryScanner;
import org.bigbase.carrot.Key;
import org.bigbase.carrot.compression.Codec;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.redis.Commons;
import org.bigbase.carrot.redis.DataType;
import org.bigbase.carrot.redis.KeysLocker;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;


/**
 * 
 * This class provides sparse bitmap implementation. Sparse bitmap
 * are more memory efficient when population do not exceed 5-10%.
 * Bitmap storage allocation is done in chunks. Chunk size 4096 bytes.
 * Each chunk is stored in a compressed form. Compression codec is LZ4HC
 * @author Vladimir Rodionov
 *
 */
public class SparseBitmaps {

  /**
   * Allocation chunk size
   */
  final static int CHUNK_SIZE = 4096;
  
  /**
   * Total space to store number of set bits in a
   * allocation chunk
   */
  final static int BITS_SIZE = Utils.SIZEOF_INT;
  
  /**
   * Bits per chunk
   */
  final static int BITS_PER_CHUNK = Utils.SIZEOF_BYTE * (CHUNK_SIZE - BITS_SIZE);
  
  /**
   * Bytes per chunk
   */
  final static int BYTES_PER_CHUNK = CHUNK_SIZE - BITS_SIZE;
  
  private static ThreadLocal<Long> keyArena = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(512);
    }
  };
  
  private static ThreadLocal<Integer> keyArenaSize = new ThreadLocal<Integer>() {
    @Override
    protected Integer initialValue() {
      return 512;
    }
  };
  
  static ThreadLocal<Long> valueArena = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(512);
    }
  };
  
  static ThreadLocal<Integer> valueArenaSize = new ThreadLocal<Integer>() {
    @Override
    protected Integer initialValue() {
      return 512;
    }
  };
  
  
  static ThreadLocal<Key> key = new ThreadLocal<Key>() {
    @Override
    protected Key initialValue() {
      return new Key(0,0);
    }
  };
  
  static int BUFFER_CAPACITY = CHUNK_SIZE + 80;
  static ThreadLocal<Long> buffer = new ThreadLocal<Long>() {

    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(BUFFER_CAPACITY);
    }    
  };
  
  static Codec codec = CodecFactory.getInstance().getCodec(CodecType.LZ4HC);
  
  
  /**
   * Thread local updates Sparse Getbit
   */
  private static ThreadLocal<SparseGetBit> sparseGetbit = new ThreadLocal<SparseGetBit>() {
    @Override
    protected SparseGetBit initialValue() {
      return new SparseGetBit();
    } 
  };
  
  /**
   * Thread local updates Sparse SetBit
   */
  private static ThreadLocal<SparseSetBit> sparseSetbit = new ThreadLocal<SparseSetBit>() {
    @Override
    protected SparseSetBit initialValue() {
      return new SparseSetBit();
    } 
  };
  
  /**
   * Thread local updates Sparse Length
   */
  private static ThreadLocal<SparseLength> sparseLength = new ThreadLocal<SparseLength>() {
    @Override
    protected SparseLength initialValue() {
      return new SparseLength();
    } 
  };

  /**
   * Thread local updates Sparse SetChunk
   */
  private static ThreadLocal<SparseSetChunk> sparseSetchunk = new ThreadLocal<SparseSetChunk>() {
    @Override
    protected SparseSetChunk initialValue() {
      return new SparseSetChunk();
    } 
  };
  
  /**
   * Get compression / decompression buffer address
   * @return address
   */
  public static long getBufferAddress() {
    return buffer.get();
  }
  
  /**
   * Gets compression codec
   * @return compression codec
   */
  static Codec getCodec() {
    return codec;
  }
  
  /**
   * Should we compress chunk? It depends on population count
   * @param popCount
   * @return true, if yes, false - otherwise
   */
  static boolean shouldCompress(int popCount) {
    double limit = 0; 
    switch (codec.getType()) {
      case LZ4: limit = 0.12; break;
      case LZ4HC: limit = 0.15; break;
      case NONE: break;
    }
    return ((double) popCount) / BITS_PER_CHUNK < limit;
  }
  
  /**
   * Decompress chunk
   * @param chunkAddress chunk address
   * @param compressedSize compressed size
   * @return address of decompressed value
   */
  
  static long decompress(long chunkAddress, int compressedSize) {
    if (!isCompressed(chunkAddress)) {
      return chunkAddress;
    }
    long ptr = buffer.get();
    codec.decompress(chunkAddress + Utils.SIZEOF_INT, compressedSize, 
      ptr + Utils.SIZEOF_INT, BUFFER_CAPACITY - Utils.SIZEOF_INT);
    UnsafeAccess.putInt(ptr, getBitCount(chunkAddress));
    return ptr;
  }
  
  /**
   * Compress chunk
   * @param chunkAddress chunk address
   * @param buffer to compress to address
   * @return compressed chunk size, address is in buffer
   */
  static int compress(long chunkAddress /*Size is CHUNK_SIZE*/, int bitCount, long buffer) {
    int compSize = codec.compress(chunkAddress, CHUNK_SIZE - BITS_SIZE, 
      buffer + BITS_SIZE, BUFFER_CAPACITY);
    setBitCount(buffer, bitCount, true);
    return compSize;
  }
  
  /**
   * Is chunk compressed
   * @param chunkAddress chunk address
   * @return true, if - yes, false - otherwise
   */
  static boolean isCompressed(long chunkAddress) {
    return UnsafeAccess.toInt(chunkAddress) < 0; // First bit is set to '1'
  }
  
  /**
   * Set chunk compressed
   * @param chunkAddress chunk address
   * @param b true - compressed, false - otherwise
   */
  static void setCompressed(long chunkAddress, boolean b) {
    int v = b? 0x8fffffff: 0x7fffffff;
    int value = UnsafeAccess.toInt(chunkAddress);
    UnsafeAccess.putInt(chunkAddress, v & value);
  }
  
  /**
   * Get total bits set in a chunk
   * @param chunkAddress chunk address
   * @return number of bits set
   */
  static int getBitCount(long chunkAddress) {
    return UnsafeAccess.toInt(chunkAddress) & 0x7fffffff;
  }
  
  /**
   * 
   * @param chunkAddress
   * @param compressed
   */
  static void setBitCount(long chunkAddress, int count, boolean compressed) {
    if (compressed) {
      count |= 0x80000000; 
    }
    UnsafeAccess.putInt(chunkAddress, count);
  }
  
  /**
   * Increments bit set count in a chunk
   * @param chunkAddress chunk address
   * @param incr increment value
   */
  static void incrementBitCount(long chunkAddress, int incr) {
    int value = UnsafeAccess.toInt(chunkAddress);
    int count = value & 0x7fffffff;
    count += incr;
    value = (value & 0x80000000) | count;
    UnsafeAccess.putInt(chunkAddress, value);
  }
  
  
  /**
   * Checks key arena size
   * @param required size
   */
  
  static void checkKeyArena (int required) {
    int size = keyArenaSize.get();
    if (size >= required ) {
      return;
    }
    long ptr = UnsafeAccess.realloc(keyArena.get(), required);
    keyArena.set(ptr);
    keyArenaSize.set(required);
  }
  
  /**
   * Checks value arena size
   * @param required size
   */
  static void checkValueArena (int required) {
    int size = valueArenaSize.get();
    if (size >= required) {
      return;
    }
    long ptr = UnsafeAccess.realloc(valueArena.get(), required);
    valueArena.set(ptr);
    valueArenaSize.set(required);
  }
  /**
   * Build key for Sparse. It uses thread local key arena 
   * TODO: data type prefix
   * @param keyPtr original key address
   * @param keySize original key size
   * @param fieldPtr field address
   * @param fieldSize field size
   * @return new key size 
   */
    
   
  private static int buildKey( long keyPtr, int keySize) {
    checkKeyArena(keySize + KEY_SIZE + Utils.SIZEOF_BYTE);
    long arena = keyArena.get();
    int kSize = KEY_SIZE + keySize + Utils.SIZEOF_BYTE;
    UnsafeAccess.putByte(arena, (byte)DataType.SBITMAP.ordinal());
    UnsafeAccess.putInt(arena + Utils.SIZEOF_BYTE, keySize);
    UnsafeAccess.copy(keyPtr, arena + KEY_SIZE + Utils.SIZEOF_BYTE, keySize);
    return kSize;
  }
  
  /**
   * Build full key for Sparse. It uses thread local key arena 
   * @param keyPtr original key address
   * @param keySize original key size
   * @param offset offset of a bit
   * @return new key size 
   */
    
   
  private static int buildKey( long keyPtr, int keySize, long offset) {
    checkKeyArena(keySize + KEY_SIZE + Utils.SIZEOF_BYTE + Utils.SIZEOF_LONG);
    long arena = keyArena.get();
    int kSize = KEY_SIZE + keySize + Utils.SIZEOF_BYTE + Utils.SIZEOF_LONG;
    UnsafeAccess.putByte(arena, (byte)DataType.SBITMAP.ordinal());
    UnsafeAccess.putInt(arena + Utils.SIZEOF_BYTE, keySize);
    UnsafeAccess.copy(keyPtr, arena + KEY_SIZE + Utils.SIZEOF_BYTE, keySize);
    long chunkOffset = offset / BITS_PER_CHUNK;
    UnsafeAccess.putLong(arena + KEY_SIZE + Utils.SIZEOF_BYTE + keySize, chunkOffset);
    return kSize;
  }
  
  /**
   * Build full key for Sparse. It uses thread local key arena 
   * @param keyPtr original key address
   * @param keySize original key size
   * @param offset offset of a bit
   * @return new key size 
   */
    
   
  private static int buildKey( long keyPtr, int keySize, long offset, long arena) {
    int kSize = KEY_SIZE + keySize + Utils.SIZEOF_BYTE + Utils.SIZEOF_LONG;
    UnsafeAccess.putByte(arena, (byte)DataType.SBITMAP.ordinal());
    UnsafeAccess.putInt(arena + Utils.SIZEOF_BYTE, keySize);
    UnsafeAccess.copy(keyPtr, arena + KEY_SIZE + Utils.SIZEOF_BYTE, keySize);
    long chunkOffset = offset / BITS_PER_CHUNK;
    UnsafeAccess.putLong(arena + KEY_SIZE + Utils.SIZEOF_BYTE + keySize, chunkOffset);
    return kSize;
  }
  
  /** 
   * Get chunk offset from a chunk key
   * @param keyAddress key address
   * @param keySize key size
   */
  static long getChunkOffsetFromKey(long keyAddress, int keySize) 
  {
    // Last 8 bytes contains offset value
    long value = UnsafeAccess.toLong(keyAddress + keySize - Utils.SIZEOF_LONG);
    return value;
  }
  
  /**
   * Gets and initializes Key
   * @param ptr key address
   * @param size key size
   * @return key instance
   */
  private static Key getKey(long ptr, int size) {
    Key k = key.get();
    k.address = ptr;
    k.length = size;
    return k;
  }

  /**
   * Checks if key exists
   * @param map sorted map 
   * @param keyPtr key address
   * @param keySize key size
   * @return true if - yes, false - otherwise
   */
  public static boolean keyExists(BigSortedMap map, long keyPtr, int keySize) {
    int kSize = buildKey(keyPtr, keySize);
    return map.exists(keyArena.get(), kSize);
  }
  
  /**
   * Count the number of set bits (population counting) in a string.
   * By default all the bytes contained in the string are examined. 
   * It is possible to specify the counting operation only in an interval 
   * passing the additional arguments start and end.
   * Like for the GETRANGE command start and end can contain negative values 
   * in order to index bytes starting from the end of the string, where -1 is 
   * the last byte, -2 is the penultimate, and so forth.
   * Non-existent keys are treated as empty strings, so the command will return zero.
   * @param map ordered map
   * @param keyPtr key address
   * @param keySize key size
   * @param start start offset(inclusive)
   * @param end end offset (inclusive), if Common.NULL_LONG - unspecified
   * @return number of bits set or 0, if key does not exists
   */
  
  public static long SBITCOUNT(BigSortedMap map, long keyPtr, int keySize, long start, long end) {
    Key kk = getKey(keyPtr, keySize);
    BigSortedMapDirectMemoryScanner scanner = null;
    long endKeyPtr = 0;
    try {
      KeysLocker.readLock(kk);
      long strlen = -1;
      if (start == Commons.NULL_LONG) {
        start = 0;
      }
      if (start < 0 || (end != Commons.NULL_LONG && end < 0)) {
        strlen = SSTRLEN(map, keyPtr, keySize);
      }
      if (start < 0) {
        start = strlen + start;
      }
      if (end != Commons.NULL_LONG &&  end < 0) {
        end = strlen + end;
      }
      
      if (end < start) {
        return 0;
      }
      if (end == Commons.NULL_LONG) {
        end = Long.MAX_VALUE >>> 3;
      }
      int kSize = buildKey(keyPtr, keySize, start * Utils.SIZEOF_BYTE);
      endKeyPtr = UnsafeAccess.malloc(kSize);
      int endKeySize = buildKey(keyPtr, keySize, end * Utils.SIZEOF_BYTE, endKeyPtr);
      scanner = map.getScanner(keyArena.get(), kSize, endKeyPtr, endKeySize);
      if (scanner == null) {
        return 0;
      }
      long total = 0;
      try {
        while(scanner.hasNext()) {
          long valueAddress = scanner.valueAddress();
          int valueSize = scanner.valueSize();
          int bitsCount = SparseBitmaps.getBitCount(valueAddress);
          if (bitsCount == 0) {
            scanner.next();
            continue;
          }
          valueAddress = SparseBitmaps.decompress(valueAddress, valueSize - SparseBitmaps.BITS_SIZE);
          long keyAddress = scanner.keyAddress();
          int keyLength = scanner.keySize();
          long offset = SparseBitmaps.getChunkOffsetFromKey(keyAddress, keyLength) ;
          total += bitCount(valueAddress, offset, start, end);
          scanner.next();
        }       
      } catch (IOException e) {
      }
      return total;
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
        }
      }
      if (endKeyPtr != 0) {
        UnsafeAccess.free(endKeyPtr);
      }
      KeysLocker.readUnlock(kk);
    }
  }
  
  /**
   * Returns the bit value at offset in the string value stored at key.
   * When offset is beyond the string length, the string is assumed to be a 
   * contiguous space with 0 bits. When key does not exist it is assumed to be 
   * an empty string, so offset is always out of range and the value is also assumed 
   * to be a contiguous space with 0 bits.
   * @param map ordered map
   * @param keyPtr key address
   * @param keySize key length
   * @param offset offset to lookup bit
   * @return 1 or 0
   */
  public static int SGETBIT(BigSortedMap map, long keyPtr, int keySize, long offset) {
    Key kk = getKey(keyPtr, keySize);
    try {
      KeysLocker.readLock(kk);
      int kSize = buildKey(keyPtr, keySize, offset);
      SparseGetBit getbit = sparseGetbit.get();
      getbit.reset();
      getbit.setKeyAddress(keyArena.get());
      getbit.setKeySize(kSize);
      getbit.setOffset(offset);
      map.execute(getbit);
      return getbit.getBit();
    } finally {
      KeysLocker.readUnlock(kk);
    }
  }
  
  /**
   * Overwrites existing chunk or inserts new one
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @param offset offset of the chunk
   * @param chunkPtr chunk data address
   * @return true on success, false - otherwise
   */
  public static boolean setChunk(BigSortedMap map, long keyPtr, int keySize, long offset,
      long chunkPtr) {
    int kSize = buildKey(keyPtr, keySize, offset);
    SparseSetChunk setchunk = sparseSetchunk.get();
    setchunk.reset();
    setchunk.setKeyAddress(keyArena.get());
    setchunk.setKeySize(kSize);
    setchunk.setChunkAddress(chunkPtr);
    return map.execute(setchunk);
  }
  
  /**
   * Sets or clears the bit at offset in the string value stored at key.
   * The bit is either set or cleared depending on value, which can be either 0 or 1.
   * When key does not exist, a new string value is created. The string is grown to make 
   * sure it can hold a bit at offset. The offset argument is required to be greater than or 
   * equal to 0, and smaller than 232 (this limits bitmaps to 512MB). When the string at key 
   * is grown, added bits are set to 0. Actually we have higher limit - 2GB per value
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @param offset offset to set bit at
   * @param bit bit value (0 or 1)
   * @return old bit value (0 if did not exists)
   */
  public static int SSETBIT(BigSortedMap map, long keyPtr, int keySize, long offset, int bit) {
    Key kk = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(kk);
      int kSize = buildKey(keyPtr, keySize, offset);
      SparseSetBit setbit = sparseSetbit.get();
      setbit.reset();
      setbit.setKeyAddress(keyArena.get());
      setbit.setKeySize(kSize);
      setbit.setOffset(offset);
      setbit.setBit(bit);
      map.execute(setbit);
      return setbit.getOldBit();
    } finally {
      KeysLocker.writeUnlock(kk);
    }
  }
  
  /**
   * Returns the length of the string value stored at key. 
   * An error is returned when key holds a non-string value.
   * @param map sorted map
   * @param keyPtr key 
   * @param keySize
   * @return size of a value or 0 if does not exists
   */
  public static long SSTRLEN(BigSortedMap map, long keyPtr, int keySize) {
    Key kk = getKey(keyPtr, keySize);
    try {
      KeysLocker.readLock(kk);
      int kSize = buildKey(keyPtr, keySize, Long.MAX_VALUE);
      SparseLength strlen = sparseLength.get();
      strlen.reset();
      strlen.setKeyAddress(keyArena.get());
      strlen.setKeySize(kSize);
      map.execute(strlen);
      return strlen.getLength();
    } finally {
      KeysLocker.readUnlock(kk);
    }
  }
  
  /**
   * Return the position of the first bit set to 1 or 0 in a string.
   * The position is returned, thinking of the string as an array of bits from left to right, 
   * where the first byte's most significant bit is at position 0, the second byte's most 
   * significant bit is at position 8, and so forth.
   * The same bit position convention is followed by GETBIT and SETBIT.
   * By default, all the bytes contained in the string are examined. It is possible to look for 
   * bits only in a specified interval passing the additional arguments start and end 
   * (it is possible to just pass start, the operation will assume that the end is the last byte 
   * of the string. However there are semantic differences as explained later). The range is interpreted 
   * as a range of bytes and not a range of bits, so start=0 and end=2 means to look at the first three bytes.
   * Note that bit positions are returned always as absolute values starting from bit zero even when start 
   * and end are used to specify a range.
   * Like for the GETRANGE command start and end can contain negative values in order to index bytes starting 
   * from the end of the string, where -1 is the last byte, -2 is the penultimate, and so forth.
   * Non-existent keys are treated as empty strings.
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @param bit bit value to look for
   * @param start start offset (in bytes) inclusive
   * @param end end position (in bytes) inclusive, if Commons.NULL_LONG - means unspecified 
   * @return The command returns the position of the first bit set to 1 or 0 according to the request.
   * If we look for set bits (the bit argument is 1) and the string is empty or composed of just zero bytes,
   *  -1 is returned.
   * If we look for clear bits (the bit argument is 0) and the string only contains bit set to 1, 
   * the function returns the first bit not part of the string on the right. So if the string is three bytes 
   * set to the value 0xff the command BITPOS key 0 will return 24, since up to bit 23 all the bits are 1.
   * Basically, the function considers the right of the string as padded with zeros if you look for clear 
   * bits and specify no range or the start argument only.
   * However, this behavior changes if you are looking for clear bits and specify a range with both start 
   * and end. If no clear bit is found in the specified range, the function returns -1 as the user specified 
   * a clear range and there are no 0 bits in that range.
   */
  public static long SBITPOS(BigSortedMap map, long keyPtr, int keySize, int bit, long start, long end) {
    Key kk = getKey(keyPtr, keySize);
    BigSortedMapDirectMemoryScanner scanner = null;
    long endKeyPtr = 0;
    try {
      KeysLocker.readLock(kk);
      long strlen = -1;
      boolean startEndSet = start != Commons.NULL_LONG;
      if (start == Commons.NULL_LONG) {
        start = 0;
      }
      if (start < 0 || (end != Commons.NULL_LONG && end < 0)) {
        strlen = SSTRLEN(map, keyPtr, keySize);
      }
      if (start < 0) {
        start = strlen + start;
      }
      if (end != Commons.NULL_LONG &&  end < 0) {
        end = strlen + end;
      }
      
      if (end < start) {
        return bit == 1? -1: strlen * Utils.SIZEOF_BYTE;
      }
      if (end == Commons.NULL_LONG) {
        end = Long.MAX_VALUE >>> 3;
      }
      int kSize = buildKey(keyPtr, keySize, start * Utils.SIZEOF_BYTE);
      endKeyPtr = UnsafeAccess.malloc(kSize);
      int endKeySize = buildKey(keyPtr, keySize, end * Utils.SIZEOF_BYTE, endKeyPtr);
      scanner = map.getScanner(keyArena.get(), kSize, endKeyPtr, endKeySize);
      if (scanner == null) {
        return 0;
      }
      // TODO Check if bit == 0 and first chunk start not with 0 offset
      try {
        while(scanner.hasNext()) {
          long valueAddress = scanner.valueAddress();
          int valueSize = scanner.valueSize();
          int bitsCount = SparseBitmaps.getBitCount(valueAddress);
          if ((bit == 1 && bitsCount == 0) || 
              (bit == 0 && bitsCount == SparseBitmaps.BITS_PER_CHUNK)) {
            scanner.next();
            continue;
          }
          valueAddress = SparseBitmaps.decompress(valueAddress, valueSize - SparseBitmaps.BITS_SIZE);
          long keyAddress = scanner.keyAddress();
          int keyLength = scanner.keySize();
          long offset = SparseBitmaps.getChunkOffsetFromKey(keyAddress, keyLength) ;
          if (bit == 0 && (start * Utils.SIZEOF_BYTE < offset)) {
            return start * Utils.SIZEOF_BYTE;
          }
          long pos = getPosition(valueAddress, bit, offset, start, end);
          if (pos >=0) return pos;
          scanner.next();
        }
        
      } catch (IOException e) {
      }
      if (startEndSet && bit == 0) {
        if (strlen < 0) {
          strlen = SSTRLEN(map, keyPtr, keySize);
        }
        return strlen * Utils.SIZEOF_BYTE;
      }
      return -1;
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
        }
      }
      if (endKeyPtr != 0) {
        UnsafeAccess.free(endKeyPtr);
      }
      KeysLocker.readUnlock(kk);
    }
  }
  
  private static long bitCount(long valueAddress, long offset, long start, long end) {
    
    if ( start * 8 < offset && end * 8 > offset + SparseBitmaps.BITS_PER_CHUNK) {
      return SparseBitmaps.getBitCount(valueAddress);
    }
    
    long limit = valueAddress + SparseBitmaps.CHUNK_SIZE;
    if ( end < Long.MAX_VALUE >>> 3) {
      if (end * 8 < offset + SparseBitmaps.BITS_PER_CHUNK) {
        limit = valueAddress + (end - offset >>> 3);
      }
    }
    long ptr = valueAddress + SparseBitmaps.BITS_SIZE;
    // skip first bytes with bits info
    if (start > offset/8) {
      ptr +=  start - (offset >>> 3);
    }
    return Utils.bitcount(ptr, (int)(limit - ptr));
  }
  
  /**
   * Get first position of a bit in a chunk
   * @param valueAddress memory start address
   * @param bit 1 or 0
   * @param offset offset of this chunk
   * @param start start offset in bytes (inclusive)
   * @param end end offset in bytes (inclusive)
   * @return 
   */
  private static long getPosition(long valueAddress, int bit, long offset, long start, long end) {
    long limit = valueAddress + SparseBitmaps.CHUNK_SIZE;
    if ( end < Long.MAX_VALUE >>> 3) {
      if (end * 8 < offset + SparseBitmaps.BITS_PER_CHUNK) {
        limit = valueAddress + (end - offset >>> 3);
      }
    }
    long ptr = valueAddress + SparseBitmaps.BITS_SIZE;
    // skip first bytes with bits info
    if (start > offset/8) {
      ptr +=  start - (offset >>> 3);
    }
    long position = 0;
    if (bit == 1) {
      position = Utils.bitposSet(ptr, (int)(limit - ptr));
    } else {
      position = Utils.bitposUnset(ptr, (int)(limit - ptr));
    }
    if (position >= 0) position += offset;
    return position;
  }

  /**
   * Returns the substring of the string value stored at key, determined by the offsets 
   * start and end (both are inclusive). Negative offsets can be used in order to provide 
   * an offset starting from the end of the string. So -1 means the last character, 
   * -2 the penultimate and so forth.
   * The function handles out of range requests by limiting the resulting range to the actual length of the string.
   * @param map
   * @param keyPtr
   * @param keySize
   * @param start
   * @param end
   * @return size of a range, or -1, if key does not exists,
   *  if size > buferSize, the call must be repeated with appropriately sized buffer
   */
  public static long SGETRANGE(BigSortedMap map, long keyPtr, int keySize , long start, long end, 
      long bufferPtr, long bufferSize)
  {
    Key kk = getKey(keyPtr, keySize);
    BigSortedMapDirectMemoryScanner scanner = null;
    long endKeyPtr = 0;
    try {
      KeysLocker.readLock(kk);
      long strlen = -1;
      if (start == Commons.NULL_LONG) {
        start = 0;
      }
      if (start < 0 || end < 0) {
        strlen = SSTRLEN(map, keyPtr, keySize);
      }
      if (start < 0) {
        start = strlen + start;
      }
      if (end != Commons.NULL_LONG &&  end < 0) {
        end = strlen + end;
      }
      
      if (end < start) {
        return 0;
      }
      if (end == Commons.NULL_LONG) {
        end = strlen;
      }
      long rangeSize = end - start + 1; 
      if(end - start + 1 > bufferSize) {
        return rangeSize; // Buffer is too small
      }
      
      int kSize = buildKey(keyPtr, keySize, start * Utils.SIZEOF_BYTE);
      endKeyPtr = UnsafeAccess.malloc(kSize);
      int endKeySize = buildKey(keyPtr, keySize, end * Utils.SIZEOF_BYTE, endKeyPtr);
      scanner = map.getScanner(keyArena.get(), kSize, endKeyPtr, endKeySize);
      if (scanner == null) {
        return 0;
      }
      long off = start;
      try {
        while(scanner.hasNext()) {
          long valueAddress = scanner.valueAddress();
          int valueSize = scanner.valueSize();
          int bitsCount = SparseBitmaps.getBitCount(valueAddress);
          if (bitsCount == 0) {
            scanner.next();
            continue;
          }
          valueAddress = SparseBitmaps.decompress(valueAddress, valueSize - SparseBitmaps.BITS_SIZE);
          long keyAddress = scanner.keyAddress();
          int keyLength = scanner.keySize();
          long offset = SparseBitmaps.getChunkOffsetFromKey(keyAddress, keyLength) ;
          long bytesOffset = offset >>> 3;
          if (bytesOffset > off) {
            UnsafeAccess.setMemory(bufferPtr + (off - start), bytesOffset - off, (byte) 0);
            off = bytesOffset;
          }
          // Copy chunk
          UnsafeAccess.copy(valueAddress + BITS_SIZE, bufferPtr + off - start, CHUNK_SIZE - BITS_SIZE);
          off += CHUNK_SIZE - BITS_SIZE;
          scanner.next();
        }       
      } catch (IOException e) {
      }
      return rangeSize;
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
        }
      }
      if (endKeyPtr != 0) {
        UnsafeAccess.free(endKeyPtr);
      }
      KeysLocker.readUnlock(kk);
    }   
  }
  
  /**
   * Overwrites part of the string stored at key, starting at the specified offset, 
   * for the entire length of value. If the offset is larger than the current length of the string at key, 
   * the string is padded with zero-bytes to make offset fit. Non-existing keys are considered as empty 
   * strings, so this command will make sure it holds a string large enough to be able to set value at offset.
   * Note that the maximum offset that you can set is 229 -1 (536870911), as Redis Strings are limited
   * to 512 megabytes. If you need to grow beyond this size, you can use multiple keys.
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @param offset offset to set value
   * @param valuePtr value address
   * @param valueSize value size
   * @return new size of key's value
   */
  public static long SSETRANGE(BigSortedMap map, long keyPtr, int keySize, long offset,
      long valuePtr, long valueSize) {
    Key kk = getKey(keyPtr, keySize);

    try {
      KeysLocker.writeLock(kk);
      long strlen = SSTRLEN(map, keyPtr, keySize);
      int firstChunkSize = (int)(offset - (offset / BYTES_PER_CHUNK) * 
          BYTES_PER_CHUNK);
      int lastChunkSize = (int)(offset + valueSize- ((offset + valueSize) / BYTES_PER_CHUNK) * 
          BYTES_PER_CHUNK);
      
      // set first chunk
      long ptr = buffer.get();
      UnsafeAccess.setMemory(ptr, firstChunkSize, (byte)0);
      int toCopy;
      if (valueSize < BYTES_PER_CHUNK) {
        toCopy = Math.min((int)valueSize, BYTES_PER_CHUNK - firstChunkSize);
      } else {
        toCopy = BYTES_PER_CHUNK - firstChunkSize;
      }
      UnsafeAccess.copy(valuePtr, ptr + firstChunkSize, toCopy);
      long off = (offset - firstChunkSize);
      setChunk(map, keyPtr, keySize, off * Utils.SIZEOF_BYTE, ptr);
      off += BYTES_PER_CHUNK;
      while(off <= offset + valueSize - BYTES_PER_CHUNK) {
        setChunk(map, keyPtr, keySize, off * Utils.SIZEOF_BYTE, valuePtr + (off - offset));
        off += BYTES_PER_CHUNK;
      }
      // Set last chunk
      // set first chunk
      ptr = buffer.get();
      UnsafeAccess.setMemory(ptr + lastChunkSize, BYTES_PER_CHUNK - lastChunkSize, (byte)0);
      UnsafeAccess.copy(valuePtr + valueSize - lastChunkSize, ptr, lastChunkSize);
      off = (offset + valueSize - lastChunkSize);
      setChunk(map, keyPtr, keySize, off * Utils.SIZEOF_BYTE, ptr);
      
      long size = ((offset + valueSize)/BYTES_PER_CHUNK) * BYTES_PER_CHUNK + BYTES_PER_CHUNK;
      if (size > strlen) {
        return size;
      }
      return strlen;
    } finally {
      KeysLocker.readUnlock(kk);
    }
  }
}
