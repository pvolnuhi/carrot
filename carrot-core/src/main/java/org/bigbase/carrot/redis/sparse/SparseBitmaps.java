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
import org.bigbase.carrot.util.Scanner;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;


/**
 * 
 * This class provides sparse bitmap implementation. Sparse bitmap
 * are more memory efficient when population do not exceed 5-10%.
 * Bitmap storage allocation is done in chunks. Chunk size 4096 bytes.
 * Each chunk is stored in a compressed form. Compression codec is LZ4
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
   * allocation chunk as well as compression codec
   */
  final static int HEADER_SIZE = Utils.SIZEOF_SHORT;
  
  /**
   * Bits per chunk
   */
  final static int BITS_PER_CHUNK = Utils.BITS_PER_BYTE * (CHUNK_SIZE - HEADER_SIZE);
  
  /**
   * Bytes per chunk
   */
  final static int BYTES_PER_CHUNK = CHUNK_SIZE - HEADER_SIZE;
  
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
  
  static Codec codec = CodecFactory.getInstance().getCodec(CodecType.LZ4);
   
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
      default: // do nothing
    }
    return ((double) popCount) / BITS_PER_CHUNK < limit;
  }
  
  /**
   * Decompress chunk
   * @param chunkAddress chunk address
   * @param compressedSize compressed size
   * @return address of decompressed value - TLS buffer address 
   */
  
  static long decompress(long chunkAddress, int compressedSize) {
    if (!isCompressed(chunkAddress)) {
      return chunkAddress;
    }
    long ptr = buffer.get();
    codec.decompress(chunkAddress + HEADER_SIZE, compressedSize, 
      ptr + HEADER_SIZE, BUFFER_CAPACITY - HEADER_SIZE);
    UnsafeAccess.putShort(ptr, getBitCount(chunkAddress));
    return ptr;
  }
  
  /**
   * Compress chunk into provided buffer
   * @param chunkAddress chunk address
   * @param bitCount bits count
   * @param newChunk is this chunk new
   * @param buffer to compress to address
   * @return compressed chunk size, address is in buffer
   */
  static int compress(long chunkAddress /*Size is CHUNK_SIZE*/, int bitCount, 
      boolean newChunk, long buf) {
    int compSize = codec.compress(chunkAddress + HEADER_SIZE, CHUNK_SIZE - HEADER_SIZE, 
      buf + HEADER_SIZE, BUFFER_CAPACITY);
    setBitCount(buf, bitCount, true);
    if (newChunk && chunkAddress != buffer.get()) {
      // deallocate previous address
      UnsafeAccess.free(chunkAddress);
      // Update memory stats
      //TODO: make sure that 
      BigSortedMap.totalAllocatedMemory.addAndGet(-CHUNK_SIZE);
    }
    return compSize;
  }
  
  /**
   * Compress chunk with possible reallocation
   * @param chunkAddress chunkAddress
   * @param bitCount bit count
   * @param originPtr original chunk Address
   * @return compressed chunk size
   */
  static int compressRealloc(long chunkAddress, int bitCount, long originPtr) {
    //TODO tricky must return both address and compressed size
    return 0;
  }
  
  /**
   * TODO: test
   * Is chunk compressed
   * @param chunkAddress chunk address
   * @return true, if - yes, false - otherwise
   */
  static boolean isCompressed(long chunkAddress) {
    return UnsafeAccess.toShort(chunkAddress) < 0; // First bit is set to '1'
  }
  
  /**
   * TODO: test
   * Set chunk compressed
   * @param chunkAddress chunk address
   * @param b true - compressed, false - otherwise
   */
  static void setCompressed(long chunkAddress, boolean b) {
    short v = (short)(b? 0xffff: 0x7fff);
    short value = UnsafeAccess.toShort(chunkAddress);
    UnsafeAccess.putShort(chunkAddress, (short)(v & value));
  }
  
  /**
   * TODO: test
   * Get total bits set in a chunk
   * @param chunkAddress chunk address
   * @return number of bits set
   */
  static short getBitCount(long chunkAddress) {
    return (short)(UnsafeAccess.toShort(chunkAddress) & 0x7fff);
  }
  
  /**
   * Sets bit count TODO: test
   * @param chunkAddress
   * @param compressed
   */
  static void setBitCount(long chunkAddress, int count, boolean compressed) {
    if (compressed) {
      count |= 0x8000; 
    }
    UnsafeAccess.putShort(chunkAddress, (short)count);
  }
  
  /**
   * TODO: test
   * Increments bit set count in a chunk
   * @param chunkAddress chunk address
   * @param incr increment value
   */
  static void incrementBitCount(long chunkAddress, int incr) {
    short value = UnsafeAccess.toShort(chunkAddress);
    short count = (short)(value & 0x7fff);
    count += incr;
    value = (short)((value & 0x8000) | count);
    UnsafeAccess.putShort(chunkAddress, value);
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
   * Build PREFIX key for Sparse. It uses thread local key arena 
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
   * Build full key for a BLOCK for Sparse. It uses thread local key arena 
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
    long chunkOffset = (offset / BITS_PER_CHUNK) * BITS_PER_CHUNK;
    UnsafeAccess.putLong(arena + KEY_SIZE + Utils.SIZEOF_BYTE + keySize, chunkOffset);
    return kSize;
  }
  
  /**
   * Build full key for Sparse into provided buffer. It uses thread local key arena 
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
    long chunkOffset = (offset / BITS_PER_CHUNK) * BITS_PER_CHUNK;
    UnsafeAccess.putLong(arena + KEY_SIZE + Utils.SIZEOF_BYTE + keySize, chunkOffset);
    return kSize;
  }
  
  /** 
   * TODO: Check this method if keySize is FULL
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
   * TODO: test
   * Checks if key exists
   * @param map sorted map 
   * @param keyPtr key address
   * @param keySize key size
   * @return true if - yes, false - otherwise
   */
  public static boolean EXISTS(BigSortedMap map, long keyPtr, int keySize) {
    int kSize = buildKey(keyPtr, keySize);
    keyPtr = keyArena.get();
    Scanner scanner = null;
    try {
      scanner = map.getPrefixScanner(keyPtr, kSize);
      boolean result = scanner == null ? false : scanner.hasNext();
      if (scanner != null) {
        scanner.close();
      }
      return result;
    } catch (IOException e) {
    }
    return false;
  }
  
  /**
   * Delete sparse bitmap by Key
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @return number of deleted K-Vs
   */
  public static void DELETE(BigSortedMap map, long keyPtr, int keySize) {
    Key k = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(k);
      int newKeySize = keySize + KEY_SIZE + Utils.SIZEOF_BYTE;
      long kPtr = UnsafeAccess.malloc(newKeySize);
      UnsafeAccess.putByte(kPtr, (byte)DataType.SBITMAP.ordinal());
      UnsafeAccess.putInt(kPtr + Utils.SIZEOF_BYTE, keySize);
      UnsafeAccess.copy(keyPtr, kPtr + KEY_SIZE + Utils.SIZEOF_BYTE, keySize);
      long endKeyPtr = Utils.prefixKeyEnd(kPtr, newKeySize);
      
      map.deleteRange(kPtr, newKeySize, endKeyPtr, newKeySize);
      
      UnsafeAccess.free(kPtr);
      UnsafeAccess.free(endKeyPtr);
    } finally {
      KeysLocker.writeUnlock(k);
    }

  }
  
  /**
   * FIXME: TOO SLOW 
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
      
      if (start < 0) {
        start = 0;
      }
      
      if (end != Commons.NULL_LONG &&  end < 0) {
        end = strlen + end;
      }
      
      if (end == Commons.NULL_LONG) {
        // Make it very large
        end = Long.MAX_VALUE / Utils.BITS_PER_BYTE  - 1;
      }
      
      if (end < start || end < 0) {
        return 0;
      } 
      
      int kSize = buildKey(keyPtr, keySize, start * Utils.BITS_PER_BYTE);
      endKeyPtr = UnsafeAccess.malloc(kSize);
      int endKeySize = buildKey(keyPtr, keySize, (end) * Utils.BITS_PER_BYTE, endKeyPtr);
      // WRONG      
      endKeyPtr = Utils.prefixKeyEndNoAlloc(endKeyPtr, endKeySize);
      long total = 0;
      try {
        scanner = map.getScanner(keyArena.get() /* start key ptr*/, kSize /* start key size*/, 
          endKeyPtr /* end key ptr */, endKeySize /* end key size*/);
        
        if (scanner == null) {
          return 0;
        }
        while(scanner.hasNext()) {
          long valueAddress = scanner.valueAddress();
          int valueSize = scanner.valueSize();
          long keyAddress = scanner.keyAddress();
          int keyLength = scanner.keySize();
          long offset = getChunkOffsetFromKey(keyAddress, keyLength) ;
          long offsetBytes = offset / Utils.BITS_PER_BYTE;
          boolean lastChunk = offsetBytes <= end && (offsetBytes + BYTES_PER_CHUNK) > end;
          boolean firstChunk = offsetBytes <= start; 
          if (firstChunk || lastChunk) {
            valueAddress = isCompressed(valueAddress)? decompress(valueAddress, 
              valueSize - HEADER_SIZE): valueAddress;
            total += bitCount(valueAddress, offset, start, end);
          } else {
            int bitsCount = getBitCount(valueAddress);
            total += bitsCount;
          }
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
      long chunkPtr, boolean before) {
    int kSize = buildKey(keyPtr, keySize, offset);
    SparseSetChunk setchunk = sparseSetchunk.get();
    setchunk.reset();
    setchunk.setKeyAddress(keyArena.get());
    setchunk.setKeySize(kSize);
    setchunk.setChunkAddress(chunkPtr);
    setchunk.setBefore(before);
    setchunk.setOffset((int)(offset - (offset/BYTES_PER_CHUNK) * BYTES_PER_CHUNK));
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
  //TODO: update bitmap length?
  public static long SBITPOS(BigSortedMap map, long keyPtr, int keySize, int bit, long start, long end) {
    Key kk = getKey(keyPtr, keySize);
    BigSortedMapDirectMemoryScanner scanner = null;
    long endKeyPtr = 0;
    try {
      KeysLocker.readLock(kk);
      long strlen = -1;
      boolean startSet = start != Commons.NULL_LONG;
      boolean endSet = end != Commons.NULL_LONG;
      
      if (start == Commons.NULL_LONG) {
        start = 0;
      }
      if (start < 0 || (end != Commons.NULL_LONG && end < 0)) {
        strlen = SSTRLEN(map, keyPtr, keySize);
      }
      if (start < 0) {
        start = strlen + start;
      }
      
      if (start < 0) {
        start = 0;
      }
      
      if (end != Commons.NULL_LONG &&  end < 0) {
        end = strlen + end;
      } else if (end == Commons.NULL_LONG) {
        end = Long.MAX_VALUE / Utils.BITS_PER_BYTE - 1;
      }
      
      if (end < start || end < 0) {
        return -1;
      }
      
      int kSize = buildKey(keyPtr, keySize, start * Utils.BITS_PER_BYTE);
      endKeyPtr = UnsafeAccess.malloc(kSize);
      // end + 1 b/c end is inclusive
      int endKeySize = buildKey(keyPtr, keySize, end * Utils.BITS_PER_BYTE, endKeyPtr);
      
      //FIXME
      endKeyPtr = Utils.prefixKeyEndNoAlloc(endKeyPtr, endKeySize);
      
      scanner = map.getScanner(keyArena.get(), kSize, endKeyPtr, endKeySize);
      if (scanner == null) {
        // Special handling for unset bit
        // scanner == null, but it does not mean that set is NULL
        if (bit == 0 && EXISTS(map, keyPtr, keySize)) {
          return start * Utils.BITS_PER_BYTE;
        }
        return endSet && bit == 0? 0: -1;
      }
      // TODO Check if bit == 0 and first chunk start not with 0 offset
      boolean exists = false;
      
      try {
        while(scanner.hasNext()) {
          long keyAddress = scanner.keyAddress();
          int keyLength = scanner.keySize();
          long offset = SparseBitmaps.getChunkOffsetFromKey(keyAddress, keyLength) ;
          if (offset > 0 && bit == 0 && !exists && !startSet) {
            return 0;
          }
          // Again - edge case when we have a hole between start * Utils.BIT_PER_BYTE and offset
          if (offset > start * Utils.BITS_PER_BYTE && bit == 0) {
            return start * Utils.BITS_PER_BYTE;
          }
          exists = true;
          long valueAddress = scanner.valueAddress();
          int valueSize = scanner.valueSize();
          int bitsCount = SparseBitmaps.getBitCount(valueAddress);
          if ((bit == 1 && bitsCount == 0) /*SHOULD NOT BE POSSIBLE*/ || 
              (bit == 0 && bitsCount == SparseBitmaps.BITS_PER_CHUNK)) {
            scanner.next();
            continue;
          }

          valueAddress = isCompressed(valueAddress)? 
              SparseBitmaps.decompress(valueAddress, valueSize - HEADER_SIZE): valueAddress;

          long pos = getPosition(valueAddress, bit, offset /*bits*/, start /*bytes*/, end /*bytes*/);
          if (pos >= 0) {
            return pos;
          } 
          scanner.next();
        }
      } catch (IOException e) {
      }
      if (!endSet && bit == 0) {
        if (exists && strlen < 0) {
          strlen = SSTRLEN(map, keyPtr, keySize);
        } else {
          strlen = 0;
        }
        return strlen * Utils.BITS_PER_BYTE;
      } else if (bit == 0 /*&& EXISTS(map, keyPtr, keySize)*/) {
        // endSet = true
        return start * Utils.BITS_PER_BYTE;
      } else {
        return -1;
      } 
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
   * TODO: test it
   * Get bit count in the bit segment given 
   * offset, start and end
   * @param valueAddress address of a bit segment (4096 bytes)
   * @param offset offset of the segment
   * @param start start offset in bytes
   * @param end  end offset in bytes 
   * @return number of bits in this segment given all above 
   */
  static long bitCount(final long valueAddress, final long offset, final long start, final long end) {
    
    if (start * Utils.BITS_PER_BYTE <= offset && end * Utils.BITS_PER_BYTE >= 
        offset + SparseBitmaps.BITS_PER_CHUNK) {
      return getBitCount(valueAddress);
    }
       
    long limit = valueAddress + CHUNK_SIZE;
    if ( end < Long.MAX_VALUE / Utils.BITS_PER_BYTE) {
      if (end * Utils.BITS_PER_BYTE < offset + BITS_PER_CHUNK) {
        limit = valueAddress + HEADER_SIZE + (end - (offset / Utils.BITS_PER_BYTE)) + 1;
      }
    }
    long ptr = valueAddress + HEADER_SIZE;
    // skip first bytes with bits info
    if (start > offset / Utils.BITS_PER_BYTE) {
      ptr += start - (offset / Utils.BITS_PER_BYTE);
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
  static long getPosition(long valueAddress, int bit, long offset, long start, long end) {
    long limit = valueAddress + SparseBitmaps.CHUNK_SIZE;
    if ( end < Long.MAX_VALUE / Utils.BITS_PER_BYTE) {
      if (end * Utils.BITS_PER_BYTE < offset + SparseBitmaps.BITS_PER_CHUNK) {
        limit = valueAddress + SparseBitmaps.HEADER_SIZE + (end - offset / Utils.BITS_PER_BYTE) + 1;
      }
    }
    long ptr = valueAddress + SparseBitmaps.HEADER_SIZE;
    // skip first bytes with bits info
    if (start > offset/ Utils.BITS_PER_BYTE) {
      ptr +=  start - (offset / Utils.BITS_PER_BYTE);
    }
    long position = 0;
    if (bit == 1) {
      position = Utils.bitposSet(ptr, (int)(limit - ptr));
    } else {
      position = Utils.bitposUnset(ptr, (int)(limit - ptr));
    }
    if (position >= 0) {
      position += (offset >= start * Utils.BITS_PER_BYTE)? offset: start * Utils.BITS_PER_BYTE;
    }
    return position;
  }

  /**
   * Returns the substring of the string value stored at key, determined by the offsets 
   * start and end (both are inclusive). Negative offsets can be used in order to provide 
   * an offset starting from the end of the string. So -1 means the last character, 
   * -2 the penultimate and so forth.
   * The function handles out of range requests by limiting the resulting range to the actual length of the string.
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @param start start offset in bytes
   * @param end end offset in bytes
   * @param bufferPtr buffer to copy to
   * @param bufferSize  buffer size
   * @return size of a range, or -1, if key does not exists,
   *  if size > buferSize, the call must be repeated with appropriately sized buffer
   */
  public static long SGETRANGE(BigSortedMap map, long keyPtr, int keySize, long start, long end,
      long bufferPtr, int bufferSize) {
    Key kk = getKey(keyPtr, keySize);
    BigSortedMapDirectMemoryScanner scanner = null;
    long endKeyPtr = 0;
    try {
      KeysLocker.readLock(kk);
      long strlen = SSTRLEN(map, keyPtr, keySize);;
      if (strlen == 0) {
        return -1;
      }
      
      if (start == Commons.NULL_LONG) {
        start = 0;
      }
      
      if (start < 0) {
        start = strlen + start;
      }
      
      if (start < 0) {
        start = 0;
      }
      
      if (end != Commons.NULL_LONG && end < 0) {
        end = strlen + end;
      }
      
      if (end == Commons.NULL_LONG || ((strlen > 0) && end >= strlen)) {
        end = strlen - 1;
      }
      
      if (end < start || end < 0) {
        return 0;
      }

      //FIXME: start is always correct (inside), end can be larger than bitmap
      // so, this calculation is not correct in a general case
      // To make it work we must guarantee that bufferSize > end-start + 1
      long rangeSize = end - start + 1;
      if (end - start + 1 > bufferSize) {
        return rangeSize; // Buffer is too small
      }

      int kSize = buildKey(keyPtr, keySize, start * Utils.BITS_PER_BYTE);
      endKeyPtr = UnsafeAccess.malloc(kSize);
      int endKeySize = buildKey(keyPtr, keySize, end * Utils.BITS_PER_BYTE, endKeyPtr);
      // FIXME
      endKeyPtr = Utils.prefixKeyEndNoAlloc(endKeyPtr, endKeySize);

      scanner = map.getScanner(keyArena.get(), kSize, endKeyPtr, endKeySize);
      if (scanner == null) {
        // Either we hit a hole of all 0's
        // Or set does not exists
        if (EXISTS(map, keyPtr, keySize)) {
          return rangeSize; // yes - rangeSize - all are 0's
        } else {
          return -1; // set does not exists
        }
      }
      long off = start;
      try {
        // still can be empty
        while (scanner.hasNext()) {
          long valueAddress = scanner.valueAddress();
          int valueSize = scanner.valueSize();
          valueAddress =
              isCompressed(valueAddress) ? decompress(valueAddress, valueSize - HEADER_SIZE)
                  : valueAddress;
          long keyAddress = scanner.keyAddress();
          int keyLength = scanner.keySize();
          long offset = getChunkOffsetFromKey(keyAddress, keyLength);
          long bytesOffset = offset / Utils.BITS_PER_BYTE;
          long toCopy = 0;
          if (bytesOffset > off) {
            UnsafeAccess.setMemory(bufferPtr + (off - start), bytesOffset - off, (byte) 0);
            off = bytesOffset;
          } else if (bytesOffset < off) {
            // First segment
            toCopy = Math.min(end - off + 1, BYTES_PER_CHUNK - (off - bytesOffset));
            UnsafeAccess.copy(valueAddress + HEADER_SIZE + off - bytesOffset,
              bufferPtr + off - start, toCopy);
            off += toCopy;
            scanner.next();
            continue;
          }
          // This handles last segment
          toCopy = Math.min(end - off + 1, BYTES_PER_CHUNK);
          // Copy chunk
          UnsafeAccess.copy(valueAddress + HEADER_SIZE, bufferPtr + off - start, toCopy);
          off += toCopy;
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
      int zeroPrefixSize = (int)(offset - (offset / BYTES_PER_CHUNK) * 
          BYTES_PER_CHUNK);
      int zeroSuffixSize = (int)((BYTES_PER_CHUNK + ((offset + valueSize) / BYTES_PER_CHUNK) * 
          BYTES_PER_CHUNK) - (offset + valueSize));
      
      // set first chunk
      long ptr = buffer.get();
      UnsafeAccess.setMemory(ptr, zeroPrefixSize, (byte)0);
      int toCopy;
     
      toCopy = Math.min((int)valueSize, BYTES_PER_CHUNK - zeroPrefixSize);

      UnsafeAccess.copy(valuePtr, ptr + zeroPrefixSize, toCopy);
      
      // Overwrite first chunk
      setChunk(map, keyPtr, keySize, offset * Utils.BITS_PER_BYTE, ptr, true);

      long off = (offset / BYTES_PER_CHUNK) * BYTES_PER_CHUNK + BYTES_PER_CHUNK;

      while(off <= offset + valueSize - BYTES_PER_CHUNK) {
        setChunk(map, keyPtr, keySize, off * Utils.BITS_PER_BYTE, valuePtr + (off - offset), true);
        off += BYTES_PER_CHUNK;
      }
      // Set last chunk
      ptr = buffer.get();
      UnsafeAccess.setMemory(ptr + zeroSuffixSize, BYTES_PER_CHUNK - zeroSuffixSize, (byte)0);
      UnsafeAccess.copy(valuePtr + valueSize - zeroSuffixSize, ptr, zeroSuffixSize);
      off = ((offset + valueSize)/ BYTES_PER_CHUNK) * BYTES_PER_CHUNK;
      setChunk(map, keyPtr, keySize, off * Utils.BITS_PER_BYTE, ptr, false);
      
      long strlen = SSTRLEN(map, keyPtr, keySize);

      return strlen;
    } finally {
      KeysLocker.writeUnlock(kk);
    }
  }
}
