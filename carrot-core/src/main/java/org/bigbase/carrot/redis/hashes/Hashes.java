package org.bigbase.carrot.redis.hashes;

import static org.bigbase.carrot.redis.Commons.KEY_SIZE;
import static org.bigbase.carrot.redis.Commons.NUM_ELEM_SIZE;
import static org.bigbase.carrot.redis.Commons.ZERO;
import static org.bigbase.carrot.redis.Commons.keySizeWithPrefix;
import static org.bigbase.carrot.redis.Commons.numElementsInValue;
import static org.bigbase.carrot.redis.KeysLocker.readLock;
import static org.bigbase.carrot.redis.KeysLocker.readUnlock;
import static org.bigbase.carrot.redis.KeysLocker.writeLock;
import static org.bigbase.carrot.redis.KeysLocker.writeUnlock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.BigSortedMapDirectMemoryScanner;
import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.Key;
import org.bigbase.carrot.KeyValue;
import org.bigbase.carrot.redis.DataType;
import org.bigbase.carrot.redis.KeysLocker;
import org.bigbase.carrot.redis.MutationOptions;
import org.bigbase.carrot.redis.OperationFailedException;
import org.bigbase.carrot.util.Pair;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Support for packing multiple field-values into one K-V value
 * under the same key. This is for compact representation of naturally ordered
 * HASHEs. key -> field -> value under common key
 * @author Vladimir Rodionov
 *
 */
public class Hashes {
  

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
  
  static int INCR_ARENA_SIZE = 64;
  static ThreadLocal<Long> incrArena = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(INCR_ARENA_SIZE);
    }
  };
  
  /**
   * Thread local updates Hash Exists
   */
  private static ThreadLocal<HashExists> hashExists = new ThreadLocal<HashExists>() {
    @Override
    protected HashExists initialValue() {
      return new HashExists();
    } 
  };
  
  /**
   * Thread local updates Hash Set
   */
  private static ThreadLocal<HashSet> hashSet = new ThreadLocal<HashSet>() {
    @Override
    protected HashSet initialValue() {
      return new HashSet();
    } 
  };
  
  /**
   * Thread local updates Hash Delete
   */
  private static ThreadLocal<HashDelete> hashDelete = new ThreadLocal<HashDelete>() {
    @Override
    protected HashDelete initialValue() {
      return new HashDelete();
    } 
  };
  
  /**
   * Thread local updates Hash Get
   */
  private static ThreadLocal<HashGet> hashGet = new ThreadLocal<HashGet>() {
    @Override
    protected HashGet initialValue() {
      return new HashGet();
    } 
  };
  
  
  /**
   * Thread local updates Hash Value Length
   */
  private static ThreadLocal<HashValueLength> hashValueLength = new ThreadLocal<HashValueLength>() {
    @Override
    protected HashValueLength initialValue() {
      return new HashValueLength();
    } 
  };
  
  
  private static ThreadLocal<Key> key = new ThreadLocal<Key>() {
    @Override
    protected Key initialValue() {
      return new Key(0,0);
    }   
  };
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
   * Build key for Hash. It uses thread local key arena 
   * TODO: data type prefix
   * @param keyPtr original key address
   * @param keySize original key size
   * @param fieldPtr field address
   * @param fieldSize field size
   * @return new key size 
   */
    
   
  public static int buildKey( long keyPtr, int keySize, long fieldPtr, int fieldSize) {
    checkKeyArena(keySize + KEY_SIZE + fieldSize + Utils.SIZEOF_BYTE);
    long arena = keyArena.get();
    int kSize = KEY_SIZE + keySize + Utils.SIZEOF_BYTE;
    UnsafeAccess.putByte(arena, (byte) DataType.HASH.ordinal());
    UnsafeAccess.putInt(arena + Utils.SIZEOF_BYTE, keySize);
    UnsafeAccess.copy(keyPtr, arena + KEY_SIZE + Utils.SIZEOF_BYTE, keySize);
    if (fieldPtr > 0) {
      UnsafeAccess.copy(fieldPtr, arena + kSize, fieldSize);
      kSize += fieldSize;
    }
    return kSize;
  }
  
  /**
   * Build key for Hash. It uses provided arena 
   * @param keyPtr original key address
   * @param keySize original key size
   * @param fieldPtr field address
   * @param fieldSize field size
   * @return new key size 
   */
    
   
  public static int buildKey(long keyPtr, int keySize, long fieldPtr, int fieldSize, long arena) {
    int kSize = KEY_SIZE + keySize + Utils.SIZEOF_BYTE;
    UnsafeAccess.putByte(arena, (byte) DataType.HASH.ordinal());
    UnsafeAccess.putInt(arena + Utils.SIZEOF_BYTE, keySize);
    UnsafeAccess.copy(keyPtr, arena + KEY_SIZE + Utils.SIZEOF_BYTE, keySize);
    if (fieldPtr > 0) {
      UnsafeAccess.copy(fieldPtr, arena + kSize, fieldSize);
      kSize += fieldSize;
    }
    return kSize;
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
   * Delete hash by Key
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @return true on success
   */
  public static boolean DELETE(BigSortedMap map, long keyPtr, int keySize) {
    Key k = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(k);
      int newKeySize = keySize + KEY_SIZE + 2 * Utils.SIZEOF_BYTE;
      long kPtr = UnsafeAccess.malloc(newKeySize);
      UnsafeAccess.putByte(kPtr, (byte) DataType.HASH.ordinal());
      UnsafeAccess.putInt(kPtr + Utils.SIZEOF_BYTE, keySize);
      UnsafeAccess.copy(keyPtr, kPtr + KEY_SIZE + Utils.SIZEOF_BYTE, keySize);
      UnsafeAccess.putByte(kPtr + keySize + KEY_SIZE + Utils.SIZEOF_BYTE, (byte)0);
      long endKeyPtr = Utils.prefixKeyEnd(kPtr, newKeySize - 1);            
      long total = map.deleteRange(kPtr, newKeySize, endKeyPtr, newKeySize - 1);
      UnsafeAccess.free(kPtr);
      UnsafeAccess.free(endKeyPtr);
      return total > 0;
    } finally {
      KeysLocker.writeUnlock(k);
    }
  }
  
  /**
   * TODO: OPTIMIZE for SPEED
   * Sets field in the hash stored at key to value. If key does not exist, a new key holding 
   * a hash is created. If field already exists in the hash, it is overwritten.
   * As of Redis 4.0.0, HSET is variadic and allows for multiple field/value pairs.
   * Return value
   * Integer reply: The number of fields that were added.   
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @return The number of fields that were added.
   */
  public static int HSET(BigSortedMap map, long keyPtr, int keySize, long[] fieldPtrs,
      int[] fieldSizes, long[] valuePtrs, int[] valueSizes) {

    Key k = getKey(keyPtr, keySize);
    int count = 0;
    try {
      writeLock(k);
      for(int i=0; i < fieldPtrs.length; i++) {
        long fieldPtr = fieldPtrs[i];
        int fieldSize = fieldSizes[i];
        long valuePtr = valuePtrs[i];
        int valueSize = valueSizes[i];
        int kSize = buildKey(keyPtr, keySize, fieldPtr, fieldSize);
        HashSet set = hashSet.get();
        set.reset();
        set.setKeyAddress(keyArena.get());
        set.setKeySize(kSize);
        set.setFieldValue(valuePtr, valueSize);
        set.setOptions(MutationOptions.NONE);
        // version?
        if(map.execute(set)) {
          count++;
        }
      }
      return count;
    } finally {
      writeUnlock(k);
    }
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param keyPtr hash key address
   * @param keySize hash key size
   * @param kvs list of K-V
   * @return number of added
   */
  public static int HSET(BigSortedMap map, long keyPtr, int keySize, List<KeyValue> kvs) {

    Key k = getKey(keyPtr, keySize);
    int count = 0;
    try {
      writeLock(k);
      
      for(KeyValue kv: kvs) {
        long fieldPtr = kv.keyPtr;
        int fieldSize = kv.keySize;
        long valuePtr = kv.valuePtr;
        int valueSize = kv.valueSize;
        int kSize = buildKey(keyPtr, keySize, fieldPtr, fieldSize);
        HashSet set = hashSet.get();
        set.reset();
        set.setKeyAddress(keyArena.get());
        set.setKeySize(kSize);
        set.setFieldValue(valuePtr, valueSize);
        set.setOptions(MutationOptions.NONE);
        // version?
        if(map.execute(set)) {
          count++;
        }
        if (count % 100000 == 0) {
          System.out.println("Loaded "+ count);
        }
      }
      return count;
    } finally {
      writeUnlock(k);
    }
  }
  
  /** 
   * For testing only
   */
  public static int HSET(BigSortedMap map, String key, List<KeyValue> kvs) {
    long keyPtr = UnsafeAccess.allocAndCopy(key.getBytes(), 0, key.length());
    int keySize = key.length();
    int result = HSET(map, keyPtr, keySize, kvs);
    UnsafeAccess.free(keyPtr);
    return result;
  }
  
  /** 
   * For testing only
   */
  public static int HSET(BigSortedMap map, Key key, List<KeyValue> kvs) {
    long keyPtr = key.address;
    int keySize = key.length;
    int result = HSET(map, keyPtr, keySize, kvs);
    return result;
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key hash key
   * @param field field name
   * @param value field value
   * @return 1 - success, 0 - otherwise
   */
  public static int HSET(BigSortedMap map, String key, String field, String value)
  {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long fieldPtr = UnsafeAccess.allocAndCopy(field, 0, field.length());
    int fieldSize = field.length();
    long valuePtr = UnsafeAccess.allocAndCopy(value, 0, value.length());
    int valueSize = value.length();
    
    int result = HSET(map, keyPtr, keySize, fieldPtr, fieldSize, valuePtr, valueSize);
    
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(fieldPtr);
    UnsafeAccess.free(valuePtr);
    return result;
  }
  
  /**
   * HSET for a single field-value (zero object allocation)
   * @param map sorted map storage
   * @param keyPtr set key address
   * @param keySize set key size
   * @param fieldPtr field name address
   * @param fieldSize field name size
   * @param valuePtr value name address
   * @param valueSize value name size
   * @return 1 or 0
   */
  public static int HSET(BigSortedMap map, long keyPtr, int keySize, long fieldPtr, int fieldSize,
      long valuePtr, int valueSize) {

    Key k = getKey(keyPtr, keySize);
    int count = 0;
    try {
      writeLock(k);
      int kSize = buildKey(keyPtr, keySize, fieldPtr, fieldSize);
      HashSet set = hashSet.get();
      set.reset();
      set.setKeyAddress(keyArena.get());
      set.setKeySize(kSize);
      set.setFieldValue(valuePtr, valueSize);
      set.setOptions(MutationOptions.NONE);
      // version?
      if (map.execute(set)) {
        count++;
      }
      return count;
    } finally {
      writeUnlock(k);
    }
  }
  
  /**
   * For testing only
   * @param map sorted map store
   * @param key key
   * @param member member
   * @param value value
   * @return 1 or 0
   */
  public static int HSET (BigSortedMap map, byte[] key, byte[] member, byte[] value) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length);
    int keySize = key.length;
    long memberPtr = UnsafeAccess.allocAndCopy(member, 0, member.length);
    int memberSize = member.length;
    long valuePtr = UnsafeAccess.allocAndCopy(value, 0, value.length);
    int valueSize = value.length;
    
    int result = HSET(map, keyPtr, keySize, memberPtr, memberSize, valuePtr, valueSize);
    
    UnsafeAccess.free(valuePtr);
    UnsafeAccess.free(memberPtr);
    UnsafeAccess.free(keyPtr);
    return result;
  }
  
  /**
   * HMSET - obsolete
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @return The number of fields that were added.
   */
  public static int HMSET(BigSortedMap map, long keyPtr, int keySize, long[] fieldPtrs,
      int[] fieldSizes, long[] valuePtrs, int[] valueSizes) {
    return HSET(map, keyPtr, keySize, fieldPtrs, fieldSizes, valuePtrs, valueSizes);
  }

  /**
   * For testing only
   * @param map sorted map storage
   * @param key hash key
   * @param field filed name
   * @param value value
   * @return 1 - OK, 0 - field exists already
   */
  
  public static int HSETNX (BigSortedMap map, String key, String field, String value) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long fieldPtr = UnsafeAccess.allocAndCopy(field, 0, field.length());
    int fieldSze = field.length();
    long valPtr = UnsafeAccess.allocAndCopy(value, 0, value.length());
    int valSize = value.length();
    
    int result = HSETNX(map, keyPtr, keySize, fieldPtr, fieldSze, valPtr, valSize);
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(fieldPtr);
    UnsafeAccess.free(valPtr);
    return result;
  }
  
  
  /**
   * Sets field in the hash stored at key to value, only if field does not yet exist. 
   * If key does not exist, a new key holding a hash is created. If field already 
   * exists, this operation has no effect.
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @return 1 if success, 0 if field exists
   */
  public static int HSETNX(BigSortedMap map, long keyPtr, int keySize, 
      long fieldPtr, int fieldSize, long valuePtr, int valueSize) {
    
    Key k = getKey(keyPtr, keySize);
   
    try {
      writeLock(k);
      int kSize = buildKey(keyPtr, keySize, fieldPtr, fieldSize);
      HashSet set = hashSet.get();
      set.reset();
      set.setKeyAddress(keyArena.get());
      set.setKeySize(kSize);
      set.setFieldValue(valuePtr, valueSize);
      set.setOptions(MutationOptions.NX);
      // version?    
      return map.execute(set) == false? 0: 1;
    } finally {
      writeUnlock(k);
    }
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key hash's key
   * @return number of fields in a hash
   */
  
  public static long HLEN (BigSortedMap map, String key) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long result = HLEN(map, keyPtr, keySize);
    UnsafeAccess.free(keyPtr);
    return result;
  }
  
  /**
   * Returns the number of fields contained in the hash stored at key.
   * Return value
   * Integer reply: number of fields in the hash, or 0 when key does not exist.   
   * @param map sorted map storage
   * @param keyPtr hash key
   * @param keySize hash key size
   * @return number of elements(fields)
   */
  public static long HLEN(BigSortedMap map, long keyPtr, int keySize) {
    
    Key k = getKey(keyPtr, keySize);

    try {
      readLock(k);
      int kSize = buildKey(keyPtr, keySize, 0, 0);
      long ptr = keyArena.get();
      BigSortedMapDirectMemoryScanner scanner = map.getPrefixScanner(ptr, kSize);
      if (scanner == null) {
        return 0; // empty or does not exists
      }
      long total = 0;
      try {
        while (scanner.hasNext()) {
          long valuePtr = scanner.valueAddress();
          total += numElementsInValue(valuePtr);
          scanner.next();
        } 
        scanner.close();
      } catch (IOException e) {
        // should never be thrown
      }
      return total;
    } finally {
      readUnlock(k);
    }
  }
  /**
   * Checks if the Hash is empty
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @return true - yes, false - otherwise
   */
  public static boolean isEmpty(BigSortedMap map, long keyPtr, int keySize) {

    Key k = getKey(keyPtr, keySize);
    long kPtr = 0, endKeyPtr = 0;
    try {
      readLock(k);
      int newKeySize = keySize + KEY_SIZE + 2 * Utils.SIZEOF_BYTE;
      kPtr = UnsafeAccess.malloc(newKeySize);
      UnsafeAccess.putByte(kPtr, (byte) DataType.HASH.ordinal());
      UnsafeAccess.putInt(kPtr + Utils.SIZEOF_BYTE, keySize);
      UnsafeAccess.copy(keyPtr, kPtr + KEY_SIZE + Utils.SIZEOF_BYTE, keySize);
      UnsafeAccess.putByte(kPtr + keySize + KEY_SIZE + Utils.SIZEOF_BYTE, (byte)0);
      
      endKeyPtr = Utils.prefixKeyEnd(kPtr, newKeySize - 1); 
      
      BigSortedMapDirectMemoryScanner scanner = map.getScanner(kPtr, newKeySize, endKeyPtr, newKeySize - 1);
      if (scanner == null) {
        return true; // empty or does not exists
      }
      long total = 0;
      while (scanner.hasNext()) {
        long valuePtr = scanner.valueAddress();
        total += numElementsInValue(valuePtr);
        if (total > 0) {
          scanner.close();
          return false;
        }
        scanner.next();
      }
      scanner.close();
    } catch (IOException e) {
      // should never be thrown
    } finally {
      readUnlock(k);
      if (endKeyPtr > 0) {
        UnsafeAccess.free(endKeyPtr);
      }
      if (kPtr > 0) {
        UnsafeAccess.free(kPtr);
      }
    }
    return true;
  }
  
  /**
   * Return serialized hash size
   * @param map ordered map
   * @param keyPtr key address
   * @param keySize key size
   * @return hash size in bytes
   */
  public static long getHashSizeInBytes(BigSortedMap map, long keyPtr, int keySize) {
    Key k = getKey(keyPtr, keySize);

    try {
      readLock(k);      
      int kSize = buildKey(keyPtr, keySize, 0, 0);
      long ptr = keyArena.get();
      BigSortedMapDirectMemoryScanner scanner = map.getPrefixScanner(ptr, kSize);
      if (scanner == null) {
        return 0; // empty or does not exists
      }
      long total = 0;
      try {
        while (scanner.hasNext()) {
          long valueSize = scanner.valueSize();
          total += valueSize - NUM_ELEM_SIZE;
        }
        scanner.close();
      } catch (IOException e) {
        // should never be thrown
      }
      return total;
    } finally {
      readUnlock(k);
    }
  }
  
  /**
   * Available since 2.0.0. Atomic operation
   * Time complexity: O(N) where N is the number of fields to be removed.
   * Removes the specified fields from the hash stored at key. Specified fields that do not exist 
   * within this hash are ignored. If key does not exist, it is treated as an empty hash and this 
   * command returns 0.
   * Return value
   * Integer reply: the number of fields that were removed from the hash, not including 
   * specified but non existing fields.
   *  History
   *  >= 2.4: Accepts multiple field arguments. Redis versions older than 2.4 can only remove a field per call.
   *  To remove multiple fields from a hash in an atomic fashion in earlier versions, use a MULTI / EXEC block.
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @return the number of fields that were removed from the hash
   */
  
  public static int HDEL(BigSortedMap map, long keyPtr, int keySize, long[] fieldPtrs, int[] fieldSizes) {
    
    Key k = getKey(keyPtr, keySize);
    int deleted = 0;
    try {
      writeLock(k);
      if (!keyExists(map, keyPtr, keySize)) {
        return 0;
      }
      for (int i = 0; i < fieldPtrs.length; i++) {
        long fieldPtr = fieldPtrs[i];
        int fieldSize = fieldSizes[i];

        int kSize = buildKey(keyPtr, keySize, fieldPtr, fieldSize);
        HashDelete update = hashDelete.get();
        update.reset();
        update.setMap(map);
        update.setKeyAddress(keyArena.get());
        update.setKeySize(kSize);        
        if (map.execute(update)) {
          deleted++;
          if (update.checkForEmpty() && isEmpty(map, keyPtr, keySize)) {
            DELETE(map, keyPtr, keySize);
            break;
          }
        }
      }
      return deleted;
    } finally {
      writeUnlock(k);
    }
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key hash key
   * @param field hash field
   * @return 1 - deleted, 0 - does not exist
   */
  public static int HDEL(BigSortedMap map, String key, String field) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long fieldPtr = UnsafeAccess.allocAndCopy(field, 0, field.length());
    int fieldSize = field.length();
    int result = HDEL(map, keyPtr, keySize, fieldPtr, fieldSize);
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(fieldPtr);
    return result;
  }
  
  /**
   * HDEL for single field ( zero object allocation)
   * @param map sorted map storage
   * @param keyPtr hash key address
   * @param keySize hash key size
   * @param fieldPtr field address
   * @param fieldSize field size
   * @return 1 or 0 (number of deleted fields)
   */
  public static int HDEL(BigSortedMap map, long keyPtr, int keySize, long fieldPtr, int fieldSize) {

    Key k = getKey(keyPtr, keySize);
    int deleted = 0;
    try {
      writeLock(k);
      int kSize = buildKey(keyPtr, keySize, fieldPtr, fieldSize);
      HashDelete update = hashDelete.get();
      update.reset();
      update.setMap(map);
      update.setKeyAddress(keyArena.get());
      update.setKeySize(kSize);
      // version?
      boolean result = map.execute(update);
      if (result) {
        deleted++;
      }
      if (result && update.checkForEmpty() && isEmpty(map, keyPtr, keySize)) {
        DELETE(map, keyPtr, keySize);
      }
      return deleted;
    } finally {
      writeUnlock(k);
    }
  }
  
  /**
   * Not in Redis API
   * Calculates number of elements between start member (inclusive) and stop member
   * (exclusive). When both": start and stop member are nulls this call is 
   * equivalent to HLEN
   */
  
  public static long HCOUNT(BigSortedMap map, long keyPtr, int keySize, long startPtr, int startSize, 
      long stopPtr, int stopSize) {
    return 0;
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key hash key
   * @param field hash field
   * @param bufSize buffer size
   * @return deleted value or null
   */
  
  public static String HDEL(BigSortedMap map, String key, String field, int bufSize) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long fieldPtr = UnsafeAccess.allocAndCopy(field, 0, field.length());
    int fieldSize = field.length();
    long buffer = UnsafeAccess.malloc(bufSize);
    int size = HDEL(map, keyPtr, keySize, fieldPtr, fieldSize, buffer, bufSize);
    String result = null;
    if (size <= bufSize && size > 0) {
      int vSize = Utils.readUVInt(buffer);
      int vSizeSize = Utils.sizeUVInt(vSize);
      result = Utils.toString(buffer + vSizeSize, size - vSizeSize);
    }
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(fieldPtr);
    UnsafeAccess.free(buffer);
    return result;
  }
  
  /**
   * HDEL for the single field ( zero object allocation) with a value
   * returned to the buffer
   * @param map sorted map storage
   * @param keyPtr hash key address
   * @param keySize hash key size
   * @param fieldPtr field address
   * @param fieldSize field size
   * @return 0 - not found, otherwise length of the value serialized, if greater than
   *         buffer size, the call must be repeated with the appropriately sized buffer
   */
  public static int HDEL(BigSortedMap map, long keyPtr, int keySize, long fieldPtr, int fieldSize, 
      long buffer, int bufferSize) {

    Key k = getKey(keyPtr, keySize);
    try {
      writeLock(k);
      int kSize = buildKey(keyPtr, keySize, fieldPtr, fieldSize);
      HashDelete update = hashDelete.get();
      update.reset();
      update.setMap(map);
      update.setKeyAddress(keyArena.get());
      update.setKeySize(kSize);
      update.setBuffer(buffer, bufferSize);
      // version?
      boolean result = map.execute(update);
      if (result && update.checkForEmpty() && isEmpty(map, keyPtr, keySize)) {
        DELETE(map, keyPtr, keySize);
      }
      return update.getValueSize();
    } finally {
      writeUnlock(k);
    }
  }
  
  public static boolean keyExists(BigSortedMap map, long keyPtr, int keySize) {
    int kSize = buildKey(keyPtr, keySize, ZERO, 1);
    return map.exists(keyArena.get(), kSize);
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key hash key
   * @param field hash field
   * @return 1 - exists, 0 - does not
   */
  public static int HEXISTS(BigSortedMap map, String key, String field) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long fieldPtr = UnsafeAccess.allocAndCopy(field, 0, field.length());
    int fieldSize = field.length();
    
    int result = HEXISTS(map, keyPtr, keySize, fieldPtr, fieldSize);
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(fieldPtr);
    return result;
  }
  /**
   * Returns if field is an existing field in the hash stored at key.
   * Return value
   * Integer reply, specifically:
   * 1 if the hash contains field.
   * 0 if the hash does not contain field, or key does not exist.
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @param fieldPtr field address
   * @param fieldSize field size
   * @return 1 if field exists, 0 - otherwise
   */
  public static int HEXISTS (BigSortedMap map, long keyPtr, int keySize, long fieldPtr, int fieldSize) {
    Key k = getKey(keyPtr, keySize);
    try {
      readLock(k);
      int kSize = buildKey(keyPtr, keySize, fieldPtr, fieldSize);
      HashExists update = hashExists.get();
      update.reset();
      update.setKeyAddress(keyArena.get());
      update.setKeySize(kSize);
      // version?
      return map.execute(update)? 1 : 0;
    } finally {
      readUnlock(k);
    }
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key hash key
   * @param field hash field
   * @param bufSize buffer size
   * @return value or null
   */
  public static String HGET(BigSortedMap map, String key, String field, int bufSize) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long fieldPtr = UnsafeAccess.allocAndCopy(field, 0, field.length());
    int fieldSize = field.length();
    long buffer = UnsafeAccess.malloc(bufSize);
    int size = HGET(map, keyPtr, keySize, fieldPtr, fieldSize, buffer, bufSize);
    String result = null;
    if (size <= bufSize && size > 0) {
      result = Utils.toString(buffer, size);
    }
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(fieldPtr);
    UnsafeAccess.free(buffer);
    
    return result;
  }
  
  /**
   *  Returns the value associated with field in the hash stored at key.
   * @param map ordered map
   * @param keyPtr hash key address
   * @param keySize hash key size
   * @param fieldPtr field to lookup address
   * @param fieldSize field size
   * @param valueBuf value buffer
   * @param valueBufSize value buffer size
   * @return size of value, one should check this and if it is greater than valueBufSize
   *         means that call should be repeated with an appropriately sized value buffer, 
   *         -1 - if does not exists
   */
  public static int HGET (BigSortedMap map, long keyPtr, int keySize, long fieldPtr, int fieldSize,
      long valueBuf, int valueBufSize) {
    Key k = getKey(keyPtr, keySize);
    try {
      readLock(k);
      int kSize = buildKey(keyPtr, keySize, fieldPtr, fieldSize);
      HashGet get = hashGet.get();
      get.reset();
      get.setKeyAddress(keyArena.get());
      get.setKeySize(kSize);
      get.setBufferPtr(valueBuf);
      get.setBufferSize(valueBufSize);
      // version?
      map.execute(get);
      return get.getFoundValueSize();
    } finally {
      readUnlock(k);
    }
  }
  
  /**
   * Get long counter value
   * @param map ordered map
   * @param keyPtr hash key address
   * @param keySize hash key size
   * @param fieldPtr field to lookup address
   * @param fieldSize field size
   * @return long counter value or exception
   */
  public static long HGETL(BigSortedMap map, long keyPtr, int keySize, long fieldPtr, int fieldSize)
    throws OperationFailedException
  {
    int size = HGET(map, keyPtr, keySize, fieldPtr, fieldSize, incrArena.get(), INCR_ARENA_SIZE);
    if (size < 0 ||size > INCR_ARENA_SIZE) {
      throw new OperationFailedException("Not a valid data type");
    }
    
    String s = Utils.toString(incrArena.get(), size);
    try {
      long val = Long.valueOf(s);
      return val;
    } catch(NumberFormatException e) {
      throw new OperationFailedException(e);
    }
  }
  
  /**
   * Get float counter value
   * @param map ordered map
   * @param keyPtr hash key address
   * @param keySize hash key size
   * @param fieldPtr field to lookup address
   * @param fieldSize field size
   * @return long counter value or exception
   */
  public static double HGETF(BigSortedMap map, long keyPtr, int keySize, long fieldPtr, int fieldSize)
    throws OperationFailedException
  {
    int size = HGET(map, keyPtr, keySize, fieldPtr, fieldSize, incrArena.get(), INCR_ARENA_SIZE);
    if (size < 0 ||size > INCR_ARENA_SIZE) {
      throw new OperationFailedException("Not a valid data type");
    }
    
    String s = Utils.toString(incrArena.get(), size);
    try {
      double val = Double.valueOf(s);
      return val;
    } catch(NumberFormatException e) {
      throw new OperationFailedException(e);
    }
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key hash key
   * @param fields list of fields
   * @param bufSize recommended buffer size
   * @return list of values or empty list
   */
  public static List<String> HMGET(BigSortedMap map, String key, String[] fields, int bufSize){
    List<String> list = new ArrayList<String>();
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long buffer = UnsafeAccess.malloc(bufSize);
    long fieldPtrs[] = new long[fields.length];
    int fieldSizes[] = new int[fields.length];
    for(int i=0; i < fields.length; i++) {
      fieldPtrs[i] = UnsafeAccess.allocAndCopy(fields[i], 0, fields[i].length());
      fieldSizes[i] = fields[i].length();
    }
    long size = HMGET(map, keyPtr, keySize, fieldPtrs, fieldSizes, buffer, bufSize);
    if (size > bufSize) {
      return list; // empty list
    }
    int total = UnsafeAccess.toInt(buffer);
    long ptr = buffer + Utils.SIZEOF_INT;
    for(int i = 0; i < total; i++) {
      int len = UnsafeAccess.toInt(ptr);
      if (len < 0) {
        list.add(null);
      } else {
        list.add(Utils.toString(ptr + Utils.SIZEOF_INT, len));
      }
      ptr += Utils.SIZEOF_INT + (len > 0? len:0);
    }
    return list;
  }
  
  /**
   * Returns the values associated with the specified fields in the hash stored at key.
   * For every field that does not exist in the hash, a nil value is returned. Because 
   * non-existing keys are treated as empty hashes, running HMGET against a non-existing key will 
   * return a list of nil values.
   * Return value:
   * Array reply: list of values associated with the given fields, in the same order as they are requested. 
   * 
   * Serialized buffer format:
   * [NUM_VALUES] - 4 bytes
   * [VALUE]+
   * [VALUE] :
   *   [SIZE] 4 bytes
   *   [VALUE_DATA]
   *   
   * @param map sorted map 
   * @param keyPtr key address
   * @param keySize key size
   * @param fieldPtrs field pointers
   * @param fieldSizes field sizes
   * @param valueBuf values buffer
   * @param valueBufSize values buffer size
   * @return total size of serialized values, if this size is greater than
   *          valueBufSize, the call must be repeated with appropriately sized buffer,
   *          -1 if key does not exist
   *          Special value size is reserved for NULL -> -1
   */
    
   
  public static long HMGET (BigSortedMap map, long keyPtr, int keySize, long[] fieldPtrs, int[] fieldSizes,
      long valueBuf, int valueBufSize) {
   
    Key k = getKey(keyPtr, keySize);
    long ptr = valueBuf + Utils.SIZEOF_INT;
    long buf = valueArena.get();
    int bufSize = valueArenaSize.get();
    int count = 0;
    try {
      KeysLocker.readLock(k);  
      for (int i = 0; i < fieldPtrs.length; i++) {
        long fieldPtr = fieldPtrs[i];
        int fieldSize = fieldSizes[i];
        int size = HGET(map, keyPtr, keySize, fieldPtr, fieldSize, buf, bufSize);
        if (size > bufSize) {
          checkValueArena(size);
          buf = valueArena.get();
          bufSize = size;
          // size == -1 - nil
          size = HGET(map, keyPtr, keySize, fieldPtr, fieldSize, buf, bufSize);
        }
        if (ptr + Utils.SIZEOF_INT + (size > 0? size:0) <= valueBuf + valueBufSize) {
          count++;
          UnsafeAccess.putInt(ptr, size);
          if (size > 0) {
            UnsafeAccess.copy(buf, ptr + Utils.SIZEOF_INT, size);
          }
          UnsafeAccess.putInt(valueBuf, count);
        }
        ptr += Utils.SIZEOF_INT + (size > 0? size: 0);
      }
      return ptr - valueBuf;
    } finally {
      KeysLocker.readUnlock(k);
    }
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key hash key
   * @param field hash field
   * @param incr increment
   * @return new value
   * @throws OperationFailedException
   */
  public static long HINCRBY(BigSortedMap map, String key, String field, long incr) 
      throws OperationFailedException {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long fieldPtr = UnsafeAccess.allocAndCopy(field, 0, field.length());
    int fieldSize = field.length();
    long result = HINCRBY(map, keyPtr, keySize, fieldPtr, fieldSize, incr);
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(fieldPtr);
    return result;
  }
  /**
   * Increments the number stored at field in the hash stored at key by increment. 
   * If key does not exist, a new key holding a hash is created. If field does not 
   * exist the value is set to 0 before the operation is performed.
   * The range of values supported by HINCRBY is limited to 64 bit signed integers.
   * Return value
   * Integer reply: the value at field after the increment operation.   
   * @param map ordered map
   * @param keyPtr hash key address
   * @param keySize hash key size
   * @param fieldPtr field to lookup address
   * @param fieldSize field size
   * @param value increment
   * @return new counter value
   * @throws OperationFailedException 
   */
  public static long HINCRBY(BigSortedMap map, long keyPtr, int keySize, long fieldPtr,
      int fieldSize, long incr) throws OperationFailedException {
    Key k = getKey(keyPtr, keySize);
    try {
      writeLock(k);
      long value = 0;
      int size = HGET(map, keyPtr, keySize, fieldPtr, fieldSize, incrArena.get(), INCR_ARENA_SIZE);
      if (size > INCR_ARENA_SIZE) {
        throw new NumberFormatException();
      } else if (size > 0) {
        value = Utils.strToLong(incrArena.get(), size);
      }
      value += incr;
      size = Utils.longToStr(value, incrArena.get(), INCR_ARENA_SIZE);
      // Execute single HSET
      int kSize = buildKey(keyPtr, keySize, fieldPtr, fieldSize);
      HashSet set = hashSet.get();
      set.reset();
      set.setKeyAddress(keyArena.get());
      set.setKeySize(kSize);
      set.setFieldValue(incrArena.get(), size);
      set.setOptions(MutationOptions.NONE);
      // version?
      if(map.execute(set)) {
        return value;
      } else {
        throw new OperationFailedException();
      }
    } finally {
      writeUnlock(k);
    }
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key hash key
   * @param field hash field
   * @param incr increment
   * @return new value
   * @throws OperationFailedException
   */
  public static double HINCRBYFLOAT(BigSortedMap map, String key, String field, double incr) 
      throws OperationFailedException {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long fieldPtr = UnsafeAccess.allocAndCopy(field, 0, field.length());
    int fieldSize = field.length();
    double result = HINCRBYFLOAT(map, keyPtr, keySize, fieldPtr, fieldSize, incr);
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(fieldPtr);
    return result;
  }
  
  /**
   * Increment the specified field of a hash stored at key, and representing a floating point 
   * number, by the specified increment. If the increment value is negative, the result is to 
   * have the hash field value decremented instead of incremented. If the field does not exist, 
   * it is set to 0 before performing the operation. An error is returned if one of the following
   *  conditions occur:
   * The field contains a value of the wrong type (not a string).
   * The current field content or the specified increment are not parsable as a double precision 
   * floating point number.
   * The exact behavior of this command is identical to the one of the INCRBYFLOAT command,
   *  please refer to the documentation of INCRBYFLOAT for further information.
   * 
   * Return value:
   * Bulk string reply: the value of field after the increment.   
   * @param map ordered map
   * @param keyPtr hash key address
   * @param keySize hash key size
   * @param fieldPtr field to lookup address
   * @param fieldSize field size
   * @param value increment
   * @return new counter value
   * @throws OperationFailedException 
   */
  public static double HINCRBYFLOAT(BigSortedMap map, long keyPtr, int keySize, long fieldPtr,
      int fieldSize, double incr) throws OperationFailedException {
    Key k = getKey(keyPtr, keySize);
    try {
      writeLock(k);
      double value = 0;
      int size = HGET(map, keyPtr, keySize, fieldPtr, fieldSize, incrArena.get(), INCR_ARENA_SIZE);
      if (size > INCR_ARENA_SIZE) {
        throw new NumberFormatException();
      } else if (size > 0) {
        value = Utils.strToDouble(incrArena.get(), size);
      }
      value += incr;
      size = Utils.doubleToStr(value, incrArena.get(), INCR_ARENA_SIZE);
      // Execute single HSET
      int kSize = buildKey(keyPtr, keySize, fieldPtr, fieldSize);
      HashSet set = hashSet.get();
      set.reset();
      set.setKeyAddress(keyArena.get());
      set.setKeySize(kSize);
      set.setFieldValue(incrArena.get(), size);
      set.setOptions(MutationOptions.NONE);
      // version?
      if(map.execute(set)) {
        return value;
      } else {
        throw new OperationFailedException();
      }
    } finally {
      writeUnlock(k);
    }
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key hash key
   * @param field field 
   * @return size of a value or -1
   */
  public static int HSTRLEN(BigSortedMap map, String key, String field) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long fieldPtr = UnsafeAccess.allocAndCopy(field, 0, field.length());
    int fieldSize = field.length();
    int result = HSTRLEN(map, keyPtr, keySize, fieldPtr, fieldSize);
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(fieldPtr);
    return result;
  }
  /**
   * Returns the string length of the value associated with field in the hash stored at key. 
   * If the key or the field do not exist, 0 is returned.
   * Return value
   * Integer reply: the string length of the value associated with field, or zero when field 
   * is not present in the hash or key does not exist at all.   
   * @param map ordered map
   * @param keyPtr key address
   * @param keySize key size
   * @param fieldPtr field address
   * @param fieldSize field size
   * @return length of value
   */
  public static int HSTRLEN(BigSortedMap map, long keyPtr, int keySize, long fieldPtr, int fieldSize) {
    Key k = getKey(keyPtr, keySize);
    try {
      readLock(k);
      int kSize = buildKey(keyPtr, keySize, fieldPtr, fieldSize);
      HashValueLength get = hashValueLength.get();
      get.reset();
      get.setKeyAddress(keyArena.get());
      get.setKeySize(kSize);
      // version?    
      map.execute(get);
      return get.getFoundValueSize();
    } finally {
      readUnlock(k);
    }
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key set's key
   * @param lastSeenMember last seen member
   * @param count number of members to return
   * @param bufferSize recommended buffer size
   * @return list of members
   */
  public static List<Pair<String>> HSCAN(BigSortedMap map, String key, String lastSeenMember, 
      int count, int bufferSize, String regex){
    
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long lastSeenPtr = lastSeenMember == null? 0: 
      UnsafeAccess.allocAndCopy(lastSeenMember, 0, lastSeenMember.length());
    int lastSeenSize = lastSeenMember == null? 0: lastSeenMember.length();
    long buffer = UnsafeAccess.malloc(bufferSize);
    // Clear first 4 bytes of a buffer
    UnsafeAccess.putInt(buffer, 0);
    long totalSize = HSCAN(map, keyPtr, keySize, lastSeenPtr, lastSeenSize, count, buffer, bufferSize, regex);
    if (totalSize == 0) {
      return null;
    }
    int total = UnsafeAccess.toInt(buffer);
    if (total == 0) return null;  
    List<Pair<String>> list = new ArrayList<Pair<String>>();
    long ptr = buffer + Utils.SIZEOF_INT;
    // the last is going to be last seen member (duplicate if regex == null)
    for (int i = 0; i < total - 1; i++) {
      int fSize = Utils.readUVInt(ptr);
      int fSizeSize = Utils.sizeUVInt(fSize);
      int vSize =  Utils.readUVInt(ptr + fSizeSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      String first = Utils.toString(ptr + fSizeSize + vSizeSize, fSize);      
      String second = Utils.toString(ptr + fSize + fSizeSize + vSizeSize, vSize);
      ptr += fSize + vSize + fSizeSize + vSizeSize;
      list.add( new Pair<String>(first, second));
    }
    // Read last seen
    int size = Utils.readUVInt(ptr);
    int sizeSize = Utils.sizeUVInt(size);
    String first = Utils.toString(ptr + sizeSize, size);
    list.add( new Pair<String>(first, first));
    
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(lastSeenPtr);
    UnsafeAccess.free(buffer);
    return list;
  }
  
  /**
   * Available since 2.8.0.
   * Time complexity: O(1) for every call. O(N) for a complete iteration, 
   * including enough command calls for the cursor to return back to 0. N is the number of elements 
   * inside the collection.
   * The SCAN command and the closely related commands SSCAN, HSCAN and ZSCAN are used in order 
   * to incrementally iterate over a collection of elements.
   * SCAN iterates the set of keys in the currently selected Redis database.
   * SSCAN iterates elements of Sets types.
   * HSCAN iterates fields of Hash types and their associated values.
   * ZSCAN iterates elements of Sorted Set types and their associated scores.
   * Since these commands allow for incremental iteration, returning only a small number of elements 
   * per call, they can be used in production without the downside of commands like KEYS or SMEMBERS 
   * that may block the server for a long time (even several seconds) when called against big collections 
   * of keys or elements.
   * 
   * @param map sorted map storage
   * @param keyPtr hash key address
   * @param keySize hash key size
   * @param lastSeenFieldPtr  last seen field address
   * @param lastSeenFieldSize last seen field size
   * @param count number of elements to return
   * @param buffer memory buffer for return items
   * @param bufferSize buffer size
   * @param regex pattern
   * @return total serialized size of the response, if greater than bufferSize, the call 
   *         must be retried with the appropriately sized buffer
   */
  public static long HSCAN(BigSortedMap map, long keyPtr, int keySize, long lastSeenFieldPtr,
      int lastSeenFieldSize, int count, long buffer, int bufferSize, String regex) {
    Key key = getKey(keyPtr, keySize);
    HashScanner scanner = null;
    try {
      KeysLocker.readLock(key);
      scanner =
          Hashes.getScanner(map, keyPtr, keySize, lastSeenFieldPtr, lastSeenFieldSize, 0, 0, false);
      if (scanner == null) {
        return 0;
      }
      // Check first member
      if (lastSeenFieldPtr > 0) {
        long ptr = scanner.fieldAddress();
        int size = scanner.fieldSize();
        if (Utils.compareTo(ptr, size, lastSeenFieldPtr, lastSeenFieldSize) == 0) {
          if (scanner.hasNext()) {
            scanner.next();
          } else {
            scanner.close();
            return 0;
          }
        }
      }
      int c = 1; // There will always be at least one element (last seen)
      long ptr = buffer + Utils.SIZEOF_INT;
      // Clear first 4 bytes
      UnsafeAccess.putInt(buffer, 0);
      // Clear first 4 bytes of value arena
      UnsafeAccess.putInt(valueArena.get(), 0);
      int lastSeenSize = 0;
      long lastPtr = ptr;

      while (scanner.hasNext() && c <= count) {
        long fPtr = scanner.fieldAddress();
        int fSize = scanner.fieldSize();
        int fSizeSize = Utils.sizeUVInt(fSize);
        if (fSize + fSizeSize > lastSeenSize) {
          lastSeenSize = fSize + fSizeSize;
        }
        long vPtr = scanner.fieldValueAddress();
        int vSize = scanner.fieldValueSize();
        int vSizeSize = Utils.sizeUVInt(vSize);
        if (ptr + 2 * (fSize + fSizeSize) + vSize + vSizeSize <= buffer + bufferSize) {
          // Update last seen
          checkValueArena(fSize + fSizeSize);
          long arena = valueArena.get();
          Utils.writeUVInt(arena, fSize);
          UnsafeAccess.copy(fPtr, arena + fSizeSize, fSize);
          if (regex == null || Utils.matches(fPtr, fSize, regex)) {
            c++;
            Utils.writeUVInt(ptr, fSize);
            Utils.writeUVInt(ptr + fSizeSize, vSize);
            UnsafeAccess.copy(fPtr, ptr + fSizeSize + vSizeSize, fSize);
            UnsafeAccess.copy(vPtr, ptr + fSizeSize + vSizeSize + fSize, vSize);
            UnsafeAccess.putInt(buffer, c);
            lastPtr += fSize + fSizeSize + vSize + vSizeSize;
          }
        } else {
          break;
        }
        ptr += fSize + fSizeSize + vSize + vSizeSize;
        scanner.next();
      }
      // Write last seen member
      long arena = valueArena.get();
      int fSize = Utils.readUVInt(arena);
      int fSizeSize = Utils.sizeUVInt(fSize);
      if (fSize > 0) {
        Utils.writeUVInt(lastPtr, fSize);
        UnsafeAccess.copy(arena + fSizeSize, lastPtr + fSizeSize, fSize);
      }
      scanner.close();
      return ptr - buffer + lastSeenSize;
    } catch (IOException e) {
      // Will never be thrown
    } finally {
      KeysLocker.readUnlock(key);
    }
    return 0;
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key hash key
   * @param bufSize buffer size
   * @return list of field value pairs
   */
  public static List<Pair<String>> HGETALL(BigSortedMap map, String key, int bufSize){
    
    List<Pair<String>> list = new ArrayList<Pair<String>> ();
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long buffer = UnsafeAccess.malloc(bufSize);
    long result = HGETALL(map, keyPtr, keySize, buffer, bufSize);
    if (result > bufSize) {
      return list;
    }
    int total = UnsafeAccess.toInt(buffer);
    long ptr = buffer + Utils.SIZEOF_INT;
    for (int i = 0; i < total; i++) {
      int fSize = Utils.readUVInt(ptr);
      int fSizeSize = Utils.sizeUVInt(fSize);
      int vSize = Utils.readUVInt(ptr + fSizeSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      String first = Utils.toString(ptr + fSizeSize + vSizeSize, fSize);
      String second = Utils.toString(ptr + fSize + fSizeSize + vSizeSize, vSize);
      list.add(new Pair<String>(first, second));
      ptr += + fSize + fSizeSize + vSize + vSizeSize;
    }
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(buffer);
    return list;
  }
  /**
   * Available since 2.0.0.
   * Time complexity: O(N) where N is the size of the hash.
   * Returns all fields and values of the hash stored at key. In the returned value, every field name 
   * is followed by its value, so the length of the reply is twice the size of the hash.
   * 
   * Return value
   * Array reply: list of fields and their values stored in the hash, or an empty list when key does not exist.
   * @param map sorted map storage
   * @param keyPtr hash key address
   * @param keySize hash key size
   * @param buffer buffer address
   * @param bufferSize buffer size
   * @return total serialized size of the response, if greater then bufferSize 
   *         the call must be retried with the appropriately sized buffer
   */
  public static long HGETALL(BigSortedMap map, long keyPtr, int keySize, long buffer, int bufferSize) 
  {
    Key key = getKey(keyPtr, keySize);
    HashScanner scanner = null;
    try {
      KeysLocker.readLock(key);
      scanner = getScanner(map, keyPtr, keySize, false);
      if (scanner == null) {
        return 0;
      }
      long ptr = buffer + Utils.SIZEOF_INT;
      int c = 0;
      while(scanner.hasNext()) {
        long fPtr = scanner.fieldAddress();
        int fSize = scanner.fieldSize();
        int fSizeSize = Utils.sizeUVInt(fSize);
        long vPtr = scanner.fieldValueAddress();
        int vSize = scanner.fieldValueSize();
        int vSizeSize = Utils.sizeUVInt(vSize);
        if ( ptr + fSize + fSizeSize + vSize + vSizeSize <= buffer + bufferSize) {
          c++;
          Utils.writeUVInt(ptr, fSize);
          Utils.writeUVInt(ptr + fSizeSize, vSize);
          UnsafeAccess.copy(fPtr, ptr + fSizeSize + vSizeSize, fSize);
          UnsafeAccess.copy(vPtr, ptr + fSizeSize + vSizeSize + fSize, vSize);
          UnsafeAccess.putInt(buffer,  c);
        }
        ptr += fSize + fSizeSize + vSize + vSizeSize;
        scanner.next();
      }
      UnsafeAccess.putInt(buffer,  c);
      return ptr - buffer;
    } catch (IOException e) {

   } finally {
      try {
        if (scanner != null) {
          scanner.close();
        }
      } catch (IOException e) {
      }
      KeysLocker.readUnlock(key);
    }
    return 0; 
  }
  
  
  /**
   * Available since 2.0.0.
   * Time complexity: O(N) where N is the size of the hash.
   * Returns all field names in the hash stored at key.
   * Return value
   * Array reply: list of fields in the hash, or an empty list when key does not exist.
   * 
   * @param map sorted map storage
   * @param keyPtr hash key address
   * @param keySize hash key size
   * @param buffer buffer address
   * @param bufferSize buffer size
   * @return total serialized size of the response, if greater then bufferSize 
   *         the call must be retried with the appropriately sized buffer
   */
  public static long HKEYS(BigSortedMap map, long keyPtr, int keySize, long buffer, int bufferSize) 
  {
    Key key = getKey(keyPtr, keySize);
    HashScanner scanner = null;
    try {
      KeysLocker.readLock(key);
      scanner = Hashes.getScanner(map, keyPtr, keySize, false);
      if (scanner == null) {
        return 0;
      }
 
      long ptr = buffer + Utils.SIZEOF_INT;
      int c = 0;
      while(scanner.hasNext()) {
        long fPtr = scanner.fieldAddress();
        int fSize = scanner.fieldSize();
        int fSizeSize = Utils.sizeUVInt(fSize);
        
        if (ptr + fSize + fSizeSize <= buffer + bufferSize) {
          c++;
          Utils.writeUVInt(ptr, fSize);
          UnsafeAccess.copy(fPtr, ptr + fSizeSize, fSize);
          UnsafeAccess.putInt(buffer,  c);

        }
        ptr += fSize + fSizeSize;
        scanner.next();
      }
      UnsafeAccess.putInt(buffer,  c);
      return ptr - buffer;
    } catch (IOException e) {

   } finally {
      KeysLocker.readUnlock(key);
    }
    return 0; 
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key hash key
   * @param bufSize buffer size
   * @return list of keys
   */
  public static List<String> HKEYS(BigSortedMap map, String key, int bufSize)
  {
    List<String> list = new ArrayList<String>();
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long buffer = UnsafeAccess.malloc(bufSize);
    long result = HKEYS(map, keyPtr, keySize, buffer, bufSize) ;
    if (result > bufSize) {
      return list;
    }
    int total = UnsafeAccess.toInt(buffer);
    long ptr = buffer + Utils.SIZEOF_INT;
    for (int i = 0; i < total; i++) {
      int size = Utils.readUVInt(ptr);
      int sizeSize = Utils.sizeUVInt(size);
      String s = Utils.toString(ptr + sizeSize, size);
      list.add(s);
      ptr += size + sizeSize;
    }
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(buffer);
    return list;
  }
  /**
   * For testing only
   * @param map sorted map storage
   * @param key hash key
   * @param bufSize buffer size
   * @return list of values
   */
  public static List<String> HVALS(BigSortedMap map, String key, int bufSize)
  {
    List<String> list = new ArrayList<String>();
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long buffer = UnsafeAccess.malloc(bufSize);
    long result = HVALS(map, keyPtr, keySize, buffer, bufSize) ;
    if (result > bufSize) {
      return list;
    }
    int total = UnsafeAccess.toInt(buffer);
    long ptr = buffer + Utils.SIZEOF_INT;
    for (int i = 0; i < total; i++) {
      int size = Utils.readUVInt(ptr);
      int sizeSize = Utils.sizeUVInt(size);
      String s = Utils.toString(ptr + sizeSize, size);
      list.add(s);
      ptr += size + sizeSize;
    }
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(buffer);
    return list;
  }
  
  /**
   * Available since 2.0.0.
   * Time complexity: O(N) where N is the size of the hash.
   * Returns all field values in the hash stored at key.
   * Return value
   * Array reply: list of values in the hash, or an empty list when key does not exist.
   * 
   * @param map sorted map storage
   * @param keyPtr hash key address
   * @param keySize hash key size
   * @param buffer buffer address
   * @param bufferSize buffer size
   * @return total serialized size of the response, if greater then bufferSize 
   *         the call must be retried with the appropriately sized buffer
   */
  public static long HVALS(BigSortedMap map, long keyPtr, int keySize, long buffer, int bufferSize) 
  {
    Key key = getKey(keyPtr, keySize);
    HashScanner scanner = null;
    try {
      KeysLocker.readLock(key);
      scanner = Hashes.getScanner(map, keyPtr, keySize, false);
      if (scanner == null) {
        return 0;
      }
 
      long ptr = buffer + Utils.SIZEOF_INT;
      int c = 0;
      while(scanner.hasNext()) {
        long vPtr = scanner.fieldValueAddress();
        int vSize = scanner.fieldValueSize();
        int vSizeSize = Utils.sizeUVInt(vSize);
        
        if (ptr + vSize + vSizeSize <= buffer + bufferSize) {
          c++;
          Utils.writeUVInt(ptr, vSize);
          UnsafeAccess.copy(vPtr, ptr + vSizeSize, vSize);
          UnsafeAccess.putInt(buffer,  c);

        }
        ptr += vSize + vSizeSize;
        scanner.next();
      }
      UnsafeAccess.putInt(buffer,  c);
      return ptr - buffer;
    } catch (IOException e) {

   } finally {
      KeysLocker.readUnlock(key);
    }
    return 0; 
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key hash key
   * @param count count to return
   * @param withValues with values?
   * @param bufSize buffer size
   * @return list of K-V or K-K
   */
  public static List<Pair<String>> HRANDFIELD(BigSortedMap map, String key, int count, 
      boolean withValues, int bufSize)
  {
    List<Pair<String>> list = new ArrayList<Pair<String>>();
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long bufferPtr = UnsafeAccess.malloc(bufSize);
    
    long result = HRANDFIELD(map, keyPtr, keySize, count, withValues, bufferPtr, bufSize);
    int total = UnsafeAccess.toInt(bufferPtr);
    long ptr = bufferPtr + Utils.SIZEOF_INT;
    for (int i = 0; i < total; i++) {
      int fSize = Utils.readUVInt(ptr);
      int fSizeSize = Utils.sizeUVInt(fSize);
      int vSize = (withValues)? Utils.readUVInt(ptr + fSizeSize): 0;
      int vSizeSize = (withValues)? Utils.sizeUVInt(vSize): 0;
      long fPtr = ptr + fSizeSize + vSizeSize;
      String s1 = Utils.toString(fPtr, fSize);
      if (withValues) {
        list.add(new Pair<String>(s1,s1));
      } else {
        long vPtr = fPtr + fSize;
        String s2 = Utils.toString(vPtr, vSize);
        list.add( new Pair<String>(s1,s2));
      }
      ptr += fSize + vSize + fSizeSize + vSizeSize;
    }
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(bufferPtr);
    return list;
  }
  
  /**
   * When called with just the key argument, return a random field from the hash value stored at key.
   * If the provided count argument is positive, return an array of distinct fields. The array's 
   * length is either count or the hash's number of fields (HLEN), whichever is lower.
   * If called with a negative count, the behavior changes and the command is allowed to return
   * the same field multiple times. In this case, the number of returned fields is the absolute 
   * value of the specified count. The optional WITHVALUES modifier changes the reply so it includes 
   * the respective values of the randomly selected hash fields.
   * 
   * @param map sorted map storage
   * @param keyPtr hash key address
   * @param keySize hash key size
   * @param count count to return
   * @param withValues true - return fields with values
   * @param buffer  buffer to keep response
   * @param bufSize buffer size
   * @return full serialized size of the response
   */
  public static long HRANDFIELD(BigSortedMap map, long keyPtr, int keySize, int count, 
      boolean withValues, long bufferPtr, int bufferSize) {
    
    Key k = getKey(keyPtr, keySize);
    boolean distinct = count > 0;
    if (!distinct) {
      count = -count;
    }
    HashScanner scanner = null;
    try {
      KeysLocker.readLock(k);
      long total =  HLEN(map, keyPtr, keySize);
      if (total == 0) {
        return 0; // Empty hash or does not exists
      }
      long[] index = null;
      if (distinct) {
        if (count < total) {
          index = Utils.randomDistinctArray(total, count);
        } 
      } else {
        index = Utils.randomArray(total, count);
      }
      if (index == null) {
        // Return all elements
        return withValues? HGETALL(map, keyPtr, keySize, bufferPtr, bufferSize): 
          HKEYS(map, keyPtr, keySize, bufferPtr, bufferSize);
      }
      scanner = getScanner(map, keyPtr, keySize, false);
      long result = readByIndex(scanner, index, bufferPtr, bufferSize, withValues);
      return result;
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
          // Should not be here
        }
      }
      KeysLocker.readUnlock(k);
    }
  }
  
  /**
   * TODO: change return value to full serialized size
   * Return number of written element
   * @param scanner set scanner
   * @param index array of random indexes to read
   * @param bufferPtr buffer 
   * @param bufferSize buffer size
   * @return
   */
  private static long readByIndex(HashScanner scanner, long[] index, long bufferPtr, int bufferSize,
      boolean withValues) {

    long ptr = bufferPtr + Utils.SIZEOF_INT;
    UnsafeAccess.putInt(bufferPtr, 0);

    for (int i = 0; i < index.length; i++) {
      scanner.skipTo(index[i]);
      int fSize = scanner.fieldSize();
      int fSizeSize = Utils.sizeUVInt(fSize);
      int vSize = scanner.fieldValueSize();
      int vSizeSize = Utils.sizeUVInt(vSize);
      int adv = fSize + fSizeSize + (withValues ? (vSize + vSizeSize) : 0);
      if (ptr + adv <= bufferPtr + bufferSize) {
        long fPtr = scanner.fieldAddress();
        if (!withValues) {
          // Write size
          Utils.writeUVInt(ptr, fSize);
          // Write field
          UnsafeAccess.copy(fPtr, ptr + fSizeSize, fSize);
        } else {
          long vPtr = scanner.fieldValueAddress();
          // Write field size
          Utils.writeUVInt(ptr, fSize);
          // Write value size
          Utils.writeUVInt(ptr + fSizeSize, vSize);
          // Write field
          UnsafeAccess.copy(fPtr, ptr + fSizeSize + vSizeSize, fSize);
          // Write value
          UnsafeAccess.copy(vPtr, ptr + fSizeSize + vSizeSize + fSize, vSize);
        }
        UnsafeAccess.putInt(bufferPtr, i + 1);
      }
      ptr += adv;
    }
    return ptr - bufferPtr;
  }
  
  /**
   * TODO: pattern matching
   * Create scanner from  a cursor, which defined by pairs of last 
   * seen key and field
   * @param map ordered map
   * @param lastSeenKeyPtr last seen key address
   * @param lastSeenKeySize  last seen key size
   * @param lastFieldSeenPtr last seen field address, if 0 - start from beginning
   * @param lastFieldSeenSize last seen field size
   * @return hash scanner
   */
  public static HashScanner getScanner(BigSortedMap map, long lastSeenKeyPtr, 
      int lastSeenKeySize, long lastFieldSeenPtr, int lastFieldSeenSize) {
    int startKeySize = keySizeWithPrefix(lastSeenKeyPtr);
    long stopKeyPtr = Utils.prefixKeyEnd(lastSeenKeyPtr, startKeySize);
    if (stopKeyPtr == -1) {
      return null;
    }
    BigSortedMapDirectMemoryScanner scanner = map.getScanner(lastSeenKeyPtr, lastSeenKeySize, 
      stopKeyPtr, startKeySize);
    // TODO - test this call
    HashScanner hs = null;
    try {
      hs = new HashScanner(scanner);
    } catch (IOException e) {
      return null;
    }
    //TODO: Memory deallocation?
    return hs;
  }
  
  /**
   * Get hash scanner for hash operations, as since we can create multiple
   * hash scanners in the same thread we can not use thread local variables
   * WARNING: we can not create multiple scanners in a single thread
   * @param map sorted map to run on
   * @param keyPtr key address
   * @param keySize key size
   * @param safe get safe instance
   * @return hash scanner
   */
  public static HashScanner getScanner(BigSortedMap map, long keyPtr, int keySize, boolean safe) {
    return getScanner(map, keyPtr, keySize, safe, false);
  }
  
  /**
   * Get hash scanner for hash operations, as since we can create multiple
   * hash scanners in the same thread we can not use thread local variables
   * WARNING: we can not create multiple scanners in a single thread
   * @param map sorted map to run on
   * @param keyPtr key address
   * @param keySize key size
   * @param safe get safe instance
   * @param reverse reverse scanner
   * @return hash scanner
   */
  public static HashScanner getScanner(BigSortedMap map, long keyPtr, int keySize, 
      boolean safe, boolean reverse) {
    
    long kPtr = UnsafeAccess.malloc(keySize + KEY_SIZE + 2 * Utils.SIZEOF_BYTE);
    UnsafeAccess.putByte(kPtr, (byte) DataType.HASH.ordinal());
    UnsafeAccess.putInt(kPtr + Utils.SIZEOF_BYTE, keySize);
    UnsafeAccess.copy(keyPtr, kPtr + KEY_SIZE + Utils.SIZEOF_BYTE, keySize);
    UnsafeAccess.putByte(kPtr + keySize + KEY_SIZE + Utils.SIZEOF_BYTE,(byte) 0);
    
    keySize += KEY_SIZE + 2 * Utils.SIZEOF_BYTE;
    
    long endPtr = Utils.prefixKeyEnd(kPtr, keySize - 1);
    int endKeySize = keySize - 1; 
        
    BigSortedMapDirectMemoryScanner scanner = safe? 
        map.getSafeScanner(kPtr, keySize, endPtr, endKeySize, reverse): 
          map.getScanner(kPtr, keySize, endPtr, endKeySize, reverse);
    if (scanner == null) {
      UnsafeAccess.free(endPtr);
      UnsafeAccess.free(kPtr);
      return null;
    }
    HashScanner hs = null;
    try {
      hs = new HashScanner(scanner, reverse);
      hs.setDisposeKeysOnClose(true);
    } catch (IOException e) {
      try {
        scanner.close();
      } catch (IOException e1) {
      }
      return null;
    }
    return hs;
  }
  
  /**
   * Get hash scanner for hash operations, as since we can create multiple
   * hash scanners in the same thread we can not use thread local variables
   * WARNING: we can not create multiple scanners in a single thread
   * @param map sorted map to run on
   * @param keyPtr key address
   * @param keySize key size
   * @param safe get safe instance
   * @return hash scanner
   */
  public static HashScanner getScanner(BigSortedMap map, long keyPtr, int keySize, long startFieldPtr, 
      int startFieldSize, long endFieldPtr, int endFieldSize, boolean safe) {
    return getScanner(map, keyPtr, keySize, startFieldPtr, startFieldSize, endFieldPtr, 
      endFieldSize, safe, false);
  }
  
  /**
   * Get hash scanner for hash operations, as since we can create multiple
   * hash scanners in the same thread we can not use thread local variables
   * WARNING: we can not create multiple scanners in a single thread
   * @param map sorted map to run on
   * @param keyPtr key address
   * @param keySize key size
   * @param safe get safe instance
   * @param reverse reverse scanner
   * @return hash scanner
   */
  public static HashScanner getScanner(BigSortedMap map, long keyPtr, int keySize, long fieldStartPtr, 
      int fieldStartSize, long fieldStopPtr, int fieldStopSize, boolean safe, boolean reverse) {
    //TODO Check start stop 0
    //TODO: for public API - primary key locking? 
    //TODO: Primary key (keyPtr, keySize) must be under lock !!!
    // Check if start == stop != null
    if (fieldStartPtr > 0 && fieldStopPtr > 0) {
      if (Utils.compareTo(fieldStartPtr, fieldStartSize, fieldStopPtr, fieldStopSize) == 0) {
        // start = stop - scanner is empty (null)
        return null;
      }
    }
    long startPtr = UnsafeAccess.malloc(keySize + KEY_SIZE + Utils.SIZEOF_BYTE + fieldStartSize);
    int startPtrSize = buildKey(keyPtr, keySize, fieldStartPtr, fieldStartSize, startPtr);
    long stopPtr = UnsafeAccess.malloc(keySize + KEY_SIZE + Utils.SIZEOF_BYTE + fieldStopSize);
    int stopPtrSize = buildKey(keyPtr, keySize, fieldStopPtr, fieldStopSize, stopPtr);
    if (fieldStopPtr == 0) {
      stopPtr = Utils.prefixKeyEndNoAlloc(stopPtr, stopPtrSize);
    }
    if (/*reverse &&*/ fieldStartPtr > 0) {
      // Get floorKey
      long size = map.floorKey(startPtr, startPtrSize, valueArena.get(), valueArenaSize.get());
      if (size < 0) {
        //TODO: should not happen if set key is locked
      }
      if (size > valueArenaSize.get()) {
        checkValueArena((int)size);
        // One more time
        size = map.floorKey(startPtr, startPtrSize, valueArena.get(), valueArenaSize.get());
      }
      // free start key
      UnsafeAccess.free(startPtr);
      startPtr = UnsafeAccess.malloc(size);
      UnsafeAccess.copy(valueArena.get(), startPtr, size);
      startPtrSize = (int)size;
    }

    
    //TODO do not use thread local in scanners - check it
    BigSortedMapDirectMemoryScanner scanner = safe? 
        map.getSafeScanner(startPtr, startPtrSize, stopPtr, stopPtrSize, reverse):
          map.getScanner(startPtr, startPtrSize, stopPtr, stopPtrSize, reverse);
    if (scanner == null) {
      UnsafeAccess.free(startPtr);
      UnsafeAccess.free(stopPtr);
      return null;
    }
    
    
    HashScanner hs = null;
    try {
      hs = new HashScanner(scanner, fieldStartPtr, fieldStartSize, fieldStopPtr, fieldStopSize, reverse);
      hs.setDisposeKeysOnClose(true);
    } catch (IOException e) {
      try {
        scanner.close();
      } catch (IOException e1) {
      }
      return null;
    }
    return hs;
    
  }
  
  /**
   * Finds location of a given field in a Value object
   * @param foundRecordAddress address of K-V record
   * @param fieldPtr field's address
   * @param fieldSize field's size
   * @return address of field-value in a Value or -1, if not found
   */
  public static long exactSearch(long foundRecordAddress, long fieldPtr, int fieldSize) {
    long valuePtr = DataBlock.valueAddress(foundRecordAddress);
    int valueSize  = DataBlock.valueLength(foundRecordAddress);
    int off = NUM_ELEM_SIZE; // skip number of elements in value
    while(off < valueSize) {
      int fSize = Utils.readUVInt(valuePtr + off);
      int skip = Utils.sizeUVInt(fSize);
      int vSize = Utils.readUVInt(valuePtr + off + skip);
      skip+= Utils.sizeUVInt(vSize);
      if (Utils.compareTo(fieldPtr, fieldSize, valuePtr + off + skip, fSize) == 0) {
        return valuePtr + off;
      }
      off+= skip + fSize + vSize;
    }
    return -1; // NOT_FOUND
  }
  
  //public static boolean DEBUG = false;
  /**
   * Finds first field which is greater or equals to a given one
   * in a Value object
   * @param foundRecordAddress address of a K-V record
   * @param fieldPtr field address
   * @param field field size
   * @return address to insert to insert to
   */
  public static long insertSearch(long foundRecordAddress, long fieldPtr, int fieldSize) {
    long valuePtr = DataBlock.valueAddress(foundRecordAddress);
    int valueSize  = DataBlock.valueLength(foundRecordAddress);
    int off = NUM_ELEM_SIZE; // skip number of elements
    while(off < valueSize) {
      int fSize = Utils.readUVInt(valuePtr + off);
      int skip = Utils.sizeUVInt(fSize);
      int vSize = Utils.readUVInt(valuePtr + off + skip);
      skip += Utils.sizeUVInt(vSize);
      if (Utils.compareTo(fieldPtr, fieldSize, valuePtr + off + skip, fSize) <= 0) {
        return valuePtr + off;
      }
      off+= skip + fSize + vSize;
    }
    return valuePtr + valueSize; // put in the end largest one
  }
  
  /**
   * Gets value size from the address of field-value pair
   * @param fieldValuePtr address
   * @return size of a value
   */
  public static int getValueSize(long fieldValuePtr) {
    int fSize = Utils.readUVInt(fieldValuePtr );
    int skip = Utils.sizeUVInt(fSize);
    int vSize = Utils.readUVInt(fieldValuePtr  + skip);
    return vSize;
  }
  
  
  /**
   * Get full value size including length prefix
   * @param fieldValuePtr address of a record
   * @return full size
   */
  public static int getFulValueSize(long fieldValuePtr) {
    int vSize = getValueSize(fieldValuePtr);
    return vSize + Utils.sizeUVInt(vSize);
  }
  
  /**
   * Gets field size from the address of field-value pair
   * @param fieldValuePtr address
   * @return size of a value
   */
  public static int getFieldSize(long fieldValuePtr) {
    int fSize = Utils.readUVInt(fieldValuePtr );
    return fSize;
  }
  /**
   * Gets value address from the address of field-value pair
   * @param fieldValuePtr address
   * @return address of a value
   */
  public static long getValueAddress(long fieldValuePtr) {
    int fSize = Utils.readUVInt(fieldValuePtr );
    int skip = Utils.sizeUVInt(fSize);
    int vSize = Utils.readUVInt(fieldValuePtr  + skip);
    skip+= Utils.sizeUVInt(vSize);
    
    return fieldValuePtr + fSize + skip;
  }
  
  /**
   * Gets field address from the address of field-value pair
   * @param fieldValuePtr address
   * @return address of a value
   */
  public static long getFieldAddress(long fieldValuePtr) {
    int fSize = Utils.readUVInt(fieldValuePtr );
    int skip = Utils.sizeUVInt(fSize);
    int vSize = Utils.readUVInt(fieldValuePtr  + skip);
    skip+= Utils.sizeUVInt(vSize);
    
    return fieldValuePtr + skip;
  }
  /**
   * Returns suggested split address
   * @param valuePtr value address
   * @return address of a split point
   */
  public static long splitAddress(final long valuePtr, final int valueSize) {
    // First try equaling sizes of splits
    long off = NUM_ELEM_SIZE;
    long prevOff = NUM_ELEM_SIZE;
    while(off < valueSize/2) {
      int fSize = Utils.readUVInt(valuePtr + off);
      int fSizeSize = Utils.sizeUVInt(fSize);
      int vSize = Utils.readUVInt(valuePtr + off + fSizeSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      prevOff = off;
      off+= fSizeSize + fSize + vSize + vSizeSize;
    }
    if (prevOff - NUM_ELEM_SIZE > valueSize - off) {
      return valuePtr + prevOff;
    } else {
      return valuePtr + off;
    }
  }
  
  /**
   * Returns suggested number of elements in a left split
   * @param valuePtr value address
   * @return address of a split point
   */
  public static int splitNumber(final long valuePtr, final int valueSize) {
    // First try equaling sizes of splits
    long off = NUM_ELEM_SIZE;
    long prevOff = NUM_ELEM_SIZE;
    int n = 0;
    while(off < valueSize/2) {
      n++;
      int fSize = Utils.readUVInt(valuePtr + off);
      int fSizeSize = Utils.sizeUVInt(fSize);
      int vSize = Utils.readUVInt(valuePtr + off + fSizeSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      prevOff = off;
      off+= fSizeSize + fSize + vSize + vSizeSize;
    }
    if (prevOff - NUM_ELEM_SIZE > valueSize - off) {
      return n-1;
    } else {
      return n;
    }
  }
  
  /**
   * Compare field, which starts in a given address 
   * with a given field
   * @param ptr address of an first field-value record
   * @param fieldPtr address of a second element
   * @param fieldSize second element size
   * @return o - if equals, -1, +1
   */
  public static int compareFields (long ptr, long fieldPtr, int fieldSize) {
    int fSize = Utils.readUVInt(ptr);
    int fSizeSize = Utils.sizeUVInt(fSize);
    int vSize = Utils.readUVInt(ptr + fSizeSize);
    int vSizeSize = Utils.sizeUVInt(vSize);
    
    return Utils.compareTo(ptr + fSizeSize + vSizeSize, fSize, fieldPtr, fieldSize); 
  }
  
}