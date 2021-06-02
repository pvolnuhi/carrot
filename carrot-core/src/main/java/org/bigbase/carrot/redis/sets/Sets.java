package org.bigbase.carrot.redis.sets;

import static org.bigbase.carrot.redis.Commons.KEY_SIZE;
import static org.bigbase.carrot.redis.Commons.NUM_ELEM_SIZE;
import static org.bigbase.carrot.redis.Commons.ZERO;
import static org.bigbase.carrot.redis.Commons.numElementsInValue;
import static org.bigbase.carrot.redis.KeysLocker.readLock;
import static org.bigbase.carrot.redis.KeysLocker.readUnlock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.BigSortedMapDirectMemoryScanner;
import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.Key;
import org.bigbase.carrot.redis.Commons;
import org.bigbase.carrot.redis.DataType;
import org.bigbase.carrot.redis.KeysLocker;
import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Support for packing multiple values into one K-V value
 * under the same key. This is for compact representation of naturally ordered SETs
 * for ordered collection of elements:
 * 
 * KEY -> [e1,e2,e3,e4, ..., eN] we store them in a sequence of Key-Value pairs as following:
 * 
 * [KEY_SIZE]KEY0-> Value([e1,e2,e3,...,eX]) [KEY_SIZE]KEYeX+1->Value([eX+1,eX+2,...])
 * [KEY_SIZE] prepends Key to avoid possible key collisions
 * @author Vladimir Rodionov
 *
 */
public class Sets {
  
  
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
  

  /**
   * Thread local updates Set Exists
   */
  private static ThreadLocal<SetExists> setExists = new ThreadLocal<SetExists>() {
    @Override
    protected SetExists initialValue() {
      return new SetExists();
    } 
  };
  
  /**
   * Thread local updates Set Add
   */
  private static ThreadLocal<SetAdd> setAdd = new ThreadLocal<SetAdd>() {
    @Override
    protected SetAdd initialValue() {
      return new SetAdd();
    } 
  };
  
  /**
   * Thread local updates Set Delete
   */
  private static ThreadLocal<SetDelete> setDelete = new ThreadLocal<SetDelete>() {
    @Override
    protected SetDelete initialValue() {
      return new SetDelete();
    } 
  };
  
  /**
   * Thread local key storage
   */
  private static ThreadLocal<Key> key = new ThreadLocal<Key>() {
    @Override
    protected Key initialValue() {
      return new Key(0,0);
    } 
  };
  
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
  
  public static boolean keyExists(BigSortedMap map, long keyPtr, int keySize) {
    int kSize = buildKey(keyPtr, keySize, ZERO, 1);
    return map.exists(keyArena.get(), kSize);
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
   * Atomic operation
   * Add the specified members to the set stored at key. Specified members that are already 
   * a member of this set are ignored. If key does not exist, a new set is created before adding 
   * the specified members. An error is returned when the value stored at key is not a set.
   * Return value
   * Integer reply: the number of elements that were added to the set, not including all 
   * the elements already present into the set.   
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @return the number of elements that were added to the set
   */
  public static int SADD(BigSortedMap map, long keyPtr, int keySize, long[] elemPtrs,
      int[] elemSizes) {
    Key k = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(k);
      
      int toAdd = elemPtrs.length;
      int count = 0;
      for (int i = 0; i < toAdd; i++) {
        int kSize = buildKey(keyPtr, keySize, elemPtrs[i], elemSizes[i]);
        SetAdd add = setAdd.get();
        add.reset();
        add.setKeyAddress(keyArena.get());
        add.setKeySize(kSize);
        // version?
        if (map.execute(add)) {
          count++;
        }
      }
      return count;
    } finally {
      KeysLocker.writeUnlock(k);
    }
  }
  
  /**
   * SADD for single member (zero memory allocation)
   * @param map sorted map storage
   * @param keyPtr set key address
   * @param keySize set key size
   * @param elemPtr member value address
   * @param elemSize member value size
   * @return 1 or 0
   */
  public static int SADD(BigSortedMap map, long keyPtr, int keySize, long elemPtr, int elemSize) {
    Key k = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(k);

      int count = 0;
      int kSize = buildKey(keyPtr, keySize, elemPtr, elemSize);
      SetAdd add = setAdd.get();
      add.reset();
      add.setKeyAddress(keyArena.get());
      add.setKeySize(kSize);
      // version?
      if (map.execute(add)) {
        count++;
      }
      return count;
    } finally {
      KeysLocker.writeUnlock(k);
    }
  }
  
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key set's key
   * @param members array of members
   * @return number of newly added members
   */
  
  public static int SADD(BigSortedMap map, String key, String[] members) {
    
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length(); 
    long[] ptrs = new long[members.length];
    int [] sizes = new int[members.length];
    int i = 0;
    for(String m : members) {
      ptrs[i] = UnsafeAccess.allocAndCopy(m, 0, m.length());
      sizes[i++] = m.length();
    }
    
    int result = SADD(map, keyPtr, keySize, ptrs, sizes);
    UnsafeAccess.free(keyPtr);
    Arrays.stream(ptrs).forEach( x-> UnsafeAccess.free(x));
    return result;
  }
  
  /**
   * For testing only
   */
  public static int SADD(BigSortedMap map, String key, String member) {
    return SADD(map, key.getBytes(), member.getBytes());
  }
  
  /**
   * For testing only
   */
  public static int SADD(BigSortedMap map, byte[] key, byte[] member) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length);
    int keySize = key.length;
    long memberPtr = UnsafeAccess.allocAndCopy(member, 0, member.length);
    int memberSize = member.length;   
    int result = SADD(map, keyPtr, keySize, memberPtr, memberSize);
    UnsafeAccess.free(memberPtr);
    UnsafeAccess.free(keyPtr);
    return result;
  }
  
  
  /**
   * Not in Redis API
   * Calculates number of elements between start member (inclusive) and stop member
   * (exclusive). When both": start and stop member are nulls this call is 
   * equivalent to SCARD
   */
  
  public static long SCOUNT(BigSortedMap map, long keyPtr, int keySize, long startPtr, int startSize, 
      long stopPtr, int stopSize) {
    return 0;
  }
  /**
   * Available since 1.0.0.
   * Time complexity: O(N) where N is the total number of elements in all given sets.
   * Returns the members of the set resulting from the difference between the first set and all the successive sets.
   * For example:
   * key1 = {a,b,c,d}
   * key2 = {c}
   * key3 = {a,c,e}
   * SDIFF key1 key2 key3 = {b,d}
   * Keys that do not exist are considered to be empty sets.
   * Return value
   * Array reply: list with members of the resulting set.
   * @param map sorted map storage
   * @param keyPtrs array of key addresses
   * @param keySizes array of key sizes
   * @param buffer buffer for result
   * @param bufferSize size of the buffer
   * @return total serialized size of an sets intersection
   */
  public static long SDIFF(BigSortedMap map, long[] keyPtrs, int[] keySizes, long buffer, int bufferSize) {
    return 0;
  }
  
  /**
   * This command is equal to SDIFF, but instead of returning the resulting set, it is stored in destination.
   * If destination already exists, it is overwritten.
   * Return value
   * Integer reply: the number of elements in the resulting set.
   * 
   * @param map sorted map storage 
   * @param keyPtrs array of key addresses
   * @param keySizes array of key sizes
   * @return the number of elements in the resulting set.
   */
  public static long SDIFFSTORE(BigSortedMap map, long[] keyPtrs, int[] keySizes) {
    return 0;
  }
  
  /**
   * Available since 1.0.0.
   * Time complexity: O(N*M) worst case where N is the cardinality of the smallest set and M is the number of sets.
   * Returns the members of the set resulting from the intersection of all the given sets.
   * For example:
   * key1 = {a,b,c,d}
   * key2 = {c}
   * key3 = {a,c,e}
   * SINTER key1 key2 key3 = {c}
   * Keys that do not exist are considered to be empty sets. With one of the keys being an empty set, 
   * the resulting set is also empty (since set intersection with an empty set always results in an empty set).
   * Return value
   * Array reply: list with members of the resulting set.
   * 
   * @param map sorted map storage 
   * @param keyPtrs array of key addresses
   * @param keySizes array of key sizes
   * @param buffer buffer for result
   * @param bufferSize size of the buffer
   * @return total serialized size of the result
   */
  public static long SINTER(BigSortedMap map, long[] keyPtrs, int[] keySizes, long buffer, int bufferSize) {
    return 0;
  }
  
  /**
   * This command is equal to SINTER, but instead of returning the resulting set, it is stored in destination.
   * If destination already exists, it is overwritten.
   * Return value
   * Integer reply: the number of elements in the resulting set.
   * @param map sorted map storage 
   * @param keyPtrs array of key addresses (first key is the destination)
   * @param keySizes array of key sizes
   * @return the number of elements in the resulting set.
   */
  
  public static long SINTERSTORE(BigSortedMap map, long[] keyPtrs, int[] keySizes) {
    return 0;
  }
  
  /**
   * Returns the members of the set resulting from the union of all the given sets.
   * For example:
   * key1 = {a,b,c,d}
   * key2 = {c}
   * key3 = {a,c,e}
   * SUNION key1 key2 key3 = {a,b,c,d,e}
   * Keys that do not exist are considered to be empty sets.
   * Return value
   * Array reply: list with members of the resulting set.
   * @param map sorted map storage 
   * @param keyPtrs array of key addresses
   * @param keySizes array of key sizes
   * @param buffer buffer for result
   * @param bufferSize size of the buffer
   * @return total serialized size of the result
   */
  public static long SUNION(BigSortedMap map, long[] keyPtrs, int[] keySizes, long buffer, int bufferSize) {
    return 0;
  }
  
  /**
   * This command is equal to SUNION, but instead of returning the resulting set, it is stored 
   * in destination. If destination already exists, it is overwritten.
   * Return value
   * Integer reply: the number of elements in the resulting set.
   * @param map sorted map storage 
   * @param keyPtrs array of key addresses (first key is the destination)
   * @param keySizes array of key sizes
   * @return the number of elements in the resulting set.
   */
  public static long SUNIONSTORE(BigSortedMap map, long[] keyPtrs, int[] keySizes) {
    return 0;
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key set's key
   * @return set's cardinality
   */
  
  public static long SCARD(BigSortedMap map, String key) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long result = SCARD(map, keyPtr, keySize);
    UnsafeAccess.free(keyPtr);
    return result;
  }
  
  /**
   * Returns the set cardinality (number of elements) of the set stored at key.
   * Return value
   * Integer reply: the cardinality (number of elements) of the set, or 0 if key does not exist.   
   * @param map ordered map
   * @param keyPtr key address
   * @param keySize key size 
   * @return number of elements
   * @throws IOException 
   */
  public static long SCARD(BigSortedMap map, long keyPtr, int keySize) {

    Key k = getKey(keyPtr, keySize);
    long total = 0;
    long startKeyPtr = 0, endKeyPtr = 0;
    try {
      KeysLocker.readLock(k);
      startKeyPtr = UnsafeAccess.malloc(keySize + KEY_SIZE + 2 * Utils.SIZEOF_BYTE) ;
      int kSize = buildKey(keyPtr, keySize, Commons.ZERO, 1, startKeyPtr);
      endKeyPtr = Utils.prefixKeyEnd(startKeyPtr, kSize - 1);      
      BigSortedMapDirectMemoryScanner scanner = map.getScanner(startKeyPtr, kSize, endKeyPtr, kSize - 1);
      if (scanner == null) {
        return 0; // empty or does not exists
      }
      while (scanner.hasNext()) {
        long valuePtr = scanner.valueAddress();
        total += numElementsInValue(valuePtr);
        scanner.next();
      }
      scanner.close();
    } catch (IOException e) {
      // should never be thrown
    } finally {
      if (startKeyPtr > 0) {
        UnsafeAccess.free(startKeyPtr);
      }
      if (endKeyPtr > 0) {
        UnsafeAccess.free(endKeyPtr);
      }
      KeysLocker.readUnlock(k);
    }
    return total;
  }
  
  /**
   * Checks if the set is empty
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
      buildKey(keyPtr, keySize, Commons.ZERO, 1, kPtr);
      
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
   * TODO: Remove if not used
   * Returns total size (in bytes) of elements in this set, defined by key
   * This method is good for reading all set elements 
   * @param map ordered map
   * @param keyPtr key address
   * @param keySize key size 
   * @return set size in bytes in serialized format (including variable size prefixes)
   * @throws IOException 
   */
  public static long getSetSizeInBytes(BigSortedMap map, long keyPtr, int keySize){
    
    long ptr = UnsafeAccess.malloc(keySize + KEY_SIZE + 2 * Utils.SIZEOF_BYTE);
    int kSize = buildKey(keyPtr, keySize, Commons.ZERO, 1, ptr);
    long endKeyPtr = Utils.prefixKeyEnd(ptr, kSize - 1);
    BigSortedMapDirectMemoryScanner scanner = map.getScanner(ptr, kSize, endKeyPtr, kSize - 1);

    long total = 0;
    try {
      if (scanner == null) {
        return 0; // empty or does not exists
      }
      while (scanner.hasNext()) {
        long valueSize = scanner.valueSize();
        total += valueSize - NUM_ELEM_SIZE;
      }
      scanner.close();
    } catch (IOException e) {
      // should never be thrown
    } finally {
      if (endKeyPtr > 0) {
        UnsafeAccess.free(endKeyPtr);
      }
      if (ptr > 0) {
        UnsafeAccess.free(ptr);
      }
    }
    return total;
  }
    
  /**
   * Build key for Set. It uses thread local key arena 
   * @param keyPtr original key address
   * @param keySize original key size
   * @param elPtr element address
   * @param elSize element size
   * @return new key size 
   */
    
   
  private static int buildKey( long keyPtr, int keySize, long elPtr, int elSize) {
    checkKeyArena(keySize + KEY_SIZE + elSize + Utils.SIZEOF_BYTE);
    long arena = keyArena.get();
    int kSize = KEY_SIZE + keySize + Utils.SIZEOF_BYTE;
    UnsafeAccess.putByte(arena, (byte)DataType.SET.ordinal());
    UnsafeAccess.putInt(arena + Utils.SIZEOF_BYTE, keySize);
    UnsafeAccess.copy(keyPtr, arena + KEY_SIZE + Utils.SIZEOF_BYTE, keySize);
    if (elPtr > 0) {
      UnsafeAccess.copy(elPtr, arena + kSize, elSize);
      kSize += elSize;
    }
    return kSize;
  }
  /**
   * Build key into allocated memory arena
   * @param keyPtr set key address
   * @param keySize set key size
   * @param elPtr member address
   * @param elSize member size
   * @param arena memory buffer
   * @return size of a key
   */
  public static int buildKey( long keyPtr, int keySize, long elPtr, int elSize, long arena) {

    int kSize = KEY_SIZE + keySize + Utils.SIZEOF_BYTE;
    UnsafeAccess.putByte(arena, (byte)DataType.SET.ordinal());
    UnsafeAccess.putInt(arena + Utils.SIZEOF_BYTE, keySize);
    UnsafeAccess.copy(keyPtr, arena + KEY_SIZE + Utils.SIZEOF_BYTE, keySize);
    if (elPtr > 0) {
      UnsafeAccess.copy(elPtr, arena + kSize, elSize);
      kSize += elSize;
    }
    return kSize;
  }
  /**
   * Remove the specified members from the set stored at key. Specified members that are not a 
   * member of this set are ignored. If key does not exist, it is treated as an empty set and 
   * this command returns 0. An error is returned when the value stored at key is not a set.
   * Return value
   * Integer reply: the number of members that were removed from the set, 
   * not including non existing members.   
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @return number of elements removed
   */
  
  public static int SREM(BigSortedMap map, long keyPtr, int keySize, long[] elemPtrs,
      int[] elemSizes) {
    Key k = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(k);
      if (!keyExists(map, keyPtr, keySize)) {
        return 0;
      }
      final int toRemove = elemPtrs.length;
      int removed = 0;
      for (int i = 0; i < toRemove; i++) {

        int kSize = buildKey(keyPtr, keySize, elemPtrs[i], elemSizes[i]);

        SetDelete remove = setDelete.get();
        remove.reset();
        remove.setMap(map);
        remove.setKeyAddress(keyArena.get());
        remove.setKeySize(kSize);
        // version?
        if (map.execute(remove)) {
          removed++;
        }
      }
      if (setDelete.get().checkForEmpty() && isEmpty(map, keyPtr, keySize)) {
        DELETE(map, keyPtr, keySize);
      }
      return removed;
    } finally {
      KeysLocker.writeUnlock(k);
    }
  }
  
  /**
   * SREM for single member (zero object allocation)
   * @param map sorted set storage
   * @param keyPtr set key address
   * @param keySize set key size
   * @param elemPtr member address
   * @param elemSize member size
   * @return 1 or 0
   */
  public static int SREM(BigSortedMap map, long keyPtr, int keySize, long elemPtr, int elemSize) {
    Key k = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(k);
      int removed = 0;
      int kSize = buildKey(keyPtr, keySize, elemPtr, elemSize);
      SetDelete remove = setDelete.get();
      remove.reset();
      remove.setMap(map);
      remove.setKeyAddress(keyArena.get());
      remove.setKeySize(kSize);
      // version?
      if (map.execute(remove)) {
        removed++;
        if (remove.checkForEmpty && isEmpty(map, keyPtr, keySize)) {
          DELETE(map, keyPtr, keySize);
        }
      }
      return removed;
    } finally {
      KeysLocker.writeUnlock(k);
    }
  }
  
  /**
   * For TESTING only
   * 
   * Returns if member is a member of the set stored at key.
   * Return value
   * Integer reply, specifically:
   * 1 if the element is a member of the set.
   * 0 if the element is not a member of the set, or if key does not exist.   
   * @param map sorted map
   * @param key set's key 
   * @param value member to check
   * @return 1 if exists, 0 - otherwise
   */
  
  public static int SISMEMBER(BigSortedMap map, String key, String value) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long elemPtr = UnsafeAccess.allocAndCopy(value, 0, value.length());
    int elemSize = value.length();
    int result = SISMEMBER(map, keyPtr, keySize, elemPtr, elemSize);
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(elemPtr);
    return result;
  }
  
  /**
   * Returns if member is a member of the set stored at key.
   * Return value
   * Integer reply, specifically:
   * 1 if the element is a member of the set.
   * 0 if the element is not a member of the set, or if key does not exist.   
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @param elemPtr value address
   * @param elemSize value size
   * @return 1 if exists, 0 - otherwise
   */
  public static int SISMEMBER(BigSortedMap map, long keyPtr, int keySize, long elemPtr, 
      int elemSize) {
    int kSize = buildKey(keyPtr, keySize, elemPtr, elemSize);
    SetExists exists = setExists.get();
    exists.reset();
    exists.setKeyAddress(keyArena.get());
    exists.setKeySize(kSize);
    // version?    
    if(map.execute(exists)) {
      return 1;
    }
    return 0;
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key set's key
   * @param members list of members to check
   * @return result array
   */
  public static byte[] SMISMEMBER(BigSortedMap map, String key, String[] members) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize= key.length();
    long[] elemPtrs = new long[members.length];
    int[] elemSizes = new int[members.length];
    int i=0;
    for(String m: members) {
      elemPtrs[i] = UnsafeAccess.allocAndCopy(m, 0, m.length());
      elemSizes[i++] = m.length();
    }
    long buffer = SMISMEMBER(map, keyPtr, keySize, elemPtrs, elemSizes);
    byte[] returnValue = new byte[members.length];
    for(i=0; i < returnValue.length; i++) {
      returnValue[i] = UnsafeAccess.toByte(buffer + i);
    }
    UnsafeAccess.free(keyPtr);
    Arrays.stream(elemPtrs).forEach(x -> UnsafeAccess.free(x));
    return returnValue;
  }
  
  /**
   * Returns whether each member is a member of the set stored at key.
   * For every member, 1 is returned if the value is a member of the set, 
   * or 0 if the element is not a member of the set or if key does not exist.
   * @param map sorted map storage
   * @param keyPtr set's key address
   * @param keySize set's key size
   * @param elemPtrs array of pointers
   * @param elemSizes array of sizes
   * @return address of a buffer to read results from
   */
  public static long SMISMEMBER (BigSortedMap map, long keyPtr, int keySize, long[] elemPtrs,
      int[] elemSizes) {
    
    checkValueArena(elemPtrs.length);
    long buffer = valueArena.get();
    for (int i=0; i < elemPtrs.length; i++) {
      int result = SISMEMBER(map, keyPtr, keySize, elemPtrs[i], elemSizes[i]);
      UnsafeAccess.putByte(buffer + i, (byte) result);
    }
    return buffer;
  }
  
  /**
   * For testing only
   * @param map sorted ordered map
   * @param srcKey source set key
   * @param dstKey destination set key
   * @param member member to move
   * @return 1 - success, 0 - member is not in source
   */
  public static int SMOVE(BigSortedMap map, String srcKey, String dstKey, String member) {
    long srcKeyPtr = UnsafeAccess.allocAndCopy(srcKey, 0, srcKey.length());
    int srcKeySize = srcKey.length();
    long dstKeyPtr = UnsafeAccess.allocAndCopy(dstKey, 0, dstKey.length());
    int dstKeySize = dstKey.length();
    long memPtr = UnsafeAccess.allocAndCopy(member, 0, member.length());
    int memSize = member.length();
    
    int result = SMOVE(map, srcKeyPtr, srcKeySize, dstKeyPtr, dstKeySize, memPtr, memSize);
    UnsafeAccess.free(srcKeyPtr);
    UnsafeAccess.free(dstKeyPtr);
    UnsafeAccess.free(memPtr);
    return result;
  }
  
  /**
   * Move member from the set at source to the set at destination. This operation is atomic. 
   * In every given moment the element will appear to be a member of source or destination 
   * for other clients.
   * If the source set does not exist or does not contain the specified element, 
   * no operation is performed and 0 is returned. Otherwise, the element is removed from 
   * the source set and added to the destination set. When the specified element already exists 
   * in the destination set, it is only removed from the source set.
   * An error is returned if source or destination does not hold a set value.
   * Return value
   * Integer reply, specifically:
   * 1 if the element is moved.
   * 0 if the element is not a member of source and no operation was performed.
   * @param map sorted map storage
   * @param srcKeyPtr source key address
   * @param srcKeySize source key size
   * @param dstKeyPtr destination key address
   * @param dstKeySize destination key size
   * @param elemPtr element address
   * @param elemSize element size
   * @return 1 if the element is moved. 0 if the element is not a member of source and no 
   *        operation was performed.
   */
  public static int SMOVE(BigSortedMap map, long srcKeyPtr, int srcKeySize, long dstKeyPtr, int dstKeySize, 
      long elemPtr, int elemSize) {
    
    Key src = new Key(srcKeyPtr, srcKeySize);
    Key dst = new Key(dstKeyPtr, dstKeySize);
    ArrayList<Key> keyList = new ArrayList<Key>();
    keyList.add(src);
    keyList.add(dst);    
    try {
      KeysLocker.writeLockAllKeys(keyList);
      int n = SREM(map, srcKeyPtr, srcKeySize, elemPtr, elemSize);
      if (n == 0) {
        return 0;
      }
      int count = SADD(map, dstKeyPtr, dstKeySize, elemPtr, elemSize);
      return count;
    } finally {
      KeysLocker.writeUnlockAllKeys(keyList);
    }    
  }
  
  /**
   * For testing only 
   * @param map sorted map storage
   * @param key set's key
   * @param count total number of elements
   * @param bufSize buffer size
   * @return number of elements written into a buffer
   */
  public static List<String> SPOP(BigSortedMap map, String key, int count , int bufSize) {
    long buffer = UnsafeAccess.malloc(bufSize);
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    
    long result = SPOP(map, keyPtr, keySize, buffer, bufSize, count) ;
    List<String> list = new ArrayList<String>();
    int total = UnsafeAccess.toInt(buffer);
    long ptr = buffer + Utils.SIZEOF_INT;
    for (int i = 0; i < total; i++) {
      int mSize = Utils.readUVInt(ptr);
      int mSizeSize = Utils.sizeUVInt(mSize);
      String s = Utils.toString(ptr + mSizeSize, mSize);
      list.add(s);
      ptr += mSize + mSizeSize;
    }
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(buffer);
    return list;
  }
  /**
   * Removes and returns one or more random elements from the set value store at key.
   * This operation is similar to SRANDMEMBER, that returns one or more random elements 
   * from a set but does not remove it.
   * The count argument is available since version 3.2.
   * Return value
   * Bulk string reply: the removed element, or nil when key does not exist.
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @param bufferPtr buffer address
   * @param bufferSize buffer size
   * @param count count
   * @return number of elements or -1 if buffer is not large enough, 0 - empty or does not exists 
   */
  public static long SPOP(BigSortedMap map, long keyPtr, int keySize, long bufferPtr, int bufferSize, 
      int count) 
  {
    if (count == 0) {
      return 0;
    }
    Key k = getKey(keyPtr, keySize);
    boolean distinct = count > 0;
    if (!distinct) {
      count = -count;
    }
    SetScanner scanner = null;
    try {
      KeysLocker.writeLock(k);
      long total =  SCARD(map, keyPtr, keySize);
      if (total == 0) {
        return 0; // Empty set or does not exists
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
        long result = SMEMBERS(map, keyPtr, keySize, bufferPtr, bufferSize);
        if (result >= 0) {
          DELETE(map, keyPtr, keySize);
        }
        return result;
      }
      scanner = getScanner(map, keyPtr, keySize, false);
      long result = readByIndex(scanner, index, bufferPtr, bufferSize);
      if (scanner != null) {
        try {
          scanner.close();
          scanner = null;
        } catch (IOException e) {
          // Should not be here
        }
      }
      // Now delete all
      long deleted = bulkDelete(map, keyPtr, keySize, bufferPtr);
      return deleted;
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
          // Should not be here
        }
      }
      KeysLocker.writeUnlock(k);
    }
  }
  
  /**
   * Deletes all members from a buffer
   * @param map sorted map
   * @param keyPtr set key address
   * @param keySize set key size
   * @param bufferPtr buffer address
   * @return number of deleted members
   */
  private static int bulkDelete(BigSortedMap map, final long keyPtr, final int keySize,
      final long bufferPtr) {
    // Reads total number of elements
    int total = UnsafeAccess.toInt(bufferPtr);
    int count = 0, deleted = 0;
    long ptr = bufferPtr + Utils.SIZEOF_INT;
    // No locking - it is safe here
    while (count++ < total) {
      int eSize = Utils.readUVInt(ptr);
      int eSizeSize = Utils.sizeUVInt(eSize);
      int kSize = buildKey(keyPtr, keySize, ptr + eSizeSize, eSize);
      SetDelete remove = setDelete.get();
      remove.reset();
      remove.setMap(map);
      remove.setKeyAddress(keyArena.get());
      remove.setKeySize(kSize);
      // version?
      if (map.execute(remove)) {
        deleted++;
      }
      ptr += eSize + eSizeSize;
    }
    return deleted;

  }
  /**
   * Delete set by Key
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @return number of deleted K-Vs
   */
  public static long DELETE(BigSortedMap map, long keyPtr, int keySize) {
    Key k = getKey(keyPtr, keySize);
    long startKeyPtr = 0, endKeyPtr = 0;
    try {
      KeysLocker.writeLock(k);
      startKeyPtr = UnsafeAccess.malloc(keySize + KEY_SIZE + 2 * Utils.SIZEOF_BYTE);
      int newKeySize = buildKey(keyPtr, keySize, Commons.ZERO, 1, startKeyPtr);
      endKeyPtr = Utils.prefixKeyEnd(startKeyPtr, newKeySize - 1);      
      long deleted = map.deleteRange(startKeyPtr, newKeySize, endKeyPtr, newKeySize - 1);      
      return deleted;
    } finally {
      if (startKeyPtr > 0) {
        UnsafeAccess.free(startKeyPtr);
      }
      if (endKeyPtr > 0) {
        UnsafeAccess.free(endKeyPtr);
      }
      KeysLocker.writeUnlock(k);
    }

  }
  /**
   * When called with just the key argument, return a random element from the set value stored at key.
   * Starting from Redis version 2.6, when called with the additional count argument, return an array 
   * of count distinct elements if count is positive. If called with a negative count the behavior 
   * changes and the command is allowed to return the same element multiple times. In this case the number
   *  of returned elements is the absolute value of the specified count.
   * When called with just the key argument, the operation is similar to SPOP, however while SPOP also 
   * removes the randomly selected element from the set, SRANDMEMBER will just return a random element 
   * without altering the original set in any way.
   * Return value
   * Bulk string reply: without the additional count argument the command returns a Bulk Reply with 
   * the randomly selected element, or nil when key does not exist. 
   * Array reply: when the additional count argument is passed the command returns an array of elements, 
   * or an empty array when key does not exist.
   * 
   * When a count argument is passed and is positive, the elements are returned as if every selected element 
   * is removed from the set (like the extraction of numbers in the game of Bingo). However elements are not 
   * removed from the Set. So basically:
   * No repeated elements are returned.
   * If count is bigger than the number of elements inside the Set, the command will only return the whole set 
   * without additional elements.
   * When instead the count is negative, the behavior changes and the extraction happens as if you put 
   * the extracted element inside the bag again after every extraction, so repeated elements are possible, 
   * and the number of elements requested is always returned as we can repeat the same elements again 
   * and again, with the exception of an empty Set (non existing key) that will always produce an empty array 
   * as a result.
   * 
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @param bufferPtr buffer address
   * @param bufferSize buffer size
   * @param count number of random elements to return
   * @return number of elements which fit the buffer, 0 - empty or does not exists 
   */
  public static long SRANDMEMBER(BigSortedMap map, long keyPtr, int keySize, long bufferPtr, int bufferSize, 
      int count) 
  {
    Key k = getKey(keyPtr, keySize);
    boolean distinct = count > 0;
    if (!distinct) {
      count = -count;
    }
    SetScanner scanner = null;
    try {
      KeysLocker.readLock(k);
      long total =  SCARD(map, keyPtr, keySize);
      if (total == 0) {
        return 0; // Empty set or does not exists
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
        return SMEMBERS(map, keyPtr, keySize, bufferPtr, bufferSize);
      }
      scanner = getScanner(map, keyPtr, keySize, false);
      long result = readByIndex(scanner, index, bufferPtr, bufferSize);
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
   * For testing only 
   * @param map sorted map storage
   * @param key set's key
   * @param count total number of elements
   * @param bufSize buffer size
   * @return number of elements written into a buffer
   */
  public static List<String> SRANDMEMBER(BigSortedMap map, String key, int count , int bufSize) {
    long buffer = UnsafeAccess.malloc(bufSize);
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    
    long result = SRANDMEMBER(map, keyPtr, keySize, buffer, bufSize, count) ;
    List<String> list = new ArrayList<String>();
    int total = UnsafeAccess.toInt(buffer);
    long ptr = buffer + Utils.SIZEOF_INT;
    for (int i = 0; i < total; i++) {
      int mSize = Utils.readUVInt(ptr);
      int mSizeSize = Utils.sizeUVInt(mSize);
      String s = Utils.toString(ptr + mSizeSize, mSize);
      list.add(s);
      ptr += mSize + mSizeSize;
    }
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(buffer);
    return list;
  }
  /**
   * Return number of written element
   * @param scanner set scanner
   * @param index array of random indexes to read
   * @param bufferPtr buffer 
   * @param bufferSize buffer size
   * @return full serialized size required
   */
  private static long readByIndex(SetScanner scanner, long[] index, long bufferPtr, int bufferSize) {
    
    long ptr = bufferPtr + Utils.SIZEOF_INT;
    UnsafeAccess.putInt(bufferPtr, 0);
    for (int i = 0; i < index.length; i++) {
      scanner.skipTo(index[i]);
      int eSize = scanner.memberSize();
      int eSizeSize = Utils.sizeUVInt(eSize);
      if (ptr + eSize + eSizeSize <= bufferPtr + bufferSize) {
        long ePtr = scanner.memberAddress();
        // Write size
        Utils.writeUVInt(ptr, eSize);
        // Copy member
        UnsafeAccess.copy(ePtr,  ptr + eSizeSize, eSize);
        UnsafeAccess.putInt(bufferPtr, i + 1);
      }
      ptr += eSize + eSizeSize;   
    }
    return ptr - bufferPtr;
  }
  
  /**
   * For testing only
   * @param map sorted ordered map
   * @param key set's key
   * @param bufferSize recommended buffer size to hold all data
   * @return list of members, which fit buffer or null (set does not exist)
   */
  public static List<byte[]> SMEMBERS (BigSortedMap map, byte[] key, int bufferSize) {
    
    long bufferPtr = UnsafeAccess.malloc(bufferSize);
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length);
    int keySize = key.length;
    long result = SMEMBERS(map, keyPtr, keySize, bufferPtr, bufferSize);
    if (result <= 0) {
      return null;
    }
    List<byte[]> list = new ArrayList<byte[]>();
    long count = UnsafeAccess.toLong(bufferPtr);
    long ptr = bufferPtr + Utils.SIZEOF_LONG;
    for (int i = 0; i < count; i++) {
      int size = Utils.readUVInt(ptr);
      int sizeSize = Utils.sizeUVInt(size);
      byte[] member = Utils.toBytes(ptr + sizeSize, size);
      list.add(member);
      ptr += size + sizeSize;
    }
    UnsafeAccess.free(bufferPtr);
    UnsafeAccess.free(keyPtr);
    return list;
  }
  
  /**
   * Returns all the members of the set value stored at key.
   * Serialized format : [SET_SIZE][MEMBER]+
   *  [SET_SIZE] = 8 bytes
   *  [MEMBER]:
   *    [SIZE] - varint 1..4 bytes
   *    [VALUE]
   *    
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @return total buffer size required to hold all members, or -1 if set does not exist 0 - empty
   */
  public static long SMEMBERS(BigSortedMap map, long keyPtr, int keySize, long bufferPtr, int bufferSize) {
    long ptr = bufferPtr + Utils.SIZEOF_LONG;
    long count = 0;
    SetScanner scanner = getScanner(map, keyPtr, keySize, false);
    if (scanner == null) {
      return -1;
    }
    try {
      // Write total number of elements
      UnsafeAccess.putLong(bufferPtr, count);// Write 0
      while (scanner.hasNext()) {
        int elSize = scanner.memberSize();
        int elSizeSize = Utils.sizeUVInt(elSize);
        if (ptr + elSize + elSizeSize <= bufferPtr + bufferSize) {
          long elPtr = scanner.memberAddress();
          // Write size
          Utils.writeUVInt(ptr, elSize);
          // Copy member
          UnsafeAccess.copy(elPtr, ptr + elSizeSize, elSize);
          count++;
          // Write total number of elements
          UnsafeAccess.putLong(bufferPtr, count);
        }
        ptr += elSize + elSizeSize;
        scanner.next();
      }
      scanner.close();
    } catch (IOException e) {
    } 
    // Return required buffer size to hold all members
    return ptr - bufferPtr;
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
  public static List<String> SSCAN(BigSortedMap map, String key, String lastSeenMember, 
      int count, int bufferSize, String regex){
    
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long lastSeenPtr = lastSeenMember == null? 0: 
      UnsafeAccess.allocAndCopy(lastSeenMember, 0, lastSeenMember.length());
    int lastSeenSize = lastSeenMember == null? 0: lastSeenMember.length();
    long buffer = UnsafeAccess.malloc(bufferSize);
    // Clear first 4 bytes of a buffer
    UnsafeAccess.putInt(buffer, 0);
    long totalSize = SSCAN(map, keyPtr, keySize, lastSeenPtr, lastSeenSize, count, buffer, bufferSize, regex);
    if (totalSize == 0) {
      return null;
    }
    int total = UnsafeAccess.toInt(buffer);
    if (total == 0) return null;
    List<String> list = new ArrayList<String>();
    long ptr = buffer + Utils.SIZEOF_INT;
    // the last is going to be last seen member (duplicate if regex == null)
    for (int i=0; i < total; i++) {
      int size = Utils.readUVInt(ptr);
      int sizeSize = Utils.sizeUVInt(size);
      String s = Utils.toString(ptr + sizeSize, size);
      list.add(s);
      ptr += size + sizeSize;
    }
    UnsafeAccess.free(keyPtr);
    if (lastSeenPtr > 0) {
      UnsafeAccess.free(lastSeenPtr);
    }
    UnsafeAccess.free(buffer);
    return list;
  }
  
  /**
   * Default (w/o offset) SSCAN
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @param lastSeenMemberPtr last seen member address
   * @param lastSeenMemberSize last seen member size
   * @param count count
   * @param buffer buffer to stotre result
   * @param bufferSize buffer size
   * @param regex pattern
   * @return full serialized size of the response
   */
  public static long SSCAN(BigSortedMap map, long keyPtr, int keySize, long lastSeenMemberPtr,
      int lastSeenMemberSize, int count, long buffer, int bufferSize, String regex) {
    return SSCAN(map, keyPtr, keySize, lastSeenMemberPtr, lastSeenMemberSize, count, buffer, bufferSize, regex, 0);
  }
  /**
   * 
   * TODO: regex flavors 
   * TODO: regex speed
   * TODO: Always put last seen member into the result
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
   * @param lastSeenMemberPtr  last seen field address
   * @param lastSeenMemberSize last seen field size
   * @param count number of elements to return
   * @param buffer memory buffer for return items
   * @param bufferSize buffer size
   * @param regex pattern to match
   * @return total serialized size of the response, if greater than bufferSize, the call 
   *         must be retried with the appropriately sized buffer
   */
  public static long SSCAN(BigSortedMap map, long keyPtr, int keySize, long lastSeenMemberPtr,
      int lastSeenMemberSize, int count, long buffer, int bufferSize, String regex, int regexOffset) {
    Key key = getKey(keyPtr, keySize);
    SetScanner scanner = null;
    try {
      KeysLocker.readLock(key);
      scanner = Sets.getScanner(map, keyPtr, keySize, lastSeenMemberPtr, lastSeenMemberSize, 0, 0,
        false);
      if (scanner == null) {
        return 0;
      }
      // Check first member
      if (lastSeenMemberPtr > 0) {
        long ptr = scanner.memberAddress();
        int size = scanner.memberSize();
        if (Utils.compareTo(ptr, size, lastSeenMemberPtr, lastSeenMemberSize) == 0) {
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
      while (scanner.hasNext() && c <= count) {
        long mPtr = scanner.memberAddress();
        int mSize = scanner.memberSize();
        int mSizeSize = Utils.sizeUVInt(mSize);
        // This is possible that bufferSize < 2 * (mSize + mSizeSize)
        // to handle this situation we need to check both: return value, which is 4 (> 0)
        // and total , which is 0
        if ((ptr + 2 * (mSize + mSizeSize)/* to add lastSeenSize*/  
            <= buffer + bufferSize)) {
          if (regex == null || Utils.matches(mPtr + regexOffset, mSize - regexOffset, regex)) {
            c++;
            Utils.writeUVInt(ptr, mSize);
            UnsafeAccess.copy(mPtr, ptr + mSizeSize, mSize);
            UnsafeAccess.putInt(buffer, c);
            ptr +=  mSize + mSizeSize;
          }
          // write last seen
          Utils.writeUVInt(ptr, mSize);
          UnsafeAccess.copy(mPtr, ptr + mSizeSize, mSize);
          UnsafeAccess.putInt(buffer, c);
        } else {
          break;
        }
        scanner.next();
      }
      scanner.close();
      return ptr - buffer;
    } catch (IOException e) {
      // Will never be thrown
    } finally {
      KeysLocker.readUnlock(key);
    }
    return 0;
  }
  
  
  /**
   * Get set scanner for set operations, as since we can create multiple
   * set scanners in the same thread we can not use thread local variables
   * WARNING: we can not create multiple scanners in a single thread
   * @param map sorted map to run on
   * @param keyPtr key address
   * @param keySize key size
   * @param safe get safe instance
   * @param reverse reverse scanner
   * @return set scanner
   */
  public static SetScanner getScanner(BigSortedMap map, long keyPtr, int keySize, boolean safe) {
    return getScanner(map, keyPtr, keySize, safe, false);
  }

  /**
   * Get set scanner for set operations, as since we can create multiple
   * set scanners in the same thread we can not use thread local variables
   * WARNING: we can not create multiple scanners in a single thread
   * @param map sorted map to run on
   * @param keyPtr key address
   * @param keySize key size
   * @param safe get safe instance
   * @return set scanner
   */
  public static SetScanner getScanner(BigSortedMap map, long keyPtr, int keySize, boolean safe, boolean reverse) {
    long kPtr = UnsafeAccess.malloc(keySize + KEY_SIZE + 2 * Utils.SIZEOF_BYTE);
    UnsafeAccess.putByte(kPtr, (byte)DataType.SET.ordinal());
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
    try {
      SetScanner sc = new SetScanner(scanner, reverse);
      sc.setDisposeKeysOnClose(true);
      return sc;
    } catch (IOException e) {
      try {
        scanner.close();
      } catch (IOException e1) {
      }
      return null;
    }
  }
  /**
   * Get set scanner (with a given range) for set operations, as since we can create multiple
   * set scanners in the same thread we can not use thread local variables
   * WARNING: we can not create multiple scanners in a single thread
   * @param map sorted map to run on
   * @param keyPtr key address
   * @param keySize key size
   * @param memberStartPtr start member address
   * @param memberStartSize size
   * @param memberStopPtr member stop address
   * @param memberStopSize member stop size
   * @param safe get safe instance
   * @return set scanner
   */
  public static SetScanner getScanner(BigSortedMap map, long keyPtr, int keySize, 
      long memberStartPtr, int memberStartSize, long memberStopPtr, int memberStopSize, boolean safe) {
    return getScanner(map, keyPtr, keySize, memberStartPtr, memberStartSize, memberStopPtr, 
      memberStopSize, safe, false);
  }
  
  /**
   * Get set scanner (with a given range) for set operations, as since we can create multiple
   * set scanners in the same thread we can not use thread local variables
   * WARNING: we can not create multiple scanners in a single thread
   * @param map sorted map to run on
   * @param keyPtr key address
   * @param keySize key size
   * @param fieldStartPtr start member address
   * @param fieldStartSize size
   * @param fieldStopPtr member stop address
   * @param fieldStopSize member stop size
   * @param safe get safe instance
   * @param reverse get reverse scanner (true), normal (false)
   * @return set scanner
   */
  public static SetScanner getScanner(BigSortedMap map, long keyPtr, int keySize, 
      long fieldStartPtr, int fieldStartSize, long fieldStopPtr, int fieldStopSize, boolean safe, boolean reverse) {
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
    
    // Special handling when fieldStartPtr == 0 (from beginning)
    long startPtr = UnsafeAccess.malloc(keySize + KEY_SIZE + Utils.SIZEOF_BYTE + 
      (fieldStartSize == 0? 1: fieldStartSize));
    int startPtrSize = buildKey(keyPtr, keySize, fieldStartPtr, fieldStartSize, startPtr);
    if (fieldStartSize == 0) {
      startPtrSize += 1;
      UnsafeAccess.putByte(startPtr + startPtrSize - 1, (byte) 0);
    }
    
    long stopPtr = UnsafeAccess.malloc(keySize + KEY_SIZE + Utils.SIZEOF_BYTE + fieldStopSize);
    int stopPtrSize = buildKey(keyPtr, keySize, fieldStopPtr, fieldStopSize, stopPtr);
    
    if (fieldStopPtr == 0) {
      stopPtr = Utils.prefixKeyEndNoAlloc(stopPtr, stopPtrSize);
    }
    
    if (fieldStartPtr > 0) {
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
      // check first 5 + keySize bytes
      if (size > 0 && Utils.compareTo(startPtr, keySize + Utils.SIZEOF_INT + Utils.SIZEOF_BYTE, 
        valueArena.get(), keySize + Utils.SIZEOF_INT + Utils.SIZEOF_BYTE) == 0) {
        // free start key
        UnsafeAccess.free(startPtr);
        startPtr = UnsafeAccess.malloc(size);
        UnsafeAccess.copy(valueArena.get(), startPtr, size);
        startPtrSize = (int)size;
      }
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
    try {
      SetScanner sc = new SetScanner(scanner, fieldStartPtr, fieldStartSize, 
        fieldStopPtr, fieldStopSize, reverse);
      sc.setDisposeKeysOnClose(true);
      return sc;
    } catch (IOException e) {
      try {
        scanner.close();
      } catch (IOException e1) {
      }
      return null;
    }
  }
  
  /**
   * Finds location of a given element in a Value object
   * @param foundRecordAddress address of K-V record
   * @param elementPtr element address
   * @param elementSize element size
   * @return address of element in a Value or -1, if not found
   */
  public static long exactSearch(long foundRecordAddress, long elementPtr, int elementSize) {
    long valuePtr = DataBlock.valueAddress(foundRecordAddress);
    int valueSize  = DataBlock.valueLength(foundRecordAddress);
    int off = NUM_ELEM_SIZE; // skip number of elements in value
    while(off < valueSize) {
      int eSize = Utils.readUVInt(valuePtr + off);
      int skip = Utils.sizeUVInt(eSize);
      if (Utils.compareTo(elementPtr, elementSize, valuePtr + off + skip, eSize) == 0) {
        return valuePtr + off;
      }
      off+= skip + eSize;
    }
    return -1; // NOT_FOUND
  }
  
  /**
   * Finds first element which is greater or equals to a given one
   * in a Value object
   * @param foundRecordAddress address of a K-V record
   * @param elementPtr element address
   * @param elementSize element size
   * @return address to insert to insert to
   */
  public static long insertSearch(long foundRecordAddress, long elementPtr, int elementSize) {
    long valuePtr = DataBlock.valueAddress(foundRecordAddress);
    int valueSize  = DataBlock.valueLength(foundRecordAddress);
    int off = NUM_ELEM_SIZE; // skip number of elements
    while(off < valueSize) {
      int eSize = Utils.readUVInt(valuePtr + off);
      int eSizeSize = Utils.sizeUVInt(eSize);
      if (Utils.compareTo(elementPtr, elementSize, valuePtr + off + eSizeSize, eSize) <= 0) {
        return valuePtr + off;
      }
      off+= eSizeSize + eSize;
    }
    return valuePtr + valueSize; // put in the end largest one
  }
  
  /**
   * Compare elements which starts in a given address 
   * with a given element
   * @param ptr address of an first element
   * @param elemPtr address of a second element
   * @param elemSize second element size
   * @return o - if equals, -1, +1
   */
  public static int compareElements (long ptr, long elemPtr, int elemSize) {
    int eSize = Utils.readUVInt(ptr);
    int eSizeSize = Utils.sizeUVInt(eSize);
    return Utils.compareTo(ptr + eSizeSize, eSize, elemPtr, elemSize); 
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
      int eSize = Utils.readUVInt(valuePtr + off);
      int eSizeSize = Utils.sizeUVInt(eSize);
      prevOff = off;
      off+= eSizeSize + eSize;
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
      int eSize = Utils.readUVInt(valuePtr + off);
      int eSizeSize = Utils.sizeUVInt(eSize);
      prevOff = off;
      off+= eSizeSize + eSize;
    }
    if (prevOff - NUM_ELEM_SIZE > valueSize - off) {
      return n-1;
    } else {
      return n;
    }
  }
  
  /**
   * Gets element size by address
   * @param addr address
   * @return size of element
   */
  public static int getElementSize(long addr) {
    return Utils.readUVInt(addr);
  }
  
  /**
   * Get element address
   * @param addr of size:element tuple
   * @return address of element (skips variable size)
   */
  
  public static long getElementAddress(long addr) {
    int size = Utils.readUVInt(addr);
    return addr + Utils.sizeUVInt(size);
  }
  /**
   * Gets total element size including variable part
   * @param addr address of size:element tuple
   * @return size total
   */
  public static int getTotalElementSize(long addr) {
    int size = Utils.readUVInt(addr);
    return size + Utils.sizeUVInt(size);
  }
  
  
  public static boolean checkCorruptedValue(long ptr, int expSize) {
    int total = UnsafeAccess.toShort(ptr);

    int off = NUM_ELEM_SIZE;
    int count = 0;
    while(count++ < total) {
      int eSize = Utils.readUVInt(ptr + off);
      int eSizeSize = Utils.sizeUVInt(eSize);

      if (eSize != expSize) {
        System.out.println("Dump value , elements="+ total);
        System.out.println("CORRUPT eSize="+ eSize + " index (+1)="+ count);
        return true;
      }
      off += eSize + eSizeSize;
    }
    return false;
  }
  
}
