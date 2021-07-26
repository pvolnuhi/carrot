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
package org.bigbase.carrot.redis.zsets;

import static org.bigbase.carrot.redis.util.Commons.KEY_SIZE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.RedisConf;
import org.bigbase.carrot.redis.hashes.HashScanner;
import org.bigbase.carrot.redis.hashes.HashSet;
import org.bigbase.carrot.redis.hashes.Hashes;
import org.bigbase.carrot.redis.sets.SetAdd;
import org.bigbase.carrot.redis.sets.SetScanner;
import org.bigbase.carrot.redis.sets.Sets;
import org.bigbase.carrot.redis.util.Aggregate;
import org.bigbase.carrot.redis.util.DataType;
import org.bigbase.carrot.redis.util.MutationOptions;
import org.bigbase.carrot.util.Key;
import org.bigbase.carrot.util.KeysLocker;
import org.bigbase.carrot.util.Pair;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Redis Sorted Sets are, similarly to Redis Sets, non repeating collections of Strings. 
 * The difference is that every member of a Sorted Set is associated with score, that is used 
 * in order to take the sorted set ordered, from the smallest to the greatest score. 
 * While members are unique, scores may be repeated.
 * With sorted sets you can add, remove, or update elements in a very fast way (in a time 
 * proportional to the logarithm of the number of elements). Since elements are taken in order and 
 * not ordered afterwards, you can also get ranges by score or by rank (position) in a very fast way.
 *  Accessing the middle of a sorted set is also very fast, so you can use Sorted Sets as a smart list 
 *  of non repeating elements where you can quickly access everything you need: elements in order, fast 
 *  existence test, fast access to elements in the middle!
 * In short with sorted sets you can do a lot of tasks with great performance that are really hard to model 
 * in other kind of databases.
 * 
 * Implementation details:
 * 
 *  ZSET uses one set and one hash (see Sets):
 *  1. One keeps key -> (score, member) combo in a Set, where it is sorted by score, then by member
 *  2. Second keeps key -> {member, score} pair in a Hash, where member is the field and score
 *     is the value
 *  
 *  Score (double value) is converted to 8 byte sequence suitable for lexicographical 
 *  comparison
 *  
 *  
 *  Key is built the following way:
 *  
 *  [TYPE = ZSET][BASE_KEY_LENGTH][KEY][ZSET_ID][(SCORE,VALUE) | (VALUE)]
 *  
 *  TYPE = 1 byte
 *  BASE_KEY_LENGTH - 4 bytes
 *  SET_ID = [0,1,2] 0 - set by score,member, 1 - hash by member-score, 2 - set cardinality
 *  
 *  Optimization for short ordered sets: we can use only one ordered set by score and convert
 *  to  set and hash representation when number of elements exceeds some threshold, similar how Redis
 *  optimizes small LIST, HASH, ZSET into ziplist representation. 
 *  
 * @author Vladimir Rodionov
 *
 */
public class ZSets {
  
  private final static int CARD_MEMBER_SIZE = 16;// random bytes
  
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
      return UnsafeAccess.malloc(4 * 1024);
    }
  };
  
  static ThreadLocal<Integer> valueArenaSize = new ThreadLocal<Integer>() {
    @Override
    protected Integer initialValue() {
      return 4 * 1024;
    }
  };
  
  static ThreadLocal<Long> auxArena = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(4 * 1024);
    }
  };
  
  static ThreadLocal<Integer> auxArenaSize = new ThreadLocal<Integer>() {
    @Override
    protected Integer initialValue() {
      return 4 * 1024;
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
   * Thread local updates Set Add
   */
  private static ThreadLocal<SetAdd> setAdd = new ThreadLocal<SetAdd>() {
    @Override
    protected SetAdd initialValue() {
      return new SetAdd();
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
   * Allocates and return hash member (16 - bytes), which keeps zset cardinality
   * @param kPtr key address
   * @param kSize key size
   * @return card pointer
   */
  private static long getCardinalityMember(long kPtr, int kSize) {
    long ptr = UnsafeAccess.malloc(CARD_MEMBER_SIZE);
    Utils.random16(kPtr, kSize, ptr);
    return ptr;
  }
  
  /**
   * Checks if a current hash member is a cardinality holder
   * @param mPtr member address
   * @param mSize member size
   * @param cardPtr cardinality pointer
   * @return false/true
   */
  private static boolean isCardinalityMember(long mPtr, int mSize, long cardPtr) {
    if (mSize != CARD_MEMBER_SIZE) return false;
    return Utils.compareTo(mPtr, mSize, cardPtr, mSize) == 0;
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
   * Checks auxiliary arena size
   * @param required size
   */
  static void checkAuxArena (int required) {
    int size = auxArenaSize.get();
    if (size >= required) {
      return;
    }
    long ptr = UnsafeAccess.realloc(auxArena.get(), required);
    auxArena.set(ptr);
    auxArenaSize.set(required);
  }
  
  /**
   * Build key for Set. It uses thread local key arena 
   * @param keyPtr original key address
   * @param keySize original key size
   * @param memberPtr element address
   * @param memberSize element size
   * @param score score
   * @return new key size 
   */
    
   
  static int buildKeyForSet( long keyPtr, int keySize, long memberPtr, 
      int memberSize, double score) {
    checkKeyArena(keySize + KEY_SIZE + memberSize + Utils.SIZEOF_BYTE + Utils.SIZEOF_DOUBLE);
    long arena = keyArena.get();
    int kSize = KEY_SIZE + keySize + Utils.SIZEOF_BYTE;
    UnsafeAccess.putByte(arena, (byte)DataType.SET.ordinal());
    UnsafeAccess.putInt(arena + Utils.SIZEOF_BYTE, keySize);
    UnsafeAccess.copy(keyPtr, arena + KEY_SIZE + Utils.SIZEOF_BYTE, keySize);
    Utils.doubleToLex(arena + kSize, score);
    kSize += Utils.SIZEOF_DOUBLE;
    if (memberPtr > 0) {
      UnsafeAccess.copy(memberPtr, arena + kSize, memberSize);
      kSize += memberSize;
    }
    return kSize;
  }
  
  /**
   * Build key for Set. It uses provided arena 
   * @param keyPtr original key address
   * @param keySize original key size
   * @param memberPtr element address
   * @param memberSize element size
   * @return new key size 
   */
    
  static int buildKeyForSet( long keyPtr, int keySize, long memberPtr, 
      int memberSize, double score, long arena) {
    int kSize = KEY_SIZE + keySize + Utils.SIZEOF_BYTE;
    UnsafeAccess.putByte(arena, (byte)DataType.SET.ordinal());
    UnsafeAccess.putInt(arena + Utils.SIZEOF_BYTE, keySize);
    UnsafeAccess.copy(keyPtr, arena + KEY_SIZE + Utils.SIZEOF_BYTE, keySize);
    Utils.doubleToLex(arena + kSize, score);
    kSize += Utils.SIZEOF_DOUBLE;
    if (memberPtr > 0) {
      UnsafeAccess.copy(memberPtr, arena + kSize, memberSize);
      kSize += memberSize;
    }
    return kSize;
  }
  
  /**
   * Build key for Hash. It uses thread local key arena 
   * @param keyPtr original key address
   * @param keySize original key size
   * @param memberPtr element address
   * @param memberSize element size
   * @return new key size 
   */
     
  static int buildKeyForHash( long keyPtr, int keySize, long memberPtr, 
      int memberSize) {
    checkKeyArena(keySize + KEY_SIZE + memberSize + Utils.SIZEOF_BYTE);
    return Hashes.buildKey(keyPtr, keySize, memberPtr, memberSize, keyArena.get());
  }
  
  /**
   * Build key for ZSet. It is used to keep cardinality 
   * @param keyPtr original key address
   * @param keySize original key size
   * @return new key size 
   */
    
   
  static int buildKey(long keyPtr, int keySize) {
    checkKeyArena(keySize + KEY_SIZE + Utils.SIZEOF_BYTE);
    long arena = keyArena.get();
    int kSize = KEY_SIZE + keySize + Utils.SIZEOF_BYTE;
    UnsafeAccess.putByte(arena, (byte)DataType.ZSET.ordinal());
    UnsafeAccess.putInt(arena + Utils.SIZEOF_BYTE, keySize);
    UnsafeAccess.copy(keyPtr, arena + KEY_SIZE + Utils.SIZEOF_BYTE, keySize);
    return kSize;
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @return number of elements in a sorted set
   */
  public static long ZCARD(BigSortedMap map, String key) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long result = ZCARD(map, keyPtr, keySize);
    UnsafeAccess.free(keyPtr);
    return result;
  }
  
  /**
   * Returns the sorted set cardinality (number of elements) of the sorted set stored at key.
   * Return value
   *  Integer reply: the cardinality (number of elements) of the sorted set, 
   *  or 0 if key does not exist.
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @return the cardinality (number of elements) of the sorted set, 
   *  or 0 if key does not exist.
   */
  public static long ZCARD(BigSortedMap map, long keyPtr, int keySize) {
    return ZCARD(map, keyPtr, keySize, true);
  }
  
  /**
   * Returns the sorted set cardinality (number of elements) of the sorted set stored at key.
   * Return value
   *  Integer reply: the cardinality (number of elements) of the sorted set, 
   *  or 0 if key does not exist.
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @param lock lock if true
   * @return the cardinality (number of elements) of the sorted set, 
   *  or 0 if key does not exist.
   */
//  public static long ZCARD(BigSortedMap map, long keyPtr, int keySize, boolean lock) {
//    Key k = getKey(keyPtr, keySize);
//    try {
//      if (lock) {
//        KeysLocker.readLock(k);
//      }
//      int kSize = buildKey(keyPtr, keySize);
//      long addr = map.get(keyArena.get(), kSize, valueArena.get(), valueArenaSize.get(), 0);
//      if (addr < 0) {
//        return 0;
//      } else {
//        return UnsafeAccess.toLong(valueArena.get());
//      }
//    } finally {
//      if (lock) {
//        KeysLocker.readUnlock(k);
//      }
//    }
//  }

  public static long ZCARD(BigSortedMap map, long keyPtr, int keySize, boolean lock) {
    Key k = getKey(keyPtr, keySize);
    try {
      if (lock) {
        KeysLocker.readLock(k);
      }
      checkKeyArena(CARD_MEMBER_SIZE);
      // Build CARDINALITY member
      long ptr  = keyArena.get();
      int size = CARD_MEMBER_SIZE;
      Utils.random16(keyPtr, keySize, ptr);
      
      long valueBuffer = valueArena.get();
      int bufferSize = valueArenaSize.get();
      // We need just 8 bytes
      int vsize = Hashes.HGET(map, keyPtr, keySize, ptr, size, valueBuffer, bufferSize);
      if (vsize == Utils.SIZEOF_LONG) {
        long card = UnsafeAccess.toLong(valueBuffer); 
        return card; 
      } else if (vsize > 0){
        //TODO - collision detected
        //PANIC
        System.err.println("ZCARD collision detected");
        Thread.dumpStack();
        System.exit(-1);
      }
      // else 
      return Sets.SCARD(map, keyPtr, keySize);
    } finally {
      if (lock) {
        KeysLocker.readUnlock(k);
      }
    }
  }

  
  /**
   * Update cardinality
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @param incr increment value
   * @return cardinality after increment
   */
//  private static long ZINCRCARD(BigSortedMap map, long keyPtr, int keySize, long incr) {
//      int kSize = buildKey(keyPtr, keySize);
//      //TODO: DEADLOCK?
//      long after = map.incrementLong(keyArena.get(), kSize, 0, incr);
//      if (after == 0) {
//        boolean res = DELETE(map, keyPtr, keySize, false);
//        assert(res = true);
//      }
//      return after;
//  }

  private static long ZSETCARD(BigSortedMap map, long keyPtr, int keySize, long card) {
    checkKeyArena(CARD_MEMBER_SIZE);
    // Build CARDINALITY member
    long ptr  = keyArena.get();
    int size = CARD_MEMBER_SIZE;
    Utils.random16(keyPtr, keySize, ptr);
    
    long valueBuffer = valueArena.get();
    UnsafeAccess.putLong(valueBuffer, card);
    int res = Hashes.HSET(map, keyPtr, keySize, ptr, size, valueBuffer, Utils.SIZEOF_LONG);
    assert(res == 1);
    return card;
  }
  
  
  /**
   * Adds all the specified members with the specified scores to the sorted set stored at key. 
   * It is possible to specify multiple score / member pairs. If a specified member is already 
   * a member of the sorted set, the score is updated and the element reinserted at the right 
   * position to ensure the correct ordering.
   * If key does not exist, a new sorted set with the specified members as sole members is created, 
   * like if the sorted set was empty. If the key exists but does not hold a sorted set, an error is returned.
   * The score values should be the string representation of a double precision floating point number.
   *  +inf and -inf values are valid values as well.
   *  
   *  Changed elements are new elements added and elements already existing for which the score was updated.
   *  So elements specified in the command line having the same score as they had in the past are not counted.
   *  
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @param scores member scores
   * @param memberPtrs member addresses
   * @param memberSizes member sizes
   * @param changed if true return new and changed member total.
   * @param options mutation options
   * 
   * @return The number of elements added to the sorted set, not including elements already existing 
   * for which the score was updated (this can be changed by specifying changed = true).
   */
  public static long ZADD_GENERIC(BigSortedMap map, long keyPtr, int keySize, double[] scores,
      long[] memberPtrs, int[] memberSizes, boolean changed /* CH */, MutationOptions options) {
    Key k = getKey(keyPtr, keySize);
    try {
      
      // Redis lock
      KeysLocker.writeLock(k);

      int toAdd = memberPtrs.length;
      int inserted = 0;
      int updated = 0;
      long count = ZCARD(map, keyPtr, keySize, false);
      RedisConf conf = RedisConf.getInstance();
      int maxCompactSize = conf.getMaxZSetCompactSize();
      boolean compactMode = count < maxCompactSize;      
      // Step one, find and remove existing members
      for (int i = 0; i < toAdd; i++) {
        Double prevScore = 
            removeIfExistsWithOptions(map, keyPtr, keySize, memberPtrs[i], memberSizes[i], compactMode, options);
        boolean existed = prevScore != null;

        if (existed && options == MutationOptions.NX || !existed && options == MutationOptions.XX) {
          continue;
        }
        int kSize = buildKeyForSet(keyPtr, keySize, memberPtrs[i], memberSizes[i], scores[i]);  
        
        // No Redis locks
        SetAdd add = setAdd.get();
        add.reset();
        add.setKeyAddress(keyArena.get());
        add.setKeySize(kSize);
        //add.setMutationOptions(options);
        map.execute(add); 
        if (!existed) {
          inserted++;
        } else if (changed && prevScore != scores[i]) {
          updated++;
        }
      }
      
      // Next do hash

      if (compactMode && (count + inserted) >= maxCompactSize ) {
        // convert to normal representation
        convertToNormalMode(map, keyPtr, keySize, count + inserted);
      } else if (count + inserted > maxCompactSize) {
        // Add to Hash
        addToHash(map, keyPtr, keySize, scores, memberPtrs, memberSizes, options);
      } 
      // Next update zset count
      //FIXME: DEADLOCK?
      if (inserted > 0 && (count + inserted >= maxCompactSize)) {
        //BSM lock
        ZSETCARD(map, keyPtr, keySize, count + inserted);
      }
      return inserted + updated;
    } finally {
      KeysLocker.writeUnlock(k);
    }
  }
  
  /**
   * Removes element only if exists and options == NONE, XX
   * @param map sorted map storage
   * @param keyPtr set key address
   * @param keySize set key size
   * @param memberPtr member name address
   * @param memberSize member size
   * @param compactMode compact mode
   * @param options mutation options
   * @return score of the element if it exists or null
   */
  private static Double removeIfExistsWithOptions(BigSortedMap map, long keyPtr, int keySize,
      long memberPtr, int memberSize, boolean compactMode, MutationOptions options) {
    
    Double value = null;
    if (compactMode) {
      return removeDirectlyFromSetWithOptions(map, keyPtr, keySize, memberPtr, memberSize, options);
    } else {
      checkValueArena(memberSize + Utils.SIZEOF_DOUBLE);
      long valueBuf = valueArena.get();
      int valueBufSize = valueArenaSize.get();
      // get key from hash
      // Do not lock again!!! No Redis locks
      // BSM READ LOCK
      long size = Hashes.HGET(map, keyPtr, keySize, memberPtr, memberSize, valueBuf, valueBufSize, false);
      // valueArena size is > 8 bytes - result size
      if (size < 0) {
        // not found
        return null;
      } else {
        value = Utils.lexToDouble(valueBuf);
      }
      if (options != MutationOptions.NX) {
        // Copy member field
        UnsafeAccess.copy(memberPtr, valueBuf + Utils.SIZEOF_DOUBLE, memberSize);
        // Delete - do not lock again!!!
        int res = Sets.SREM(map, keyPtr, keySize, valueBuf, Utils.SIZEOF_DOUBLE + memberSize, false);
        assert (res == 1);
      }
    }
    return value;
  }
  /**
   * Removes element directly from Set with options
   * @param map sorted map storage
   * @param keyPtr set key address
   * @param keySize set key size
   * @param memberPtr member address
   * @param memberSize member size
   * @param options mutation options
   * @return score if exists or null
   */
  private static Double removeDirectlyFromSetWithOptions(BigSortedMap map, long keyPtr, int keySize,
      long memberPtr, int memberSize, MutationOptions options) {

    Double value = null;
    SetScanner scanner = Sets.getScanner(map, keyPtr, keySize, false);
    if (scanner == null) {
      return null;
    }
    try {
      long ptr = 0;
      int size = 0;
      while (scanner.hasNext()) {
        long mPtr = scanner.memberAddress();
        int mSize = scanner.memberSize();
        if (Utils.compareTo(mPtr + Utils.SIZEOF_DOUBLE, mSize - Utils.SIZEOF_DOUBLE, memberPtr,
          memberSize) == 0) {
          ptr = mPtr;
          size = mSize;
          scanner.close();
          scanner = null;
          break;
        }
        scanner.next();
      }
      if (ptr > 0) {
        value = Utils.lexToDouble(ptr);
        if (options != MutationOptions.NX) {
          checkValueArena(size);
          UnsafeAccess.copy(ptr, valueArena.get(), size);
          int res = Sets.SREM(map, keyPtr, keySize, valueArena.get(), size, false);
          assert (res == 1);
        }
      }
    } catch (IOException e) {
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
        }
      }
    }
    return value;
  }

  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @param members list of members
   * @param scores list of corresponding scores
   * @param changed if true return new and changed member total.
   * @return The number of elements added to the sorted set, not including elements already existing 
   * for which the score was updated (this can be changed by specifying changed = true).
   */
  public static long ZADD(BigSortedMap map, String key, String[] members, double[] scores, boolean changed) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long[] memPtrs = new long[members.length];
    int[] memSizes = new int[members.length];
    for (int i=0; i < memPtrs.length; i++) {
      memPtrs[i] = UnsafeAccess.allocAndCopy(members[i], 0, members[i].length());
      memSizes[i] = members[i].length();
    }
    long result = ZADD(map, keyPtr, keySize, scores, memPtrs, memSizes, changed);
    UnsafeAccess.free(keyPtr);
    Arrays.stream(memPtrs).forEach(x -> UnsafeAccess.free(x));
    return result;
    
  }
  
  /**
   * ZADD command
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @param scores member scores
   * @param memberPtrs member addresses
   * @param memberSizes member sizes
   * @param changed if true return new and changed member total.
   * @param options mutation options
   * 
   * @return The number of elements added to the sorted set, not including elements already existing 
   * for which the score was updated (this can be changed by specifying changed = true).
   */
  public static long ZADD(BigSortedMap map, long keyPtr, int keySize, double[] scores,
      long[] memberPtrs, int[] memberSizes, boolean changed /* CH */)
  {
    return ZADD_GENERIC(map, keyPtr, keySize, scores, memberPtrs, memberSizes, 
      changed, MutationOptions.NONE);
  }
  
  
  /**
   * Creates Hash from Set. Normal mode has two data structures: Hash and Set
   * Compact mode has only Set
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   */
  private static void convertToNormalMode(BigSortedMap map, long keyPtr, int keySize, long cardinality) {
    // TODO Auto-generated method stub
    //Key k = getKey(keyPtr, keySize);
    SetScanner scanner = null;
    try {
      //KeysLocker.writeLock(k);
      scanner = Sets.getScanner(map, keyPtr, keySize, false);
      if (scanner == null) {
        // TODO - report
        return;
      }      
      
      long[] ptrs = new long[(int)cardinality];
      int[] sizes = new int[(int)cardinality];
      double[] scores = new double[(int)cardinality];
      int count = 0;
      
      try {
        while(scanner.hasNext()) {
          long ePtr = scanner.memberAddress();
          int eSize = scanner.memberSize();    
          double score = Utils.lexToDouble(ePtr);
          ePtr += Utils.SIZEOF_DOUBLE;
          eSize -= Utils.SIZEOF_DOUBLE;
          long ptr = UnsafeAccess.allocAndCopy(ePtr, eSize);
          ptrs[count] = ptr;
          sizes[count] = eSize;
          scores[count] = score;
          count++;
          scanner.next();
        }
      } catch (IOException e) {
        // TODO Auto-generated catch block
      } finally {
        if (scanner != null) {
          try {
            scanner.close();
          } catch (IOException e) {
          }
        }
      }
      addToHash(map, keyPtr, keySize, scores, ptrs, sizes, MutationOptions.NONE);
      // Free memory
      for (long ptr: ptrs) {
        UnsafeAccess.free(ptr);
      }
    } finally {
      //KeysLocker.writeUnlock(k);
    }
  }
  
  /**
   * Deletes Hash from ZSet. Normal mode has two data structures: Hash and Set
   * Compact mode has only Set
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   */
  static void convertToCompactMode(BigSortedMap map, long keyPtr, int keySize) {
    //Key k = getKey(keyPtr, keySize);
    try {
      //KeysLocker.writeLock(k);
      Hashes.DELETE(map, keyPtr, keySize, false);
    } finally {
      //KeysLocker.writeUnlock(k);
    }
  }
  /**
   * Adds members and scores to a Hash data structure
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @param scores array of scores
   * @param memberPtrs array of pointers
   * @param memberSizes array of sizes
   * @param options mutation options
   * @return number of added elements
   */
  private static long addToHash(BigSortedMap map, long keyPtr, int keySize, double[] scores,
    long[] memberPtrs, int[] memberSizes, MutationOptions options) {
    
    int toAdd = memberPtrs.length;
    long count = 0;
    for (int i = 0; i < toAdd; i++) {
      long fieldPtr = memberPtrs[i];
      int fieldSize = memberSizes[i];
      long valuePtr = valueArena.get();
      int valueSize = Utils.SIZEOF_DOUBLE;

      Utils.doubleToLex(valuePtr, scores[i]);

      int kSize = buildKeyForHash(keyPtr, keySize, fieldPtr, fieldSize);
      HashSet set = hashSet.get();
      set.reset();
      set.setKeyAddress(keyArena.get());
      set.setKeySize(kSize);
      set.setFieldValue(valuePtr, valueSize);
      set.setOptions(options);
      // version?
      if (map.execute(set)) {
        count ++;
      }
    }
    return count;
  }
  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @param members list of members
   * @param scores list of corresponding scores
   * @param changed if true return new and changed member total.
   * @return The number of elements added to the sorted set, not including elements already existing 
   * for which the score was updated (this can be changed by specifying changed = true).
   */
  public static long ZADDNX(BigSortedMap map, String key, String[] members, double[] scores, boolean changed) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long[] memPtrs = new long[members.length];
    int[] memSizes = new int[members.length];
    for (int i=0; i < memPtrs.length; i++) {
      memPtrs[i] = UnsafeAccess.allocAndCopy(members[i], 0, members[i].length());
      memSizes[i] = members[i].length();
    }
    long result = ZADDNX(map, keyPtr, keySize, scores, memPtrs, memSizes, changed);
    UnsafeAccess.free(keyPtr);
    Arrays.stream(memPtrs).forEach(x -> UnsafeAccess.free(x));
    return result;
    
  }
  /**
   * The same as ZADD with the following difference:
   *  
   * NX:  Don't update already existing elements. Always add new elements.
   *   
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @param scores member scores
   * @param memberPtrs member addresses
   * @param memberSizes member sizes
   * @param changed if true return new and changed member total.
   * 
   * @return The number of elements added to the sorted set, not including elements already existing 
   * for which the score was updated (this can be changed by specifying changed = true).
   */
  public static long ZADDNX (BigSortedMap map, long keyPtr, int keySize, double[] scores, 
      long[] memberPtrs, int[] memberSizes, boolean changed /*CH*/)
  {
    return ZADD_GENERIC(map, keyPtr, keySize, scores, memberPtrs, memberSizes, changed, MutationOptions.NX);
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @param members list of members
   * @param scores list of corresponding scores
   * @param changed if true return new and changed member total.
   * @return The number of elements added to the sorted set, not including elements already existing 
   * for which the score was updated (this can be changed by specifying changed = true).
   */
  public static long ZADDXX(BigSortedMap map, String key, String[] members, double[] scores, boolean changed) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long[] memPtrs = new long[members.length];
    int[] memSizes = new int[members.length];
    for (int i=0; i < memPtrs.length; i++) {
      memPtrs[i] = UnsafeAccess.allocAndCopy(members[i], 0, members[i].length());
      memSizes[i] = members[i].length();
    }
    long result = ZADDXX(map, keyPtr, keySize, scores, memPtrs, memSizes, changed);
    UnsafeAccess.free(keyPtr);
    Arrays.stream(memPtrs).forEach(x -> UnsafeAccess.free(x));
    return result;
    
  }
  /**
   * The same as ZADD with the following difference:
   *  
   * XX:  Only update elements that already exist. Never add elements.
   *   
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @param scores member scores
   * @param memberPtrs member addresses
   * @param memberSizes member sizes
   * @param changed if true return new and changed member total.
   * 
   * @return The number of elements added to the sorted set, not including elements already existing 
   * for which the score was updated (this can be changed by specifying changed = true).
   */
  public static long ZADDXX (BigSortedMap map, long keyPtr, int keySize, double[] scores, 
      long[] memberPtrs, int[] memberSizes, boolean changed /*CH*/)
  {
    return ZADD_GENERIC(map, keyPtr, keySize, scores, memberPtrs, memberSizes, changed, MutationOptions.XX);
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @param min minimum score value
   * @param minInclusive is minimum inclusive
   * @param max maximum score value
   * @param maxInclusive is maximum inclusive
   * @return number of elements
   */
  public static long ZCOUNT(BigSortedMap map, String key, double min, boolean minInclusive,
      double max, boolean maxInclusive) {
    
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long result = ZCOUNT(map, keyPtr, keySize, min, minInclusive, max, maxInclusive);
    UnsafeAccess.free(keyPtr);
    return result;
  }
  
  /**
   * Returns the number of elements in the sorted set at key with a score between min and max.
   * The min and max arguments have the same semantic as described for ZRANGEBYSCORE.
   * Note: the command has a complexity of just O(log(N)) because it uses elements ranks (see ZRANK) to 
   * get an idea of the range. Because of this there is no need to do a work proportional to the size
   *  of the range.
   * -inf = -Double.MAX_VALUE
   * +inf =  Double.MAX_VALUE; 
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @param min minimum score (inclusive?)
   * @param max maximum score (inclusive?)
   * @return the number of elements in the specified score range.
   */
  public static long ZCOUNT (BigSortedMap map, long keyPtr, int keySize, double min, 
      boolean minInclusive, double max, boolean maxInclusive) {

    Key k = getKey(keyPtr, keySize);
    long count = 0;
    long startPtr = 0, stopPtr = 0;
    try {
      KeysLocker.readLock(k);
      
      startPtr = UnsafeAccess.malloc(Utils.SIZEOF_DOUBLE);
      Utils.doubleToLex(startPtr, min);
      int startSize = Utils.SIZEOF_DOUBLE;
      
      stopPtr =UnsafeAccess.malloc(Utils.SIZEOF_DOUBLE);
      Utils.doubleToLex(stopPtr, max);
      int stopSize = Utils.SIZEOF_DOUBLE;
      if (maxInclusive && max < Double.MAX_VALUE) {
        stopPtr = Utils.prefixKeyEndNoAlloc(stopPtr, stopSize);
      }
      if (!minInclusive && min > -Double.MAX_VALUE) {
        startPtr = Utils.prefixKeyEndNoAlloc(startPtr, startSize);
      }      
      // TODO: can be optimized for speed: see Sets SCOUNT
      SetScanner scanner = Sets.getScanner(map, keyPtr, keySize, startPtr, startSize, 
        stopPtr, stopSize, false);
      if (scanner == null) {
        return 0;
      }
      try {
        while(scanner.hasNext()) {
          count++;
          scanner.next();
        }
      } catch (IOException e) {
        // Should not be here
      } finally {
        try {
          scanner.close();
        } catch (IOException e) {
        }
      }
      return count;
    }finally {
      if (startPtr != 0) UnsafeAccess.free(startPtr);
      if (stopPtr != 0) UnsafeAccess.free(stopPtr);
      KeysLocker.readUnlock(k);
    }
  }
  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @param incr score increment
   * @param member member
   * @return new score
   */
  public static double ZINCRBY(BigSortedMap map, String key, double incr, String member) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long memPtr = UnsafeAccess.allocAndCopy(member, 0, member.length());
    int memSize = member.length();
    double result = ZINCRBY(map, keyPtr, keySize, incr, memPtr, memSize);
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(memPtr);
    return result;
  }
  
  /**
   * Increments the score of member in the sorted set stored at key by increment. If member does 
   * not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0). 
   * If key does not exist, a new sorted set with the specified member as its sole member is created.
   * An error is returned when key exists but does not hold a sorted set.
   * The score value should be the string representation of a numeric value, and accepts double precision 
   * floating point numbers. It is possible to provide a negative value to decrement the score.
   * Return value
   * Bulk string reply: the new score of member (a double precision floating point number), 
   * represented as string.
   * @param map
   * @param keyPtr
   * @param keySize
   * @param incr
   * @param memberPtr
   * @param memberSize
   * @return the new score of member (double)
   */
  public static double ZINCRBY (BigSortedMap map, long keyPtr, int keySize, double incr, 
      long memberPtr, int memberSize) {
    Key key = getKey(keyPtr, keySize);
    SetScanner scanner = null;
    double score = incr;
    boolean exists = false;
    try {
      KeysLocker.writeLock(key);
      long cardinality = ZCARD(map, keyPtr, keySize, false);
      int maxCompactSize = RedisConf.getInstance().getMaxZSetCompactSize();
      boolean normalMode = maxCompactSize <= cardinality;
      if (normalMode) {
        long buffer = valueArena.get();
        int bufferSize = valueArenaSize.get();
        int result = Hashes.HGET(map, keyPtr, keySize, memberPtr, memberSize, buffer, bufferSize, false);
        if (result > 0) {
          // member exists in ZSET   
          //Remove from SET first - create full member in a buffer
          UnsafeAccess.copy(memberPtr, buffer + Utils.SIZEOF_DOUBLE, memberSize);
          int res = Sets.SREM(map, keyPtr, keySize, buffer, memberSize + Utils.SIZEOF_DOUBLE, false);
          assert(res == 1);
          score += Utils.lexToDouble(buffer);
          exists = true;
        }
        Utils.doubleToLex(buffer, score);
        // set new score for member
        Hashes.HSET(map, keyPtr, keySize, memberPtr, memberSize, buffer, Utils.SIZEOF_DOUBLE, false);
        checkValueArena(memberSize + Utils.SIZEOF_DOUBLE);
        buffer = valueArena.get();
        UnsafeAccess.copy(memberPtr, buffer + Utils.SIZEOF_DOUBLE, memberSize);
        int res = Sets.SADD(map, keyPtr, keySize, buffer, memberSize + Utils.SIZEOF_DOUBLE, false);
        assert(res == 1);

      } else {
        // Search in set for member
        scanner = Sets.getScanner(map, keyPtr, keySize, false);
        if (scanner != null) {
          while(scanner.hasNext()) {
            long ptr = scanner.memberAddress();
            int size = scanner.memberSize();
            // First 8 bytes is the score, so we have to skip it
            if (Utils.compareTo(ptr + Utils.SIZEOF_DOUBLE, size - Utils.SIZEOF_DOUBLE, 
              memberPtr, memberSize) == 0) {
              exists = true;
              score += Utils.lexToDouble(ptr);
              scanner.close();
              scanner = null;
              // Delete it
              int res = Sets.SREM(map, keyPtr, keySize, ptr, size, false);
              assert (res == 1);
              break;
            }
            scanner.next();
          }
          // Close scanner asap
          if (scanner != null) {
            scanner.close();
            scanner = null;
          }
        }

        // Now update Set with a new score
        checkValueArena(memberSize + Utils.SIZEOF_DOUBLE);
        long buffer = valueArena.get();
        Utils.doubleToLex(buffer, score);
        UnsafeAccess.copy(memberPtr, buffer + Utils.SIZEOF_DOUBLE, memberSize);
        int res = Sets.SADD(map, keyPtr, keySize, buffer, memberSize + Utils.SIZEOF_DOUBLE, false);
        assert (res == 1);
      }

      if (!exists) {
        //ZINCRCARD(map, keyPtr, keySize, 1);
        if (cardinality + 1 >= maxCompactSize) {
          ZSETCARD(map, keyPtr, keySize, cardinality + 1);
        }
        if (!normalMode && (cardinality + 1) == maxCompactSize) {
          convertToNormalMode(map, keyPtr, keySize, cardinality + 1);
        }
      }
    } catch (IOException e) {
      // TODO Get rid of this annoying exception
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
        }
      }
      KeysLocker.writeUnlock(key);
    }
    return score;

  }
  
  /**
   * Available since 2.0.0.
   * Time complexity: O(N*K)+O(M*log(M)) worst case with N being the smallest input sorted set,
   * K being the number of input sorted sets and M being the number of elements in the 
   * resulting sorted set.
   * Computes the intersection of numkeys sorted sets given by the specified keys, and stores 
   * the result in destination. It is mandatory to provide the number of input keys (numkeys) 
   * before passing the input keys and the other (optional) arguments.
   * By default, the resulting score of an element is the sum of its scores in the sorted sets 
   * where it exists. Because intersection requires an element to be a member of every given 
   * sorted set, this results in the score of every element in the resulting sorted set to be 
   * equal to the number of input sorted sets.
   * For a description of the WEIGHTS and AGGREGATE options, see ZUNIONSTORE.
   * If destination already exists, it is overwritten.
   * Return value
   * Integer reply: the number of elements in the resulting sorted set at destination.
   * @param map sorted map storage
   * @param dstKeyPtr destination key address
   * @param dstKeySize destination key size
   * @param keys sorted sets keys
   * @param keySizes sorted sets key sizes
   * @param weights corresponding weights
   * @param aggregate aggregate function
   * @return number of members in a destination set
   */
  public static long ZINTERSTORE(BigSortedMap map, long dstKeyPtr, int dstKeySize, long[] keys, 
      int[] keySizes, double[] weights, Aggregate aggregate) {
    //TODO
    return 0;
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @param startMember start member
   * @param startInclusive is start inclusive
   * @param stopMember stop member 
   * @param stopInclusive is stop inclusive
   * @return number of elements
   */
  public static long ZLEXCOUNT(BigSortedMap map, String key, String startMember,
      boolean startInclusive, String stopMember, boolean stopInclusive) {
    
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long startPtr = startMember!= null? UnsafeAccess.allocAndCopy(startMember, 0, startMember.length()):0;
    int startSize = startMember != null? startMember.length(): 0;
    long endPtr = stopMember!= null? UnsafeAccess.allocAndCopy(stopMember, 0, stopMember.length()):0;
    int endSize = stopMember != null? stopMember.length(): 0;
    long result = ZLEXCOUNT(map, keyPtr, keySize, startPtr, startSize, startInclusive, endPtr, endSize, stopInclusive);
    
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(startPtr);
    UnsafeAccess.free(endPtr);
    return result;
    
  }
  /**
   * 
   * Available since 2.8.9.
   * Time complexity: O(log(N)) with N being the number of elements in the sorted set.
   * When all the elements in a sorted set are inserted with the same score, in order 
   * to force lexicographical ordering, this command returns the number of elements in 
   * the sorted set at key with a value between min and max.
   * The min and max arguments have the same meaning as described for ZRANGEBYLEX.
   * Note: the command has a complexity of just O(log(N)) because it uses elements ranks 
   * (see ZRANK) to get an idea of the range. Because of this there is no need to do a work 
   * proportional to the size of the range.
   * Return value
   * Integer reply: the number of elements in the specified  range.

   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @param startPtr start interval address (0 - negative infinity)
   * @param startSize start interval size
   * @param startInclusive is interval start inclusive?
   * @param endPtr interval end address (0 - positive infinity string)
   * @param endSize interval end size
   * @param endInclusive is interval end inclusive
   * @return number of members in a specified interval
   */
  public static long ZLEXCOUNT(BigSortedMap map, long keyPtr, int keySize, long startPtr,
      int startSize, boolean startInclusive, long endPtr, int endSize, boolean endInclusive) {
    
    //FIXME - prefixKeyEndNoAlloc is not safe
    // let us say we have key = 'faf0'
    // prefixKeyEndNoAlloc ('faf0') = 'faf1'
    // 'faf00' is now between 'faf0' and 'faf1'
    // FIXED!!! See Utils.prefixKeyEndCorrect
    // The problem with above API - who is responsible for
    // releasing start and end?
    boolean freeStart = false;
    boolean freeEnd = false;
    if (endInclusive && endPtr > 0) {
      endPtr = Utils.prefixKeyEndCorrect(endPtr, endSize);
      endSize += 1;
      freeEnd = true;
    }
    if (!startInclusive && startPtr > 0) {
      startPtr = Utils.prefixKeyEndCorrect(startPtr, startSize);
      startSize += 1;
      freeStart = true;
    }
    
    Key key = getKey(keyPtr, keySize);
    long count = 0;
    HashScanner hashScanner = null;
    SetScanner setScanner = null;
    
    long cardPtr = getCardinalityMember(keyPtr, keySize);
    
    try {
      KeysLocker.readLock(key);
      hashScanner =
          Hashes.getScanner(map, keyPtr, keySize, startPtr, startSize, endPtr, endSize, false);
      if (hashScanner != null) {
        while (hashScanner.hasNext()) {
          long fPtr = hashScanner.fieldAddress();
          int fSize = hashScanner.fieldSize();
          if (!isCardinalityMember(fPtr, fSize, cardPtr)) {
            count++;
          }
          hashScanner.next();
        }
      } else {
        // Run through the Set
        setScanner = Sets.getScanner(map, keyPtr, keySize, false);
        while (setScanner.hasNext()) {
          long mPtr = setScanner.memberAddress();
          int mSize = setScanner.memberSize();
          // Discard score part
          mPtr += Utils.SIZEOF_DOUBLE;
          // Discard score part
          mSize -= Utils.SIZEOF_DOUBLE;
          int res1 = startPtr == 0? 1: Utils.compareTo(mPtr, mSize, startPtr, startSize);
          int res2 = endPtr == 0? -1: Utils.compareTo(mPtr, mSize, endPtr, endSize);
          if (res2 < 0 && res1 >= 0) {
            count++;
          } else if (res2 >= 0) {
            break;
          }
          setScanner.next();
        }
      }
    } catch (IOException e) {
    } finally {
      try {
        if (hashScanner != null) {
          hashScanner.close();
        }
        if (setScanner != null) {
          setScanner.close();
        }
      } catch (IOException e) {
      }
      if (cardPtr > 0) {
        UnsafeAccess.free(cardPtr);
      }
      if (freeStart) {
        UnsafeAccess.free(startPtr);
      }
      if (freeEnd) {
        UnsafeAccess.free(endPtr);
      }
      KeysLocker.readUnlock(key);
    }
    return count;
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key
   * @param count
   * @param bufSize
   * @return
   */
  public static List<Pair<String>> ZPOPMAX (BigSortedMap map, String key, int count, int bufSize) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long buffer = UnsafeAccess.malloc(bufSize);
    List<Pair<String>> list = new ArrayList<Pair<String>>();
    long result = ZPOPMAX(map, keyPtr, keySize, count, buffer, bufSize);
    if (result > bufSize || result <= 0) {
      return list;
    }
    int total = UnsafeAccess.toInt(buffer);
    long ptr = buffer + Utils.SIZEOF_INT;
    
    for (int i=0; i < total; i++) {
      int mSize = Utils.readUVInt(ptr);
      int mSizeSize = Utils.sizeUVInt(mSize);
      double score = Utils.lexToDouble(ptr + mSizeSize);
      String sscore = Double.toString(score);
      String member = Utils.toString(ptr + mSizeSize + Utils.SIZEOF_DOUBLE, mSize - Utils.SIZEOF_DOUBLE);
      list.add(new Pair<String>(member, sscore));
      ptr += mSize + mSizeSize;
    }
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(buffer);
    return list;
  }
  
  /**
   * 
   * Available since 5.0.0.
   * Time complexity: O(log(N)*M) with N being the number of elements in the sorted set, 
   * and M being the number of elements popped.
   * Removes and returns up to count members with the highest scores in the sorted set 
   * stored at key.
   * When left unspecified, the default value for count is 1. Specifying a count value that 
   * is higher than the sorted set's cardinality will not produce an error. 
   * When returning multiple elements, the one with the highest score will be the first, 
   * followed by the elements with lower scores.
   * Return value
   * Array reply: list of popped elements and scores.
   * 
   * Serialized format:
   * [LIST_SIZE] - 4 bytes
   * PAIR +
   *  PAIR : [VARINT][MEMBER][SCORE]
   * VARINT - size of a member in bytes
   * 
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @param count count of elements to return
   * @param buffer buffer for return pairs {member, score}
   * @param bufferSize buffer size
   * @return total serialized size of a response, if it is greater than 
   *         bufferSize - repeat call with appropriately sized buffer 
   */
  public static long ZPOPMAX(BigSortedMap map, long keyPtr, int keySize, int count, long buffer, int bufferSize)
  {
    
    if (count <= 0) return 0L;
    
    Key key = getKey(keyPtr, keySize);
    long ptr = buffer + Utils.SIZEOF_INT;
    SetScanner scanner = null;
    int maxCompactSize = RedisConf.getInstance().getMaxZSetCompactSize();
    try {
      KeysLocker.writeLock(key);
      // Make sure first 4 bytes does not contain garbage
      UnsafeAccess.putInt(buffer, 0);
      scanner = Sets.getScanner(map, keyPtr, keySize, false, true);
      if (scanner == null) {
        return 0;
      }
 
      int c = 0;

      do {
        long mPtr = scanner.memberAddress();
        int mSize = scanner.memberSize();
        int mSizeSize = Utils.sizeUVInt(mSize);
        if (ptr + mSize + mSizeSize <= buffer + bufferSize) {
          // Write to buffer
          Utils.writeUVInt(ptr, mSize);
          UnsafeAccess.copy(mPtr, ptr + mSizeSize, mSize);
          c++;
          // Write number of elements
          UnsafeAccess.putInt(buffer, c);
        }
        ptr += mSize + mSizeSize;
        if (c == count) {
          break;
        }
      } while(scanner.previous());

      // Close scanner
      scanner.close();
      scanner = null;
      if (ptr > buffer + bufferSize) {
        return ptr - buffer;
      }
      // Now delete them
      long cardinality = ZCARD(map, keyPtr, keySize, false);
      boolean normalMode = maxCompactSize <= cardinality;
      int n = 0;
      ptr = buffer + Utils.SIZEOF_INT;
      while (n++ < c) {
        int mSize = Utils.readUVInt(ptr);
        int mSizeSize = Utils.sizeUVInt(mSize);
        int res = Sets.SREM(map, keyPtr, keySize, ptr + mSizeSize, mSize, false);
        assert(res == 1);
        if (normalMode) {
          // For Hash storage we do not use first 8 bytes (score)
          res = Hashes.HDEL(map, keyPtr, keySize, ptr + mSizeSize + Utils.SIZEOF_DOUBLE,
            mSize - Utils.SIZEOF_DOUBLE, false);
          assert(res == 1);
        }
        ptr += mSize + mSizeSize;
      }
      //Update count
      if (c > 0 && (cardinality - c >= maxCompactSize)) {
        //ZINCRCARD(map, keyPtr, keySize, -c);
        ZSETCARD(map, keyPtr, keySize, cardinality - c);
      }
      cardinality -= c;
      if (cardinality > 0 && cardinality < maxCompactSize) {
        convertToCompactMode(map, keyPtr, keySize);
      } else if (cardinality == 0) {
        DELETE(map, keyPtr, keySize, false);
      }
    } catch (IOException e) {
      // Ignore this - should never be here
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
        }
      }
      KeysLocker.writeUnlock(key);
    }
    return ptr - buffer;
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key
   * @param count
   * @param bufSize
   * @return
   */
  public static List<Pair<String>> ZPOPMIN (BigSortedMap map, String key, int count, int bufSize) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long buffer = UnsafeAccess.malloc(bufSize);
    List<Pair<String>> list = new ArrayList<Pair<String>>();
    long result = ZPOPMIN(map, keyPtr, keySize, count, buffer, bufSize);
    if (result > bufSize || result <= 0) {
      return list;
    }
    int total = UnsafeAccess.toInt(buffer);
    long ptr = buffer + Utils.SIZEOF_INT;
    
    for (int i = 0; i < total; i++) {
      int mSize = Utils.readUVInt(ptr);
      int mSizeSize = Utils.sizeUVInt(mSize);
      double score = Utils.lexToDouble(ptr + mSizeSize);
      String sscore = Double.toString(score);
      String member = Utils.toString(ptr + mSizeSize + Utils.SIZEOF_DOUBLE, mSize - Utils.SIZEOF_DOUBLE);
      list.add(new Pair<String>(member, sscore));
      ptr += mSize + mSizeSize;
    }
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(buffer);
    return list;
  }
  /**
   * Available since 5.0.0.
   * Time complexity: O(log(N)*M) with N being the number of elements in the sorted set,
   *  and M being the number of elements popped.
   * Removes and returns up to count members with the lowest scores in the sorted set stored at key.
   * When left unspecified, the default value for count is 1. Specifying a count value that is 
   * higher than the sorted set's cardinality will not produce an error. When returning multiple 
   * elements, the one with the lowest score will be the first, followed by the elements 
   * with greater scores.
   * Return value
   * Array reply: list of popped elements and scores.
   * 
   * Serialized format:
   * [LIST_SIZE] - 4 bytes
   * PAIR +
   *  PAIR : [VARINT][MEMBER][SCORE]
   * VARINT - size of a score + member in bytes (score is always 8)
   * score is in lexicographical representation
   * 
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @param count count of elements to return
   * @param buffer buffer for return pairs {member, score}
   * @param bufferSize buffer size
   * @return total serialized size of a response, if it is greater than 
   *         bufferSize - repeat call with appropriately sized buffer, 0 - means empty set or does not exists 
   */
  public static long ZPOPMIN(BigSortedMap map, final long keyPtr, final int keySize,
      final int count, final long buffer, final int bufferSize) {
    
    if (count <=0 ) return 0L;
    
    Key key = getKey(keyPtr, keySize);
    // Make sure first 4 bytes does not contain garbage
    UnsafeAccess.putInt(buffer, 0);
    long ptr = buffer + Utils.SIZEOF_INT;
    int maxCompactSize = RedisConf.getInstance().getMaxZSetCompactSize();
    SetScanner scanner = null;
    try {
      KeysLocker.writeLock(key);
      // Direct scanner
      scanner = Sets.getScanner(map, keyPtr, keySize, false);
      if (scanner == null) {
        return 0;
      }
      int c = 0;
 
      while(scanner.hasNext()) {
        long mPtr = scanner.memberAddress();
        int mSize = scanner.memberSize();
        int mSizeSize = Utils.sizeUVInt(mSize);
        if (ptr + mSize + mSizeSize <= buffer + bufferSize) {
          // Write to buffer
          Utils.writeUVInt(ptr, mSize);
          UnsafeAccess.copy(mPtr, ptr + mSizeSize, mSize);
          // Write number of elements
          c++;
          UnsafeAccess.putInt(buffer, c);
        }
        ptr += mSize + mSizeSize;
        if (c == count) {
          break;
        }
        scanner.next();
      };
      // Close scanner
      scanner.close();
      scanner = null;
      if (ptr > buffer + bufferSize) {
        return ptr - buffer;
      }
      // Now delete them
      long cardinality = ZCARD(map, keyPtr, keySize, false);
      boolean normalMode = maxCompactSize <= cardinality;
      int n = 0;
      ptr = buffer + Utils.SIZEOF_INT;
      while (n++ < c) {
        int mSize = Utils.readUVInt(ptr);
        int mSizeSize = Utils.sizeUVInt(mSize);
        int res = Sets.SREM(map, keyPtr, keySize, ptr + mSizeSize, mSize, false);
        assert(res == 1);
        if (normalMode) {
          // For Hash storage we do not use first 8 bytes (score)
          res = Hashes.HDEL(map, keyPtr, keySize, ptr + mSizeSize + Utils.SIZEOF_DOUBLE,
            mSize - Utils.SIZEOF_DOUBLE);
          assert(res == 1);
        }
        ptr += mSize + mSizeSize;
      }
      if (c > 0 && (cardinality - c >= maxCompactSize)) {
        //ZINCRCARD(map, keyPtr, keySize, -c);
        ZSETCARD(map, keyPtr, keySize, cardinality - c);
      }
      cardinality -= c;
      // Check if card = 0
      if (cardinality > 0 && cardinality < maxCompactSize) {
        convertToCompactMode(map, keyPtr, keySize);
      } else if (cardinality == 0) {
        DELETE(map, keyPtr, keySize, false);
      }
    } catch (IOException e) {
      // Ignore this - should never be here
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
        }
      }
      KeysLocker.writeUnlock(key);
    }
    return ptr - buffer;
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @param start start offset inclusive
   * @param end end offset inclusive
   * @param withScores with scores?
   * @param bufSize buffer size
   * @return list of pairs {member, member} or {member, score}
   */
  public static List<Pair<String>> ZRANGE(BigSortedMap map, String key, long start, long end, 
      boolean withScores, int bufSize){
    
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long buffer = UnsafeAccess.malloc(bufSize);
    long result = ZRANGE(map, keyPtr, keySize, start, end, withScores, buffer, bufSize);
    List<Pair<String>> list = new ArrayList<Pair<String>>();
    if (result > bufSize || result <= 0) {
      return list;
    }
    
    int total = UnsafeAccess.toInt(buffer);
    long ptr = buffer + Utils.SIZEOF_INT;
    for (int i = 0; i < total; i++) {
      int mSize = Utils.readUVInt(ptr);
      int mSizeSize = Utils.sizeUVInt(mSize);
      String member = null;
      String sscore = null;
      if (withScores) {
        double score = Utils.lexToDouble(ptr + mSizeSize);
        sscore = Double.toString(score);
        member = Utils.toString(ptr + mSizeSize + Utils.SIZEOF_DOUBLE, mSize - Utils.SIZEOF_DOUBLE);
      } else {
        member =  Utils.toString(ptr + mSizeSize, mSize);
        sscore = member;
      }
      list.add(new Pair<String>(member, sscore));
      ptr += mSize + mSizeSize;
    }
    
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(buffer);
    return list;
  }
  
  /**
   * Available since 1.2.0.
   * Time complexity: O(log(N)+M) with N being the number of elements in the sorted set 
   * and M the number of elements returned.
   * Returns the specified range of elements in the sorted set stored at key. 
   * The elements are considered to be ordered from the lowest to the highest score. 
   * Lexicographical order is used for elements with equal score.
   * See ZREVRANGE when you need the elements ordered from highest to lowest score 
   * (and descending lexicographical order for elements with equal score).
   * Both start and stop are zero-based indexes, where 0 is the first element, 1 is the next 
   * element and so on. They can also be negative numbers indicating offsets from the end of 
   * the sorted set, with -1 being the last element of the sorted set, -2 the penultimate element and so on.
   * start and stop are inclusive ranges, so for example ZRANGE myzset 0 1 will return both the first and 
   * the second element of the sorted set.
   * Out of range indexes will not produce an error. If start is larger than the largest index in the sorted set, 
   * or start > stop, an empty list is returned. If stop is larger than the end of the sorted set Redis will treat
   * it like it is the last element of the sorted set.
   * It is possible to pass the WITHSCORES option in order to return the scores of the elements together with 
   * the elements. The returned list will contain value1,score1,...,valueN,scoreN instead of value1,...,valueN.
   *  Client libraries are free to return a more appropriate data type (suggestion: an array with (value, score) 
   *  arrays/tuples).
   * Return value
   * Array reply: list of elements in the specified range (optionally with their scores, 
   * in case the WITHSCORES option is given).
   * 
   * Serialized format:
   * [LIST_SIZE] - 4 bytes
   * PAIR +
   *  PAIR : [VARINT][MEMBER][SCORE]
   * VARINT - size of a member in bytes
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @param start inclusive
   * @param end inclusive
   * @param withScores with scores?
   * @param long buffer buffer address
   * @param int bufferSize buffer size
   * @return total serialized size of a response, if it is greater than 
   *         bufferSize - repeat call with appropriately sized buffer 
   */
  public static long ZRANGE (BigSortedMap map, long keyPtr, int keySize, long start, long end, 
      boolean withScores, long buffer, int bufferSize)
  {
    // TODO: range is limited only to 32 signed int - make it long
    Key key = getKey(keyPtr, keySize);
    SetScanner scanner = null;
    long ptr = 0;
    try {
      KeysLocker.readLock(key);      
      long cardinality = ZCARD(map, keyPtr, keySize, false);
      if (cardinality == 0) {
        return 0;
      }
      if (start < 0) {
        start = start + cardinality;
      }
      if (end < 0) {
        end = end + cardinality;
      }
      if (start > end || start >= cardinality) {
        return 0;
      }
      if (end >= cardinality) {
        end = cardinality - 1;
      }
      scanner = Sets.getScanner(map, keyPtr, keySize, false);
      if (scanner == null) {
        return 0;
      }
      long counter = start;
      ptr = buffer + Utils.SIZEOF_INT;
      // Make sure first 4 bytes does not contain garbage
      UnsafeAccess.putInt(buffer, 0);
      // skip to start
      scanner.skipTo(start);
      while(scanner.hasNext()) {
        if (counter > end) break;
        if (counter >= start && counter <= end) {
          int mSize = withScores? scanner.memberSize(): scanner.memberSize() - Utils.SIZEOF_DOUBLE;
          int mSizeSize = Utils.sizeUVInt(mSize);
          int adv = mSize + mSizeSize;
          if (ptr + adv < buffer + bufferSize) {
            long mPtr = withScores? scanner.memberAddress(): scanner.memberAddress() + Utils.SIZEOF_DOUBLE;
            Utils.writeUVInt(ptr, mSize);
            UnsafeAccess.copy(mPtr, ptr + mSizeSize, mSize);
            UnsafeAccess.putInt(buffer, (int)(counter - start) + 1);
          }
          ptr += adv;
        }
        counter++;
        scanner.next();
      }
    } catch (IOException e) {
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
        }
      }
      KeysLocker.readUnlock(key);
    }
    return ptr - buffer;
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @param start start member
   * @param startInclusive is start inclusive
   * @param end end member
   * @param endInclusive is end inclusive
   * @param offset offset to start
   * @param limit limit
   * @param bufSize buffer size
   * @return list of members
   */
  public static List<Pair<String>> ZRANGEBYLEX(BigSortedMap map, String key, String start, 
      boolean startInclusive, String end, boolean endInclusive,long offset, long limit,  int bufSize){
    
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long startPtr = start != null? UnsafeAccess.allocAndCopy(start, 0, start.length()): 0;
    int startSize = start != null? start.length(): 0;
    long endPtr = end != null? UnsafeAccess.allocAndCopy(end, 0, end.length()): 0;
    int endSize = end != null? end.length(): 0;
    
    long buffer = UnsafeAccess.malloc(bufSize);
    long result = ZRANGEBYLEX(map, keyPtr, keySize, startPtr, startSize, startInclusive, endPtr, 
      endSize, endInclusive, offset, limit, buffer, bufSize);
    List<Pair<String>> list = new ArrayList<Pair<String>>();
    if (result > bufSize || result <= 0) {
      return list;
    }
    
    int total = UnsafeAccess.toInt(buffer);
    long ptr = buffer + Utils.SIZEOF_INT;
    for (int i = 0; i < total; i++) {
      int mSize = Utils.readUVInt(ptr);
      int mSizeSize = Utils.sizeUVInt(mSize);
      String member = null;
      String sscore = null;
      member =  Utils.toString(ptr + mSizeSize, mSize);
      sscore = member;
      list.add(new Pair<String>(member, sscore));
      ptr += mSize + mSizeSize;
    }
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(buffer);
    if (startPtr > 0) {
      UnsafeAccess.free(startPtr);
    }
    if (endPtr > 0) {
      UnsafeAccess.free(endPtr);
    }
    return list;
  }
  
  /**
   * Available since 2.8.9.
   * Time complexity: O(log(N)+M) with N being the number of elements in the sorted set and M 
   * the number of elements being returned. If M is constant (e.g. always asking for the first 
   * 10 elements with LIMIT), you can consider it O(log(N)).
   * When all the elements in a sorted set are inserted with the same score, in order to force 
   * lexicographical ordering, this command returns all the elements in the sorted set at key with 
   * a value between min and max.
   * If the elements in the sorted set have different scores, the returned elements are unspecified.
   * The elements are considered to be ordered from lower to higher strings as compared 
   * byte-by-byte using the memcmp() C function. Longer strings are considered greater than shorter strings 
   * if the common part is identical.
   * The optional LIMIT argument can be used to only get a range of the matching elements 
   * (similar to SELECT LIMIT offset, count in SQL). A negative count returns all elements from the offset.
   *  Keep in mind that if offset is large, the sorted set needs to be traversed for offset elements
   *   before getting to the elements to return, which can add up to O(N) time complexity.
   * 
   * How to specify intervals
   * 
   * Valid start and stop must start with ( or [, in order to specify if the range item is respectively 
   * exclusive or inclusive. The special values of + or - for start and stop have the special meaning or 
   * positively infinite and negatively infinite strings, so for instance the command ZRANGEBYLEX myzset - + 
   * is guaranteed to return all the elements in the sorted set, if all the elements have the same score.
   * 
   * Details on strings comparison
   * 
   * Strings are compared as binary array of bytes. Because of how the ASCII character set is specified, 
   * this means that usually this also have the effect of comparing normal ASCII characters in an obvious 
   * dictionary way. However this is not true if non plain ASCII strings are used (for example utf8 strings).
   * However the user can apply a transformation to the encoded string so that the first part of the element 
   * inserted in the sorted set will compare as the user requires for the specific application. 
   * For example if I want to add strings that will be compared in a case-insensitive way, but I still want to retrieve the real case when querying, I can add strings in the following way:
   * ZADD autocomplete 0 foo:Foo 0 bar:BAR 0 zap:zap
   * Because of the first normalized part in every element (before the colon character), 
   * we are forcing a given comparison, however after the range is queries using ZRANGEBYLEX the application 
   * can display to the user the second part of the string, after the colon.
   * The binary nature of the comparison allows to use sorted sets as a general purpose index, for example the
   * first part of the element can be a 64 bit big endian number: since big endian numbers have the most 
   * significant bytes in the initial positions, the binary comparison will match the numerical comparison 
   * of the numbers. This can be used in order to implement range queries on 64 bit values. As in the example
   *  below, after the first 8 bytes we can store the value of the element we are actually indexing.
   * Return value
   *  Array reply: list of elements in the specified score range.
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @param startPtr interval start address (0 - negative string infinity)
   * @param startSize interval start size
   * @param startInclusive is start inclusive?
   * @param endPtr interval end address (0 - positive string infinity)
   * @param endSize interval end size 
   * @param endInclusive is end inclusive?
   * @param offset offset index
   * @param limit max count after offset
   * @param buffer buffer for return
   * @param bufferSize buffer size
   * @return total serialized size of a response, if it is greater than 
   *         bufferSize - repeat call with appropriately sized buffer 
   */
  public static long ZRANGEBYLEX(BigSortedMap map, long keyPtr, int keySize, long startPtr,
      int startSize, boolean startInclusive, long endPtr, int endSize, boolean endInclusive,
      long offset, long limit, long buffer, int bufferSize) {
    
    
    //TODO: Use HashScanner if available
    // See: ZCOUNTBYLEX as an example
    
    boolean freeStart = false;
    boolean freeEnd = false;
    if (endInclusive && endPtr > 0) {
      endPtr = Utils.prefixKeyEndCorrect(endPtr, endSize);
      endSize += 1;
      freeEnd = true;
    }
    if (!startInclusive && startPtr > 0) {
      startPtr = Utils.prefixKeyEndCorrect(startPtr, startSize);
      startSize += 1;
      freeStart = true;
    }
    
    Key key = getKey(keyPtr, keySize);
    
    // Clean first 4 bytes
    UnsafeAccess.putInt(buffer,  0);
    
    SetScanner setScanner = null;
    long ptr = 0;
    int counter = 0;
    long cardinality = ZCARD(map, keyPtr, keySize);
    if (cardinality == 0) {
      return 0;
    }
    if (offset >= cardinality) {
      return 0;
    } else if (offset < 0) {
      offset += cardinality;
      if (offset < 0) {
        offset = 0;
      }
    }
    if (limit < 0) {
      limit = Long.MAX_VALUE / 2; // VERY LARGE
    }
    try {
      KeysLocker.readLock(key);
      ptr = buffer + Utils.SIZEOF_INT;
      // make sure first 4 bytes does not contain garbage
      UnsafeAccess.putInt(buffer, 0);
      //TODO: Optimize using SetScanner.skipTo API
      setScanner = Sets.getScanner(map, keyPtr, keySize, false);
      if (setScanner == null) {
        return 0;
      }
      long pos = 0;
      while (setScanner.hasNext()) {
        if (pos == offset + limit) {
          break;
        }
        if (pos < offset) {
          setScanner.next();
          pos++;
          continue;
        }
        pos++;
        long mPtr = setScanner.memberAddress();
        int mSize = setScanner.memberSize();
        mPtr += Utils.SIZEOF_DOUBLE;
        mSize -= Utils.SIZEOF_DOUBLE;
        int res1 = startPtr == 0? 1: Utils.compareTo(mPtr, mSize, startPtr, startSize);
        int res2 = endPtr == 0? -1: Utils.compareTo(mPtr, mSize, endPtr, endSize); 
        if (res2 < 0 && res1 >= 0) {  
          int mSizeSize = Utils.sizeUVInt(mSize);
          if (ptr + mSize + mSizeSize <= buffer + bufferSize) {
            counter++;
            Utils.writeUVInt(ptr, mSize);
            UnsafeAccess.copy(mPtr, ptr + mSizeSize, mSize);
            // Update count
            UnsafeAccess.putInt(buffer, counter);
          }
          ptr += mSize + mSizeSize;
        } else if (res2 >= 0) {
          break;
        }
        setScanner.next();
      }
    } catch (IOException e) {
    } finally {
      if (setScanner != null) {
        try {
          setScanner.close();
        } catch (IOException e) {
        }
      }
      if (freeStart) {
        UnsafeAccess.free(startPtr);
      }
      if (freeEnd) {
        UnsafeAccess.free(endPtr);
      }
      
      KeysLocker.readUnlock(key);
    }
    return ptr - buffer;
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @param start start member
   * @param startInclusive is start inclusive
   * @param end end member
   * @param endInclusive is end inclusive
   * @param bufSize buffer size
   * @return list of members
   */
  public static List<Pair<String>> ZRANGEBYLEX(BigSortedMap map, String key, String start, 
      boolean startInclusive, String end, boolean endInclusive, int bufSize){
    
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long startPtr = start != null? UnsafeAccess.allocAndCopy(start, 0, start.length()): 0;
    int startSize = start != null? start.length(): 0;
    long endPtr = end != null? UnsafeAccess.allocAndCopy(end, 0, end.length()): 0;
    int endSize = end != null? end.length(): 0;
    
    long buffer = UnsafeAccess.malloc(bufSize);
    long result = ZRANGEBYLEX(map, keyPtr, keySize, startPtr, startSize, startInclusive, endPtr, 
      endSize, endInclusive, buffer, bufSize);
    List<Pair<String>> list = new ArrayList<Pair<String>>();
    if (result > bufSize || result <= 0) {
      return list;
    }
    
    int total = UnsafeAccess.toInt(buffer);
    long ptr = buffer + Utils.SIZEOF_INT;
    for (int i = 0; i < total; i++) {
      int mSize = Utils.readUVInt(ptr);
      int mSizeSize = Utils.sizeUVInt(mSize);
      String member = null;
      String sscore = null;
      member =  Utils.toString(ptr + mSizeSize, mSize);
      sscore = member;
      list.add(new Pair<String>(member, sscore));
      ptr += mSize + mSizeSize;
    }
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(buffer);
    if (startPtr > 0) {
      UnsafeAccess.free(startPtr);
    }
    if (endPtr > 0) {
      UnsafeAccess.free(endPtr);
    }
    return list;
  }
  /**
   * 
   * ZRANGEBYLEX without offset and limit
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @param startPtr interval start address (0 - negative string infinity)
   * @param startSize interval start size
   * @param startInclusive is start inclusive?
   * @param endPtr interval end address (0 - positive string infinity)
   * @param endSize interval end size 
   * @param endInclusive is end inclusive?
   * @param buffer buffer for return
   * @param bufferSize buffer size
   * @return total serialized size of a response, if it is greater than 
   *         bufferSize - repeat call with appropriately sized buffer 
   */
  public static long ZRANGEBYLEX(BigSortedMap map, long keyPtr, int keySize, long startPtr, int startSize, 
      boolean startInclusive, long endPtr, int endSize, boolean endInclusive, long buffer, int bufferSize)
  {
    boolean freeStart = false;
    boolean freeEnd = false;
    if (endInclusive && endPtr > 0) {
      endPtr = Utils.prefixKeyEndCorrect(endPtr, endSize);
      endSize += 1;
      freeEnd = true;
    }
    if (!startInclusive && startPtr > 0) {
      startPtr = Utils.prefixKeyEndCorrect(startPtr, startSize);
      startSize += 1;
      freeStart = true;
    }
    
    Key key = getKey(keyPtr, keySize);
    
    // Clean first 4 bytes
    UnsafeAccess.putInt(buffer, 0);
    
    HashScanner hashScanner = null;
    SetScanner setScanner = null;
    long cardPtr = getCardinalityMember(keyPtr, keySize);
    long ptr = 0;
    int count = 0;
    ptr = buffer + Utils.SIZEOF_INT;
    // Make sure first 4 bytes does not contain garbage
    UnsafeAccess.putInt(buffer, 0);
    try {
      KeysLocker.readLock(key);
      hashScanner =
          Hashes.getScanner(map, keyPtr, keySize, startPtr, startSize, endPtr, endSize, false);
      if (hashScanner != null) {
        
        while (hashScanner.hasNext()) {
          long fPtr = hashScanner.fieldAddress();
          int fSize = hashScanner.fieldSize();
          int fSizeSize = Utils.sizeUVInt(fSize);
          boolean isCardMember = isCardinalityMember(fPtr, fSize, cardPtr);
          if ((ptr + fSize + fSizeSize <= buffer + bufferSize) 
              && !isCardMember) {
            count++;
            Utils.writeUVInt(ptr, fSize);
            UnsafeAccess.copy(fPtr, ptr + fSizeSize, fSize);
            // Update count
            UnsafeAccess.putInt(buffer, count);
          }
          if (!isCardMember) {
            ptr += fSize + fSizeSize;
          }
          hashScanner.next();
        }
      } else {
        // Run through the Set
        setScanner = Sets.getScanner(map, keyPtr, keySize, false);
        if (setScanner == null) {
          return 0;
        }
        while (setScanner.hasNext()) {
          long mPtr = setScanner.memberAddress();
          int mSize = setScanner.memberSize();
          mPtr += Utils.SIZEOF_DOUBLE;
          mSize -= Utils.SIZEOF_DOUBLE;
          int res1 = startPtr == 0? 1: Utils.compareTo(mPtr, mSize, startPtr, startSize);
          int res2 = endPtr == 0? -1: Utils.compareTo(mPtr, mSize, endPtr, endSize);
          if (res2 < 0 && res1 >= 0) {  
            int mSizeSize = Utils.sizeUVInt(mSize);
            if (ptr + mSize + mSizeSize <= buffer + bufferSize) {
              count++;
              Utils.writeUVInt(ptr, mSize);
              UnsafeAccess.copy(mPtr, ptr + mSizeSize, mSize);
              // Update count
              UnsafeAccess.putInt(buffer, count);
            }
            ptr += mSize + mSizeSize;
          } else if (res2 >= 0) {
            break;
          }
          setScanner.next();
        }
      }
    } catch (IOException e) {
    } finally {
      try {
        if (hashScanner != null) {
          hashScanner.close();
        }
        if (setScanner != null) {
          setScanner.close();
        }
      } catch (IOException e) {
      }
      if (cardPtr > 0) {
        UnsafeAccess.free(cardPtr);
      }
      if (freeStart) {
        UnsafeAccess.free(startPtr);
      }
      if (freeEnd) {
        UnsafeAccess.free(endPtr);
      }
      KeysLocker.readUnlock(key);
    }
    return ptr - buffer;
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @param minScore start score
   * @param minInclusive is start inclusive
   * @param maxScore end score
   * @param maxInclusive is end inclusive
   * @param offset offset 
   * @param limit limit
   * @param withScores with scores
   * @param bufSize buffer size
   * @return list of pairs {member, member} or {member, score}
   */
  
  public static List<Pair<String>> ZRANGEBYSCORE(BigSortedMap map, String key, double minScore, 
      boolean minInclusive, double maxScore, boolean maxInclusive, long offset, long limit, 
      boolean withScores, int bufSize){
    
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    
    long buffer = UnsafeAccess.malloc(bufSize);
    long result = ZRANGEBYSCORE(map, keyPtr, keySize, minScore, minInclusive, maxScore, 
      maxInclusive, offset, limit, withScores, buffer, bufSize);
    List<Pair<String>> list = new ArrayList<Pair<String>>();
    if (result > bufSize || result <= 0) {
      return list;
    }
    
    int total = UnsafeAccess.toInt(buffer);
    long ptr = buffer + Utils.SIZEOF_INT;
    for (int i = 0; i < total; i++) {
      int mSize = Utils.readUVInt(ptr);
      int mSizeSize = Utils.sizeUVInt(mSize);
      String member = null;
      String sscore = null;
      if (withScores) {
        double score = Utils.lexToDouble(ptr + mSizeSize);
        sscore = Double.toString(score);
        member = Utils.toString(ptr + mSizeSize + Utils.SIZEOF_DOUBLE, mSize - Utils.SIZEOF_DOUBLE);
      } else {
        member =  Utils.toString(ptr + mSizeSize, mSize);
        sscore = member;
      }
      list.add(new Pair<String>(member, sscore));
      ptr += mSize + mSizeSize;
    }
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(buffer);
    return list;
  }
  
  /**
   * Available since 1.0.5.
   * Time complexity: O(log(N)+M) with N being the number of elements in the sorted set 
   * and M the number of elements being returned. If M is constant (e.g. always asking 
   * for the first 10 elements with LIMIT), you can consider it O(log(N)).
   * Returns all the elements in the sorted set at key with a score between min and max 
   * (including elements with score equal to min or max). The elements are considered to be 
   * ordered from low to high scores.
   * The elements having the same score are returned in lexicographical order (this follows 
   * from a property of the sorted set implementation in Redis and does not involve further computation).
   * The optional LIMIT argument can be used to only get a range of the matching elements 
   * (similar to SELECT LIMIT offset, count in SQL). A negative count returns all elements from 
   * the offset. Keep in mind that if offset is large, the sorted set needs to be traversed for offset
   *  elements before getting to the elements to return, which can add up to O(N) time complexity.
   * The optional WITHSCORES argument makes the command return both the element and its score, instead 
   * of the element alone. This option is available since Redis 2.0.
   * 
   * Exclusive intervals and infinity
   * 
   * min and max can be -inf and +inf, so that you are not required to know the highest or lowest
   *  score in the sorted set to get all elements from or up to a certain score.
   * By default, the interval specified by min and max is closed (inclusive). It is possible to 
   * specify an open interval (exclusive) by prefixing the score with the character (.
   * 
   * -inf = -Double.MAX_VALUE
   * +inf = Double.MAX_VALUE
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @param minScore minimum score
   * @param minInclusive minimum score inclusive
   * @param maxScore maximum score
   * @param maxInclusive maximum score inclusive
   * @param offset offset (index) (default - 0)
   * @param limit - limit after offset (-1 - means till the last member)
   * @param withScores include scores?
   * @param buffer buffer address
   * @param bufferSize buffer size
   * @return total serialized size of a response, if it is greater than 
   *         bufferSize - repeat call with appropriately sized buffer 
   */
  public static long ZRANGEBYSCORE(BigSortedMap map, long keyPtr, int keySize, double minScore, 
      boolean minInclusive, double maxScore, boolean maxInclusive, long offset, long limit, boolean withScores, 
      long buffer, int bufferSize)
  {
    if (minScore > maxScore) {
      return 0;
    }
    Key k = getKey(keyPtr, keySize);
    long cardinality = ZCARD(map, keyPtr, keySize);
    if (offset < 0) {
      if (cardinality == 0) {
        return 0;
      }
      offset = offset + cardinality;
      if (offset < 0) {
        offset = 0;
      }
    } else if (offset >= cardinality) {
      return 0;
    }
    if (limit < 0) {
      // Set very high value unreachable in a real life
      limit = Long.MAX_VALUE / 2;
    }
    int count = 0;
    try {
      KeysLocker.readLock(k);
      // TODO : SetScanner skipTo
      SetScanner scanner = Sets.getScanner(map, keyPtr, keySize, false);
      if (scanner == null) {
        return 0;
      }
      long ptr = buffer + Utils.SIZEOF_INT;
      long off = 0;
      // Make sure first 4 bytes does not contain garbage
      UnsafeAccess.putInt(buffer, 0);
      try {
        while (scanner.hasNext()) {
          if (off >= offset && off < (offset + limit)) {
            int mSize = scanner.memberSize();
            long mPtr = scanner.memberAddress();
            double score = Utils.lexToDouble(mPtr);
            if (between(score, minScore, maxScore, minInclusive, maxInclusive)) {
              if (!withScores) {
                mSize -= Utils.SIZEOF_DOUBLE;
                mPtr += Utils.SIZEOF_DOUBLE;
              }
              int mSizeSize = Utils.sizeUVInt(mSize);
              count++;
              if (ptr + mSize + mSizeSize <= buffer + bufferSize) {
                Utils.writeUVInt(ptr, mSize);
                UnsafeAccess.copy(mPtr, ptr + mSizeSize, mSize);
                // Update count
                UnsafeAccess.putInt(buffer, count);
              }
              ptr += mSize + mSizeSize;
            }
          } else if (off >= offset + limit) {
            break;
          }
          off++;
          scanner.next();
        }
      } catch (IOException e) {
        // Should not be here
      } finally {
        try {
          if (scanner != null) {
            scanner.close();
          }
        } catch (IOException e) {
        }
      }
      return ptr - buffer;
    }finally {
      KeysLocker.readUnlock(k);
    }
  }
   
  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @param minScore start score
   * @param minInclusive is start inclusive
   * @param maxScore end score
   * @param maxInclusive is end inclusive
   * @param offset offset 
   * @param limit limit
   * @param withScores with scores
   * @param bufSize buffer size
   * @return list of pairs {member, member} or {member, score}
   */
  
  public static List<Pair<String>> ZRANGEBYSCORE(BigSortedMap map, String key, double minScore, 
      boolean minInclusive, double maxScore, boolean maxInclusive,  
      boolean withScores, int bufSize){
    
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
  
    
    long buffer = UnsafeAccess.malloc(bufSize);
    long result = ZRANGEBYSCORE(map, keyPtr, keySize, minScore, minInclusive, maxScore, 
      maxInclusive, withScores, buffer, bufSize);
    List<Pair<String>> list = new ArrayList<Pair<String>>();
    if (result > bufSize || result <= 0) {
      return list;
    }
    
    int total = UnsafeAccess.toInt(buffer);
    long ptr = buffer + Utils.SIZEOF_INT;
    for (int i = 0; i < total; i++) {
      int mSize = Utils.readUVInt(ptr);
      int mSizeSize = Utils.sizeUVInt(mSize);
      String member = null;
      String sscore = null;
      if (withScores) {
        double score = Utils.lexToDouble(ptr + mSizeSize);
        sscore = Double.toString(score);
        member = Utils.toString(ptr + mSizeSize + Utils.SIZEOF_DOUBLE, mSize - Utils.SIZEOF_DOUBLE);
      } else {
        member =  Utils.toString(ptr + mSizeSize, mSize);
        sscore = member;
      }
      list.add(new Pair<String>(member, sscore));
      ptr += mSize + mSizeSize;
    }
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(buffer);
    return list;
  }
  /**
   * ZRANGEBYSCORE without offset and limit
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @param minScore minimum score
   * @param minInclusive minimum score inclusive
   * @param maxScore maximum score
   * @param maxInclusive maximum score inclusive
   * @param withScores include scores?
   * @param buffer buffer address
   * @param bufferSize buffer size
   * @return total serialized size of a response, if it is greater than 
   *         bufferSize - repeat call with appropriately sized buffer 
   */
  public static long ZRANGEBYSCORE(BigSortedMap map, long keyPtr, int keySize, double minScore,
      boolean minInclusive, double maxScore, boolean maxInclusive, boolean withScores, long buffer,
      int bufferSize) {
    Key k = getKey(keyPtr, keySize);
    if (minScore > maxScore) {
      return 0;
    }
    int count = 0;
    long startPtr = 0, stopPtr = 0;
    SetScanner scanner = null;
    try {
      KeysLocker.readLock(k);
      
      startPtr = UnsafeAccess.malloc(Utils.SIZEOF_DOUBLE);
      Utils.doubleToLex(startPtr, minScore);
      int startSize = Utils.SIZEOF_DOUBLE;

      stopPtr = UnsafeAccess.malloc(Utils.SIZEOF_DOUBLE);
      Utils.doubleToLex(stopPtr, maxScore);
      int stopSize = Utils.SIZEOF_DOUBLE;
      //FIXME: prefixKeyEndNoAlloc
      if (maxInclusive && maxScore < Double.MAX_VALUE) {
        stopPtr = Utils.prefixKeyEndNoAlloc(stopPtr, stopSize);
      }
      if (!minInclusive && minScore > -Double.MAX_VALUE) {
        startPtr = Utils.prefixKeyEndNoAlloc(startPtr, startSize);
      }
      scanner =
          Sets.getScanner(map, keyPtr, keySize, startPtr, startSize, stopPtr, stopSize, false);
      if (scanner == null) {
        return 0;
      }
      long ptr = buffer + Utils.SIZEOF_INT;
      // Make sure first 4 bytes does not contain garbage
      UnsafeAccess.putInt(buffer, 0);

      while (scanner.hasNext()) {
        int mSize = scanner.memberSize();
        long mPtr = scanner.memberAddress();
        if (!withScores) {
          mSize -= Utils.SIZEOF_DOUBLE;
          mPtr += Utils.SIZEOF_DOUBLE;
        }
        int mSizeSize = Utils.sizeUVInt(mSize);
        count++;

        if (ptr + mSize + mSizeSize <= buffer + bufferSize) {
          Utils.writeUVInt(ptr, mSize);
          UnsafeAccess.copy(mPtr, ptr + mSizeSize, mSize);
          // Update count
          UnsafeAccess.putInt(buffer, count);
        }
        ptr += mSize + mSizeSize;
        scanner.next();
      }
      return ptr - buffer;
    } catch (IOException e) {
    } finally {
      try {
        if (scanner != null) {
          scanner.close();
        }
      } catch (IOException e) {
      }
      if (startPtr > 0) {
        UnsafeAccess.free(startPtr);
      }
      if (stopPtr > 0) {
        UnsafeAccess.free(stopPtr);
      }
      KeysLocker.readUnlock(k);
    }
    return 0;
  }

  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @param member zset member
   * @return rank (0 - based) or -1 if not exists
   */
  public static long ZRANK(BigSortedMap map, String key, String member) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long memberPtr = UnsafeAccess.allocAndCopy(member, 0, member.length());
    int memberSize = member.length();
    long result = ZRANK(map, keyPtr, keySize, memberPtr, memberSize);
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(memberPtr);
    return result;
  }
  
  /**
   * Returns the rank of member in the sorted set stored at key, with the scores ordered from 
   * low to high. The rank (or index) is 0-based, which means that the member with the lowest 
   * score has rank 0.
   * Use ZREVRANK to get the rank of an element with the scores ordered from high to low.
   * 
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @param memberPtr member name address 
   * @param memberSize member name size
   * @return rank (0- based), smallest has the lowest rank
   *         -1 if key or member do not exist
   */
  public static long ZRANK(BigSortedMap map, long keyPtr, int keySize, long memberPtr,
      int memberSize) {
    if (memberPtr <= 0) return -1;
    long rank = 0;
    Key key = getKey(keyPtr, keySize);
    // TODO this operation can be optimized
    // Use hash to retrieve score?
    // See Sets SCOUNT (not implemented yet)
    SetScanner scanner = null;
    try {
      KeysLocker.readLock(key);
      scanner = Sets.getScanner(map, keyPtr, keySize, false);
      if (scanner == null) {
        return -1;
      }
      while (scanner.hasNext()) {
        long mPtr = scanner.memberAddress();
        int mSize = scanner.memberSize();
        // First 8 bytes - score, skip it
        if (Utils.compareTo(mPtr + Utils.SIZEOF_DOUBLE, mSize - Utils.SIZEOF_DOUBLE, 
          memberPtr, memberSize) == 0) {
          return rank;
        }
        rank++;
        scanner.next();
      }
    } catch (IOException e) {
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
        }
      }
      KeysLocker.readUnlock(key);
    }
    return -1;
  }

  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @param members zset members
   * @return 1 - success, 0 - otherwise
   */
  public static long ZREM(BigSortedMap map, String key, String[] members) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize= key.length();
    long memberPtrs[] = new long[members.length];
    int memberSizes[] = new int[members.length];
    for (int i = 0; i < members.length; i++) {
      memberPtrs[i] = UnsafeAccess.allocAndCopy(members[i], 0, members[i].length());
      memberSizes[i] = members[i].length();
    }
    long result = ZREM(map, keyPtr, keySize, memberPtrs, memberSizes);
    UnsafeAccess.free(keyPtr);
    
    Arrays.stream(memberPtrs).forEach(x -> UnsafeAccess.free(x));
    
    return result;
  }
  
  /**
   * 
   * Removes the specified members from the sorted set stored at key. 
   * Non existing members are ignored.
   * An error is returned when key exists and does not hold a sorted set.
   * Return value
   * Integer reply, specifically:
   * The number of members removed from the sorted set, not including non existing members.
   * 
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @param memberPtrs array member name address (MUTABLE!!!)
   * @param memberSizes array member name size
   * @return The number of members removed from the sorted set
   */
  public static long ZREM(BigSortedMap map, long keyPtr, int keySize, long[] memberPtrs,
      int[] memberSizes) {

    Key key = getKey(keyPtr, keySize);
    int deleted = 0;
    SetScanner scanner = null;
    try {
      KeysLocker.writeLock(key);
      long cardinality = ZCARD(map, keyPtr, keySize, false);
      if (cardinality == 0) {
        return 0;
      }
      int maxCompactModeSize = RedisConf.getInstance().getMaxZSetCompactSize();
      boolean normalMode =  maxCompactModeSize <= cardinality;
      if (normalMode) {
        for (int i = 0; i < memberPtrs.length; i++) {
          long memberPtr = memberPtrs[i];
          int memberSize = memberSizes[i];
          checkValueArena(memberSize + Utils.SIZEOF_DOUBLE + Utils.SIZEOF_BYTE);
          long buffer = valueArena.get();
          int bufferSize = valueArenaSize.get();
          // minimum bufferSize = 512, value size is 8 (DOUBLE)
          int result = Hashes.HDEL(map, keyPtr, keySize, memberPtr, memberSize, buffer, bufferSize, false);
          if (result == 0) {
            // Not found
            continue;
          }
          // copy member to buffer + 1 (first byte is length of the value, which is 8)
          UnsafeAccess.copy(memberPtr, buffer + Utils.SIZEOF_DOUBLE + Utils.SIZEOF_BYTE, memberSize);
          int res = Sets.SREM(map, keyPtr, keySize, buffer + Utils.SIZEOF_BYTE, 
            memberSize + Utils.SIZEOF_DOUBLE, false);
          deleted++;
        }
        
        // Convert to compact if necessary
        if (cardinality - deleted > 0 && (cardinality - deleted) < maxCompactModeSize) {
          convertToCompactMode(map, keyPtr, keySize);
        }
      } else {
        // Compact mode 
        //TODO: Optimize for multiple members
        for (int i = 0; i < memberPtrs.length; i++) {

          scanner = Sets.getScanner(map, keyPtr, keySize, false);
          if (scanner == null) {
            break;
          }
          while (scanner.hasNext()) {

            long mPtr = scanner.memberAddress();
            int mSize = scanner.memberSize();
            if(Utils.compareTo(mPtr + Utils.SIZEOF_DOUBLE, mSize - Utils.SIZEOF_DOUBLE, 
              memberPtrs[i], memberSizes[i]) == 0)
            {
              scanner.close();
              scanner = null;
              checkValueArena(mSize);
              long buffer = valueArena.get();
              UnsafeAccess.copy(mPtr, buffer, mSize);
              int res = Sets.SREM(map, keyPtr, keySize, buffer, mSize);
              // assertions not enabled by default
              assert(res == 1);
              deleted++;
              break;
            }
            scanner.next();
          }
        }
      }
      // Update count
      if (deleted > 0 && (cardinality - deleted >= maxCompactModeSize)) {
        //ZINCRCARD(map, keyPtr, keySize, -deleted);
        ZSETCARD(map, keyPtr, keySize, cardinality - deleted);
      } else if (cardinality - deleted == 0) {
        DELETE(map, keyPtr, keySize, false);
      }
    } catch (IOException e) {
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
        }
      }
      KeysLocker.writeUnlock(key);
    }
    return deleted;
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @param member zset member
   * @return 1 - success, 0 - otherwise
   */
  public static long ZREM(BigSortedMap map, String key, String member) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize= key.length();
    long memberPtr = UnsafeAccess.allocAndCopy(member, 0, member.length());
    int memberSize = member.length();
    
    long result = ZREM(map, keyPtr, keySize, memberPtr, memberSize);
    
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(memberPtr);
    return result;
  }
  /**
   * No allocation version of ZREM
   * @param map sorted map storage
   * @param keyPtr zset key address
   * @param keySize zset key size
   * @param memberPtr member address
   * @param memberSize member size
   * @return 1 - success, 0 - otherwise
   */
  public static long ZREM(BigSortedMap map, long keyPtr, int keySize, long memberPtr,
      int memberSize) {
    
    Key key = getKey(keyPtr, keySize);
    int deleted = 0;
    SetScanner scanner = null;
    try {
      KeysLocker.writeLock(key);
      long cardinality = ZCARD(map, keyPtr, keySize, false);
      if (cardinality == 0) return 0;
      int maxCompactModeSize = RedisConf.getInstance().getMaxZSetCompactSize();
      boolean normalMode = maxCompactModeSize <= cardinality;
      if (normalMode) {
        checkValueArena(memberSize + Utils.SIZEOF_DOUBLE + Utils.SIZEOF_BYTE);
        long buffer = valueArena.get();
        int bufferSize = valueArenaSize.get();
        // minimum bufferSize = 512, value size is 8 (DOUBLE)
        int result = Hashes.HDEL(map, keyPtr, keySize, memberPtr, memberSize, buffer, bufferSize, false);
        if (result == 0) {
          return 0;
        }
        // copy member to buffer + 1 (first byte is length of the value, which is 8)
        UnsafeAccess.copy(memberPtr, buffer + Utils.SIZEOF_DOUBLE + Utils.SIZEOF_BYTE, memberSize);
        int res = Sets.SREM(map, keyPtr, keySize, buffer + Utils.SIZEOF_BYTE,
          memberSize + Utils.SIZEOF_DOUBLE, false);
        deleted++;
        // Convert to compact if necessary
        if (cardinality - deleted > 0 && (cardinality - deleted) < maxCompactModeSize) {
          convertToCompactMode(map, keyPtr, keySize);
        }
      } else {
        // Compact mode
        // TODO: Optimize for multiple members
        scanner = Sets.getScanner(map, keyPtr, keySize, false);
        while (scanner.hasNext()) {
          long mPtr = scanner.memberAddress();
          int mSize = scanner.memberSize();
          if (Utils.compareTo(mPtr + Utils.SIZEOF_DOUBLE, mSize - Utils.SIZEOF_DOUBLE, memberPtr,
            memberSize) == 0) {
            scanner.close();
            scanner = null;
            checkValueArena(mSize);
            long buffer = valueArena.get();
            UnsafeAccess.copy(mPtr, buffer, mSize);
            int res = Sets.SREM(map, keyPtr, keySize, buffer, mSize, false);
            // assertions not enabled by default
            assert (res == 1);
            deleted++;
            break;
          }
          scanner.next();
        }
      }
      // Update count
      if (deleted > 0 && (cardinality - deleted >= maxCompactModeSize)) {
        //ZINCRCARD(map, keyPtr, keySize, -deleted);
        ZSETCARD(map, keyPtr, keySize, cardinality - deleted);
      }
      if (cardinality - deleted == 0) {
        DELETE(map, keyPtr, keySize, false);
      }
    } catch (IOException e) {
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
        }
      }
      KeysLocker.writeUnlock(key);
    }
    return deleted;
  }
    
  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @param start start member
   * @param startInclusive is start inclusive
   * @param end end member
   * @param endInclusive is end inclusive
   * @param bufSize buffer size
   * @return number of members removed
   */
  public static long ZREMRANGEBYLEX(BigSortedMap map, String key, String start, 
      boolean startInclusive, String end, boolean endInclusive){
    
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long startPtr = start != null? UnsafeAccess.allocAndCopy(start, 0, start.length()): 0;
    int startSize = start != null? start.length(): 0;
    long endPtr = end != null? UnsafeAccess.allocAndCopy(end, 0, end.length()): 0;
    int endSize = end != null? end.length(): 0;
    
    long result = ZREMRANGEBYLEX(map, keyPtr, keySize, startPtr, startSize, startInclusive, endPtr, 
      endSize, endInclusive);
   
    UnsafeAccess.free(keyPtr);
    if (startPtr > 0) {
      UnsafeAccess.free(startPtr);
    }
    if (endPtr > 0) {
      UnsafeAccess.free(endPtr);
    }
    return result;
  }
  
  /**
   * TODO: VERIFY NEW CARDINALITY CODE
   * 
   * When all the elements in a sorted set are inserted with the same score, in order 
   * to force lexicographical ordering, this command removes all elements in the sorted set 
   * stored at key between the lexicographical range specified by min and max.
   * The meaning of min and max are the same of the ZRANGEBYLEX command. Similarly, 
   * this command actually returns the same elements that ZRANGEBYLEX would return if called 
   * with the same min and max arguments.
   * Return value
   * Integer reply: the number of elements removed.
   * 
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @param startPtr range start (lex) address
   * @param startSize range start size
   * @param startInclusive is start inclusive
   * @param endPtr range end address
   * @param endSize range end size
   * @param endInclusive is end inclusive
   * @return number of members removed
   */
  public static long ZREMRANGEBYLEX(BigSortedMap map, long keyPtr, int keySize, long startPtr,
      int startSize, boolean startInclusive, long endPtr, int endSize, boolean endInclusive) {
    
    if (endInclusive && endPtr > 0) {
      endPtr = Utils.prefixKeyEndNoAlloc(endPtr, endSize);
    }
    if (!startInclusive && startPtr > 0) {
      startPtr = Utils.prefixKeyEndNoAlloc(startPtr, startSize);
    }
    HashScanner hashScanner = null;
    SetScanner setScanner = null;
    long cardPtr = getCardinalityMember(keyPtr, keySize);
    
    long buffer = valueArena.get();
    int bufferSize = valueArenaSize.get();
    long ptr = buffer + Utils.SIZEOF_INT;
    int deleted = 0, cc = 0;
    long cardinality = ZCARD(map, keyPtr, keySize);
    int maxCompactSize = RedisConf.getInstance().getMaxZSetCompactSize();
    boolean normalMode = false;
    boolean recycleStartPtr = false;
    // Make sure first 4 bytes of a bulk delete buffer 
    // does not contain garbage
    UnsafeAccess.putInt(buffer, 0);
    Key key = getKey(keyPtr, keySize);
    long sPtr = 0;
    int sSize = 0;
    
    try {
      KeysLocker.writeLock(key);
      hashScanner =
          Hashes.getScanner(map, keyPtr, keySize, startPtr, startSize, endPtr, endSize, false);
      
      if (hashScanner != null) {
        normalMode = true;
        while (hashScanner.hasNext()) {
          long fPtr = hashScanner.fieldAddress();
          int fSize = hashScanner.fieldSize();
          long vPtr = hashScanner.fieldValueAddress();
          int vSize = hashScanner.fieldValueSize();
          int size = fSize + vSize;
          int sizeSize = Utils.sizeUVInt(size);
          
          boolean isCardMember = isCardinalityMember(fPtr, fSize, cardPtr);
          if (isCardMember) {
            hashScanner.next();
            continue;
          }
          
          if (ptr + size + sizeSize > buffer + bufferSize) {
            // Copy field - value to aux buffer
            checkAuxArena(size + sizeSize);
            long buf = auxArena.get();
            Utils.writeUVInt(buf, size);
            // Value size is 8
            UnsafeAccess.copy(vPtr, buf + sizeSize, vSize);
            UnsafeAccess.copy(fPtr, buf + sizeSize + vSize, fSize);
            // Close scanner first, b/c it will block deletes
            hashScanner.close();
            hashScanner = null;
            
            bulkDelete(map, buffer, keyPtr, keySize, true);
            
            ptr = buffer + Utils.SIZEOF_INT;
            if (ptr + size + sizeSize > buffer + bufferSize) {
              checkValueArena(2 * (size + sizeSize));
              buffer = valueArena.get();
              bufferSize = valueArenaSize.get();
              ptr = buffer + Utils.SIZEOF_INT;
            }
            cc = 1;
            // Write item to bulk delete buffer from aux buffer
            Utils.writeUVInt(ptr, size);
            UnsafeAccess.copy(buf + sizeSize, ptr + sizeSize, size);
          } else {
            cc++;
            // Write item to bulk delete buffer
            Utils.writeUVInt(ptr, size);
            UnsafeAccess.copy(vPtr, ptr + sizeSize, vSize);
            UnsafeAccess.copy(fPtr, ptr + sizeSize + vSize, fSize);
          }
          
          deleted++;

          // Update count in the bulk delete buffer
          UnsafeAccess.putInt(buffer, cc);
          ptr += size + sizeSize;
          if (hashScanner != null) {
            hashScanner.next();
          } else {
            if (recycleStartPtr) { 
              UnsafeAccess.free(startPtr);
            }
            recycleStartPtr = true;
            // Create new scanner
            long buf = auxArena.get();
            size = Utils.readUVInt(buf);
            sizeSize = Utils.sizeUVInt(size);
            startPtr = Utils.prefixKeyEnd(buf + sizeSize + Utils.SIZEOF_DOUBLE, size - Utils.SIZEOF_DOUBLE);
            startSize = size - Utils.SIZEOF_DOUBLE;
            hashScanner =
                Hashes.getScanner(map, keyPtr, keySize, startPtr, startSize, endPtr, endSize, false);
            if (hashScanner == null) {
              break;
            }
          }
        }
      } else {
        // Run through the Set
        setScanner = Sets.getScanner(map, keyPtr, keySize, false);
        if (setScanner == null) {
          return 0;
        }
        while (setScanner.hasNext()) {
          long mPtr = setScanner.memberAddress();
          int mSize = setScanner.memberSize();
          int res1 = startPtr == 0? 1: Utils.compareTo(mPtr + Utils.SIZEOF_DOUBLE, mSize - Utils.SIZEOF_DOUBLE, 
            startPtr, startSize);
          int res2 = endPtr == 0? -1: Utils.compareTo(mPtr + Utils.SIZEOF_DOUBLE, mSize - Utils.SIZEOF_DOUBLE, 
            endPtr, endSize);
          if (res2 < 0 && res1 >= 0) {  
            int mSizeSize = Utils.sizeUVInt(mSize);
            if (ptr + mSize + mSizeSize > buffer + bufferSize) {
              // Copy current member to aux buffer
              checkAuxArena(mSize + mSizeSize);
              long buf = auxArena.get();
              Utils.writeUVInt(buf, mSize);
              UnsafeAccess.copy(mPtr, buf + mSizeSize, mSize);
              
              // Close scanner first
              setScanner.close();
              setScanner = null;
              // We still keep write lock on ZSet key
              bulkDelete(map, buffer, keyPtr, keySize, normalMode);
              ptr = buffer + Utils.SIZEOF_INT;
              if (ptr + mSize + mSizeSize > buffer + bufferSize) {
                // Expand value buffer
                checkValueArena(2 * (mSize + mSizeSize));
                buffer = valueArena.get();
                bufferSize = valueArenaSize.get();
                ptr = buffer + Utils.SIZEOF_INT;
              }
              // Reset buffer variables
              cc = 1; 
              Utils.writeUVInt(ptr, mSize);
              UnsafeAccess.copy(buf + mSizeSize, ptr + mSizeSize, mSize);
            } else {
              cc++;
              Utils.writeUVInt(ptr, mSize);
              UnsafeAccess.copy(mPtr, ptr + mSizeSize, mSize);
            }
            deleted++;
            // Update count
            UnsafeAccess.putInt(buffer, cc);
            ptr += mSize + mSizeSize;
          } 
          
          if (setScanner != null) {
            setScanner.next();
          } else {
            if (recycleStartPtr) { 
              UnsafeAccess.free(sPtr);
            }
            recycleStartPtr = true;
            // Create new one
            long buf = auxArena.get();
            int size = Utils.readUVInt(buf);
            int sizeSize = Utils.sizeUVInt(size);
            sPtr = Utils.prefixKeyEnd(buf + sizeSize, size);
            sSize = size;
            setScanner = Sets.getScanner(map, keyPtr, keySize, sPtr, sSize, 0, 0, false);
            if (setScanner == null) { 
              break; 
            }
          }
        }
      }
      // Close scanner first
      if (setScanner != null) {
        setScanner.close();
        setScanner = null;
      } else if (hashScanner != null){
        hashScanner.close();
        hashScanner = null;
      }
      // Do last bulk delete
      bulkDelete(map, buffer, keyPtr, keySize, normalMode);
      // Check compact mode
      if (normalMode && (cardinality - deleted < maxCompactSize)) {
        convertToCompactMode(map, keyPtr, keySize);
      }
      if (deleted > 0 && (cardinality - deleted) >= maxCompactSize) {
        //ZINCRCARD(map, keyPtr, keySize, -deleted);
        ZSETCARD(map, keyPtr, keySize, cardinality - deleted);
      } else if (cardinality - deleted == 0) {
        DELETE(map, keyPtr, keySize, false);
      }
    } catch (IOException e) {
    } finally {
      try {
        if (hashScanner != null) {
          hashScanner.close();
        }
        if (setScanner != null) {
          setScanner.close();
        }
        if (recycleStartPtr) {
          UnsafeAccess.free(sPtr);
        }
      } catch (IOException e) {
      }
      if (cardPtr > 0) {
        UnsafeAccess.free(cardPtr);
      }
      KeysLocker.writeUnlock(key);
    }
    return deleted;
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @param startRank start rank
   * @param stopRank stop rank
   * @return number deleted
   */
  public static long ZREMRANGEBYRANK(BigSortedMap map, String key, long startRank, long stopRank) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long result = ZREMRANGEBYRANK(map, keyPtr, keySize, startRank, stopRank);
    UnsafeAccess.free(keyPtr);
    return result;
  }
  
  /**
   * Available since 2.0.0.
   * Time complexity: O(log(N)+M) with N being the number of elements in the sorted set and M 
   * the number of elements removed by the operation.
   * Removes all elements in the sorted set stored at key with rank between start and stop. 
   * Both start and stop are 0 -based indexes with 0 being the element with the lowest score. 
   * These indexes can be negative numbers, where they indicate offsets starting at the element 
   * with the highest score. For example: -1 is the element with the highest score, -2 the element 
   * with the second highest score and so forth.
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @param startRank start rank
   * @param stopRank stop rank
   * @return number of elements removed
   */
  public static long ZREMRANGEBYRANK (BigSortedMap map, long keyPtr, int keySize, long startRank, 
      long stopRank)
  {
 
    long cardinality = ZCARD(map, keyPtr, keySize);   
    if (cardinality == 0) {
      return 0;
    }
    if (startRank < 0) {
      startRank += cardinality;
    }
    if (stopRank < 0) {
      stopRank += cardinality;
    }
    if (startRank < 0 || stopRank < 0) {
      return 0;
    } else if (startRank > stopRank) {
      return 0;
    } else if (startRank >= cardinality) {
      return 0;
    }
    
    long deleted = 0;
    // TODO this operation can be optimized
    SetScanner scanner = null;
    int maxCompactSize = RedisConf.getInstance().getMaxZSetCompactSize();
    boolean normalMode = cardinality >= maxCompactSize;
    long startPtr = 0;
    int startSize = 0;
    long buffer = valueArena.get();
    int bufferSize = valueArenaSize.get();
    // Clear first 4 bytes
    UnsafeAccess.putInt(buffer, 0);
    long ptr = buffer + Utils.SIZEOF_INT;

    Key key = getKey(keyPtr, keySize);
    
    try {
      KeysLocker.writeLock(key);
      scanner = Sets.getScanner(map, keyPtr, keySize, false);
      if (scanner == null) {
        return 0;
      }
      long cc = startRank;
      int count = 0;
      
      scanner.skipTo(startRank);
      
      while (scanner.hasNext() && (cc <= stopRank)) {
        long mPtr = 0;
        int mSize = 0;
        if (cc >= startRank) {
          deleted++;
          mPtr = scanner.memberAddress();
          mSize = scanner.memberSize();
          int mSizeSize = Utils.sizeUVInt(mSize);
          // copy to bulk delete buffer
          if (ptr + mSize + mSizeSize > buffer + bufferSize) {
            // Save current score-member pair into aux arena
            checkAuxArena(mSize + mSizeSize);
            long buf = auxArena.get();
            Utils.writeUVInt(buf, mSize);
            UnsafeAccess.copy(mPtr, buf + mSizeSize, mSize);
            // Close scanner first
            scanner.close();
            scanner = null;
            bulkDelete(map, buffer, keyPtr, keySize, normalMode);
            ptr = buffer + Utils.SIZEOF_INT;
            if (ptr + mSize + mSize > buffer + bufferSize) {
              checkValueArena(2 * (mSizeSize + mSize));
              buffer = valueArena.get();
              bufferSize = valueArenaSize.get();
              ptr = buffer + Utils.SIZEOF_INT;
            }
            count = 1;
            Utils.writeUVInt(ptr, mSize);
            // Copy from aux arena
            UnsafeAccess.copy(buf + mSizeSize, ptr + mSizeSize, mSize);
          } else {
            count++;
            Utils.writeUVInt(ptr, mSize);
            UnsafeAccess.copy(mPtr, ptr + mSizeSize, mSize);
          }
          UnsafeAccess.putInt(buffer,  count);
          ptr += mSize + mSizeSize;
        }
        
        if (scanner != null) {
          scanner.next();
        } else {
          // free start pointer
          if (startPtr > 0) {
            UnsafeAccess.free(startPtr);
          }
          // Get current score and member from aux buffer
          long buf = auxArena.get();
          startSize = Utils.readUVInt(buf);
          int sizeSize = Utils.sizeUVInt(startSize);
          startPtr = UnsafeAccess.malloc(startSize);
          UnsafeAccess.copy(buf + sizeSize, startPtr, startSize);
          // Advance start a bit
          startPtr = Utils.prefixKeyEndNoAlloc(startPtr, startSize);
          // Create scanner
          scanner = 
              Sets.getScanner(map, keyPtr, keySize, startPtr, startSize, 0, 0, false);
          if (scanner == null) {
            break; 
          }
        }
        cc++;
      }
      if (scanner != null) {
        scanner.close();
        scanner = null;
      }
      // Delete from buffer
      bulkDelete(map, buffer, keyPtr, keySize, normalMode);
      if (normalMode && (cardinality - deleted < maxCompactSize)) {
        convertToCompactMode(map, keyPtr, keySize);
      }
      if (deleted > 0 && (cardinality - deleted >= maxCompactSize)) {
        //ZINCRCARD(map, keyPtr, keySize, -deleted);
        ZSETCARD(map, keyPtr, keySize, cardinality - deleted);
      } else if (cardinality - deleted == 0) {
        DELETE(map, keyPtr, keySize, false);
      }
      return deleted;
    } catch (IOException e) {
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
        }
      }
      if (startPtr > 0) {
        UnsafeAccess.free(startPtr);
      }
      KeysLocker.writeUnlock(key);
    }
    // Should not here, but just in case
    return deleted;
  }
  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @param minScore start score
   * @param minInclusive is start inclusive
   * @param maxScore end score
   * @param maxInclusive is end inclusive
   * @param offset offset 
   * @param limit limit
   * @param withScores with scores
   * @return total removed
   */
  
  public static long ZREMRANGEBYSCORE(BigSortedMap map, String key, double minScore, 
      boolean minInclusive, double maxScore, boolean maxInclusive){
    
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();    
    long result = ZREMRANGEBYSCORE(map, keyPtr, keySize, minScore, minInclusive, maxScore, 
      maxInclusive);    
    UnsafeAccess.free(keyPtr);
    return result;
  }
  /**
   * 
   * Available since 1.2.0.
   * Time complexity: O(log(N)+M) with N being the number of elements in the sorted set and M 
   * the number of elements removed by the operation.
   * Removes all elements in the sorted set stored at key with a score between min and max (inclusive).
   * Since version 2.1.6, min and max can be exclusive, following the syntax of ZRANGEBYSCORE.
   * 
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @param minScore start score
   * @param minInclusive start score inclusive?
   * @param maxScore stop score
   * @param maxInclusive stop score inclusive?
   * @return number of elements removed
   */
  public static long ZREMRANGEBYSCORE(BigSortedMap map, long keyPtr, int keySize, double minScore,
      boolean minInclusive, double maxScore, boolean maxInclusive) {

    if (minScore > maxScore) {
      return 0;
    }

    long startPtr = UnsafeAccess.malloc(Utils.SIZEOF_DOUBLE);
    Utils.doubleToLex(startPtr, minScore);
    int startSize = Utils.SIZEOF_DOUBLE;

    long stopPtr = UnsafeAccess.malloc(Utils.SIZEOF_DOUBLE);
    Utils.doubleToLex(stopPtr, maxScore);
    int stopSize = Utils.SIZEOF_DOUBLE;

    if (maxInclusive && maxScore < Double.MAX_VALUE) {
      stopPtr = Utils.prefixKeyEndNoAlloc(stopPtr, stopSize);
    }
    if (!minInclusive && minScore > -Double.MAX_VALUE) {
      startPtr = Utils.prefixKeyEndNoAlloc(startPtr, startSize);
    }

    long cardinality = ZCARD(map, keyPtr, keySize);
    int maxCompactSize = RedisConf.getInstance().getMaxZSetCompactSize();
    boolean normalMode = cardinality >= maxCompactSize;
    
    long deleted = 0;
    long buffer = valueArena.get();
    int bufferSize = valueArenaSize.get();
    // Clear first 4 bytes
    UnsafeAccess.putInt(buffer, 0);
    long ptr = buffer + Utils.SIZEOF_INT;
    SetScanner scanner = null;
    Key k = getKey(keyPtr, keySize);
    
    try {
      KeysLocker.writeLock(k);
      scanner =
          Sets.getScanner(map, keyPtr, keySize, startPtr, startSize, stopPtr, stopSize, false);
      if (scanner == null) { 
        return 0; 
      }
      int cc = 0;
      while (scanner.hasNext()) {
        deleted++;
        long mPtr = scanner.memberAddress();
        int mSize = scanner.memberSize();
        int mSizeSize = Utils.sizeUVInt(mSize);
        // copy to bulk delete buffer
        if (ptr + mSize + mSizeSize > buffer + bufferSize) {
          // Save current score-member pair into aux arena
          checkAuxArena(mSize + mSizeSize);
          long buf = auxArena.get();
          Utils.writeUVInt(buf, mSize);
          UnsafeAccess.copy(mPtr, buf + mSizeSize, mSize);
          // Done
          // close scanner first b/c it holds read lock
          scanner.close();
          scanner = null;
          bulkDelete(map, buffer, keyPtr, keySize, normalMode);
          // Reset bulk buffer pointer back
          ptr = buffer + Utils.SIZEOF_INT;
          if (ptr + mSize + mSize > buffer + bufferSize) {
            checkValueArena(2 * (mSizeSize + mSize));
            buffer = valueArena.get();
            bufferSize = valueArenaSize.get();
            ptr = buffer + Utils.SIZEOF_INT;
          }
          cc = 1;
          Utils.writeUVInt(ptr, mSize);
          // Copy from aux arena
          UnsafeAccess.copy(buf + mSizeSize, ptr + mSizeSize, mSize);
        } else {
          cc++;
          Utils.writeUVInt(ptr, mSize);
          UnsafeAccess.copy(mPtr, ptr + mSizeSize, mSize);
//          if (deleted == 1) {
//            /*DEBUG*/ System.out.println("ZREMRANGEBYSCORE start: score="+ Utils.lexToDouble(mPtr) 
//            + " field=" + Utils.toString(mPtr + mSizeSize + Utils.SIZEOF_DOUBLE, mSize - Utils.SIZEOF_DOUBLE));
//          }
        }
        
        UnsafeAccess.putInt(buffer, cc);
        ptr += mSize + mSizeSize;
        
        if (scanner != null) {
          scanner.next();
        } else {
          // free start pointer
          if (startPtr > 0) {
            UnsafeAccess.free(startPtr);
          }
          // Get current score and member from aux buffer
          long buf = auxArena.get();
          startSize = Utils.readUVInt(buf);
          int sizeSize = Utils.sizeUVInt(startSize);
          startPtr = UnsafeAccess.malloc(startSize);
          UnsafeAccess.copy(buf + sizeSize, startPtr, startSize);
          // Advance start a bit
          startPtr = Utils.prefixKeyEndNoAlloc(startPtr, startSize);
          // Create scanner
          scanner = 
              Sets.getScanner(map, keyPtr, keySize, startPtr, startSize, stopPtr, stopSize, false);
          if (scanner == null) { 
            break; 
          } 
        }
      }
      if (scanner != null) {
        scanner.close();
        scanner = null;
      }
      // Delete from buffer
      bulkDelete(map, buffer, keyPtr, keySize, normalMode);
      if (normalMode && (cardinality - deleted < maxCompactSize)) {
        convertToCompactMode(map, keyPtr, keySize);
      }
      if (deleted > 0 && (cardinality - deleted) >= maxCompactSize) {
        //ZINCRCARD(map, keyPtr, keySize, -deleted);
        ZSETCARD(map, keyPtr, keySize, cardinality - deleted);
      } else if (cardinality - deleted == 0) {
        DELETE(map, keyPtr, keySize, false);
      }
    } catch (IOException e) {
    } finally {
      try {
        if (scanner != null) {
          scanner.close();
        }
      } catch (IOException e) {
      }
      UnsafeAccess.free(startPtr);
      UnsafeAccess.free(stopPtr);
      KeysLocker.writeUnlock(k);
    }
    return deleted;
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @param start start offset inclusive
   * @param end end offset inclusive
   * @param withScores with scores?
   * @param bufSize buffer size
   * @return list of pairs {member, member} or {member, score}
   */
  public static List<Pair<String>> ZREVRANGE(BigSortedMap map, String key, long start, long end, 
      boolean withScores, int bufSize){
    
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long buffer = UnsafeAccess.malloc(bufSize);
    long result = ZREVRANGE(map, keyPtr, keySize, start, end, withScores, buffer, bufSize);
    List<Pair<String>> list = new ArrayList<Pair<String>>();
    if (result > bufSize || result <= 0) {
      return list;
    }
    
    int total = UnsafeAccess.toInt(buffer);
    long ptr = buffer + Utils.SIZEOF_INT;
    for (int i = 0; i < total; i++) {
      int mSize = Utils.readUVInt(ptr);
      int mSizeSize = Utils.sizeUVInt(mSize);
      String member = null;
      String sscore = null;
      if (withScores) {
        double score = Utils.lexToDouble(ptr + mSizeSize);
        sscore = Double.toString(score);
        member = Utils.toString(ptr + mSizeSize + Utils.SIZEOF_DOUBLE, mSize - Utils.SIZEOF_DOUBLE);
      } else {
        member =  Utils.toString(ptr + mSizeSize, mSize);
        sscore = member;
      }
      list.add(new Pair<String>(member, sscore));
      ptr += mSize + mSizeSize;
    }
    
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(buffer);
    return list;
  }
  
  /**
   * Returns the specified range of elements in the sorted set stored at key. The elements 
   * are considered to be ordered from the highest to the lowest score. Descending lexicographical 
   * order is used for elements with equal score.
   * Apart from the reversed ordering, ZREVRANGE is similar to ZRANGE.
   * Return value
   * Array reply: list of elements in the specified range (optionally with their scores, 
   * in case the WITHSCORES option is given).
   * 
   * Serialized format:
   * [LIST_SIZE] - 4 bytes
   * PAIR +
   *  PAIR : [VARINT][MEMBER][SCORE]
   * VARINT - size of a member in bytes
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @param start start offset
   * @param end end offset
   * @param withScores with scores?
   * @param long buffer buffer address
   * @param int bufferSize buffer size
   * @return total serialized size of a response, if it is greater than 
   *         bufferSize - repeat call with appropriately sized buffer 
   */
  public static long ZREVRANGE (BigSortedMap map, long keyPtr, int keySize, long start, long end, 
      boolean withScores, long buffer, int bufferSize)
  {
    Key key = getKey(keyPtr, keySize);
    SetScanner scanner = null;
    long ptr = 0;
    try {
      KeysLocker.readLock(key);      
      long cardinality = ZCARD(map, keyPtr, keySize);
      if (cardinality == 0) {
        return 0;
      }
      if (start < 0) {
        start = start + cardinality;
      }
      if (end < 0) {
        end = end + cardinality;
      }
      if (start > end || start >= cardinality) {
        return 0;
      }
      if (end >= cardinality) {
        end = cardinality - 1;
      }
      // Reverse scanner
      scanner = Sets.getScanner(map, keyPtr, keySize, false, true);
      if (scanner == null) {
        return 0;
      }
      long counter = cardinality - 1;
      ptr = buffer + Utils.SIZEOF_INT;
      // Make sure first 4 bytes does not contain garbage
      UnsafeAccess.putInt(buffer, 0);
      do {
        if (counter < start) break;
        if (counter >= start && counter <= end) {
          int mSize = withScores? scanner.memberSize(): scanner.memberSize() - Utils.SIZEOF_DOUBLE;
          int mSizeSize = Utils.sizeUVInt(mSize);
          int adv = mSize + mSizeSize;
          if (ptr + adv <= buffer + bufferSize) {
            long mPtr = withScores? scanner.memberAddress(): scanner.memberAddress() + Utils.SIZEOF_DOUBLE;
            Utils.writeUVInt(ptr, mSize);
            UnsafeAccess.copy(mPtr, ptr + mSizeSize, mSize);
            UnsafeAccess.putInt(buffer, (int)(end - counter + 1));
          }
          ptr += adv;
        }
        counter--;
      } while(scanner.previous());
    } catch (IOException e) {
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
        }
      }
      KeysLocker.readUnlock(key);
    }
    return ptr - buffer;
 }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @param start start member
   * @param startInclusive is start inclusive
   * @param end end member
   * @param endInclusive is end inclusive
   * @param offset offset to start
   * @param limit limit
   * @param bufSize buffer size
   * @return list of members
   */
  public static List<Pair<String>> ZREVRANGEBYLEX(BigSortedMap map, String key, String start, 
      boolean startInclusive, String end, boolean endInclusive,long offset, long limit,  int bufSize){
    
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long startPtr = start != null? UnsafeAccess.allocAndCopy(start, 0, start.length()): 0;
    int startSize = start != null? start.length(): 0;
    long endPtr = end != null? UnsafeAccess.allocAndCopy(end, 0, end.length()): 0;
    int endSize = end != null? end.length(): 0;
    
    long buffer = UnsafeAccess.malloc(bufSize);
    long result = ZREVRANGEBYLEX(map, keyPtr, keySize, startPtr, startSize, startInclusive, endPtr, 
      endSize, endInclusive, offset, limit, buffer, bufSize);
    List<Pair<String>> list = new ArrayList<Pair<String>>();
    if (result > bufSize || result <= 0) {
      return list;
    }
    
    int total = UnsafeAccess.toInt(buffer);
    long ptr = buffer + Utils.SIZEOF_INT;
    for (int i = 0; i < total; i++) {
      int mSize = Utils.readUVInt(ptr);
      int mSizeSize = Utils.sizeUVInt(mSize);
      String member = null;
      String sscore = null;
      member =  Utils.toString(ptr + mSizeSize, mSize);
      sscore = member;
      list.add(new Pair<String>(member, sscore));
      ptr += mSize + mSizeSize;
    }
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(buffer);
    return list;
  } 
 /**
  * Available since 2.8.9.
  * Time complexity: O(log(N)+M) with N being the number of elements in the sorted set 
  * and M the number of elements being returned. If M is constant (e.g. always asking 
  * for the first 10 elements with LIMIT), you can consider it O(log(N)).
  * When all the elements in a sorted set are inserted with the same score, in order 
  * to force lexicographical ordering, this command returns all the elements in the sorted 
  * set at key with a value between max and min.
  * Apart from the reversed ordering, ZREVRANGEBYLEX is similar to ZRANGEBYLEX.
  * @param map sorted map storage
  * @param keyPtr sorted set key address
  * @param keySize sorted set key size
  * @param startPtr interval start address (0 - negative string infinity)
  * @param startSize interval start size
  * @param startInclusive is start inclusive?
  * @param endPtr interval end address (0 - positive string infinity)
  * @param endSize interval end size 
  * @param endInclusive is end inclusive?
  * @param offset offset index
  * @param count max count after offset
  * @param buffer buffer for return
  * @param bufferSize buffer size
  * @return total serialized size of a response, if it is greater than 
  *         bufferSize - repeat call with appropriately sized buffer 
  */
 public static long ZREVRANGEBYLEX(BigSortedMap map, long keyPtr, int keySize, long startPtr, int startSize, 
     boolean startInclusive, long endPtr, int endSize, boolean endInclusive, long offset, long limit, 
     long buffer, int bufferSize)
 {
   if (endInclusive && endPtr > 0) {
     endPtr = Utils.prefixKeyEndNoAlloc(endPtr, endSize);
   }
   if (!startInclusive && startPtr > 0) {
     startPtr = Utils.prefixKeyEndNoAlloc(startPtr, startSize);
   }
   Key key = getKey(keyPtr, keySize);
   SetScanner setScanner = null;
   long ptr = 0;
   int counter = 0;
   long cardinality = ZCARD(map, keyPtr, keySize);
   if (cardinality == 0) {
     return 0;
   }
   if (offset < 0) {
     offset += cardinality;
     if (offset < 0) {
       offset = 0;
     }
   }
   if (limit < 0) {
     limit = Long.MAX_VALUE / 2; // VERY LARGE
   }
   try {
     KeysLocker.readLock(key);
     ptr = buffer + Utils.SIZEOF_INT;
     // make sure first 4 bytes does not contain garbage
     UnsafeAccess.putInt(buffer, 0);
     // Reverse scanner
     setScanner = Sets.getScanner(map, keyPtr, keySize, false, true);
     if (setScanner == null) {
       return 0;
     }
     long pos = cardinality - 1;
     do {
       if (pos < offset) {
         break;
       }
       if (pos >= offset + limit) {
         pos--;
         continue;
       }
       long mPtr = setScanner.memberAddress();
       int mSize = setScanner.memberSize();
       mPtr += Utils.SIZEOF_DOUBLE;
       mSize -= Utils.SIZEOF_DOUBLE;
       int res1 = startPtr == 0? 1: Utils.compareTo(mPtr, mSize, startPtr, startSize);
       int res2 = endPtr == 0? -1: Utils.compareTo(mPtr, mSize, endPtr, endSize);
       if (res2 < 0 && res1 >= 0) {  
         int mSizeSize = Utils.sizeUVInt(mSize);
         if (ptr + mSize + mSizeSize <= buffer + bufferSize) {
           counter++;
           Utils.writeUVInt(ptr, mSize);
           UnsafeAccess.copy(mPtr, ptr + mSizeSize, mSize);
           // Update count
           UnsafeAccess.putInt(buffer, counter);
         }
         ptr += mSize + mSizeSize;
       } else if (res1 < 0) {
         break;
       }
       pos--;
     } while(setScanner.previous());
   } catch (IOException e) {
   } finally {
     if (setScanner != null) {
       try {
        setScanner.close();
      } catch (IOException e) {
      }
     }
     KeysLocker.readUnlock(key);
   }
   return ptr - buffer;
 }

 /**
  * For testing only
  * @param map sorted map storage
  * @param key zset key
  * @param start start member
  * @param startInclusive is start inclusive
  * @param end end member
  * @param endInclusive is end inclusive
  * @param bufSize buffer size
  * @return list of members
  */
 public static List<Pair<String>> ZREVRANGEBYLEX(BigSortedMap map, String key, String start, 
     boolean startInclusive, String end, boolean endInclusive, int bufSize){
   
   long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
   int keySize = key.length();
   long startPtr = start != null? UnsafeAccess.allocAndCopy(start, 0, start.length()): 0;
   int startSize = start != null? start.length(): 0;
   long endPtr = end != null? UnsafeAccess.allocAndCopy(end, 0, end.length()): 0;
   int endSize = end != null? end.length(): 0;
   
   long buffer = UnsafeAccess.malloc(bufSize);
   long result = ZREVRANGEBYLEX(map, keyPtr, keySize, startPtr, startSize, startInclusive, endPtr, 
     endSize, endInclusive, buffer, bufSize);
   List<Pair<String>> list = new ArrayList<Pair<String>>();
   if (result > bufSize || result <= 0) {
     return list;
   }
   
   int total = UnsafeAccess.toInt(buffer);
   long ptr = buffer + Utils.SIZEOF_INT;
   for (int i = 0; i < total; i++) {
     int mSize = Utils.readUVInt(ptr);
     int mSizeSize = Utils.sizeUVInt(mSize);
     String member = null;
     String sscore = null;
     member =  Utils.toString(ptr + mSizeSize, mSize);
     sscore = member;
     list.add(new Pair<String>(member, sscore));
     ptr += mSize + mSizeSize;
   }
   UnsafeAccess.free(keyPtr);
   UnsafeAccess.free(buffer);
   return list;
 }
 /**
  * ZREVRANGEBYLEX without offset and count
  * @param map sorted map storage
  * @param keyPtr sorted set key address
  * @param keySize sorted set key size
  * @param startPtr interval start address (0 - negative string infinity)
  * @param startSize interval start size
  * @param startInclusive is start inclusive?
  * @param endPtr interval end address (0 - positive string infinity)
  * @param endSize interval end size 
  * @param endInclusive is end inclusive?
  * @param offset offset index
  * @param count max count after offset
  * @param buffer buffer for return
  * @param bufferSize buffer size
  * @return total serialized size of a response, if it is greater than 
  *         bufferSize - repeat call with appropriately sized buffer 
  */
 public static long ZREVRANGEBYLEX(BigSortedMap map, long keyPtr, int keySize, long startPtr, int startSize, 
     boolean startInclusive, long endPtr, int endSize, boolean endInclusive,  long buffer, int bufferSize)
 {
   if (endInclusive && endPtr > 0) {
     endPtr = Utils.prefixKeyEndNoAlloc(endPtr, endSize);
   }
   if (!startInclusive && startPtr > 0) {
     startPtr = Utils.prefixKeyEndNoAlloc(startPtr, startSize);
   }
   Key key = getKey(keyPtr, keySize);
   HashScanner hashScanner = null;
   SetScanner setScanner = null;
   long cardPtr = getCardinalityMember(keyPtr, keySize);
   
   long ptr = 0;
   int count = 0;
   ptr = buffer + Utils.SIZEOF_INT;
   // Make sure first 4 bytes does not contain garbage
   UnsafeAccess.putInt(buffer, 0);
   try {
     KeysLocker.readLock(key);
     // Reverse scanner
     hashScanner =
         Hashes.getScanner(map, keyPtr, keySize, startPtr, startSize, endPtr, endSize, false, true);
     if (hashScanner != null) {
       do {
         long fPtr = hashScanner.fieldAddress();
         int fSize = hashScanner.fieldSize();
         if (isCardinalityMember(fPtr, fSize, cardPtr)) {
           // WILL it run while()?
           continue;
         }
         int fSizeSize = Utils.sizeUVInt(fSize);
         if (ptr + fSize + fSizeSize <= buffer + bufferSize) {
           count++;
           Utils.writeUVInt(ptr, fSize);
           UnsafeAccess.copy(fPtr, ptr + fSizeSize, fSize);
           // Update count
           UnsafeAccess.putInt(buffer, count);
         }
         ptr += fSize + fSizeSize;
       } while(hashScanner.previous());
     } else {
       // Run through the Set
       setScanner = Sets.getScanner(map, keyPtr, keySize, false, true);
       if (setScanner == null) {
         return 0;
       }
       do {
         long mPtr = setScanner.memberAddress();
         int mSize = setScanner.memberSize();
         mPtr += Utils.SIZEOF_DOUBLE;
         mSize -= Utils.SIZEOF_DOUBLE;
         int res1 = startPtr == 0? 1: Utils.compareTo(mPtr, mSize, startPtr, startSize);
         int res2 = endPtr == 0? -1: Utils.compareTo(mPtr, mSize, endPtr, endSize);
         if (res2 < 0 && res1 >= 0) {  
           int mSizeSize = Utils.sizeUVInt(mSize);
           if (ptr + mSize + mSizeSize <= buffer + bufferSize) {
             count++;
             Utils.writeUVInt(ptr, mSize);
             UnsafeAccess.copy(mPtr, ptr + mSizeSize, mSize);
             // Update count
             UnsafeAccess.putInt(buffer, count);
           }
           ptr += mSize + mSizeSize;
         } else if (res1 < 0){
           break;
         }
       } while(setScanner.previous());
     }
   } catch (IOException e) {
   } finally {
     try {
       if (hashScanner != null) {
         hashScanner.close();
       }
       if (setScanner != null) {
         setScanner.close();
       }
     } catch (IOException e) {
     }
     if (cardPtr > 0) {
       UnsafeAccess.free(cardPtr);
     }
     KeysLocker.readUnlock(key);
   }
   return ptr - buffer;
 }
 
 /**
  * For testing only
  * @param map sorted map storage
  * @param key zset key
  * @param minScore start score
  * @param minInclusive is start inclusive
  * @param maxScore end score
  * @param maxInclusive is end inclusive
  * @param offset offset 
  * @param limit limit
  * @param withScores with scores
  * @param bufSize buffer size
  * @return list of pairs {member, member} or {member, score}
  */
 
 public static List<Pair<String>> ZREVRANGEBYSCORE(BigSortedMap map, String key, double minScore, 
     boolean minInclusive, double maxScore, boolean maxInclusive, long offset, long limit, 
     boolean withScores, int bufSize){
   
   long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
   int keySize = key.length();
   
   long buffer = UnsafeAccess.malloc(bufSize);
   long result = ZREVRANGEBYSCORE(map, keyPtr, keySize, minScore, minInclusive, maxScore, 
     maxInclusive, offset, limit, withScores, buffer, bufSize);
   List<Pair<String>> list = new ArrayList<Pair<String>>();
   if (result > bufSize || result <= 0) {
     return list;
   }
   
   int total = UnsafeAccess.toInt(buffer);
   long ptr = buffer + Utils.SIZEOF_INT;
   for (int i = 0; i < total; i++) {
     int mSize = Utils.readUVInt(ptr);
     int mSizeSize = Utils.sizeUVInt(mSize);
     String member = null;
     String sscore = null;
     if (withScores) {
       double score = Utils.lexToDouble(ptr + mSizeSize);
       sscore = Double.toString(score);
       member = Utils.toString(ptr + mSizeSize + Utils.SIZEOF_DOUBLE, mSize - Utils.SIZEOF_DOUBLE);
     } else {
       member =  Utils.toString(ptr + mSizeSize, mSize);
       sscore = member;
     }
     list.add(new Pair<String>(member, sscore));
     ptr += mSize + mSizeSize;
   }
   UnsafeAccess.free(keyPtr);
   UnsafeAccess.free(buffer);
   return list;
 }
 
  /**
   * 
   * Available since 2.2.0.
   * Time complexity: O(log(N)+M) with N being the number of elements in the sorted set 
   * and M the number of elements being returned. If M is constant (e.g. always asking 
   * for the first 10 elements with LIMIT), you can consider it O(log(N)).
   * Returns all the elements in the sorted set at key with a score between max and min 
   * (including elements with score equal to max or min). In contrary to the default ordering 
   * of sorted sets, for this command the elements are considered to be ordered from high to low scores.
   * The elements having the same score are returned in reverse lexicographical order.
   * Apart from the reversed ordering, ZREVRANGEBYSCORE is similar to ZRANGEBYSCORE.
   * 
   * -inf = -Double.MAX_VALUE
   * +inf = Double.MAX_VALUE
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @param minScore minimum score
   * @param minInclusive minimum score inclusive
   * @param maxScore maximum score
   * @param maxInclusive maximum score inclusive
   * @param offset offset (index) (default - 0)
   * @param limit - limit after offset (-1 - means till the last member)
   * @param withScores include scores?
   * @param buffer buffer address
   * @param bufferSize buffer size
   * @return total serialized size of a response, if it is greater than bufferSize - repeat call
   *         with appropriately sized buffer
   */
  public static long ZREVRANGEBYSCORE(BigSortedMap map, long keyPtr, int keySize, double minScore,
      boolean minInclusive, double maxScore, boolean maxInclusive, long offset, long limit,
      boolean withScores, long buffer, int bufferSize) {
    Key k = getKey(keyPtr, keySize);

    long cardinality = ZCARD(map, keyPtr, keySize);
    if (cardinality == 0 || offset >= cardinality) {
      return 0;
    }

    if (offset < 0) {
      offset = offset + cardinality;
      if (offset < 0) offset = 0;
    }
    if (limit < 0) {
      // Set very high value unreachable in a real life
      limit = Long.MAX_VALUE / 2;
    }
    int count = 0;
    long ptr = buffer + Utils.SIZEOF_INT;
    SetScanner scanner = null;
    try {
      KeysLocker.readLock(k);
      // Reverse scanner
      scanner = Sets.getScanner(map, keyPtr, keySize, false, true);
      if (scanner == null) {
        return 0;
      }
      long off = cardinality - 1;
      // Make sure first 4 bytes does not contain garbage
      UnsafeAccess.putInt(buffer, 0);
      do {
        if (off >= offset && off < (offset + limit)) {
          int mSize = scanner.memberSize();
          long mPtr = scanner.memberAddress();
          double score = Utils.lexToDouble(mPtr);
          if (between(score, minScore, maxScore, minInclusive, maxInclusive)) {
            if (!withScores) {
              mSize -= Utils.SIZEOF_DOUBLE;
              mPtr += Utils.SIZEOF_DOUBLE;
            }
            int mSizeSize = Utils.sizeUVInt(mSize);
            count++;
            //TODO: when buffer is full - stop
            if (ptr + mSize + mSizeSize <= buffer + bufferSize) {
              Utils.writeUVInt(ptr, mSize);
              UnsafeAccess.copy(mPtr, ptr + mSizeSize, mSize);
              // Update count
              UnsafeAccess.putInt(buffer, count);
            }
            ptr += mSize + mSizeSize;
          } else if (off < offset) {
            break;
          }
        }
        off--;
      } while (scanner.previous());
    } catch (IOException e) {
    } finally {
      try {
        if (scanner != null) {
          scanner.close();
        }
      } catch (IOException e) {
      }
      KeysLocker.readUnlock(k);
    }
    return ptr - buffer;

  }
 
  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @param minScore start score
   * @param minInclusive is start inclusive
   * @param maxScore end score
   * @param maxInclusive is end inclusive
   * @param offset offset 
   * @param limit limit
   * @param withScores with scores
   * @param bufSize buffer size
   * @return list of pairs {member, member} or {member, score}
   */
  
  public static List<Pair<String>> ZREVRANGEBYSCORE(BigSortedMap map, String key, double minScore, 
      boolean minInclusive, double maxScore, boolean maxInclusive,  
      boolean withScores, int bufSize){
    
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
  
    long buffer = UnsafeAccess.malloc(bufSize);
    long result = ZREVRANGEBYSCORE(map, keyPtr, keySize, minScore, minInclusive, maxScore, 
      maxInclusive, withScores, buffer, bufSize);
    List<Pair<String>> list = new ArrayList<Pair<String>>();
    if (result > bufSize || result <= 0) {
      return list;
    }
    
    int total = UnsafeAccess.toInt(buffer);
    long ptr = buffer + Utils.SIZEOF_INT;
    for (int i = 0; i < total; i++) {
      int mSize = Utils.readUVInt(ptr);
      int mSizeSize = Utils.sizeUVInt(mSize);
      String member = null;
      String sscore = null;
      if (withScores) {
        double score = Utils.lexToDouble(ptr + mSizeSize);
        sscore = Double.toString(score);
        member = Utils.toString(ptr + mSizeSize + Utils.SIZEOF_DOUBLE, mSize - Utils.SIZEOF_DOUBLE);
      } else {
        member =  Utils.toString(ptr + mSizeSize, mSize);
        sscore = member;
      }
      list.add(new Pair<String>(member, sscore));
      ptr += mSize + mSizeSize;
    }
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(buffer);
    return list;
  }
  /**
   * 
   * Available since 2.2.0.
   * Time complexity: O(log(N)+M) with N being the number of elements in the sorted set 
   * and M the number of elements being returned. If M is constant (e.g. always asking 
   * for the first 10 elements with LIMIT), you can consider it O(log(N)).
   * Returns all the elements in the sorted set at key with a score between max and min 
   * (including elements with score equal to max or min). In contrary to the default ordering 
   * of sorted sets, for this command the elements are considered to be ordered from high to low scores.
   * The elements having the same score are returned in reverse lexicographical order.
   * Apart from the reversed ordering, ZREVRANGEBYSCORE is similar to ZRANGEBYSCORE.
   * 
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @param minScore minimum score
   * @param minInclusive minimum score inclusive
   * @param maxScore maximum score
   * @param maxInclusive maximum score inclusive
   * @param offset offset (index) (default - 0)
   * @param limit - limit after offset (-1 - means till the last member)
   * @param withScores include scores?
   * @param buffer buffer address
   * @param bufferSize buffer size
   * @return total serialized size of a response, if it is greater than bufferSize - repeat call
   *         with appropriately sized buffer
   */
  public static long ZREVRANGEBYSCORE(BigSortedMap map, long keyPtr, int keySize, double minScore,
      boolean minInclusive, double maxScore, boolean maxInclusive, boolean withScores, long buffer,
      int bufferSize) {

    if (minScore > maxScore) {
      return 0;
    }
    Key k = getKey(keyPtr, keySize);
    long startPtr = 0, stopPtr = 0;
    SetScanner scanner = null;
    long ptr = buffer + Utils.SIZEOF_INT;

    try {
      KeysLocker.readLock(k);
      startPtr = UnsafeAccess.malloc(Utils.SIZEOF_DOUBLE);
      Utils.doubleToLex(startPtr, minScore);
      int startSize = Utils.SIZEOF_DOUBLE;
      stopPtr = UnsafeAccess.malloc(Utils.SIZEOF_DOUBLE);
      Utils.doubleToLex(stopPtr, maxScore);
      int stopSize = Utils.SIZEOF_DOUBLE;
      if (maxInclusive && maxScore < Double.MAX_VALUE) {
        stopPtr = Utils.prefixKeyEndNoAlloc(stopPtr, stopSize);
      }
      if (!minInclusive && minScore > -Double.MAX_VALUE) {
        startPtr = Utils.prefixKeyEndNoAlloc(startPtr, startSize);
      }
      scanner = Sets.getScanner(map, keyPtr, keySize, startPtr, startSize, stopPtr, stopSize, false,
        true);
      if (scanner == null) {
        return 0;
      }
      // Make sure first 4 bytes does not contain garbage
      UnsafeAccess.putInt(buffer, 0);
      int count = 0;
      do {
        int mSize = scanner.memberSize();
        long mPtr = scanner.memberAddress();
        if (!withScores) {
          mSize -= Utils.SIZEOF_DOUBLE;
          mPtr += Utils.SIZEOF_DOUBLE;
        }
        int mSizeSize = Utils.sizeUVInt(mSize);
        count++;
        //TODO: when buffer is full - finish execution
        if (ptr + mSize + mSizeSize <= buffer + bufferSize) {
          Utils.writeUVInt(ptr, mSize);
          UnsafeAccess.copy(mPtr, ptr + mSizeSize, mSize);
          // Update count
          UnsafeAccess.putInt(buffer, count);
        }
        ptr += mSize + mSizeSize;
      } while (scanner.previous());
    } catch (IOException e) {
      // should not be here
    } finally {
      try {
        if (scanner != null) {
          scanner.close();
        }
      } catch (IOException e) {
      }
      if (startPtr > 0) {
        UnsafeAccess.free(startPtr);
      }
      if (stopPtr > 0) {
        UnsafeAccess.free(stopPtr);
      }
      KeysLocker.readUnlock(k);
    }
    return ptr - buffer;
  }

  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @param member zset member
   * @return rank (0 - based) or -1 if not exists
   */
  public static long ZREVRANK(BigSortedMap map, String key, String member) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long memberPtr = UnsafeAccess.allocAndCopy(member, 0, member.length());
    int memberSize = member.length();
    long result = ZREVRANK(map, keyPtr, keySize, memberPtr, memberSize);
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(memberPtr);
    return result;
  }
  
 /**
  * Available since 2.0.0.
  * Time complexity: O(log(N))
  * Returns the rank of member in the sorted set stored at key, with the scores ordered 
  * from high to low. The rank (or index) is 0-based, which means that the member with 
  * the highest score has rank 0.
  * Use ZRANK to get the rank of an element with the scores ordered from low to high.
  * Return value
  * If member exists in the sorted set, Integer reply: the rank of member.
  * If member does not exist in the sorted set or key does not exist, Bulk string reply: nil.
  * 
  * @param map sorted map storage
  * @param keyPtr sorted set key address
  * @param keySize sorted set key size
  * @param memberPtr member name address 
  * @param memberSize member name size
  * @return rank (0- based), highest has the lowest rank
  *         -1 if key or member do not exist
  */
 public static long ZREVRANK(BigSortedMap map, long keyPtr, int keySize, long memberPtr, int memberSize) {
   // TODO: Optimize
   long rank = 0;
   Key key = getKey(keyPtr, keySize);
   // TODO this operation can be optimized
   SetScanner scanner = null;
   try {
     KeysLocker.readLock(key);
     scanner = Sets.getScanner(map, keyPtr, keySize, false, true);
     if (scanner == null) {
       return -1;
     }
     do {
       long mPtr = scanner.memberAddress();
       int mSize = scanner.memberSize();
       // First 8 bytes - score, skip it
       if (Utils.compareTo(mPtr + Utils.SIZEOF_DOUBLE, mSize - Utils.SIZEOF_DOUBLE, 
         memberPtr, memberSize) == 0) {
         return rank;
       }
       rank++;
     } while (scanner.previous());
   } catch (IOException e) {
   } finally {
     if (scanner != null) {
       try {
         scanner.close();
       } catch (IOException e) {
       }
     }
     KeysLocker.readUnlock(key);
   }
   return -1;
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
  * However while blocking commands like SMEMBERS are able to provide all the elements that are part 
  * of a Set in a given moment, The SCAN family of commands only offer limited guarantees about the 
  * returned elements since the collection that we incrementally iterate can change during 
  * the iteration process.
  * Note that SCAN, SSCAN, HSCAN and ZSCAN all work very similarly, so this documentation covers 
  * all the four commands. However an obvious difference is that in the case of SSCAN, HSCAN and 
  * ZSCAN the first argument is the name of the key holding the Set, Hash or Sorted Set value. 
  * The SCAN command does not need any key name argument as it iterates keys in the current database, 
  * so the iterated object is the database itself.
  * 
  * SCAN basic usage
  * 
  * SCAN is a cursor based iterator. This means that at every call of the command, the server returns 
  * an updated cursor that the user needs to use as the cursor argument in the next call.
  * An iteration starts when the cursor is set to 0, and terminates when the cursor returned by the server is 0. 
  * The following is an example of SCAN iteration:
  * 
  * redis 127.0.0.1:6379> scan 0
  * 
  * 1) "17"
  * 2)  1) "key:12"
  *     2) "key:8"
  *     3) "key:4"
  *     4) "key:14"
  *     5) "key:16"
  *     6) "key:17"
  *     7) "key:15"
  *     8) "key:10"
  *     9) "key:3"
  *     10) "key:7"
  *     11) "key:1"
  * 
  * redis 127.0.0.1:6379> scan 17
  * 
  * 1) "0"
  * 2) 1) "key:5"
  *    2) "key:18"
  *    3) "key:0"
  *    4) "key:2"
  *    5) "key:19"
  *    6) "key:13"
  *    7) "key:6"
  *    8) "key:9"
  *    9) "key:11"
  * In the example above, the first call uses zero as a cursor, to start the iteration. The second call 
  * uses the cursor returned by the previous call as the first element of the reply, that is, 17.
  * As you can see the SCAN return value is an array of two values: the first value is the new cursor 
  * to use in the next call, the second value is an array of elements.
  * Since in the second call the returned cursor is 0, the server signaled to the caller that 
  * the iteration finished, and the collection was completely explored. Starting an iteration with 
  * a cursor value of 0, and calling SCAN until the returned cursor is 0 again is called a full iteration.
  *
  * Scan guarantees
  * 
  * The SCAN command, and the other commands in the SCAN family, are able to provide to the user a set
  *  of guarantees associated to full iterations.
  * A full iteration always retrieves all the elements that were present in the collection from 
  * the start to the end of a full iteration. This means that if a given element is inside the collection
  *  when an iteration is started, and is still there when an iteration terminates, then at some point 
  *  SCAN returned it to the user.
  * A full iteration never returns any element that was NOT present in the collection from the start to 
  * the end of a full iteration. So if an element was removed before the start of an iteration, and is 
  * never added back to the collection for all the time an iteration lasts, SCAN ensures that this element
  *  will never be returned.
  * However because SCAN has very little state associated (just the cursor) it has the following drawbacks:
  * 
  * A given element may be returned multiple times. It is up to the application to handle the case of 
  * duplicated elements, for example only using the returned elements in order to perform operations 
  * that are safe when re-applied multiple times.
  * Elements that were not constantly present in the collection during a full iteration, may be returned 
  * or not: 
  * it is undefined.
  * 
  * Number of elements returned at every SCAN call
  * 
  * SCAN family functions do not guarantee that the number of elements returned per call are in a given 
  * range. The commands are also allowed to return zero elements, and the client should not consider 
  * the iteration complete as long as the returned cursor is not zero.
  * However the number of returned elements is reasonable, that is, in practical terms SCAN may return 
  * a maximum number of elements in the order of a few tens of elements when iterating a large collection,
  *  or may return all the elements of the collection in a single call when the iterated collection is 
  *  small enough to be internally represented as an encoded data structure (this happens for small sets,
  *   hashes and sorted sets).
  * However there is a way for the user to tune the order of magnitude of the number of returned elements 
  * per call using the COUNT option.
  * 
  * The COUNT option
  *
  * While SCAN does not provide guarantees about the number of elements returned at every iteration, it 
  * is possible to empirically adjust the behavior of SCAN using the COUNT option. Basically with COUNT 
  * the user specified the amount of work that should be done at every call in order to retrieve elements 
  * from the collection. This is just a hint for the implementation, however generally speaking this is 
  * what you could expect most of the times from the implementation.
  * The default COUNT value is 10.
  * When iterating the key space, or a Set, Hash or Sorted Set that is big enough to be represented by a 
  * hash table, assuming no MATCH option is used, the server will usually return count or a bit more than 
  * count elements per call. Please check the why SCAN may return all the elements at once section later 
  * in this document.
  * When iterating Sets encoded as intsets (small sets composed of just integers), or Hashes and Sorted Sets 
  * encoded as ziplists (small hashes and sets composed of small individual values), usually all the elements 
  * are returned in the first SCAN call regardless of the COUNT value.
  * Important: there is no need to use the same COUNT value for every iteration. The caller is free to change 
  * the count from one iteration to the other as required, as long as the cursor passed in the next call is 
  * the one obtained in the previous call to the command.
  * 
  * The MATCH option
  * 
  * It is possible to only iterate elements matching a given glob-style pattern, similarly to the behavior 
  * of the KEYS command that takes a pattern as only argument.
  * To do so, just append the MATCH <pattern> arguments at the end of the SCAN command (it works with all 
  * the SCAN family commands).
  * This is an example of iteration using MATCH:
  * 
  * redis 127.0.0.1:6379> sadd myset 1 2 3 foo foobar feelsgood
  * (integer) 6
  * redis 127.0.0.1:6379> sscan myset 0 match f*
  * 1) "0"
  * 2) 1) "foo"
  *    2) "feelsgood"
  *    3) "foobar"
  * redis 127.0.0.1:6379>
  * 
  * It is important to note that the MATCH filter is applied after elements are retrieved from the collection,
  *  just before returning data to the client. This means that if the pattern matches very little elements 
  *  inside the collection, SCAN will likely return no elements in most iterations. An example is shown below:
  *  
  * redis 127.0.0.1:6379> scan 0 MATCH *11*
  * 1) "288"
  * 2) 1) "key:911"
  * 
  * redis 127.0.0.1:6379> scan 288 MATCH *11*
  * 1) "224"
  * 2) (empty list or set)
  * 
  * redis 127.0.0.1:6379> scan 224 MATCH *11*
  * 1) "80"
  * 2) (empty list or set)
  * redis 127.0.0.1:6379> scan 80 MATCH *11*
  * 1) "176"
  * 2) (empty list or set)
  * redis 127.0.0.1:6379> scan 176 MATCH *11* COUNT 1000
  * 1) "0"
  * 2)  1) "key:611"
  *     2) "key:711"
  *     3) "key:118"
  *     4) "key:117"
  *     5) "key:311"
  *     6) "key:112"
  *     7) "key:111"
  *     8) "key:110"
  *     9) "key:113"
  *    10) "key:211"
  *    11) "key:411"
  *    12) "key:115"
  *    13) "key:116"
  *    14) "key:114"
  *    15) "key:119"
  *    16) "key:811"
  *    17) "key:511"
  *   18) "key:11"
  * redis 127.0.0.1:6379>
  *
  * As you can see most of the calls returned zero elements, but the last call where a COUNT of 1000 was used
  *  in order to force the command to do more scanning for that iteration.
  * 
  * The TYPE option
  * 
  * As of version 6.0 you can use this option to ask SCAN to only return objects that match a given type, 
  * allowing you to iterate through the database looking for keys of a specific type. The TYPE option is 
  * only available on the whole-database SCAN, not HSCAN or ZSCAN etc.
  * The type argument is the same string name that the TYPE command returns. Note a quirk where some Redis
  *  types, such as GeoHashes, HyperLogLogs, Bitmaps, and Bitfields, may internally be implemented using 
  *  other Redis types, such as a string or zset, so can't be distinguished from other keys of that same 
  *  type by SCAN. For example, a ZSET and GEOHASH:
  *  
  * redis 127.0.0.1:6379> GEOADD geokey 0 0 value
  * (integer) 1
  * redis 127.0.0.1:6379> ZADD zkey 1000 value
  * (integer) 1
  * redis 127.0.0.1:6379> TYPE geokey
  * zset
  * redis 127.0.0.1:6379> TYPE zkey
  * zset
  * redis 127.0.0.1:6379> SCAN 0 TYPE zset
  * 1) "0"
  * 2) 1) "geokey"
  *    2) "zkey"
  * It is important to note that the TYPE filter is also applied after elements are retrieved from the database, 
  * so the option does not reduce the amount of work the server has to do to complete a full iteration, and 
  * for rare types you may receive no elements in many iterations.
  * 
  * Multiple parallel iterations
  * 
  * It is possible for an infinite number of clients to iterate the same collection at the same time, 
  * as the full state of the iterator is in the cursor, that is obtained and returned to the client 
  * at every call. Server side no state is taken at all.
  * 
  * Terminating iterations in the middle
  * 
  * Since there is no state server side, but the full state is captured by the cursor, the caller is 
  * free to terminate an iteration half-way without signaling this to the server in any way. An infinite 
  * number of iterations can be started and never terminated without any issue.
  * 
  * Calling SCAN with a corrupted cursor
  * 
  * Calling SCAN with a broken, negative, out of range, or otherwise invalid cursor, will result into 
  * undefined behavior but never into a crash. What will be undefined is that the guarantees about 
  * the returned elements can no longer be ensured by the SCAN implementation.
  * The only valid cursors to use are:
  * The cursor value of 0 when starting an iteration.
  * The cursor returned by the previous call to SCAN in order to continue the iteration.
  * 
  * Guarantee of termination
  * 
  * The SCAN algorithm is guaranteed to terminate only if the size of the iterated collection remains 
  * bounded to a given maximum size, otherwise iterating a collection that always grows may result into 
  * SCAN to never terminate a full iteration.
  * This is easy to see intuitively: 
  * if the collection grows there is more and more work to do in order to visit all the possible elements,
  *  and the ability to terminate the iteration depends on the number of calls to SCAN and its COUNT 
  *  option value compared with the rate at which the collection grows.
  *  
  * Why SCAN may return all the items of an aggregate data type in a single call?
  * 
  * In the COUNT option documentation, we state that sometimes this family of commands may return all 
  * the elements of a Set, Hash or Sorted Set at once in a single call, regardless of the COUNT option value.
  *  The reason why this happens is that the cursor-based iterator can be implemented, and is useful, 
  *  only when the aggregate data type that we are scanning is represented as an hash table. However 
  *  Redis uses a memory optimization where small aggregate data types, until they reach a given amount 
  *  of items or a given max size of single elements, are represented using a compact single-allocation
  *   packed encoding. When this is the case, SCAN has no meaningful cursor to return, and must iterate 
  *   the whole data structure at once, so the only sane behavior it has is to return everything in a call.
  * However once the data structures are bigger and are promoted to use real hash tables, the SCAN family of 
  * commands will resort to the normal behavior. Note that since this special behavior of returning all 
  * the elements is true only for small aggregates, it has no effects on the command complexity or latency.
  *  However the exact limits to get converted into real hash tables are user configurable, so the maximum 
  *  number of elements you can see returned in a single call depends on how big an aggregate data type could
  *   be and still use the packed representation.
  * Also note that this behavior is specific of SSCAN, HSCAN and ZSCAN. SCAN itself never shows this 
  * behavior because the key space is always represented by hash tables.
  * Return value
  * SCAN, SSCAN, HSCAN and ZSCAN return a two elements multi-bulk reply, where the first element is 
  * a string representing an unsigned 64 bit number (the cursor), and the second element is a 
  * multi-bulk with an array of elements.
  * 
  * SCAN array of elements is a list of keys.
  * SSCAN array of elements is a list of Set members.
  * HSCAN array of elements contain two elements, a field and a value, for every returned element of the Hash.
  * ZSCAN array of elements contain two elements, a member and its associated score, for every returned 
  * element of the sorted set.
  * 
  * History
  * 
  * >= 6.0: Supports the TYPE subcommand.
  * 
  * Additional examples
  * Iteration of a Hash value.
  * redis 127.0.0.1:6379> hmset hash name Jack age 33
  * OK
  * redis 127.0.0.1:6379> hscan hash 0
  * 1) "0"
  * 2) 1) "name"
  *    2) "Jack"
  *    3) "age"
  *    4) "33"
  *    
  * @param map sorted map storage
  * @param keyPtr sorted set key address
  * @param keySize sorted set key size
  * @param lastSeenMemberPtr last seen member (INCLUDES 8 bytes Score)
  * @param lastSeenMemberSize last seen member name size
  * @param count count to return in a single call
  * @param buffer for response
  * @param bufferSize buffer size
  * @return total serialized size of the response.  
  *     
  */
 public static long ZSCAN(BigSortedMap map, long keyPtr, int keySize, long lastSeenMemberPtr,
     int lastSeenMemberSize, int count, String regex, long buffer, int bufferSize) {
   Key key = getKey(keyPtr, keySize);
   try {
     KeysLocker.readLock(key);

     long result = Sets.SSCAN(map, keyPtr, keySize, lastSeenMemberPtr, lastSeenMemberSize, count,
       buffer, bufferSize, regex, Utils.SIZEOF_DOUBLE);
     return result;
   } finally {
     KeysLocker.readUnlock(key);
   }
 }
 
 /**
  * For testing only
  * @param map sorted map storage
  * @param key set's key
  * @param lastSeenMember last seen member
  * @param count number of members to return
  * @param bufferSize recommended buffer size
  * @return list of members (with scores concatenated)
  */
 public static List<Pair<String>> ZSCAN(BigSortedMap map, String key, double lastSeenScore, 
     String lastSeenMember, 
     int count, int bufferSize, String regex){
   
   long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
   int keySize = key.length();
   long lastSeenPtr = lastSeenMember == null? 0: 
     UnsafeAccess.malloc(lastSeenMember.length() + Utils.SIZEOF_DOUBLE);
   if (lastSeenPtr > 0) {
     Utils.doubleToLex(lastSeenPtr, lastSeenScore);
     UnsafeAccess.copy(lastSeenMember.getBytes(), 0, 
       lastSeenPtr + Utils.SIZEOF_DOUBLE, lastSeenMember.length());
   }
   int lastSeenSize = lastSeenMember == null? 0: lastSeenMember.length() + Utils.SIZEOF_DOUBLE;
   long buffer = UnsafeAccess.malloc(bufferSize);

   List<Pair<String>> list = new ArrayList<Pair<String>>();

   // Clear first 4 bytes of a buffer
   UnsafeAccess.putInt(buffer, 0);
   long totalSize = ZSCAN(map, keyPtr, keySize, lastSeenPtr, lastSeenSize, count, regex, buffer, bufferSize);
   if (totalSize == 0) {
     return null;
   }
   int total = UnsafeAccess.toInt(buffer);
   if (total == 0) {
     return null;
   }
   long ptr = buffer + Utils.SIZEOF_INT;
   // the last is going to be last seen member (duplicate if regex == null)
   for (int i=0; i < total; i++) {
     int size = Utils.readUVInt(ptr);
     int sizeSize = Utils.sizeUVInt(size);
     double score = Utils.lexToDouble(ptr + sizeSize);
     String sscore = Double.toString(score);
     String member = Utils.toString(ptr + sizeSize + Utils.SIZEOF_DOUBLE, size - Utils.SIZEOF_DOUBLE);
     list.add( new Pair<String>(member, sscore));
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
  * For testing only
  * @param map sorted map storage
  * @param key zset key
  * @param member zset member
  * @return score or null if not exists
  */
 
 public static Double ZSCORE(BigSortedMap map, String key, String member) {
   long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
   int keySize= key.length();
   long memberPtr = UnsafeAccess.allocAndCopy(member, 0, member.length());
   int memberSize = member.length();
   
   Double result = ZSCORE(map, keyPtr, keySize, memberPtr, memberSize);
   
   UnsafeAccess.free(keyPtr);
   UnsafeAccess.free(memberPtr);
   return result;
 }
 
 /**
  * 
  * Returns the score of member in the sorted set at key.
  * If member does not exist in the sorted set, or key does not exist, nil is returned.
  * 
  * @param map sorted map storage
  * @param keyPtr sorted set key address
  * @param keySize sorted set key size
  * @param memberPtr member name address
  * @param memberSize member name size
  * @return score or NULL
  */
 
 public static Double ZSCORE(BigSortedMap map, long keyPtr, int keySize, long memberPtr,
     int memberSize) {
   Key key = getKey(keyPtr, keySize);
   try {
     KeysLocker.readLock(key);
     long cardinality = ZCARD(map, keyPtr, keySize, false);
     RedisConf conf = RedisConf.getInstance();
     long maxCompactSize = conf.getMaxZSetCompactSize();
     if (cardinality < maxCompactSize) {
       // Scan Set to search member
       // TODO: Reverse search?
       SetScanner scanner = Sets.getScanner(map, keyPtr, keySize, false);
       if (scanner == null) {
         return null;
       }
       try {
         while (scanner.hasNext()) {
           long ptr = scanner.memberAddress();
           int size = scanner.memberSize();
           // First 8 bytes of a member is double score
           int res = Utils.compareTo(ptr + Utils.SIZEOF_DOUBLE, size - Utils.SIZEOF_DOUBLE,
             memberPtr, memberSize);
           if (res == 0) {
             return Utils.lexToDouble(ptr);
           }
           scanner.next();
         }
       } catch (IOException e) {
       } finally {
         try {
           if (scanner != null) {
             scanner.close();
           }
         } catch (IOException e) {
         }
       }
       return null;
     } else {
       // Get score from Hash
       int size = Hashes.HGET(map, keyPtr, keySize, memberPtr, memberSize, valueArena.get(),
         valueArenaSize.get(), false);
       if (size < 0) {
         return null;
       }
       return Utils.lexToDouble(valueArena.get());
     }
   } finally {
     KeysLocker.readUnlock(key);
   }
 }
 
 /**
  * 
  * TODO: optimize for speed
  * 
  * Returns the scores associated with the specified members in the sorted set stored at key.
  * For every member that does not exist in the sorted set, a nil value is returned.
  * Return value
  * Array reply: list of scores or nil associated with the specified member values 
  * (a double precision floating point number), represented as strings.
  * 
  * @param map sorted map storage
  * @param keyPtr key address
  * @param keySize key size
  * @param memberPtrs array of member pointers
  * @param memberSizes array of member sizes
  * @param bufferPtr buffer 
  * @param bufferSize buffer size
  * @return total serialized size of a response
  */
 public static long ZMSCORE(BigSortedMap map, long keyPtr, int keySize, long[] memberPtrs, int[] memberSizes, 
     long bufferPtr, int bufferSize) {
   
   long ptr = bufferPtr + Utils.SIZEOF_INT;
   long max = bufferPtr + bufferSize;
   int count = 0;
   
   for (int i = 0; i < memberPtrs.length; i++) {
     Double d = ZSCORE(map, keyPtr, keySize, memberPtrs[i], memberSizes[i]);
     if (d != null) {
       int len = Utils.doubleToStr(d.doubleValue(), ptr + Utils.SIZEOF_INT, (int) (max - ptr - Utils.SIZEOF_INT));
       if (len + Utils.SIZEOF_INT <= max - ptr) {
         UnsafeAccess.putInt(ptr, len);
         count++;
       }
       ptr += Utils.SIZEOF_INT + len;
     } else {
       if (Utils.SIZEOF_INT <= max - ptr) {
         // NULL
         UnsafeAccess.putInt(ptr, -1);
         count++;
       }
       ptr += Utils.SIZEOF_INT;
     }
   }
   
   // Write number of elements
   UnsafeAccess.putInt(bufferPtr, count);
   // Return full serialized size of a response
   return ptr - bufferPtr;
   
 }
  /**
   * Available since 2.0.0. Time complexity: O(N)+O(M log(M)) with N being the sum of the sizes of
   * the input sorted sets, and M being the number of elements in the resulting sorted set. Computes
   * the union of numkeys sorted sets given by the specified keys, and stores the result in
   * destination. It is mandatory to provide the number of input keys (numkeys) before passing the
   * input keys and the other (optional) arguments. By default, the resulting score of an element is
   * the sum of its scores in the sorted sets where it exists. Using the WEIGHTS option, it is
   * possible to specify a multiplication factor for each input sorted set. This means that the
   * score of every element in every input sorted set is multiplied by this factor before being
   * passed to the aggregation function. When WEIGHTS is not given, the multiplication factors
   * default to 1. With the AGGREGATE option, it is possible to specify how the results of the union
   * are aggregated. This option defaults to SUM, where the score of an element is summed across the
   * inputs where it exists. When this option is set to either MIN or MAX, the resulting set will
   * contain the minimum or maximum score of an element across the inputs where it exists. If
   * destination already exists, it is overwritten. Return value Integer reply: the number of
   * elements in the resulting sorted set at destination.
   * @param map sorted map storage
   * @param dstKeyPtr destination key address
   * @param dstKeySize destination key size
   * @param keys sorted sets keys
   * @param keySizes sorted sets key sizes
   * @param weights corresponding weights
   * @param aggregate aggregate function
   * @return number of members in a destination set
   */
  public static long ZUNIONSTORE(BigSortedMap map, long dstKeyPtr, int dstKeySize, long[] keys,
      int[] keySizes, double[] weights, Aggregate aggregate) {
    return 0;
  }
  
  /**
   * 
   * Available since 5.0.0.
   * Time complexity: O(log(N)) with N being the number of elements in the sorted set.
   * BZPOPMIN is the blocking variant of the sorted set ZPOPMIN primitive.
   * It is the blocking version because it blocks the connection when there are no members to pop 
   * from any of the given sorted sets. A member with the lowest score is popped from first sorted 
   * set that is non-empty, with the given keys being checked in the order that they are given.
   * The timeout argument is interpreted as an integer value specifying the maximum number of seconds 
   * to block. A timeout of zero can be used to block indefinitely.
   * See the BLPOP documentation for the exact semantics, since BZPOPMIN is identical to BLPOP with 
   * the only difference being the data structure being popped from.
   * 
   * Return value
   * 
   * Array reply: specifically:
   * A nil multi-bulk when no element could be popped and the timeout expired.
   * A three-element multi-bulk with the first element being the name of the key where a member was popped, 
   * the second element is the popped member itself, and the third element is the score of the popped element.  
   * 
   * @param map sorted map storage
   * @param keys list of key addresses
   * @param sizes list of key sizes
   * @param timeout timeout in ms
   * @param buffer buffer for the result
   * @param bufferSize buffer size
   * @return size of a serialized response, if this size is greater than buffer size,
   *         the call must be repeated with the appropriately sized buffer
   */
  public static long BZPOPMIN (BigSortedMap map, long[] keys, int[] sizes, long timeout, long buffer, int bufferSize) {
    // This is non-blocking call
    // Timeout MUST be implemented outside of a handler thread
    for(int i=0; i < keys.length; i++) {
      long size = ZPOPMIN(map, keys[i], sizes[i], 1, buffer, bufferSize);
      if (size > 0) {
        return size;
      }
    }
    return 0;
  }
  
  /**
   * Available since 5.0.0.
   * Time complexity: O(log(N)) with N being the number of elements in the sorted set.
   * BZPOPMAX is the blocking variant of the sorted set ZPOPMAX primitive.
   * It is the blocking version because it blocks the connection when there are no members to pop 
   * from any of the given sorted sets. A member with the highest score is popped from first sorted 
   * set that is non-empty, with the given keys being checked in the order that they are given.
   * The timeout argument is interpreted as an integer value specifying the maximum number of seconds
   *  to block. A timeout of zero can be used to block indefinitely.
   * See the BZPOPMIN documentation for the exact semantics, since BZPOPMAX is identical to BZPOPMIN 
   * with the only difference being that it pops members with the highest scores instead of popping 
   * the ones with the lowest scores.
   * 
   * Return value
   * 
   * Array reply: specifically:
   * A nil multi-bulk when no element could be popped and the timeout expired.
   * A three-element multi-bulk with the first element being the name of the key where a member was popped, 
   * the second element is the popped member itself, and the third element is the score of the popped element.
   *
   * @param map sorted map storage
   * @param keys list of key addresses
   * @param sizes list of key sizes
   * @param timeout timeout in ms
   * @param buffer buffer for the result
   * @param bufferSize buffer size
   * @return size of a serialized response, if this size is greater than buffer size,
   *         the call must be repeated with the appropriately sized buffer
   */
  public static long BZPOPMAX (BigSortedMap map, long[] keys, int[] sizes, long timeout, long buffer, int bufferSize) {
    // This is non-blocking call
    // Timeout MUST be implemented outside of a handler thread
    for(int i=0; i < keys.length; i++) {
      long size = ZPOPMAX(map, keys[i], sizes[i], 1, buffer, bufferSize);
      if (size > 0) {
        return size;
      }
    }
    return 0;
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @return true - success, false - s&*%t
   */
  public static boolean DELETE(BigSortedMap map, String key) {
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    boolean result = DELETE(map, keyPtr, keySize);
    UnsafeAccess.free(keyPtr);
    return result;
  }
  
  /**
   * Delete sorted set 
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @return true if success, false (if does not exists)
   */
  public static boolean DELETE(BigSortedMap map, long keyPtr, int keySize) {
    return DELETE(map, keyPtr, keySize, true);
  }
  
  /**
   * Delete sorted set 
   * @param map sorted map storage
   * @param keyPtr sorted set key address
   * @param keySize sorted set key size
   * @param lock lock if true
   * @return true if success, false (if does not exists)
   */
  public static boolean DELETE(BigSortedMap map, long keyPtr, int keySize, boolean lock) {
    Key key = getKey(keyPtr, keySize);
    try {
      if(lock) {
        KeysLocker.writeLock(key);
      }
      boolean b = Sets.DELETE(map, keyPtr, keySize);
      // Can be false
      Hashes.DELETE(map, keyPtr, keySize);
      
      //int kSize = buildKey(keyPtr, keySize);
      //boolean b = map.delete(keyArena.get(), kSize);
      return b;
    } finally {
      if (lock) {
        KeysLocker.writeUnlock(key);
      }
    }
  }
  
  /**
   * For testing only
   * @param map sorted map storage
   * @param key zset key
   * @param count count to return
   * @param withScores with scores?
   * @param bufSize buffer size
   * @return list of pairs {member, member} or {member, score}
   */
  public static List<Pair<String>> ZRANDMEMBER(BigSortedMap map, String key, int count, 
      boolean withScores, int bufSize){
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    long buffer = UnsafeAccess.malloc(bufSize);
    long result = ZRANDMEMBER(map, keyPtr, keySize, count, withScores, buffer, bufSize);
    List<Pair<String>> list = new ArrayList<Pair<String>>();
    if (result > bufSize || result <= 0) {
      return list;
    }
    
    int total = UnsafeAccess.toInt(buffer);
    long ptr = buffer + Utils.SIZEOF_INT;
    for (int i = 0; i < total; i++) {
      int mSize = Utils.readUVInt(ptr);
      int mSizeSize = Utils.sizeUVInt(mSize);
      String member = null;
      String sscore = null;
      if (withScores) {
        double score = Utils.lexToDouble(ptr + mSizeSize);
        sscore = Double.toString(score);
        member = Utils.toString(ptr + mSizeSize + Utils.SIZEOF_DOUBLE, mSize - Utils.SIZEOF_DOUBLE);
      } else {
        member =  Utils.toString(ptr + mSizeSize, mSize);
        sscore = member;
      }
      list.add(new Pair<String>(member, sscore));
      ptr += mSize + mSizeSize;
    }
    
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(buffer);
    return list;
    
  }
  
  /**
   * When called with just the key argument, return a random element from the sorted set value stored at key.
   * If the provided count argument is positive, return an array of distinct elements. The array's length 
   * is either count or the sorted set's cardinality (ZCARD), whichever is lower.
   * If called with a negative count, the behavior changes and the command is allowed to return the same element
   * multiple times. In this case, the number of returned elements is the absolute value of the specified count.
   * The optional WITHSCORES modifier changes the reply so it includes the respective scores of the randomly 
   * selected elements from the sorted set.
   * Return value
   * Bulk string reply: 
   * without the additional count argument, the command returns a Bulk Reply with 
   * the randomly selected element, or nil when key does not exist.
   * Array reply: 
   * when the additional count argument is passed, the command returns an array of elements, or an empty 
   * array when key does not exist. If the WITHSCORES modifier is used, the reply is a list elements and 
   * their scores from the sorted set.
   * 
   * Notes: this implementation always return withScores = true 
   * 
   * @param map sorted map storage
   * @param keyPtr zset key address
   * @param keySize key size
   * @param count count to return
   * @param withScores (ignored) always true
   * @param buffer buffer address to store values
   * @param bufSize buffer size
   * @return full serialized size of the response
   * 
   */
  
  public static long ZRANDMEMBER (BigSortedMap map, long keyPtr, int keySize, int count, boolean withScores, 
      long buffer, int bufSize) {
    Key key = getKey(keyPtr, keySize);
    try {
      // To simplify code we ALWAYS return withScores=true
      KeysLocker.readLock(key);
      long result = Sets.SRANDMEMBER(map, keyPtr, keySize, buffer, bufSize, count);
      return result;
    } finally {
      KeysLocker.readUnlock(key);
    }
  }
  
  
  /**
   * Utility classes and methods
   */
  
  private static boolean between(double score, double min, double max, boolean minInclusive, 
      boolean maxInclusive) {
    
    if (maxInclusive && minInclusive) {
      return score >= min && score <= max;
    } else if (!maxInclusive && !minInclusive) {
      return score > min && score < max;
    } else if (maxInclusive) {
      return score > min && score <= max;
    } else {
      return score >= min && score < max;
    }
  }
  
  /**
   * Deletes in bulk
   * @param chunkPtr address of a memory 
   * @return number of deleted members
   */
  private static int bulkDelete(BigSortedMap map, long memory, long keyPtr, 
      int keySize, boolean normalMode) {
    int total = UnsafeAccess.toInt(memory);
    int deleted = 0;
    long ptr = memory + Utils.SIZEOF_INT;
    for(int i = 0; i < total; i++) {
      int mSize = Utils.readUVInt(ptr);
      int mSizeSize = Utils.sizeUVInt(mSize);
      int res = Sets.SREM(map, keyPtr, keySize, ptr + mSizeSize, mSize);
      assert(res == 1);
      deleted += res;
      if (normalMode) {
        res = Hashes.HDEL(map, keyPtr, keySize, ptr + mSizeSize + Utils.SIZEOF_DOUBLE, 
          mSize - Utils.SIZEOF_DOUBLE);
        assert(res == 1);
      }
      ptr += mSize + mSizeSize;
    }
    UnsafeAccess.putInt(memory,  0);
    return deleted;
  }
}

