package org.bigbase.carrot.redis.zsets;

import static org.bigbase.carrot.redis.Commons.KEY_SIZE;
import static org.bigbase.carrot.redis.Commons.ZERO;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.Key;
import org.bigbase.carrot.redis.DataType;
import org.bigbase.carrot.redis.sets.SetAdd;
import org.bigbase.carrot.redis.sets.SetDelete;
import org.bigbase.carrot.redis.sets.SetExists;
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
 *  1. One keeps key -> (score, member) combo in a Set
 *  2. Second keeps key -> {member, score} pair in a Hash, where member is the field and score
 *     is the value
 *  
 *  Score is translated to 8 byte sequence suitable for lexicographical 
 *  comparison
 *  
 *  
 *  Key is built the following way:
 *  
 *  [TYPE = ZSET][BASE_KEY_LENGTH][KEY][SET_ID][(SCORE,VALUE) | (VALUE)]
 *  
 *  TYPE = 1 byte
 *  BASE_KEY_LENGTH - 4 bytes
 *  SET_ID = [0,1] 0 - set by score, 1 - hash by member-score
 *  
 *  Optimization for short ordered sets: we can use only one ordered set by score and convert
 *  to  set and hash representation when number of elements exceeds some threshold, similar how Redis
 *  optimizes small LIST, HASH, ZSET into ziplist representation. 
 *  
 * @author Vladimir Rodionov
 *
 */
public class ZSets {
  
  static enum SetID {
    SCORE, MEMBER
  }
  
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
   * Build key for Set. It uses thread local key arena 
   * @param keyPtr original key address
   * @param keySize original key size
   * @param memberPtr element address
   * @param memberSize element size
   * @return new key size 
   */
    
   
  private static int buildKey( long keyPtr, int keySize, long memberPtr, int memberSize, double score, SetID id) {
    checkKeyArena(keySize + KEY_SIZE + memberSize + 2*Utils.SIZEOF_BYTE);
    long arena = keyArena.get();
    int kSize = KEY_SIZE + keySize + Utils.SIZEOF_BYTE;
    UnsafeAccess.putByte(arena, (byte)DataType.ZSET.ordinal());
    UnsafeAccess.putInt(arena + Utils.SIZEOF_BYTE, keySize);
    UnsafeAccess.copy(keyPtr, arena + KEY_SIZE + Utils.SIZEOF_BYTE, keySize);
    if (memberPtr > 0) {
      UnsafeAccess.putByte(arena + kSize, (byte)id.ordinal());
      kSize += Utils.SIZEOF_BYTE;
      UnsafeAccess.copy(memberPtr, arena + kSize, memberSize);
      kSize += memberSize;
    }
    return kSize;
  }
}
