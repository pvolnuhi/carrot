package org.bigbase.carrot.redis.zsets;

import static org.bigbase.carrot.redis.Commons.KEY_SIZE;

import java.io.IOException;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.Key;
import org.bigbase.carrot.redis.Aggregate;
import org.bigbase.carrot.redis.DataType;
import org.bigbase.carrot.redis.KeysLocker;
import org.bigbase.carrot.redis.MutationOptions;
import org.bigbase.carrot.redis.RedisConf;
import org.bigbase.carrot.redis.hashes.HashSet;
import org.bigbase.carrot.redis.hashes.Hashes;
import org.bigbase.carrot.redis.sets.SetAdd;
import org.bigbase.carrot.redis.sets.SetDelete;
import org.bigbase.carrot.redis.sets.SetExists;
import org.bigbase.carrot.redis.sets.SetScanner;
import org.bigbase.carrot.redis.sets.Sets;
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
    
   
  static int buildKeyForSet( long keyPtr, int keySize, long memberPtr, 
      int memberSize, double score) {
    checkKeyArena(keySize + KEY_SIZE + memberSize + Utils.SIZEOF_BYTE + Utils.SIZEOF_DOUBLE);
    long arena = keyArena.get();
    int kSize = KEY_SIZE + keySize + Utils.SIZEOF_BYTE;
    UnsafeAccess.putByte(arena, (byte)DataType.SET.ordinal());
    UnsafeAccess.putInt(arena + Utils.SIZEOF_BYTE, keySize);
    UnsafeAccess.copy(keyPtr, arena + KEY_SIZE + Utils.SIZEOF_BYTE, keySize);
    if (memberPtr > 0) {
      Utils.doubleToLex(arena + kSize, score);
      kSize += Utils.SIZEOF_DOUBLE;
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
    return Hashes.buildKey(keyPtr, keySize, memberPtr, memberSize);
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
    Key k = getKey(keyPtr, keySize);
    try {
      KeysLocker.readLock(k);
      int kSize = buildKey(keyPtr, keySize);
      long addr = map.get(keyArena.get(), kSize, valueArena.get(), valueArenaSize.get(), 0);
      if (addr < 0) {
        return 0;
      } else {
        return UnsafeAccess.toLong(valueArena.get());
      }
    } finally {
      KeysLocker.readUnlock(k);
    }
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
      KeysLocker.writeLock(k);

      int toAdd = memberPtrs.length;
      int inserted = 0;
      int updated = 0;
      for (int i = 0; i < toAdd; i++) {
        int kSize = buildKeyForSet(keyPtr, keySize, memberPtrs[i], memberSizes[i], scores[i]);
        SetAdd add = setAdd.get();
        add.reset();
        add.setKeyAddress(keyArena.get());
        add.setKeySize(kSize);
        add.setMutationOptions(options);
        // version?
        if (map.execute(add)) {
          inserted += add.getInserted();
          if (changed) {
            updated += add.getUpdated();
          }
        }
      }
      // Next do hash
      RedisConf conf = RedisConf.getInstance();
      long count = ZCARD(map, keyPtr, keySize);
      if (count == conf.getMaximumZSetCompactSize()) {
        // convert to normal representation
        convertToNormalMode(map, keyPtr, keySize);
      } else if (count > conf.getMaximumZSetCompactSize()) {
        // Add to Hash
        addToHash(map, keyPtr, keySize, scores, memberPtrs, memberSizes, options);
      } else {
        // Compact mode - no Hash
      }
      // Next update zset count
      int kSize = buildKey(keyPtr, keySize);
      if (inserted > 0) {
        map.increment(keyPtr, kSize, 0, inserted);
      }
      return inserted + updated;
    } finally {
      KeysLocker.writeUnlock(k);
    }
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
  private static void convertToNormalMode(BigSortedMap map, long keyPtr, int keySize) {
    // TODO Auto-generated method stub
    Key k = getKey(keyPtr, keySize);
    SetScanner scanner = null;
    try {
      KeysLocker.writeLock(k);
      scanner = Sets.getSetScanner(map, keyPtr, keySize, false);
      if (scanner == null) {
        // TODO - report
        return;
      }
      // TODO - object allocations
      long[] ptrs = new long[1];
      int[] sizes = new int[1];
      double[] scores = new double[1];
      try {
        while(scanner.hasNext()) {
          long ePtr = scanner.elementAddress();
          int eSize = scanner.elementSize();
          
          double score = Utils.lexToDouble(ePtr);
          ePtr += Utils.SIZEOF_DOUBLE;
          eSize -= Utils.SIZEOF_DOUBLE;
          ptrs[0] = ePtr;
          sizes[0] = eSize;
          scores[0] = score;
          addToHash(map, keyPtr, keySize, scores, ptrs, sizes, MutationOptions.NONE);
        }
      } catch (IOException e) {
        // TODO Auto-generated catch block
      }
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
        }
      }
      KeysLocker.writeUnlock(k);
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
    // TODO Auto-generated method stub
    Key k = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(k);
      Sets.DELETE(map, keyPtr, keySize);
    } finally {
      KeysLocker.writeUnlock(k);
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
   * Returns the number of elements in the sorted set at key with a score between min and max.
   * The min and max arguments have the same semantic as described for ZRANGEBYSCORE.
   * Note: the command has a complexity of just O(log(N)) because it uses elements ranks (see ZRANK) to 
   * get an idea of the range. Because of this there is no need to do a work proportional to the size
   *  of the range.
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
    if (maxInclusive) {
      max += Double.MIN_NORMAL;
    }
    long count = 0;
    try {
      KeysLocker.readLock(k);
      
      
      return count;
    }finally {
      KeysLocker.readUnlock(k);
    }
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
    return 0;
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
    return 0;
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
  public static long ZLEXCOUNT(BigSortedMap map, long keyPtr, int keySize, long startPtr, int startSize, boolean 
      startInclusive, long endPtr, int endSize, boolean endInclusive) {
    return 0;
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
    return 0;
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
  public static long ZPOPMIN(BigSortedMap map, long keyPtr, int keySize, int count, long buffer, int bufferSize)
  {
    return 0;
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
   * @param map
   * @param keyPtr
   * @param keySize
   * @param start
   * @param end
   * @param withScores
   * @param long buffer buffer address
   * @param int bufferSize buffer size
   * @return total serialized size of a response, if it is greater than 
   *         bufferSize - repeat call with appropriately sized buffer 
   */
  public static long ZRANGE (BigSortedMap map, long keyPtr, int keySize, long start, long end, 
      boolean withScores, long buffer, int bufferSize)
  {
    return 0;
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
   * @param count max count after offset
   * @param buffer buffer for return
   * @param bufferSize buffer size
   * @return total serialized size of a response, if it is greater than 
   *         bufferSize - repeat call with appropriately sized buffer 
   */
  public static long ZRANGEBYLEX(BigSortedMap map, long keyPtr, int keySize, long startPtr, int startSize, 
      boolean startInclusive, long endPtr, int endSize, boolean endInclusive, long offset, int count, 
      long buffer, int bufferSize)
  {
    return 0;
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
    return 0;
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
  public static long ZRANK(BigSortedMap map, long keyPtr, int keySize, long meberPtr, int memberSize) {
    return 0;
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
   * @param memberPtrs array member name address 
   * @param memberSizes array member name size
   * @return The number of members removed from the sorted set
   */
  public static long ZREM (BigSortedMap map, long keyPtr, int keySize, long[] memberPtrs, int[] memberSizes)
  {
    return 0;
  }
  
  /**
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
   * @param rangeStartPtr range start (lex) address
   * @param rangeStartSize range start size
   * @param rangeStartInclusive is start inclusive
   * @param rangeEndPtr range end address
   * @param rangeEndSize range end size
   * @param rangeEndInclusive is end inclusive
   * @return number of members removed
   */
  public static long ZREMRANGEBYLEX(BigSortedMap map, long keyPtr, int keySize, long rangeStartPtr, 
      int rangeStartSize, boolean rangeStartInclusive, long rangeEndPtr, int rangeEndSize, 
      boolean rangeEndInclusive)
  {
    return 0;
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
    return 0;
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
   * @param startScore start score
   * @param startScoreInclusive start score inclusive?
   * @param stopScore stop score
   * @param stopScoreInclusive stop score inclusive?
   * @return number of elements removed
   */
  public static long ZREMRANGEBYSCORE(BigSortedMap map, long keyPtr, int keySize, double startScore, boolean
      startScoreInclusive, double stopScore, boolean stopScoreInclusive)
  {
    return 0;
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
   * @param map
   * @param keyPtr
   * @param keySize
   * @param start
   * @param end
   * @param withScores
   * @param long buffer buffer address
   * @param int bufferSize buffer size
   * @return total serialized size of a response, if it is greater than 
   *         bufferSize - repeat call with appropriately sized buffer 
   */
  public static long ZREVRANGE (BigSortedMap map, long keyPtr, int keySize, long start, long end, 
      boolean withScores, long buffer, int bufferSize)
  {
    return 0;
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
     boolean startInclusive, long endPtr, int endSize, boolean endInclusive, long offset, int count, 
     long buffer, int bufferSize)
 {
   return 0;
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
      boolean minInclusive, double maxScore, boolean maxInclusive, long offset, long limit,
      boolean withScores, long buffer, int bufferSize) {
    return 0;
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
 public static long ZREVRANK(BigSortedMap map, long keyPtr, int keySize, long meberPtr, int memberSize) {
   return 0;
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
  * @param lastSeenMemberPtr last seen member
  * @param lastSeenMemberSize last seen member name size
  * @param lastSeenScore last seen score
  * @param count count to return in a single call
  * @return total serialized size of a response. If it is greater than bufferSize
  *     call must be repeated with the appropriately sized buffer 
  */
 public static long ZSCAN(BigSortedMap map, long keyPtr, int keySize, long lastSeenMemberPtr, 
     int lastSeenMemberSize, double lastSeenScore, int count) {
  return 0; 
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
 
 public static Double ZSCORE (BigSortedMap map, long keyPtr, int keySize, long memberPtr, int memberSize)
 {
   return null;
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

}
