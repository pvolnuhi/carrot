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
package org.bigbase.carrot.redis.keys;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.hashes.Hashes;
import org.bigbase.carrot.redis.lists.Lists;
import org.bigbase.carrot.redis.sets.Sets;
import org.bigbase.carrot.redis.sparse.SparseBitmaps;
import org.bigbase.carrot.redis.strings.Strings;
import org.bigbase.carrot.redis.util.MutationOptions;
import org.bigbase.carrot.redis.zsets.ZSets;

public class Keys {

  /**
   * Available since 1.0.0.
   * Time complexity: O(1)
   * Returns the remaining time to live of a key that has a timeout. This introspection capability 
   * allows a Redis client to check how many seconds a given key will continue to be part of the dataset.
   * In Redis 2.6 or older the command returns -1 if the key does not exist or if the key exist 
   * but has no associated expire.
   * Starting with Redis 2.8 the return value in case of error changed:
   * The command returns -2 if the key does not exist.
   * The command returns -1 if the key exists but has no associated expire.
   * See also the PTTL command that returns the same information with milliseconds resolution (Only available in Redis 2.6 or greater).
   * 
   * Return value:
   * Integer reply: TTL in seconds, or a negative value in order to signal an error 
   * (see the description above).
   */
  public static long TTL(BigSortedMap map, long keyPtr, int keySize) {
    return 0;
  }
  
  /**
   * Available since 2.6.0.
   * Time complexity: O(1)
   * Like TTL this command returns the remaining time to live of a key that has an expire set, 
   * with the sole difference that TTL returns the amount of remaining time in seconds while 
   * PTTL returns it in milliseconds.
   * In Redis 2.6 or older the command returns -1 if the key does not exist or if the key exist
   *  but has no associated expire.
   * Starting with Redis 2.8 the return value in case of error changed:
   * The command returns -2 if the key does not exist.
   * The command returns -1 if the key exists but has no associated expire.
   * Return value
   * Integer reply: TTL in milliseconds, or a negative value in order to signal an error 
   * (see the description above).
   */
  
  public static long PTTL(BigSortedMap map, long keyPtr, int keySize) {
    return 0;
  }
  
  /**
   * DEL key [key ...]
   * Available since 1.0.0.
   * Time complexity: O(N) where N is the number of keys that will be removed. When a key 
   * to remove holds a value other than a string, the individual complexity for this key is O(M) 
   * where M is the number of elements in the list, set, sorted set or hash. Removing a single key 
   * that holds a string value is O(1).
   * Removes the specified keys. A key is ignored if it does not exist.
   * Return value
   * Integer reply: The number of keys that were removed.
   * @param map sorted map storage
   * @param keyPtr key addresses
   * @param keySize key sizes
   * @return number of keys removed
   */
  public static int DEL(BigSortedMap map, long[] keyPtrs, int[] keySizes) {
    int total = 0;
    for (int i = 0; i < keyPtrs.length; i++) {
      total += DELETE(map, keyPtrs[i], keySizes[i]);
    }
    return total;
  }
  
  /**
   * Deletes first key ONLY!!!
   * The issue: we can have duplicate keys across types, 
   * but not in a type.
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @return 1 - success, 0 - key does not exists
   */
  private static int DELETE(BigSortedMap map, long keyPtr, int keySize) {
    
    if(Strings.DELETE(map, keyPtr, keySize)) {
      return 1;
    }
    if(ZSets.DELETE(map, keyPtr, keySize)) {
      return 1;
    }
    if (Sets.DELETE(map, keyPtr, keySize)){
      return 1;
    }
    if(Hashes.DELETE(map, keyPtr, keySize)) {
      return 1;
    }
    if(Lists.DELETE(map, keyPtr, keySize)) {
      return 1;
    }
    if (SparseBitmaps.DELETE(map, keyPtr, keySize)) {
      return 1;
    }
    return 0;
  }
  
  /**
   * EXPIRE key seconds [NX|XX|GT|LT]
   *
   * Available since 1.0.0.
   * Time complexity: O(1)
   * Set a timeout on key. After the timeout has expired, the key will automatically be deleted. 
   * A key with an associated timeout is often said to be volatile in Redis terminology.
   * The timeout will only be cleared by commands that delete or overwrite the contents of 
   * the key, including DEL, SET, GETSET and all the *STORE commands. This means that all 
   * the operations that conceptually alter the value stored at the key without replacing it 
   * with a new one will leave the timeout untouched. For instance, incrementing the value 
   * of a key with INCR, pushing a new value into a list with LPUSH, or altering the field 
   * value of a hash with HSET are all operations that will leave the timeout untouched.
   * The timeout can also be cleared, turning the key back into a persistent key, using the PERSIST command.
   * If a key is renamed with RENAME, the associated time to live is transferred to the new key name.
   * If a key is overwritten by RENAME, like in the case of an existing key Key_A that is overwritten
   *  by a call like RENAME Key_B Key_A, it does not matter if the original Key_A had a timeout associated 
   *  or not, the new key Key_A will inherit all the characteristics of Key_B.
   * Note that calling EXPIRE/PEXPIRE with a non-positive timeout or EXPIREAT/PEXPIREAT with a time 
   * in the past will result in the key being deleted rather than expired (accordingly, the emitted key 
   * event will be del, not expired).
   * 
   * Options
   * The EXPIRE command supports a set of options since Redis 7.0:
   *   NX -- Set expiry only when the key has no expiry
   *   XX -- Set expiry only when the key has an existing expiry
   *   GT -- Set expiry only when the new expiry is greater than current one
   *   LT -- Set expiry only when the new expiry is less than current one
   * A non-volatile key is treated as an infinite TTL for the purpose of GT and LT. The GT, LT and NX options are mutually exclusive.
   *
   * @param map sorted map set
   * @param keyPtr key address
   * @param keySize key size
   * @param seconds seconds to live
   * @return 1 - success, 0 - was not set, -1 - key does not exist 
   */
  public static int EXPIRE(BigSortedMap map, long keyPtr, int keySize, long seconds, MutationOptions opps) {
    // TODO
    return 1;
  }
}
