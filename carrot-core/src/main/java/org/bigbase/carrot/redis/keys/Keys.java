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
}
