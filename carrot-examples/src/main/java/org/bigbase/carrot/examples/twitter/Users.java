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
 */

package org.bigbase.carrot.examples.twitter;

import java.util.List;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.sets.Sets;
import org.bigbase.carrot.redis.zsets.ZSets;
import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

import redis.clients.jedis.Jedis;
/**
 * Simple social network test: Carrot vs Redis
 * Follower or Following base class - represents user's followers/following - 
 * all users who follows or whom this user is following in a reverse chronological
 * order
 * @author vrodionov
 *
 */
public abstract class Users {

  List<GenuineUser> users;  
  User user; 
  
  Users(User user) {
    this.user = user;
    generateUsers();
  }
  
  List<GenuineUser> users() {
    return users;
  }
  
  User user() {
    return user;
  }
  
  public long size() {
    return users.size();
  }
  
  public abstract String getKey();
  
  public abstract void generateUsers();
  
  /**
   * Save to Carrot
   * @param map sorted map store
   */
  void saveToCarrot(BigSortedMap map) {
    String key = getKey();
    users.stream().forEach( x-> x.saveToCarrot(map, key));
  }
  
  void saveToRedis(Jedis client) {
    String key = getKey();
    users.stream().forEach( x-> x.saveToRedis(client, key));    
  }
}


class GenuineUser {
  String userId;     // user id
  long time;         // time - when user started following
  
  GenuineUser(String userId, long time) {
    this.userId = userId;
    this.time = time;
  }
  
  void saveToCarrot(BigSortedMap map, String key) {
    
    long memberPtr = UnsafeAccess.allocAndCopy(userId, 0, userId.length());
    long keyPtr = UnsafeAccess.allocAndCopy(key.getBytes(), 0, key.length());
    int keySize = key.length();
    int memberSize = userId.length();
    ZSets.ZADD(map, keyPtr, keySize, new double[]{time},
      new long[] {memberPtr}, new int[] {memberSize}, true);
    UnsafeAccess.free(memberPtr);
    UnsafeAccess.free(keyPtr);
  }
  
  void saveToRedis(Jedis client, String key) {
    double score = time;
    // We convert userId to long value to save space
    byte[] member = Bytes.toBytes(Long.valueOf(userId)); 
    client.zadd(key.getBytes(), score, member);
  }
}