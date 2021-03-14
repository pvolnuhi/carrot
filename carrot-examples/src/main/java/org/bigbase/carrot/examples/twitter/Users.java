package org.bigbase.carrot.examples.twitter;

import java.util.List;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.sets.Sets;
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
    
    long memberPtr = UnsafeAccess.malloc(16); // 8 bytes for posted, 8 bytes for userId
    long keyPtr = UnsafeAccess.allocAndCopy(key.getBytes(), 0, key.length());
    int keySize = key.length();
    int memberSize = 16;
    UnsafeAccess.putLong(memberPtr, Long.MAX_VALUE - time);
    // We convert userId to long value to save space
    UnsafeAccess.putLong(memberPtr + Utils.SIZEOF_LONG,Long.valueOf(userId));
    // We use Set b/c it is ordered by member in Carrot
    // Because each member starts with a time (reversed) -> all members 
    // are ordered by time in reverse order - this is what we need
    Sets.SADD(map, keyPtr, keySize, memberPtr, memberSize);
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