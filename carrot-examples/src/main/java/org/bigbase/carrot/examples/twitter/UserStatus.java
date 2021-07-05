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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.examples.util.KeyValues;
import org.bigbase.carrot.redis.hashes.Hashes;
import org.bigbase.carrot.util.KeyValue;
import org.bigbase.carrot.util.Utils;

import redis.clients.jedis.Jedis;

/**
 * 
 * Twitter user status object (simple)
 * 
 *  posted
 *  id
 *  uid
 *  login
 *  private
 *  liked
 *  retweeted
 */

public class UserStatus extends KeyValues{
  public final static String POSTED = "posted";
  public final static String ID = "id";
  public final static String UID = "uid";
  public final static String LOGIN = "login";
  public final static String PRIVATE = "private";
  public final static String LIKED = "liked";
  public final static String RETWEETED = "retweeted";
  
  static Random rnd = new Random();
  
  UserStatus(Properties p) {
    super(p);    
  }
  
  public static UserStatus newUserStatus(User u) {
    UserStatus us = null;
    Properties p = new Properties();
    
    p.setProperty(LOGIN, u.getLogin());
    p.setProperty(ID, u.getId());
    long creationTime = newCreationTime(u.getSignup()); 
    p.setProperty(UID, newUid(creationTime));
    p.setProperty(POSTED, newPosted(creationTime));
    p.setProperty(PRIVATE, newPrivate());
    p.setProperty(LIKED, newLiked());
    p.setProperty(RETWEETED, newRetweeted());
    us = new UserStatus(p);
    return us;
  }
  
  public static List<UserStatus> newUserStatuses(User user){
    List<UserStatus> list = new ArrayList<UserStatus>();
    int posts = user.getPosts();
    for (int i=0; i < posts; i++) {
      list.add(newUserStatus(user));
    }
    return list;
  }
  
  public String getKey() {
    return "status:" + getUid();
  }
  
  public String getPosted() {
    return props.getProperty(POSTED);
  }
  
  public String getLogin() {
    return props.getProperty(LOGIN);
  }
  
  public String getId() {
    return props.getProperty(ID);
  }
  
  public String getUid() {
    return props.getProperty(POSTED);
  }
  
  public String getPrivate() {
    return props.getProperty(PRIVATE);    
  }
  
  public String getLiked() {
    return props.getProperty(LIKED);    
  }
  
  public String getRetweeted() {
    return props.getProperty(RETWEETED);    
  }
  
  
  @Override
  public List<KeyValue> asList(){
    // Special handling for numeric properties
    ArrayList<KeyValue> list = new ArrayList<KeyValue>();
    String key = LOGIN;
    String value = getLogin();
    list.add(KeyValues.fromKeyValue(key, value));
    
    key = PRIVATE;
    value = getPrivate();
    list.add(KeyValues.fromKeyValue(key, value));

    key = ID;
    value = getId();
    list.add(KeyValues.fromKeyAndNumericValue(key, value));
    
    key = UID;
    value = getUid();
    list.add(KeyValues.fromKeyAndNumericValue(key, value));
    
    key = POSTED;
    value = getPosted();
    list.add(KeyValues.fromKeyAndNumericValue(key, value));

    key = LIKED;
    value = getLiked();
    list.add(KeyValues.fromKeyAndNumericValue(key, value));
    
    key = RETWEETED;
    value = getRetweeted();
    list.add(KeyValues.fromKeyAndNumericValue(key, value));
    
    return list;
  }
  
  @Override
  public Map<byte[], byte[]> asMap() {
    Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();
    String key = LOGIN;
    String value = getLogin();
    map.put(key.getBytes(), value.getBytes());
    
    key = PRIVATE;
    value = getPrivate();
    map.put(key.getBytes(), value.getBytes());

    key = ID;
    value = getId();
    map.put(key.getBytes(), Utils.numericStrToBytes(value));
    
    key = UID;
    value = getUid();
    map.put(key.getBytes(), Utils.numericStrToBytes(value));
    
    key = POSTED;
    value = getPosted();
    map.put(key.getBytes(), Utils.numericStrToBytes(value));

    key = LIKED;
    value = getLiked();
    map.put(key.getBytes(), Utils.numericStrToBytes(value));
    
    key = RETWEETED;
    value = getRetweeted();
    map.put(key.getBytes(), Utils.numericStrToBytes(value));
    return map;
  }
  
  public void saveToCarrot(BigSortedMap map) {
    List<KeyValue> list = asList();
    Hashes.HSET(map, getKey(), list);
    Utils.freeKeyValues(list);
  }
  
  public void saveToRedis(Jedis client) {
    client.hset (getKey().getBytes(), asMap());
  }
  
  private static long newCreationTime(long signup) {
    long current = System.currentTimeMillis();
    double d = rnd.nextDouble();
    long time = (long)(signup + d * (current - signup));
    return time / 1000; // up to seconds
  }
  
  private static String newUid(long time) {
    return Long.toString(Id.nextId(time));
  }
  
  private static String newPosted(long time) {
    return Long.toString(time);
  }
  
  /**
   * 10% are private
   * @return
   */
  private static String newPrivate() {
    double d = rnd.nextDouble();
    if (d < 0.1) return "yes";
    return "no";
  }
  
  /**
   * Skewed between 0 and 1000
   * @return
   */
  private static String newLiked() {
    int max = 1000000;
    double d = rnd.nextDouble();
    d = Math.pow(d, 10);
    return Integer.toString( (int)(d * max));
  }
  
  /**
   * Skewed between 0 and 1000
   * @return
   */
  private static String newRetweeted() {
    int max = 1000000;
    double d = rnd.nextDouble();
    d = Math.pow(d, 10);
    return Integer.toString( (int)(d * max));
  }
  
}
