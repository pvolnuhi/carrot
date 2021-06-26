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
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.sets.Sets;
import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

import redis.clients.jedis.Jedis;

/**
 * Simple social network test: Carrot vs Redis
 * Timeline class - represents user's timeline - 
 * all messages posted by a user in a reverse chronological
 * order
 * @author vrodionov
 *
 */
public class Timeline {

  List<TimelineItem> items;  
  User user;
  
  Timeline(User user) {
    this.user = user;
    generateTimeline();
  }
  
  List<TimelineItem> items() {
    return items;
  }
  
  User user() {
    return user;
  }
  
  public long size() {
    return items.size();
  }
  
  public String getKey() {
    return "profile:timeline:" + user.getId();
  }
  
  /**
   * On average, user posts 2 statuses per day
   * @param registered user's registration time
   * @return user's timeline
   */
  void generateTimeline() {
    long registered = Long.valueOf(user.getSignup());
    Calendar cal = Calendar.getInstance();
    Date today = cal.getTime();
    cal.setTimeInMillis(registered);
    Date regtime = cal.getTime();
    items = new ArrayList<TimelineItem>();
    while(today.after(regtime)) {
      long time = regtime.getTime();
      String id = Long.toString(Id.nextId(time));
      items.add(new TimelineItem(id, time / 1000)); // We keep seconds only
      regtime.setTime (time + 12 * 3600 * 1000);
    }
  }
  
  /**
   * Saves timeline to Carrot store, using Set  SADD command
   * @param map sorted map store
   */
  void saveToCarrot(BigSortedMap map) {
    String key = getKey();
    items.stream().forEach( x-> x.saveToCarrot(map, key));
  }
  /**
   * Saves timeline to Redis store, using ZSet  ZADD command
   * Redis SET does not support ordering of members
   * @param client Redis client
   */
  void saveToRedis(Jedis client) {
    String key = getKey();
    items.stream().forEach( x-> x.saveToRedis(client, key));    
  }
}


class TimelineItem {
  String messageId;     // message id
  long posted;          // posted - when it was posted
  
  TimelineItem(String messageId, long posted) {
    this.messageId = messageId;
    this.posted = posted;
  }
  
  void saveToCarrot(BigSortedMap map, String key) {
    
    long memberPtr = UnsafeAccess.malloc(16); // 8 bytes for posted, 8 bytes for mesageId
    long keyPtr = UnsafeAccess.allocAndCopy(key.getBytes(), 0, key.length());
    int keySize = key.length();
    int memberSize = 16;
    UnsafeAccess.putLong(memberPtr, Long.MAX_VALUE - posted);
    // We convert messageId to long value to save space
    UnsafeAccess.putLong(memberPtr + Utils.SIZEOF_LONG,Long.valueOf(messageId));
    // We use Set b/c it is ordered by member in Carrot
    // Because each member starts with a time (reversed) -> all members 
    // are ordered by time in reverse order - this is what we need
    Sets.SADD(map, keyPtr, keySize, memberPtr, memberSize);
    UnsafeAccess.free(memberPtr);
    UnsafeAccess.free(keyPtr);
  }
  
  void saveToRedis(Jedis client, String key) {
    double score = posted;
    // We convert messageId to long value to save space
    byte[] member = Bytes.toBytes(Long.valueOf(messageId)); 
    client.zadd(key.getBytes(), score, member);
  }
}