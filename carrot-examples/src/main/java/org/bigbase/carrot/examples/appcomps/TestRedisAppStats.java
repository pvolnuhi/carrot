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

package org.bigbase.carrot.examples.appcomps;

import java.io.IOException;

import redis.clients.jedis.Jedis;

/**
 * Counters and statistics.
 * Redis Book, Chapter 5.1:
 * https://redislabs.com/ebook/part-2-core-concepts/chapter-5-using-redis-for-application-support/
 * 5-2-counters-and-statistics/5-2-2-storing-statistics-in-redis/ 
 * 
 * We implement simple application which stores Web-application's page access time statistics. 
 * The app is described in the Redis book (see the link above) 
 * 
 * We collect hourly statistics for 1 year on web page access time
 * 
 * The key = "stats:profilepage:access:hour"
 * 
 * The key consists from several parts:
 * 
 *  stats       - This is top group name - means "Statistics"
 *  profilepage - page we colect statistics on
 *  access      - statistics on total access time
 *  hour        - 8 byte timestamp for the hour  
 * 
 * There are 24*365 = 8,760 hours in a year, so there are 8,760 keys in the application.
 * For Redis we will use ordered sets (ZSET) as recommended in the book), for Carrot we will use
 * HASH type to store the data.
 * 
 * We collect the following statistics:
 * 
 * "min"   - minimum access time  
 * "max"   - maximum access time  
 * "count" - total number of accesses 
 * "sum"   - sum of access times 
 * "sumsq" - sum of squares of access time 
 * 
 * The above info information will allow us to calculate std deviation, min, max, average, total.
 * 
 * 
 * 
 */

public class TestRedisAppStats {
  final static String KEY_PREFIX = "stats:profilepage:access:";
  final static int hoursToKeep = 10 * 365 * 24;
  
  public static void main(String[] args) throws IOException {
    runTest();
  }
  
  private static void runTest() throws IOException {
    Jedis client = new Jedis("localhost");
    long start = System.currentTimeMillis();
    for(int i = 0; i < hoursToKeep; i++) {
      Stats st = Stats.newStats(i);
      //st.saveToRedisNative(client);
      client.del(st.getKeyBytes());
    }
    long end = System.currentTimeMillis();
    System.out.println("Loaded " + hoursToKeep + " in " + (end - start)+ "ms. Press any button ...");
    System.in.read();
    client.close();
  }
}
