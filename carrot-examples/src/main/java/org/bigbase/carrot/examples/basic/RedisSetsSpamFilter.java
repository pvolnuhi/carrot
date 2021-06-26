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

package org.bigbase.carrot.examples.basic;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import redis.clients.jedis.Jedis;


/**
 * This example shows how to use Redis Set to keep
 * list of a REAL spam domains. This test uses Redis Set 
 * to keep all data 
 * 
 * File: spam_domains.txt.s
 * 
 * Test loads list of real spam domains (51672 records) into 10 different sets,
 * total number of loaded records is 516720 (51672 * 10)
 * 
 * RESULTS:
 * 
 * 1. Raw size of all data is              7,200,570 bytes
 * 2. Carrot NoCompression     - RAM usage 8,566,630, COMPRESSION = 0.84
 * 3  Carrot LZ4 compression   - RAM usage 6,657,078, COMPRESSION = 1.08
 * 4. Carrot LZ4HC compression - RAM usage 6,154,038, COMPRESSION = 1.17
 * 
 * LZ4 compression relative to NoCompression = 1.08/0.84 = 1.29
 * LZ4HC compression  relative to NoCompression = 1.17/0.84 = 1.4
 * 
 * Redis SET usage per object is 68.5 bytes. 35MB total
 * 
 * RAM usage (Redis-to-Carrot)
 * 
 * 1) No compression    35M/8.5M ~ 4.1x
 * 2) LZ4   compression 35M/6.6M ~ 5.3x
 * 3) LZ4HC compression 35M/6.1M ~ 5.7x 
 * 
 * Effect of a compression:
 * 
 * LZ4  - 1.08/0.84 = 1.29    (to no compression)
 * LZ4HC - 1.17/0.84 = 1.4  (to no compression)
 * 
 * 
 * Notes: Redis can store these URLS in a Hash with ziplist encoding
 * using HSET ( key, field = URL, "1")
 * 
 *  Avg. URL size is 14. ziplist overhead for {URL, "1"} pair is 4 = 2+2. So usage is going 
 *  to be 18 per URL
 *  
 *  LZ4HC compression = 12 bytes per URL. So, we have at least 1.5x advantage even for this Redis hack
 * 
 * Notes: Using Hash hack to store large set of objects has one downside - you can't use SET
 * specific API: union, intersect etc.
 *  
 * @author vrodionov
 *
 */
public class RedisSetsSpamFilter {
  
  static String[] keys = new String[] { "key1", "key2", "key3", "key4", "key5",
                                        "key6", "key7", "key8", "key9", "key10"};
  
  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      usage();
    }
    System.out.println("RUN Redis test");
    runTest(args[0]);
  }
  
  @SuppressWarnings("deprecation")
  private static void runTest(String fileName) throws IOException {
    
    Jedis client = new Jedis("localhost");
    
    File f = new File(fileName);
    FileInputStream fis = new FileInputStream(f);
    DataInputStream dis = new DataInputStream(fis);
    String line = null;
    long totalLength = 0;
    int count = 0;
    
    long startTime = System.currentTimeMillis();
    while((line = dis.readLine()) != null) {
      totalLength += line.length() * keys.length;
      count++;
      for (String key : keys) {
        client.sadd(key, line);
      }
      if ((count % 10000) == 0 && count > 0) {
        System.out.println("Loaded " + count);
      }
    }
    long endTime = System.currentTimeMillis();
    
    System.out.println("Loaded " + count * keys.length +" records, total size="+ totalLength + 
        " in " + (endTime - startTime) + "ms."); 
    dis.close();
    
    System.out.println("Press any key ...");
    System.in.read();
    
    for(String key: keys) {
      client.del(key);
    }
    
    client.close();
  }

  private static void usage() {
    System.out.println("usage: java org.bigbase.carrot.examples.RedisSetsSpamFilter domain_list_file");
    System.exit(-1);
  } 
}
