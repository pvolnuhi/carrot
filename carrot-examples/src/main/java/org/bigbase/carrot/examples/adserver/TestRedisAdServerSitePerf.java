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

package org.bigbase.carrot.examples.adserver;

import java.io.IOException;
import java.util.Random;

import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.Utils;

import redis.clients.jedis.Jedis;

/**
 * ----- Data structures to keep site performance
 * 
 * 13. SiteAdsRank: ZSET - ordered set. key = siteId, member = adId, score = CTR (click through rate). This data
 * allows us to estimate how does the ad perform on a particular site relative to other ads.
 * 
 * 14. SiteWordsRank: ZSET - ordered set. key = siteId, member = word, score - word's value. This data store keeps 
 *    keywords with corresponding scores. Every time someone acts on ads on the site, all keywords from the ad 
 *    are added to the site's ordered set with a some score value. The more a keyword appears in the ads - the higher
 *    it is going to be in the site's list.   This data allows us to estimate the most important keywords for the site
 *    as well as targeting attributes.
 * 
 * 
 * Results:
 * 
 * Redis 6.0.10             = 
 * Carrot no compression    =   
 * Carrot LZ4 compression   =   
 * Carrot LZ4HC compression =   
 * 
 * Notes:
 * 
 * 1. The test uses synthetic data, which is mostly random and not compressible
 * 
 */
public class TestRedisAdServerSitePerf {
  
  final static int MAX_ADS = 10000;
  final static int MAX_WORDS = 10000;
  final static int MAX_SITES = 1000;
  
  public static void main(String[] args) throws IOException {
    runTest();
  }
  
  private static void runTest() throws IOException {
    Jedis client = new Jedis("localhost"); 
    doSiteAdsRank(client);
    doSiteWordsRank(client);
    System.out.println("Press any button ...");
    System.in.read();
    client.flushAll();
    client.close();
  }  
  
  private static void doSiteWordsRank(Jedis client) {
    System.out.println("Loading SiteWordsRank data");
    String key = "sites:words:rank:";
    Random r = new Random();
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_SITES; i++) {
      String k = key + i;
      
      for (int j = 0; j < MAX_WORDS; j++) {
        String word = Utils.getRandomStr(r, 8);
        client.zadd(k, r.nextDouble(), word);
        count++;
        if (count % 100000 == 0) {
          System.out.println("SiteWordsRank :"+ count);
        }
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("SiteWordsRank : loaded "+ count + " in "+ (end-start)+"ms");

  }
  
  private static void doSiteAdsRank(Jedis client) {
    System.out.println("Loading  SiteAdsRank data");
    String key = "sites:ads:rank:";
    Random r = new Random();
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_SITES; i++) {
      String k = key + i;
      for (int j = 0; j < MAX_ADS; j++) {
        int adsId = j;
        client.zadd(k.getBytes(), r.nextDouble(), Bytes.toBytes(adsId));
        count++;
        if (count % 100000 == 0) {
          System.out.println("SiteAdsRank :"+ count);
        }
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("SiteAdsRank : loaded "+ count + " in "+ (end-start)+"ms");
  }  
}
