package org.bigbase.carrot.examples.geoip;

import java.io.IOException;
import java.util.List;
import redis.clients.jedis.Jedis;

/**
 * RedisBook GeoIP example
 * 
 * 
 * Total memory usage: 388.5 MB
 * 
 * @author vrodionov
 *
 */
public class TestRedisGeoIP {
  static List<CityBlock> blockList;
  static List<CityLocation> locList;
  static Jedis client;
  
  public static void main(String[] args) throws IOException {
    client = new Jedis("localhost");
    runTest(args[0], args[1]);
  }
  
  private static void runTest(String f1, String f2) throws IOException {
    if (blockList == null) {
      blockList = CityBlock.load(f1);
    }
    byte[] key = "key1".getBytes();
    long start = System.currentTimeMillis();
    int total = 0;
    for (CityBlock cb: blockList) {
      cb.saveToRedis(client, key);
      total++;
      if (total % 100000 == 0) {
        System.out.println("Total blocks="+ total);
      }
    }
    long end = System.currentTimeMillis();
    
    System.out.println("Loaded "+ blockList.size() +" blocks in "+ (end-start)+"ms");
    total = 0;
    if (locList == null) {
      locList = CityLocation.load(f2);
    }
    start = System.currentTimeMillis();
    for (CityLocation cl: locList) {
      cl.saveToRedis(client);
      total++;
      if (total % 100000 == 0) {
        System.out.println("Total locs="+ total);
      }
    }
    end = System.currentTimeMillis();
    
    System.out.println("Loaded "+ locList.size() +" locations in "+ (end-start)+"ms");
    System.out.println("Press any button ...");
    System.in.read();
    client.del(key);
    
    for (CityLocation cl: locList) {
      byte[] k = cl.getKey();
      client.del(k);
    }
    
  }
}
