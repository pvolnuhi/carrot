package org.bigbase.carrot.examples.basic;

import java.io.IOException;
import java.util.Random;

import org.bigbase.carrot.redis.OperationFailedException;

import redis.clients.jedis.Jedis;

/**
 * This example shows how to use Redis Hashes.HINCRBY 
 * to keep huge list of atomic counters
 * 
 * Test Description:
 * 
 * Key format: "counter:number" number = [0:1M]
 * 
 * 1. Load 1M long and double counters
 * 2. Increment each by random number between 1:1000
 * 3. Calculate Memory usage
 * 

 * 
 * Redis
 * 
 * In Redis Hashes with ziplist encodings can be used to keep counters
 * TODO: we need to compare Redis optimized version with our default
 * 
 * 
 * Hashes:
 * 
 * Redis uses hashes to minimize RAM usage
 * 
 * key="counter:"+ num;
 * 
 * Hash key = Math.max(8, key.length - 3) // we use last 3 chars (digits) as a field name
 * in a hash
 * 
 * Redis usage is 8.3 bytes per counter for normal score distribution
 * and 7.3 for skewed distribution
 * 
 * This is still greater than Carrot with LZ4, LZ4HC compression enabled:
 * 
 * Normal distribution:
 * 
 * Redis = 8.3 bytes per counter
 * Carrot No compression = 8.8
 * Carrot LZ4 = 7.2
 * Carrot LZ4HC = 7.1
 * 
 * Skewed distribution:
 * 
 * Redis = 7.3 bytes per counter
 * Carrot No compression = 7.7
 * Carrot LZ4 = 6.5
 * Carrot LZ4HC = 6.4
 * 
 * 
 * Overall we still see 20-25% RAM efficiency over Redis with Carrot LZ4/LZ4HC
 * 
 * @author vrodionov
 *
 */
public class RedisHashesAtomicCounters {
  

  static long N = 1000000;
  static long totalDataSize = 0;
  static int MAX_VALUE = 1000;
  
  public static void main(String[] args) throws IOException, OperationFailedException {

    System.out.println("RUN Redis");
    runTest();

  }
  
  private static void runTest() throws IOException, OperationFailedException {
    System.out.println("Running Redis Strings test ...");

    Jedis client = new Jedis("localhost");    
    totalDataSize = 0;
    System.out.println("Running Redis Hashes test ...");
    Random r = new Random();
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < N; i++) {
      String skey = "counter:" + i;
      
      int keySize = Math.max(8, skey.length() -3);
      String key = skey.substring(0, keySize);
      String field = skey.substring(keySize);
      
      client.hincrBy(key, field, nextScoreSkewed(r));
      if (i % 10000 == 0 && i > 0) {
        System.out.println("set hash "+ i);
      }
    }
    long endTime = System.currentTimeMillis();
        
    System.out.println("Loaded " + N +" counters into hash, total size="+totalDataSize
      + " in "+ (endTime - startTime) );
    System.out.println("Press any button ...");
    System.in.read();
    
    deleteAllHash(client);
    client.close();
  }
  
  private static void deleteAllHash(Jedis client) {
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < N; i++) {
      String skey = "counter:" + i;
      int keySize = Math.max(8, skey.length() -3);
      skey = skey.substring(0, keySize);
      client.del(skey);
      if (i % 10000 == 0 && i > 0) {
        System.out.println("del "+ i);
      }
    }
    long endTime = System.currentTimeMillis();
        
    System.out.println("Deleted " + N +" counters"
      + " in "+ (endTime - startTime) );
  }
  
  private static int nextScoreSkewed(Random r) {
    double d = r.nextDouble();
    return (int)Math.rint(d*d*d*d*d * MAX_VALUE);
  }
  
  private static int nextScore(Random r) {
    return r.nextInt(MAX_VALUE);
  }
}
