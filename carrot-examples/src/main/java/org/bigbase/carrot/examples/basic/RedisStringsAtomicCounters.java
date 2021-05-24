package org.bigbase.carrot.examples.basic;

import java.io.IOException;
import java.util.Random;

import org.bigbase.carrot.ops.OperationFailedException;

import redis.clients.jedis.Jedis;

/**
 * This example shows how to use Redis Strings.INCRBY 
 * and Strings.INCRBYFLOAT to keep huge list of atomic counters
 * Test Description:
 * 
 * Key format: "counter:number" number = [0:1M]
 * 
 * 1. Load 1M long and double counters
 * 2. Increment each by random number between 1:1000
 * 3. Calculate Memory usage
 * 
 * Results:
 * 
 * 1. Average counter size is 21 (13 bytes - key, 8 - value)
 * 2. Carrot No compression. 37.5 bytes per counter
 * 3. Carrot LZ4      -  10.8 bytes per counter
 * 4. Carrot LZ4HC    - 10.3 bytes per counter 
 * 5. Redis memory usage per counter is 57.5 bytes
 * 
 * RAM usage (Redis-to-Carrot)
 * 
 * 1) No compression    57.5/37.5 ~ 1.5x
 * 2) LZ4   compression 57.5/10.8 ~ 5.3x
 * 3) LZ4HC compression 57.5/10.3 ~ 5.6x 
 *  
 * 
 * @author vrodionov
 *
 */
public class RedisStringsAtomicCounters {
  

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
    
    long startTime = System.currentTimeMillis();
    Random r = new Random();
    for (int i = 0; i < N; i++) {
      String skey = "counter:" + i;
      totalDataSize += skey.length() + 8 /*length of a counter*/;    
      client.incrBy(skey, nextScoreSkewed(r));
      if (i % 10000 == 0 && i > 0) {
        System.out.println("set string "+ i);
      }
    }
    long endTime = System.currentTimeMillis();
        
    System.out.println("Loaded " + N +" counters, total size="+totalDataSize
      + " in "+ (endTime - startTime) );
   
    System.out.println("Press any button ...");
    System.in.read();
    deleteAll(client);
    client.close();
  }

  private static void deleteAll(Jedis client) {
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < N; i++) {
      String skey = "counter:" + i;
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
