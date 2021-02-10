package org.bigbase.carrot.examples;

import java.io.IOException;
import java.util.Random;


import redis.clients.jedis.Jedis;

/**
 * 
 * The Test runs sparse bitmap tests in Redis with different population count = 0.01
 *  
 * Results (Carrot sparse bitmaps with LZ4HC compression vs. Redis bitmaps):
 * 
 * Bitmap size = 100,000,000
 * sizeUncompressedBitmap = 12,500,000 bytes
 * Test_consumed_RAM ~ 13,311,264 (Redis)
 * 
 *                        COMPRESSION  
 * 
 * population           CARROT LZ4HC        REDIS   
 * count (dencity)
 * 
 * dencity=0.01             4.2              0.94        
 * 
 * 
 * Carrot/Redis = 4.5
 * 
 * Notes: COMPRESSION = sizeUncompressedBitmap/Test_consumed_RAM
 * 
 * sizeUncompressedBitmap - size of an uncompressed bitmap, which can hold all the bits
 * Test_consumed_RAM - RAM consumed by test.
 * 
 * @author vrodionov
 *
 */

public class RedisSparseBitmapsComparison {
 
  static int bufferSize = 64;
  static int keySize = 8;
  static int N = 1000000;
  static int delta = 100;
  static double dencity = 0.01;
  
  static double[] dencities = 
      new double[] {0.01};
    
   
  private static void testPerformance() throws IOException {
    Jedis client = new Jedis("localhost");
    
    System.out.println("\nTest Redis Performance sparse bitmaps. dencity="+ dencity + "\n");
    long offset= 0;
    long MAX =  (long)(N / dencity);
    Random r = new Random();
    
    long start = System.currentTimeMillis();
    for (int i = 0; i < N ; i++) {
      offset = Math.abs(r.nextLong()) % MAX;
      client.setbit("key", offset, true);
      if (i % 10000 == 0 && i > 0) {
        System.out.println("i=" + i);
      }
    }
    long end  = System.currentTimeMillis();    
    
    System.out.println("Time for " + N + " population dencity="+ dencity + 
     " bitmap size=" + (MAX) +  " new SetBit=" + (end - start) + "ms");

    System.out.println("Press any button ...");
    System.in.read();
    client.del("key");
    client.close();
  }
  
  public static void main(String[] args) throws IOException {
    testPerformance();
  }
}
