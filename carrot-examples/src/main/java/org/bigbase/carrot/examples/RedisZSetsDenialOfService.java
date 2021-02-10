package org.bigbase.carrot.examples;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import redis.clients.jedis.Jedis;


/**
 * This example shows how to use Redis ZSets to store 
 * 
 *  {host_name, number_of_requests} pairs in a sorted sets to track
 *  most offensive sites (for TopN queries or BottomN). We track information on 
 *  each host and number of requests for a particular resource, which came from this host.
 *  Sorted set in Redis can't use efficient ziplist encoding due to its large size.
 
 * Test description: <br>
 *  
 * 1. Load 1M {host, number_of_requests} pairs into Carrot sorted set  
 * 2. Calculate RAM usage
 * 
 * number_of_requests is random number between 1 and 1000 heavily
 * skewed towards 0
 * host - synthetic (xx.xx.xx.xx) string or a real host name from a provided file
 * 
 * Results:
 * 0. Average data size {host, number_of_requests} = 21 bytes
 * 1. No compression. Used RAM per session object is 53 bytes 
 * 2. LZ4 compression. Used RAM per session object is 34 bytes 
 * 3. LZ4HC compression. Used RAM per session object is 30 bytes 
 * 
 * Redis usage per record , using ZSets is 116
 * 
 * Total used Redis memory is shown by 'used_memory' in Redis CLI. 
 * 
 * RAM usage (Redis-to-Carrot)
 * 
 * 1) No compression    116/53 ~ 2.2x
 * 2) LZ4   compression 116/34 ~ 3.4x
 * 3) LZ4HC compression 116/30 = 3.9x
 * 
 * Effect of a compression:
 * 
 * LZ4  - 53/34 ~ 1.6x (to no compression)
 * LZ4HC - 53/30 ~ 1.8x (to no compression)
 * 
 * @author vrodionov
 *
 */
public class RedisZSetsDenialOfService {
  
  /*
   * Maximum request count
   */
  static double MAX= 1000d;
  
  static long totalDataSize = 0;
  static Random rnd = new Random();
  static int index = 0;
  
  static List<String> hosts = new ArrayList<String>() ;
  
  public static void main(String[] args) throws IOException {
    // Now load hosts
    if (args.length > 0) {
      loadHosts(args[0]);
      System.out.println("RUN Redis - REAL DATA");
      runTest();
    } else {
      System.out.println("You forgot host site list file. Boom. Exited.");
    }
  }
  
  @SuppressWarnings("deprecation")
  private static void loadHosts(String fileName) throws IOException {
    File f = new File(fileName);
    FileInputStream fis = new FileInputStream(f);
    DataInputStream dis = new DataInputStream(fis);
    String line = null;
    while((line = dis.readLine()) != null) {
      hosts.add(line);
    }
    dis.close();
  }
  
  /**
   * This test runs real data set
   * request numbers are non-uniformly distributed
   * between 0 and 100000 (heavily skewed towards 0)
   * @throws IOException
   */
  private static void runTest() throws IOException {
    
    Jedis client = new Jedis("localhost");
        
    long startTime = System.currentTimeMillis();
    String key = "key";
    long max = hosts.size();
    
    for (int i=0; i < max; i++) {
      
      String host = getNextHost();
      double score = getNextScore();
      client.zadd(key, score, host);
      if (i % 10000 == 0 && i > 0) {
        System.out.println("zadd "+ i);
      }      
    }
    long endTime = System.currentTimeMillis();
    
    long num =  max;
    
    System.out.println("Loaded " + num +" [host, number] pairs"
      + ", total size=" + totalDataSize
      + " in "+ (endTime - startTime) + "ms. Press any button ...");
    System.in.read();
    
    client.del(key);
    client.close();
    
  }
  
  static double getNextScore() {
    double d = rnd.nextDouble();
    return Math.rint(Math.pow(d,  10) * MAX);
  }
  
  static String getNextHost() {
    return hosts.get((index++) % hosts.size());
  }
}
