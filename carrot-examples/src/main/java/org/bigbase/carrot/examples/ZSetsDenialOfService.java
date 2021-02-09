package org.bigbase.carrot.examples;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.redis.OperationFailedException;
import org.bigbase.carrot.redis.zsets.ZSets;
import org.bigbase.carrot.util.UnsafeAccess;

/**
 * This example shows how to use Carrot ZSets to store 
 * 
 *  {host_name, number_of_requests} pairs in a sorted sets to track
 *  most offensive sites (for TopN queries). We track information on 
 *  each host and number of requests for a particular resource 
 *  which came from this host.
 *  
 
 * Test description: <br>
 *  
 *  
 * 1. Load 1M {host, number_of_requests} pairs into Carrot sorted set  
 * 2. Calculate RAM usage
 * 
 * number_of_requests is random number between 1 and 100000
 * host - xx.xx.xx.xx string 
 * 
 * Results:
 * 0. Average data size {host, number_of_requests} = 21 bytes
 * 1. No compression. Used RAM per session object is 51 bytes (COMPRESSION= 0.41)
 * 2. LZ4 compression. Used RAM per session object is 26 bytes (COMPRESSION = 0.83)
 * 3. LZ4HC compression. Used RAM per session object is 24 bytes (COMPRESSION = 0.89)
 * 
 * Redis estimate per record , using ZSets is 150 
 * (see this question:
 * https://stackoverflow.com/questions/22965766/optimizing-redis-sorted-set-memory-usage) 
 * (actually it can be more, this is a low estimate based on evaluating Redis code) 
 * 
 * RAM usage (Redis-to-Carrot)
 * 
 * 1) No compression    150/51 ~ 3x
 * 2) LZ4   compression 150/26 ~ 6.x
 * 3) LZ4HC compression 150/24 = 6.x
 * 
 * Effect of a compression:
 * 
 * LZ4  - 51/26 ~ 2x (to no compression)
 * LZ4HC - 51/24 ~ 2x (to no compression)
 * 
 * @author vrodionov
 *
 */
public class ZSetsDenialOfService {
  
  static {
    UnsafeAccess.debug = true;
  }
  
  static long buffer = UnsafeAccess.malloc(4096);

  static long fieldBuf = UnsafeAccess.malloc(64);
  
  static long N = 1000000;
  /*
   * We use low maximum request count
   * to reflect the fact that majority of hosts are legitimate
   * and do not overwhelm the service wit a bogus requests.
   */
  static double MAX= 10d;
  
  static long totalDataSize = 0;
  static Random rnd = new Random();
  static int index = 0;
  
  static List<String> hosts = new ArrayList<String>() ;
  
  public static void main(String[] args) throws IOException, OperationFailedException {

    System.out.println("RUN compression = NONE");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    runTest();
    System.out.println("RUN compression = LZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    runTest();
    System.out.println("RUN compression = LZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    runTest();
    
    // Now load hosts
    if (args.length > 0) {
      loadHosts(args[0]);
      System.out.println("RUN compression = NONE - REAL DATA");
      BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
      runTest();
      System.out.println("RUN compression = LZ4 - REAL DATA");
      BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
      runTest();
      System.out.println("RUN compression = LZ4HC - REAL DATA");
      BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
      runTest();
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
   * This test runs pure synthetic data: host names are random IPv4 addresses, 
   * request numbers are uniformly distributed
   * between 0 and 100000
   * @throws IOException
   * @throws OperationFailedException
   */
  private static void runTest() throws IOException, OperationFailedException {
    
    index = 0;
    
    BigSortedMap map =  new BigSortedMap(1000000000);
    
    totalDataSize = 0;
    
    long startTime = System.currentTimeMillis();
    String key = "key";
    long keyPtr = UnsafeAccess.malloc(key.length());
    int keySize = key.length();
    
    UnsafeAccess.copy(key.getBytes(), 0, keyPtr, keySize);
    
    double[] scores = new double[1];
    long[] memPtrs = new long[1];
    int[] memSizes = new int[1];
    long max = hosts.size() > 0? hosts.size(): N;
    for (int i=0; i < max; i++) {
  
      String host = getNextHost();
      double score = getNextScore();
      int fSize = host.length();
      UnsafeAccess.copy(host.getBytes(), 0, fieldBuf, fSize);
      
      scores[0] = score;
      memPtrs[0] = fieldBuf;
      memSizes[0] = fSize;
      totalDataSize += fSize + 8 /*SCORE size*/; 
     
      ZSets.ZADD(map, keyPtr, keySize, scores, memPtrs, memSizes, false);
      
      if (i % 10000 == 0 && i > 0) {
        System.out.println("zset "+ i);
      }      
    }
    long endTime = System.currentTimeMillis();
    
    long num = hosts.size() > 0? hosts.size(): N;
    
    System.out.println("Loaded " + num +" [host, number] pairs"
      + ", total size="+totalDataSize
      + " in "+ (endTime - startTime) + "ms. RAM usage="+ (UnsafeAccess.getAllocatedMemory()
      + " RAM per record=" +((double)UnsafeAccess.getAllocatedMemory()/num))  
      + " COMPRESSION=" + (((double)totalDataSize))/ UnsafeAccess.getAllocatedMemory());
    
    BigSortedMap.printMemoryAllocationStats();
    map.dispose();
    
  }
  
  static double getNextScore() {
    return Math.rint(rnd.nextDouble() * MAX);
  }
  
  static String getNextHost() {
    if (hosts.size() > 0) {
      return hosts.get(index++);
    }
    
    return Integer.toString(rnd.nextInt(256)) + "." + Integer.toString(rnd.nextInt(256)) +
        "."+ Integer.toString(rnd.nextInt(256)) + "." + Integer.toString(rnd.nextInt(256));
  }
}
