package org.bigbase.carrot.examples.basic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.Key;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.redis.OperationFailedException;
import org.bigbase.carrot.redis.strings.Strings;
import org.bigbase.carrot.util.UnsafeAccess;

/**
 * This example shows how to use Carrot Strings.INCRBY 
 * and Strings.INCRBYFLOAT to keep huge list of atomic counters
 * Test Description:
 * 
 * Key format: "counter:number" number = [0:1M]
 * 
 * 1. Load 1M long and double counters
 * 2. Increment each by random number 0 - 1000
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
 * Effect of a compression:
 * 
 * LZ4  - 37.5/10.8 = 3.5    (to no compression)
 * LZ4HC - 37.5/10.3 = 3.6  (to no compression)
 * 
 * Redis
 * 
 * In Redis Hashes with ziplist encodings can be used to keep counters
 * TODO: we need to compare Redis optimized version with our default
 * 
 * @author vrodionov
 *
 */
public class StringsAtomicCounters {
  
  static {
    UnsafeAccess.debug = true;
  }
  
  static long buffer = UnsafeAccess.malloc(4096);
  static List<Key> keys = new ArrayList<Key>(); 
  static long keyTotalSize = 0;
  static long N = 1000000;
  static int MAX_VALUE = 1000;

  static {
    for (int i = 0; i < N; i++) {
      String skey = "counter:" + i;
      byte[] bkey = skey.getBytes();
      long key = UnsafeAccess.malloc(bkey.length);
      UnsafeAccess.copy(bkey, 0, key, bkey.length);
      keys.add(new Key(key, bkey.length));
      keyTotalSize += skey.length();
    }
    Collections.shuffle(keys);
  }
  
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

  }
  
  private static void runTest() throws IOException, OperationFailedException {
    
    BigSortedMap map =  new BigSortedMap(100000000);

    long startTime = System.currentTimeMillis();
    int count =0;
    Random r = new Random();
    for (Key key: keys) {
      count++;
      Strings.INCRBY(map, key.address, key.length, r.nextInt(MAX_VALUE));
      if (count % 100000 == 0) {
        System.out.println("set long "+ count);
      }
    }
    long endTime = System.currentTimeMillis();
    
    System.out.println("Loaded " + keys.size() +" long counters of avg size=" +(keyTotalSize/N + 8)+ " each in "
      + (endTime - startTime) + "ms. RAM usage="+ (UnsafeAccess.getAllocatedMemory() - keyTotalSize));
    
    BigSortedMap.printMemoryAllocationStats();
    // Delete all
    for (Key key: keys) {
      Strings.DELETE(map, key.address, key.length);
    }
    
//    // Now test doubles
//    count = 0;
//    startTime = System.currentTimeMillis();
//    
//    for (Key key: keys) {
//      count++;
//      Strings.INCRBYFLOAT(map, key.address, key.length, 1d);
//      if (count % 100000 == 0) {
//        System.out.println("set float "+ count);
//      }
//    }
//    
//    endTime = System.currentTimeMillis();
//    
//    System.out.println("Loaded " + keys.size() +" double counters of avg size=" +(keyTotalSize/N + 8)+ " each in "
//        + (endTime - startTime) + "ms. RAM usage="+ (UnsafeAccess.getAllocatedMemory() - keyTotalSize));
    BigSortedMap.printMemoryAllocationStats();

    map.dispose();
    
  }

}
