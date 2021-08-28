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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.ops.OperationFailedException;
import org.bigbase.carrot.redis.hashes.Hashes;
import org.bigbase.carrot.util.Key;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * This example shows how to use Carrot Hashes.INCRBY 
 * and Hashes.INCRBYFLOAT to keep huge list of atomic counters
 * Test Description:
 * 
 * Key format: "counter:number" number = [0:100000]
 * 
 * 1. Load 100000 long and double counters
 * 2. Increment each by random number between 0-1000
 * 3. Calculate Memory usage
 * 
 * Notes: in a real usage scenario, counter values are not random
 * and can be compressed more
 * 
 * Results (Random 1..1000):
 * 
 * 1. Average counter size is 21 (13 bytes - key, 8 - value)
 * 2. Carrot No compression. 8.8 bytes per counter
 * 3. Carrot LZ4      - 7.2 bytes per counter
 * 4. Carrot LZ4HC    - 7.1 bytes per counter 
 * 5. Redis memory usage per counter is 8 bytes (HINCRBY)
 * 
 * RAM usage (Redis-to-Carrot)
 * 
 * 1) No compression    8/8.8 ~ 0.9x
 * 2) LZ4   compression 8/7.2 ~ 1.1x
 * 3) LZ4HC compression 90/7.1 ~ 1.3x 
 * 
 * Effect of a compression:
 * 
 * LZ4  - 8.8/7.2 = 1.22    (to no compression)
 * LZ4HC - 8.8/7.1 = 1.24  (to no compression)
 * 
 * Results (Semi - Random 1..100 - skewed to 0):
 * 
 * 1. Average counter size is 21 (13 bytes - key, 8 - value)
 * 2. Carrot No compression. 7.7 bytes per counter
 * 3. Carrot LZ4      - 6.5 bytes per counter
 * 4. Carrot LZ4HC    - 6.4 bytes per counter 
 * 5. Redis memory usage per counter is 7.3 bytes (HINCRBY)
 * 
 * RAM usage (Redis-to-Carrot)
 * 
 * 1) No compression    7.3/7.7 ~ 0.95x
 * 2) LZ4   compression 7.3/6.5 ~ 1.12x
 * 3) LZ4HC compression 7.3/6.4 ~ 1.14x 
 * 
 * Effect of a compression:
 * 
 * LZ4  - 7.7/6.5 = 1.18    (to no compression)
 * LZ4HC - 7.7/6.4 = 1.2  (to no compression)
 * 
 * Redis
 * 
 * In Redis Hashes with ziplist encodings can be used to keep counters
 * TODO: we need to compare Redis optimized version with our default
 * 
 * @author vrodionov
 *
 */
public class HashesAtomicCounters {
  
  static {
   // UnsafeAccess.debug = true;
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
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED1=" + seed);
    Collections.shuffle(keys, r);
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
    Utils.freeKeys(keys);
    UnsafeAccess.mallocStats.printStats();

  }
  
  private static void runTest() throws IOException, OperationFailedException {
    
    BigSortedMap map =  new BigSortedMap(100000000);

    long startTime = System.currentTimeMillis();
    int count =0;
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED2=" + seed);
        
    for (Key key: keys) {
      count++;
      // We use first 8 bytes as hash key, the rest as a field name
      int keySize = Math.max(8, key.length - 3);
      int val = nextScoreSkewed(r);
      Hashes.HINCRBY(map, key.address, keySize, key.address + keySize, key.length - keySize, 
        val);  
      if (count % 100000 == 0) {
        System.out.println("set long "+ count);
      }
    }
    long endTime = System.currentTimeMillis();
    
    System.out.println("Loaded " + keys.size() +" long counters of avg size=" +(keyTotalSize/N + 8)+ " each in "
      + (endTime - startTime) + "ms.");

    BigSortedMap.printGlobalMemoryAllocationStats();
    UnsafeAccess.mallocStats.printStats(false);
        
    count = 0;
    startTime = System.currentTimeMillis();
    for (Key key: keys) {
      count++;
      // We use first 8 bytes as hash key, the rest as a field name
      int keySize = Math.max(8, key.length - 3);
      int val = nextScoreSkewed(r);
      Hashes.HINCRBYFLOAT(map, key.address, keySize, key.address + keySize, key.length - keySize, 
        val);  
      if (count % 100000 == 0) {
        System.out.println("set float "+ count);
      }
    }
    endTime = System.currentTimeMillis();
    System.out.println("Loaded " + keys.size() +" float counters of avg size=" +(keyTotalSize/N + 8)+ " each in "
        + (endTime - startTime) + "ms.");
    // Delete keys
    count = 0;
    System.out.println("Deleting keys ...");
    for (Key key: keys) {
      int keySize = Math.max(8, key.length - 3);    
      Hashes.DELETE(map, key.address, keySize);
      if (++count % 100000 == 0) {
        System.out.println("Deleted key "+ count);
      }
    }
    
    map.dispose();
    BigSortedMap.printGlobalMemoryAllocationStats();

  }

  private static void verify (BigSortedMap map) {
    long buf = UnsafeAccess.malloc(64);
    long count = 0;
    int bufSize = 64;
    for (Key key: keys) {
      count++;
      // We use first 8 bytes as hash key, the rest as a field name
      int keySize = Math.max(8, key.length - 3);
      int val = 10;//nextScoreSkewed(r);
      int size = Hashes.HGET(map, key.address, keySize, key.address + keySize, key.length - keySize, 
        buf, bufSize);  
      if (size < 0) {
        System.err.println("FAILED count=" + count + " keySize=" + keySize + " keyLength=" + key.length);
        System.exit(-1);
      }
      String s = Utils.toString(buf, size);
      if (Integer.parseInt(s) != val) {
        System.err.println("Failed with s=" + s);
        System.exit(-1);
      }
      if (count % 100000 == 0) {
        System.out.println("verified "+ count);
      }
    }

  }
  private static int nextScoreSkewed(Random r) {
    double d = r.nextDouble();
    return (int)Math.rint(d*d*d*d*d * MAX_VALUE);
  }
}
