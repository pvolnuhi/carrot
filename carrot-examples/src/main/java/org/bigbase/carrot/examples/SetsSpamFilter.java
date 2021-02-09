package org.bigbase.carrot.examples;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.Key;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.redis.sets.Sets;
import org.bigbase.carrot.util.UnsafeAccess;

/**
 * This example shows how to use Carrot Set to keep
 * list of a REAL spam domains 
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
 * Redis SET estimated RAM usage is 50MB (~ 100 bytes per record)
 * (actually it can be more, this is a low estimate based on evaluating Redis code) 
 * 
 * RAM usage (Redis-to-Carrot)
 * 
 * 1) No compression    50M/8.5M ~ 5.9x
 * 2) LZ4   compression 50M/6.6M ~ 7.6x
 * 3) LZ4HC compression 50M/6.1M ~ 8.2x 
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
public class SetsSpamFilter {
  
  static {
    UnsafeAccess.debug = true;
  }
  
  static long buffer = UnsafeAccess.malloc(4096);
  static List<Key> keys = new ArrayList<Key>(); 
  
  static {
    Random r = new Random();
    byte[] bkey = new byte[8];
    for (int i = 0; i < 10; i++) {
      r.nextBytes(bkey);
      long key = UnsafeAccess.malloc(bkey.length);
      UnsafeAccess.copy(bkey, 0, key, bkey.length);
      keys.add(new Key(key, bkey.length));
    }
  }
  
  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      usage();
    }
    System.out.println("RUN compression = NONE");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    runTest(args[0]);
    System.out.println("RUN compression = LZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    runTest(args[0]);
    System.out.println("RUN compression = LZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    runTest(args[0]);

  }
  
  @SuppressWarnings("deprecation")
  private static void runTest(String fileName) throws IOException {
    
    BigSortedMap map =  new BigSortedMap(100000000);
    File f = new File(fileName);
    FileInputStream fis = new FileInputStream(f);
    DataInputStream dis = new DataInputStream(fis);
    String line = null;
    long totalLength = 0;
    int count = 0;
    
    long startTime = System.currentTimeMillis();
    while((line = dis.readLine()) != null) {
      byte[] data = line.getBytes();
      UnsafeAccess.copy(data, 0, buffer, data.length);
      totalLength += data.length * keys.size();
      count++;
      for (Key key : keys) {
        Sets.SADD(map, key.address,  key.length, buffer, data.length);
      }
      if ((count % 100000) == 0 && count > 0) {
        System.out.println("Loaded " + count);
      }
    }
    long endTime = System.currentTimeMillis();
    
    System.out.println("Loaded " + count * keys.size() +" records, total size="+ totalLength + 
        " in " + (endTime - startTime) + "ms. RAM usage="+ UnsafeAccess.getAllocatedMemory());
    System.out.println("COMPRESSION ="+ ((double) totalLength)
      / UnsafeAccess.getAllocatedMemory());
    dis.close();
    
    BigSortedMap.printMemoryAllocationStats();
    for(Key key: keys) {
      Sets.DELETE(map, key.address, key.length);
    }
    map.dispose();
  }

  private static void usage() {
    System.out.println("usage: java org.bigbase.carrot.examples.SpamFilter domain_list_file");
    System.exit(-1);
  } 
}
