package org.bigbase.carrot.examples.basic;

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
 * list of all English words 
 * 
 * File: words_alpha.txt.s.
 * 
 * RESULTS:
 * 0. Total number of words is 370099
 * 1. Raw size of all words is             3,494,665 bytes
 * 2. Carrot NoCompression     - RAM usage 4,306,191, COMPRESSION = 0.81
 * 3  Carrot LZ4 compression   - RAM usage 2,857,311, COMPRESSION = 1.22
 * 4. Carrot LZ4HC compression - RAM usage 2,601,695, COMPRESSION = 1.34
 * 
 * LZ4 compression relative to NoCompression = 1.22/0.81 = 1.5
 * LZ4HC compression  relative to NoCompression = 1.34/0.81 = 1.65
 * 
 * Redis SET estimated RAM usage is 35MB ( ~ 100 bytes per word)
 * (actually it can be more, this is a low estimate based on evaluating Redis code) 
 * 
 * RAM usage (Redis-to-Carrot)
 * 
 * 1) No compression    35M/3.5M ~ 10x
 * 2) LZ4   compression 35M/2.8M ~ 15x
 * 3) LZ4HC compression 35M/2.6M ~ 16.5x 
 * 
 * Effect of a compression:
 * 
 * LZ4  - 1.22/0.81 = 1.5    (to no compression)
 * LZ4HC - 1.34/0.81 = 1.65  (to no compression)
 * 
 * @author vrodionov
 *
 */
public class SetsAllEnglishWords {
  
  static {
    UnsafeAccess.debug = true;
  }
  
  static long buffer = UnsafeAccess.malloc(4096);
  static List<Key> keys = new ArrayList<Key>(); 
  
  static {
    Random r = new Random();
    byte[] bkey = new byte[8];
    for (int i = 0; i < 1; i++) {
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
    
    System.out.println("Loaded " + count * keys.size() +" words, total size="+ totalLength + 
      " in " + (endTime - startTime) + "ms. RAM usage="+ UnsafeAccess.getAllocatedMemory());
    System.out.println("COMPRESSION="+ ((double) totalLength) / UnsafeAccess.getAllocatedMemory());
    dis.close();
    
    BigSortedMap.printMemoryAllocationStats();
    for(Key key: keys) {
      Sets.DELETE(map, key.address, key.length);
    }
    map.dispose();
  }

  private static void usage() {
    System.out.println("usage: java org.bigbase.carrot.examples.AllEnglishWords domain_list_file");
    System.exit(-1);
  } 
}
