package org.bigbase.carrot.examples.geoip;

import java.io.IOException;
import java.util.List;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.UnsafeAccess;

/**
 * Test Carrot GeoIp
 * 
 * Memory usage:
 * 
 * 1. No compression    -  69MB
 * 2. LZ4 compression   -  35.43MB
 * 3. LZ4HC compression -  34.86MB
 * 
 * Redis usage - 388.5 MB
 * 
 * Redis/Carrot:
 * 
 * No compression  388.5/69 = 5.63
 * LZ4             388.5/35.43 = 10.97
 * LZ4HC           388.5/34.86 = 11.14
 * @author vrodionov
 *
 */
public class TestCarrotGeoIP {
  static List<CityBlock> blockList;
  static List<CityLocation> locList;
  
  public static void main(String[] args) throws IOException {
    runNoCompression(args[0], args[1]);
    runCompressionLZ4(args[0], args[1]);
    runCompressionLZ4HC(args[0], args[1]);
  }
  
  
  private static void runNoCompression(String f1, String f2) throws IOException {
    System.out.println("Compression=NONE");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    runTest(f1, f2);
  }
  
  private static void runCompressionLZ4(String f1, String f2) throws IOException {
    System.out.println("Compression=LZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    runTest(f1, f2);
  }
  
  private static void runCompressionLZ4HC(String f1, String f2) throws IOException {
    System.out.println("Compression=LZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    runTest(f1, f2);
  }
  
  private static void runTest(String f1, String f2) throws IOException {
    BigSortedMap map = new BigSortedMap(1000000000);
    if (blockList == null) {
      blockList = CityBlock.load(f1);
    }
    long ptr = UnsafeAccess.allocAndCopy("key1", 0, "key1".length());
    int size = "key1".length();
    long start = System.currentTimeMillis();
    int total = 0;
    for (CityBlock cb: blockList) {
      cb.saveToCarrot(map, ptr, size);
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
      cl.saveToCarrot(map);
      total++;
      if (total % 100000 == 0) {
        System.out.println("Total locs="+ total);
      }
    }
    end = System.currentTimeMillis();
    
    System.out.println("Loaded "+ locList.size() +" locations in "+ (end-start)+"ms");
    System.out.println("Total memory used="+ BigSortedMap.getTotalAllocatedMemory());
    
    map.dispose();
  }
}
