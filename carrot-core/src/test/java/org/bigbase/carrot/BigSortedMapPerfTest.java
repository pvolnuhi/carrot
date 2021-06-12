package org.bigbase.carrot;

import java.io.IOException;

import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.junit.BeforeClass;
import org.junit.Test;

public class BigSortedMapPerfTest {

  static BigSortedMap map;
  static long totalLoaded;
  static long totalScanned = 0;
  
  @BeforeClass 
  public static void setUp() {
    System.out.println("Set up: block = 4096; Mem="+ 10000000);
    
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    
    BigSortedMap.setMaxBlockSize(4096);
    map = new BigSortedMap(4000000000L);
    totalLoaded = 1;
    long start = System.currentTimeMillis();
    while(true) {
      byte[] key = ("KEY"+ (totalLoaded)).getBytes();
      byte[] value = ("VALUE"+ (totalLoaded)).getBytes();
      boolean res = map.put(key, 0, key.length, value, 0, value.length, 0);
      if (res == false) {
        totalLoaded--;
        break;
      }
      totalLoaded++; 
      if (totalLoaded % 100000 == 0) {
        System.out.println("Loaded " + totalLoaded + " RAM alocated=" + BigSortedMap.getTotalAllocatedMemory());
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("Time to load="+ totalLoaded+" ="+(end -start)+"ms");
    System.out.println("Total memory="+BigSortedMap.getTotalAllocatedMemory());
  }
  
  @Test
  public void testCountRecords() throws IOException {
    System.out.println("testCountRecords");
    int n = 10;
    long start = System.currentTimeMillis();
    for (int i=0; i < n; i++) {
      System.out.println("Scan Run started "+ i);
      totalScanned += countRecords();
      System.out.println("Scan Run finished "+ i);
    }
    long end = System.currentTimeMillis();
    
    System.out.println( totalScanned * 1000 / (end -start) + " RPS");
  }
  
  long countRecords() throws IOException {
    BigSortedMapDirectMemoryScanner scanner = map.getScanner(0, 0, 0, 0);
    long counter = 0;
    while(scanner.hasNext()) {
      counter++;
      scanner.next();
    }
    scanner.close();
    return counter;
  }
}
