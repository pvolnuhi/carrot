package org.bigbase.zcache;

import org.junit.BeforeClass;
import org.junit.Test;

public class BigSortedMapPerfTest {

  static BigSortedMapOld map;
  static long totalLoaded;
  static long totalScanned = 0;
  
  @BeforeClass 
  public static void setUp() {
    System.out.println("Set up: block = 4096; Mem="+ 10000000);
    BigSortedMapOld.setMaxBlockSize(4096);
    map = new BigSortedMapOld(1000000000);
    totalLoaded = 1;
    long start = System.currentTimeMillis();
    while(true) {
      byte[] key = ("KEY"+ (totalLoaded)).getBytes();
      byte[] value = ("VALUE"+ (totalLoaded)).getBytes();
      boolean res = map.put(key, 0, key.length, value, 0, value.length);
      if (res == false) {
        totalLoaded--;
        break;
      }
      totalLoaded++;      
    }
    long end = System.currentTimeMillis();
    System.out.println("Time to load="+ totalLoaded+" ="+(end -start)+"ms");
    System.out.println("Total memory="+map.getMemoryAllocated());
  }
  
  @Test
  public void testCountRecords() {
    System.out.println("testCountRecords");
    int n = 100;
    long start = System.currentTimeMillis();
    for (int i=0; i < n; i++) {
      totalScanned += countRecords();
     // System.out.println("c="+ i);
    }
    long end = System.currentTimeMillis();
    
    System.out.println( totalScanned * 1000 / (end -start) + " RPS");
  }
  
  long countRecords() {
    BigSortedMapScannerOld scanner = map.getScanner(null, null);
    long counter = 0;
    while(scanner.hasNext()) {
      counter++;
      scanner.next();
    }
    return counter;
  }
  
}
