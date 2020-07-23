package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.bigbase.carrot.util.Utils;
import org.junit.Ignore;
import org.junit.Test;

public class BigSortedMapPerformanceTests {

  static BigSortedMap map;
  static long totalLoaded;
  static List<byte[]> keys = new ArrayList<byte[]>();
  
 // @BeforeClass 
  public void setUp() throws IOException {
    BigSortedMap.setMaxBlockSize(4096);
    map = new BigSortedMap(100000000L);
    totalLoaded = 0;
    long start = System.currentTimeMillis();
    while(totalLoaded < 1000000) {
      totalLoaded++;
      byte[] key = ("KEY"+ (totalLoaded)).getBytes();
      keys.add(key);
      byte[] value = ("VALUE"+ (totalLoaded)).getBytes();
      map.put(key, 0, key.length, value, 0, value.length, 0);
      if (totalLoaded % 1000000 == 0) {
        System.out.println("Loaded = " + totalLoaded+" of "+ 100000000);
      }
    }
    
    Utils.sort(keys);
    
    long end = System.currentTimeMillis();
    map.dumpStats();
    System.out.println("Time to load= "+ totalLoaded+" ="+(end -start)+"ms");
    long scanned = countRecords();
    System.out.println("Scanned="+ countRecords());
    System.out.println("Total memory="+BigSortedMap.getTotalAllocatedMemory());
    System.out.println("Total   data="+BigSortedMap.getTotalBlockDataSize());
    System.out.println("Total  index=" + BigSortedMap.getTotalBlockIndexSize());
    assertEquals(totalLoaded, scanned);
  }
    
  
  @Test
  public void testAll() throws IOException {
    for (int i=0; i < 100; i++) {
      setUp();
      testDeleteAll();
      map.dispose();
      keys.clear();
    }
    
  }
  
  @Ignore
  @Test
  public void testDeleteAll() throws IOException {
    System.out.println("testDeleteAll");
    int n = keys.size(); 
    long start = System.currentTimeMillis();
    for(byte[] key: keys) {
      boolean res = map.delete(key, 0, key.length);
      assertTrue(res);
    }
    long end = System.currentTimeMillis();
    assertEquals(0, (int)countRecords());
    System.out.println("Deleted " + n+" in "+ (end - start)+"ms");
  }
  
  static long countRecords() throws IOException {
    BigSortedMapScanner scanner = map.getScanner(null, null);
    long counter = 0;
    while(scanner.hasNext()) {
      counter++;
      scanner.next();
    }
    scanner.close();
    return counter;
  }
  
}
