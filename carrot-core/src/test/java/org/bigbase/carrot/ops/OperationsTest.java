package org.bigbase.carrot.ops;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.BigSortedMapScanner;
import org.bigbase.carrot.ops.Append;
import org.bigbase.carrot.ops.IncrementLong;
import org.bigbase.carrot.util.UnsafeAccess;
import org.junit.Test;

public class OperationsTest {

  static BigSortedMap map;
  static long totalLoaded;
  
  public void load() throws IOException {
    BigSortedMap.setMaxBlockSize(4096);
    map = new BigSortedMap(100000000L);
    totalLoaded = 0;
    long start = System.currentTimeMillis();
    byte[] LONG_ZERO = new byte[] {0,0,0,0,0,0,0,0};
    while(true) {
      totalLoaded++;
      byte[] key = ("KEY"+ (totalLoaded)).getBytes();
      byte[] value = LONG_ZERO;
      boolean result = map.put(key, 0, key.length, value, 0, value.length, 0);
      if (result == false) {
        totalLoaded--;
        break;
      }
      if (totalLoaded % 1000000 == 0) {
        System.out.println("Loaded = " + totalLoaded);
      }
    }
    long end = System.currentTimeMillis();
    map.dumpStats();
    System.out.println("Time to load= "+ totalLoaded+" ="+(end -start)+"ms");
    System.out.println("Total memory="+BigSortedMap.getTotalAllocatedMemory());
    System.out.println("Total   data="+BigSortedMap.getTotalBlockDataSize());
    System.out.println("Total  index=" + BigSortedMap.getTotalBlockIndexSize());
  }
    
  @Test
  public void testIncrement() throws IOException {
    try {
      System.out.println("Increment test");
      load();
      IncrementLong incr = new IncrementLong();
      long ptr = UnsafeAccess.malloc(16);
      int keySize;
      long totalIncrement = 2000000;
      Random r = new Random();
      long start = System.currentTimeMillis();
      for (int i = 0; i < totalIncrement; i++) {
        int n = r.nextInt((int) totalLoaded) + 1;
        keySize = getKey(ptr, n);
        incr.reset();
        incr.setIncrement(1);
        incr.setKeyAddress(ptr);
        incr.setKeySize(keySize);
        boolean res = map.execute(incr);
        assertTrue(res);
      }
      long end = System.currentTimeMillis();
      System.out.println("Time to increment " + totalIncrement + " " + (end - start) + "ms");
      BigSortedMapScanner scanner = map.getScanner(0, 0, 0, 0);
      long total = 0;
      while (scanner.hasNext()) {
        long addr = scanner.valueAddress();
        total += UnsafeAccess.toLong(addr);
        scanner.next();
      }

      assertEquals(totalIncrement, total);
    } finally {
      if (map != null) {
        map.dispose();
        map = null;
      }
    }
  }
  
  
  @Test
  public void testAppend() throws IOException {
    try {
      System.out.println("Append test");
      loadForAppend();
      Append append = new Append();
      long key = UnsafeAccess.malloc(16);
      long value = UnsafeAccess.malloc(8);
      int keySize;
      long totalAppend = 2000000;
      Random r = new Random();
      long seed = r.nextLong();
      r.setSeed(seed);
      System.out.println("SEED="+seed);
      long start = System.currentTimeMillis();
      for (int i = 0; i < totalAppend; i++) {
        
        int n = r.nextInt((int) totalLoaded) + 1;
        keySize = getKey(key, n);
        //System.out.println(" i="+i);
        append.reset();
        append.setKeyAddress(key);
        append.setKeySize(keySize);
        append.setAppendValue(value, 8);
        boolean res = map.execute(append);
        assertTrue(res);
      }
      long end = System.currentTimeMillis();
      System.out.println("Time to append " + totalAppend + " " + (end - start) + "ms");
      BigSortedMapScanner scanner = map.getScanner(0, 0, 0, 0);
      long total = 0;
      while (scanner.hasNext()) {
        int size = scanner.valueSize();
        total += size;
        scanner.next();
      }

      assertEquals((totalAppend + totalLoaded) * 8 , total);
    } finally {
      if (map != null) {
        map.dispose();
        map = null;
      }
    }
  }
  
  
  public void loadForAppend() throws IOException {
    BigSortedMap.setMaxBlockSize(4096);
    map = new BigSortedMap(1000000000L);
    totalLoaded = 0;
    long start = System.currentTimeMillis();
    byte[] LONG_ZERO = new byte[] {0,0,0,0,0,0,0,0};
    int count = 0;
    while(count++ < 2000000) {
      totalLoaded++;
      byte[] key = ("KEY"+ (totalLoaded)).getBytes();
      byte[] value = LONG_ZERO;
      boolean result = map.put(key, 0, key.length, value, 0, value.length, 0);
      if (result == false) {
        totalLoaded--;
        break;
      }
      if (totalLoaded % 1000000 == 0) {
        System.out.println("Loaded = " + totalLoaded);
      }
    }
    long end = System.currentTimeMillis();
    map.dumpStats();
    System.out.println("Time to load= "+ totalLoaded+" ="+(end -start)+"ms");
    System.out.println("Total memory="+BigSortedMap.getTotalAllocatedMemory());
    System.out.println("Total   data="+BigSortedMap.getTotalBlockDataSize());
    System.out.println("Total  index=" + BigSortedMap.getTotalBlockIndexSize());
  }
  private int getKey (long ptr, int n) {
    //System.out.print(n);
    byte[] key = ("KEY"+ (n)).getBytes();
    UnsafeAccess.copy(key, 0, ptr, key.length);
    return key.length;
  }
  
}
