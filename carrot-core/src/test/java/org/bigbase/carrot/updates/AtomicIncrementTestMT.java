package org.bigbase.carrot.updates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.BigSortedMapDirectMemoryScanner;
import org.bigbase.carrot.util.UnsafeAccess;
import org.junit.Test;

/**
 * This test load data (key - long value) and in parallel increments keys - multithreaded.
 * At the end it scans all the keys and calculated total value of all keys, 
 * compares it with expected number of increments. 
 */
public class AtomicIncrementTestMT {

  static BigSortedMap map;
  static AtomicLong totalLoaded = new AtomicLong();
  static AtomicLong totalIncrements = new AtomicLong();
  static int totalThreads = 8;
  
  static class IncrementRunner extends Thread {
    
    
    public IncrementRunner(String name) {
      super(name);
    }

    public void run() {
      Increment incr = new Increment();
      long ptr = UnsafeAccess.malloc(16);
      int keySize;
      Random r = new Random();
      byte[] LONG_ZERO = new byte[] {0,0,0,0,0,0,0,0};

      while (true) {        
        double d = r.nextDouble();
        if (d < 0.5 && totalLoaded.get() > 1000) {
          // Run increment
          int n = r.nextInt((int) totalLoaded.get()) + 1;
          keySize = getKey(ptr, n);
          if(!map.exists(ptr, keySize)) {
            continue;
          }
          incr.reset();
          incr.setIncrement(1);
          incr.setKeyAddress(ptr);
          incr.setKeySize(keySize);
          boolean res = map.update(incr);
          assertTrue(res);
          totalIncrements.incrementAndGet();
        } else {
          // Run put
          byte[] key = ("KEY"+ (totalLoaded.incrementAndGet())).getBytes();
          byte[] value = LONG_ZERO;
          boolean result = map.put(key, 0, key.length, value, 0, value.length, 0);
          if (result == false) {
            totalLoaded.decrementAndGet();
            break;
          }
        }
        if (totalLoaded.get() % 1000000 == 0) {
          System.out.println(getName() + " loaded = " + totalLoaded+" increments="+ totalIncrements);
        }
      }// end while
      UnsafeAccess.free(ptr);
    }// end run()
  }// end IncrementRunner
 
    
  @Test
  public void testIncrement() throws IOException {
    for (int k = 1; k <= 100; k++) {
      System.out.println("Increment test run #" + k);

      BigSortedMap.setMaxBlockSize(4096);
      map = new BigSortedMap(100000000L);
      totalLoaded.set(0);
      totalIncrements.set(0);
      try {
        long start = System.currentTimeMillis();
        IncrementRunner[] runners = new IncrementRunner[totalThreads];
        for (int i = 0; i < totalThreads; i++) {
          runners[i] = new IncrementRunner("Increment Runner#" + i);
          runners[i].start();
        }
        for(int i = 0; i < totalThreads; i++) {
          try {
            runners[i].join();
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
        
        long end = System.currentTimeMillis();
        BigSortedMapDirectMemoryScanner scanner = map.getScanner(0, 0, 0, 0);
        long total = 0;
        long count = 0;
        while (scanner.hasNext()) {
          count++;
          long addr = scanner.valueAddress();
          total += UnsafeAccess.toLong(addr);
          scanner.next();
        }
        assertEquals(totalIncrements.get(), total);
        assertEquals(totalLoaded.get(), count);
        map.dumpStats();
        System.out.println("Time to load= "+ totalLoaded+" and to increment =" 
            + totalIncrements+"="+(end -start)+"ms");
        System.out.println("Total memory="+BigSortedMap.getTotalAllocatedMemory());
        System.out.println("Total   data="+BigSortedMap.getTotalBlockDataSize());
        System.out.println("Total  index=" + BigSortedMap.getTotalBlockIndexSize());
      } finally {
        if (map != null) {
          map.dispose();
          map = null;
        }
      }
    }

  }
  
  static private int getKey (long ptr, int n) {
    byte[] key = ("KEY"+ (n)).getBytes();
    UnsafeAccess.copy(key, 0, ptr, key.length);
    return key.length;
  }
  
}
