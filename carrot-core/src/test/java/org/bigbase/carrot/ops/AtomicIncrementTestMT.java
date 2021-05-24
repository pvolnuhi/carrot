package org.bigbase.carrot.ops;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.BigSortedMapDirectMemoryScanner;
import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.Key;
import org.bigbase.carrot.ops.IncrementLong;
import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.After;
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
  static int totalThreads = 16;
  static List<Key> keys = Collections.synchronizedList(new ArrayList<Key>());

  static class IncrementRunner extends Thread {
    
    
    public IncrementRunner(String name) {
      super(name);
    }

    public void run() {
      
      int keySize = 16;
      byte[] key = new byte[keySize];      
      long LONG_ZERO = UnsafeAccess.mallocZeroed(Utils.SIZEOF_LONG);
      Random r = new Random();

      while (true) {        
 
        double d = r.nextDouble();
        if (d < 0.5 && totalLoaded.get() > 1000) {
          // Run increment
          int n = r.nextInt((int) keys.size());
          Key k = keys.get(n);
          try {
            map.incrementLongOp(k.address, k.length, 1);
          } catch(OperationFailedException e) {
            System.err.println("Increment failed.");
            break;
          }
          totalIncrements.incrementAndGet();

        } else {
          // Run put
          totalLoaded.incrementAndGet();
          Key k = nextKey(r, key);
          boolean result = map.put(k.address, k.length, LONG_ZERO, Utils.SIZEOF_LONG, 0);
          if (result == false) {
            totalLoaded.decrementAndGet();
            break;
          } else {
            keys.add(k);
          }
          if (totalLoaded.get() % 1000000 == 0) {
            System.out.println(getName() + " loaded = " + totalLoaded+" increments="+ totalIncrements + " mem="+
          BigSortedMap.getTotalAllocatedMemory() + " max="+ map.getMaxMemory());
          }
        }
      }// end while
    }// end run()
    
    private Key nextKey(Random r, byte[] buf) {
      r.nextBytes(buf);
      long ptr = UnsafeAccess.allocAndCopy(buf, 0, buf.length);
      Key k = new Key(ptr, buf.length);
      return k;
    }
    
  }// end IncrementRunner
 

  
  @Test
  public void testIncrement() throws IOException {
    for (int k = 1; k <= 1; k++) {
      System.out.println("Increment test run #" + k);

      BigSortedMap.setMaxBlockSize(4096);
      map = new BigSortedMap(1000000000L);
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
        System.out.println("totalLoaded=" + totalLoaded + " actual="+ count);

        assertEquals(totalIncrements.get(), total);
        // CHECK THIS 
        assertEquals(keys.size(), (int) count);
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
  
}
