package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Test;

import com.google.common.util.concurrent.AtomicDouble;

public class BigSortedMapDirectMemoryTestMT {

  static AtomicDouble putsPs = new AtomicDouble();
  static AtomicDouble getsPs = new AtomicDouble();
  static AtomicDouble scanPs = new AtomicDouble();
  
  static class Worker extends Thread {
    long totalLoaded = 1;
    BigSortedMap map;
    long totalOps = 0;
    long key;
    int keySize;
    long value;
    int valueSize;
    
    public Worker(BigSortedMap map, String name) {
      super(name);
      this.map = map;
    }
    public void run() {      
      initKeyValue();
      long start = System.currentTimeMillis();
      runPuts();
      long end = System.currentTimeMillis();
      putsPs.addAndGet((double) (totalOps * 1000)/(end-start));
      totalOps = 0;
      start = System.currentTimeMillis();
      runGets();
      end = System.currentTimeMillis();
      getsPs.addAndGet((double) (totalOps * 1000)/(end-start));
      totalOps = 0;
      start = System.currentTimeMillis();
      try {
        runScan();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        fail(e.getMessage());
      }
      end = System.currentTimeMillis();
      scanPs.addAndGet((double) (totalOps * 1000)/(end-start));
      totalOps = 0;
    }  
    
    
    private void runScan() throws IOException {
        System.out.println(Thread.currentThread().getName()+ "-test SCANs");  

      byte[] startKey = getThreadName();
      byte[] endKey = getThreadName();
      endKey[endKey.length -1]++;
      long startPtr = UnsafeAccess.allocAndCopy(startKey, 0, startKey.length);
      long endPtr = UnsafeAccess.allocAndCopy(endKey, 0, endKey.length);
      int size = startKey.length;
      BigSortedMapScanner scanner = map.getScanner(startPtr, size,  endPtr, size);
      long counter = 0;
      //TODO: change this if keys are diff sizes
//      byte[] current = new byte[key.length];
//      byte[] value = new byte[this.value.length];
//      byte[] prev = new byte[key.length];
      long start = System.currentTimeMillis();
      
      while(scanner.hasNext()) {
        int keySize = scanner.keySize();
        assertTrue(keySize > 0);
//        scanner.key(current, 0);
//        scanner.value(value, 0);
//        if (counter > 0) {
//          int result = Utils.compareTo(prev, 0, prev.length, current, 0, current.length);
//          assertTrue(Thread.currentThread().getName()+ 
//                " ERROR prev="+ new String (prev) + " cur="+new String(current), result < 0);
//        }
        counter++;        
//        System.arraycopy(current,  0,  prev,  0, prev.length);
        scanner.next();
      }
      scanner.close();
      totalOps = counter;
      long end = System.currentTimeMillis();
      System.out.println(getName()+": Time to scan "+ counter+" records is "+ (end -start)+"ms");
      assertEquals(Thread.currentThread().getName()+" ERROR totalLoaded="+ totalLoaded +" scanned="+ counter,
        totalLoaded, counter);
      
    }
    
    private void runGets() {
        System.out.println(Thread.currentThread().getName()+ "-test GETs ");  

      long start = System.currentTimeMillis();

      long tmp = UnsafeAccess.malloc(valueSize);
      for (int i = 1; i <= totalLoaded; i++) {
        getKey(key, i);
//        getValue(value, i);
        long size = map.get(key, keySize, tmp, valueSize, Long.MAX_VALUE);
        if (size < 0) {
          System.out.println(Thread.currentThread().getName() + " ERROR i="+i+" size="+size+" key=" + i);
          System.exit(-1);
        }
        
        if(i % 1000000 == 0) {
            System.out.println(Thread.currentThread().getName()+ "- " + i); 
        }
        totalOps++;
      }

      long end = System.currentTimeMillis();
      System.out.println("Time to get " + totalLoaded + " =" + (end - start) + "ms");
    }
    
    
    private final void getKey(long key, long v) {     
      UnsafeAccess.putLong(key + 7, v);
    }
    
    
    private final void getValue(long value, long v) {
      UnsafeAccess.putLong(value + 5, v);
    }
    
    private byte[] getThreadName() {
      return getName().getBytes();
    }
    
    void initKeyValue() {
      keySize = 17;
      valueSize = 15;
      key = UnsafeAccess.malloc(keySize);
      byte[] tnBytes = getThreadName();
      UnsafeAccess.copy(tnBytes, 0, key, tnBytes.length);  
      UnsafeAccess.copy("KEY".getBytes(), 0, key + tnBytes.length, 3);
      value = UnsafeAccess.malloc(valueSize);
      UnsafeAccess.copy("VALUE".getBytes(), 0, value, 5);
    }
    
    private void runPuts() {
      System.out.println(Thread.currentThread().getName()+ "-test PUTs"); 
      long start = System.currentTimeMillis();

      while (true) {        
        getKey(key, totalLoaded);
        getValue(value, totalLoaded);
        boolean res = map.put(key,  keySize, value,  valueSize, 0);
        if (res == false) {
          totalLoaded--;
          break;
        } else {
          totalLoaded++;
        }
        if(totalLoaded % 1000000 == 0) {
            System.out.println(Thread.currentThread().getName()+ "- " + totalLoaded); 
        }
      }
      totalOps = totalLoaded;
      long end = System.currentTimeMillis();
      System.out.println("Time to put "+ totalLoaded+" = "+ (end-start)+"ms");
    }
    
    public long getTotalLoaded() {
      return totalLoaded;
    }
  }
  
  
  
  @Test
  public void testPerf() throws IOException {

    BigSortedMap.setMaxBlockSize(4096);
    int numThreads = 8;

    int totalCycles = 2;
    int cycle = 0;
    while (cycle++ < totalCycles) {
      System.out.println("LOOP="+ cycle);
      BigSortedMap map = new BigSortedMap((long)  50 * 1024 * 1024 * 1024);

      Worker[] workers = new Worker[numThreads];
      for (int i = 0; i < numThreads; i++) {
        workers[i] = new Worker(map, getName(i));
        workers[i].start();
      }

      for (int i = 0; i < numThreads; i++) {
        try {
          workers[i].join();
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }

      System.out.println("MEM=" + BigSortedMap.getTotalAllocatedMemory() + 
        "\nDATA=" + BigSortedMap.getTotalDataSize()
          + "\nUTILIZATION=" + (((double) BigSortedMap.getTotalDataSize()) / BigSortedMap.getTotalAllocatedMemory()));
      System.out.println("num threads=" + numThreads + " PUT=" + putsPs.get() + " GET=" + getsPs.get() + " SCAN="
          + scanPs.get());
      map.dispose();
      putsPs.set(0);
      getsPs.set(0);
      scanPs.set(0);
    }
  }



  private String getName(int i) {
    return "T" + format(i);
  }



private String format(int i) {
  if ( i < 10) {
    return "00" + i;
  } else if (i < 100) {
    return "0" + i;
  };
  return Integer.toString(i);
}
        
}
