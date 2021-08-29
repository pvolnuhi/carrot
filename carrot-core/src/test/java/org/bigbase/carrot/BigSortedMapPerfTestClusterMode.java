/**
 *    Copyright (C) 2021-present Carrot, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 */
package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.junit.Test;

public class BigSortedMapPerfTestClusterMode {

  class SingleRun implements Runnable {
    
    BigSortedMap map;
    long totalLoaded;
    long totalScanned = 0;
    
    private void setUp() {
            
      map = new BigSortedMap();
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
          System.out.println(Thread.currentThread().getId()+": Loaded " + totalLoaded + " RAM alocated=" + BigSortedMap.getGlobalAllocatedMemory());
        }
      }
      long end = System.currentTimeMillis();
      System.out.println("Time to load="+ totalLoaded+" ="+(end -start)+"ms" + " RPS=" + (totalLoaded * 1000)/(end-start));
      System.out.println("Total memory="+BigSortedMap.getGlobalAllocatedMemory());
    }
    
    private void tearDown() {
      map.dispose();
    }
    
    public void testCountRecords() throws IOException {
      System.out.println("testCountRecords");
      int n = 10;
      long start = System.currentTimeMillis();
      for (int i=0; i < n; i++) {
        System.out.println(Thread.currentThread().getId() + ": Scan Run started "+ i);
        totalScanned += countRecords();
        System.out.println(Thread.currentThread().getId()+": Scan Run finished "+ i);
      }
      long end = System.currentTimeMillis();
      
      System.out.println( Thread.currentThread().getId() + ": "+ totalScanned * 1000 / (end -start) + " RPS");
      assertEquals(n * totalLoaded, totalScanned);
    }
    
    long countRecords() throws IOException {
      BigSortedMapScanner scanner = map.getScanner(0, 0, 0, 0);
      long counter = 0;
      while(scanner.hasNext()) {
        counter++;
        scanner.next();
      }
      scanner.close();
      return counter;
    }
    
    
    @Override
    public void run() {
      setUp();
      try {
        testCountRecords();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      tearDown();
    }
  }
  
  @Test
  public void testClusterPerformance() {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    BigSortedMap.setGlobalMemoryLimit(10000000000L);
    int numThreads = 8;
    Thread[] workers = new Thread[numThreads];
    
    for (int i = 0; i < numThreads; i++) {
      SingleRun sr = new SingleRun();
      workers[i] = new Thread(sr);
      workers[i].start();
    }
    
    for(int i = 0; i < numThreads; i++) {
      try {
        workers[i].join();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    
    BigSortedMap.printGlobalMemoryAllocationStats();
  }
  

}
