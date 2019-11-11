package org.bigbase.xcache.util;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.bigbase.util.UnsafeAccess;
import org.junit.Test;

public class TestMultipleWriters {

  public AtomicLong position = new AtomicLong(0);
  public long address;
  public long length = 100000000;
  
  @Test
  public void testMultipleWriters() throws InterruptedException {
    address = UnsafeAccess.theUnsafe.allocateMemory(length);
    int numThreads = 8;
    int counter = 0;
    
    while (counter++ < 100) {
      position.set(0);
      long t1 = System.currentTimeMillis();
      Writer[] workers = new Writer[numThreads];
      for (int i = 0; i < numThreads; i++) {
        workers[i] = new Writer(i + 1);
        workers[i].start();
      }

      for (int i = 0; i < numThreads; i++) {
        workers[i].join();
      }
      long t2 = System.currentTimeMillis();
      // Do we need this?
      UnsafeAccess.theUnsafe.fullFence();
      System.out.println(length / 10 + "sps in " + (t2 - t1) + "ms");
      // Verification
      for (int i = 0; i < numThreads; i++) {
        int id = workers[i].getid();
        List<Long> poss = workers[i].getPositions();
        for (Long address : poss) {
          for (int ii = 0; ii < 10; ii++, address++) {
            assertTrue(UnsafeAccess.theUnsafe.getByte(address) == id);
          }
        }
      }
    }
  }
  
  
  class Writer extends Thread {
    private int id;
    List<Long> poss = new ArrayList<Long>();
    byte[] buf = new byte[10];
    public Writer(int id) {
      this.id = id;
      for(int i=0; i < buf.length; i++) {
        buf[i] = (byte) id;
      }
    }
    
    public void run() {
      while(true) {
        long off = position.addAndGet(buf.length);
        if(off > length - buf.length) {
          return;
        }
        poss.add(address + off -buf.length);
        UnsafeAccess.copy(buf, 0, address + off -buf.length, buf.length);
      }
    }
    
    public int getid() {
      return id;
    }
    
    public List<Long> getPositions()
    {
      return poss;
    }
  }
}
