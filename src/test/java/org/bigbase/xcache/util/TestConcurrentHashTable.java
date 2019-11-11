package org.bigbase.xcache.util;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.bigbase.util.Bytes;
import org.bigbase.util.ConcurrentHashTable;
import org.junit.Test;

public class TestConcurrentHashTable {

  int keyLength = 16;
  int valueLength = 25;
  
  //@Test
  public void _testSinglePutGetDelete() {
    ConcurrentHashTable table = new ConcurrentHashTable(1000000, keyLength, valueLength);
    byte[] key = new byte[keyLength];
    Random r = new Random();
    r.nextBytes(key);
    byte[] value = new byte[valueLength];
    r.nextBytes(value);
    
    boolean result = table.put(key, 0, value, 0);
    assertTrue(result);
    byte[] v = table.get(key, 0);
    assertNotNull(v);
    assertTrue(Bytes.compareTo(value, v) == 0);
    
    result = table.delete(key, 0);
    assertTrue(result);
    v = table.get(key, 0);
    assertNull(v);
    table.dispose();
        
  }
  
  //@Test
  public void _testMultiplePutGetDelete() {

        for(int i=0; i < 1; i ++) {
          test();
        }
  }
  
  private final void test() {
    ConcurrentHashTable table = new ConcurrentHashTable(10000000, keyLength, valueLength);
    byte[][] keys = new byte[(int)table.maxCapacity()/10][keyLength];
    Random r = new Random();
    for(int i=0; i < keys.length; i++) {
      keys[i] = new byte[keyLength];
      r.nextBytes(keys[i]);
    }
    byte[] value = new byte[valueLength];
    r.nextBytes(value);
    
    long t1 = System.nanoTime();
    for(int i=0; i < keys.length; i++){    
      boolean result = table.put(keys[i], 0, value, 0);
      assertTrue(result);
    }
    long t2 = System.nanoTime();
    System.out.println("Put "+ keys.length+" kvs in "+(t2-t1)/1000 +" micros");
    t1 = System.nanoTime();;
    for(int i=0; i < keys.length; i++) {    
      byte[] v = table.get(keys[i], 0);
      assertNotNull(v);
      assertTrue(Bytes.compareTo(value, v) == 0);
    }
    t2 = System.nanoTime();
    System.out.println("Get "+ keys.length+" kvs in "+(t2-t1)/1000 +" micros");

    t1 =System.nanoTime();
    for(int i=0; i < keys.length; i++) {    
      boolean result = table.delete(keys[i], 0);
      assertTrue(result);
    }
    
    t2 =System.nanoTime();

    System.out.println("Delete "+ keys.length+" kvs in "+(t2-t1)/1000 +" micros");

    t1 = System.nanoTime();;
    for(int i=0; i < keys.length; i++) {    
      byte[] v = table.get(keys[i], 0);
      assertNull(v);
    }
    t2 =System.nanoTime();
    System.out.println("Get (not found) "+ keys.length+" kvs in "+(t2-t1)/1000 +" micros");
    table.dispose();
  }
  
 // @Test
  
  public void _testPerformanceFilled() {
    ConcurrentHashTable table = new ConcurrentHashTable(1000000, keyLength, valueLength);
    byte[][] keys = new byte[(int)table.maxCapacity()* 3/4][keyLength];
    Random r = new Random();
    for(int i=0; i < keys.length; i++) {
      keys[i] = new byte[keyLength];
      r.nextBytes(keys[i]);
    }
    byte[] value = new byte[valueLength];
    r.nextBytes(value);
    
    long puts = 0;
    long t1 = System.nanoTime();
    for(int i=0; i < keys.length; i++){    
      boolean result = table.put(keys[i], 0, value, 0);
      
    }
    long t2 = System.nanoTime();
    System.out.println("Put (up to 0.75)"+ keys.length+" kvs in "+(t2-t1)/1000 +" micros");
    
    keys = new byte[(int)table.maxCapacity()][keyLength];
    for(int i=0; i < keys.length; i++) {
      keys[i] = new byte[keyLength];
      r.nextBytes(keys[i]);
    }
    
    t1 = System.nanoTime();
    for(int i = 0; i < keys.length; i++) {
      boolean result = table.put(keys[i], 0, value, 0);
      if(result) puts++;
      byte[] v = table.get(keys[i], 0);
      table.delete(keys[i], 0);
    }
    t2 = System.nanoTime();
    
    System.out.println("Put/Get/Delete (filled)"+ keys.length+" kvs in "+(t2-t1)/1000 +" micros. Puts="+puts);
    table.dispose();        
  }
  
  @Test
  public void testConcurrentAccess() throws InterruptedException {
    
    final ConcurrentHashTable table = new ConcurrentHashTable(50000000, keyLength, valueLength);
    table.setDebug(true);
    
    final byte[][] keys = new byte[(int)table.maxCapacity()* 3/4][keyLength];
    Random r = new Random();
    for(int i=0; i < keys.length; i++) {
      keys[i] = new byte[keyLength];
      r.nextBytes(keys[i]);
    }
    final byte[][] values = new byte[(int)table.maxCapacity()* 3/4][valueLength];
    for(int i=0; i < values.length; i++) {
      values[i] = new byte[valueLength];
      r.nextBytes(values[i]);
    }
    
    long puts = 0;
    long t1 = System.nanoTime();
    long failed = 0;
    for(int i=0; i < keys.length; i++){    
      boolean result = table.put(keys[i], 0, values[i], 0);  
      if(!result) failed ++;
    }
    
    long t2 = System.nanoTime();
    System.out.println("Put (up to 0.75)"+ keys.length+" kvs in "+(t2-t1)/1000 +" micros. Failed "+failed);
    
    // Start multiple threads to get/delete/put
    t1 = System.currentTimeMillis();
    final long numIterations = 1000000000;
    Thread[] workers = new Thread[8];
    Runnable run = new Runnable() {
      public void run() {
        Random r = new Random();
        int count = 0;
        int totalKeys = keys.length;
        int found = 0, notFound = 0;
        byte[] value = new byte[valueLength];
        while(count++ < numIterations) {
          int i = r.nextInt(totalKeys);
          byte[] key = keys[i];
          boolean result = table.get(key, 0, value, 0);
          if(result) {
            found++;
            assertTrue(Bytes.compareTo(value, values[i]) == 0);
          } else {
            notFound++;
          }
          if(result) {
            // delete
            table.delete(key, 0);
          } else {
            // Put it back
            table.put(key, 0, values[i], 0);
          }          
          if(count % 1000000 == 0) {
            System.out.println(Thread.currentThread().getName()+" iter="+count+
              " found="+ found+ " notFound="+notFound);
          }
        }       
      }
    };
    
    for( int i = 0; i < workers.length; i++) {
       workers[i] = new Thread(run);
       workers[i].start();
    }
    
    for( int i = 0; i < workers.length; i++) {      
      workers[i].join();
    }
    
    t2 = System.currentTimeMillis();
    
    System.out.println("Finished " + (2 *workers.length*numIterations)+" ops in "+(t2-t1)+"ms");
  }
  
}
