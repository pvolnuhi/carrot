package org.bigbase.carrot.redis.hashes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.Value;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Ignore;
import org.junit.Test;

public class HashesMultithreadedTest {

  BigSortedMap map;
  int valueSize = 16;
  int keySize = 16;
  int setSize = 10000;
  int keysNumber = 10000; // per thread
  int numThreads = 6;
  List<Value> values;
  long setupTime ;

  private List<Value> getValues() {
    byte[] buffer = new byte[valueSize / 2];
    Random r = new Random();
    values = new ArrayList<Value>();
    for (int i = 0; i < setSize; i++) {
      long ptr = UnsafeAccess.malloc(valueSize);
      int size = valueSize;
      r.nextBytes(buffer);
      UnsafeAccess.copy(buffer, 0, ptr, valueSize / 2);
      UnsafeAccess.copy(buffer, 0, ptr + valueSize / 2, valueSize / 2);
      values.add(new Value(ptr, size));
    }
    return values;
  }

  //@Before
  private void setUp() {
    setupTime = System.currentTimeMillis();
    map = new BigSortedMap(100000000000L);
    values = getValues();
  }

  //@After
  private void tearDown() {
    map.dispose();
    values.stream().forEach(x -> UnsafeAccess.free(x.address));
  }

  //@Ignore
  @Test
  public void runAllNoCompression() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    System.out.println();
    for (int i = 0; i < 100; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=NULL");
      setUp();
      runTest();
      tearDown();
      BigSortedMap.printMemoryAllocationStats();      
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  @Ignore
  @Test
  public void runAllCompressionLZ4() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    System.out.println();
    for (int i = 0; i < 100; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=LZ4");
      setUp();
      runTest();
      tearDown();
      BigSortedMap.printMemoryAllocationStats();      
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  
  @Ignore
  @Test
  public void runTest() {

    Runnable load = new Runnable() {

      @Override
      public void run() {
        int loaded = 0;
        // Name is string int
        String name = Thread.currentThread().getName();
        int id = Integer.parseInt(name);
        Random r = new Random(setupTime + id);
        long ptr = UnsafeAccess.malloc(keySize);
        byte[] buf = new byte[keySize];
        for (int i = 0; i < keysNumber; i++) {
          r.nextBytes(buf);
          UnsafeAccess.copy(buf, 0, ptr, keySize);
          for (Value v : values) {
            int res = Hashes.HSET(map, ptr, keySize, v.address, v.length, v.address, v.length);
            assertEquals(1, res);
            loaded++;
            if (loaded % 1000000 == 0) {
              System.out.println(Thread.currentThread().getName() + " loaded "+ loaded);
            }
          }
          int card = (int) Hashes.HLEN(map, ptr, keySize);
          if (card != values.size()) {
            System.err.println("First CARD=" + card);
            //int total = Hashes.elsize.get();
            //int[] prev = Arrays.copyOf(Hashes.elarr.get(), total);
            card = (int) Hashes.HLEN(map, ptr, keySize);
            System.err.println("Second CARD=" + card);

            //int total2 = Hashes.elsize.get();
            //int[] prev2 = Arrays.copyOf(Hashes.elarr.get(), total2);
            //dump(prev, total, prev2, total2);
            
            Thread.dumpStack();
            System.exit(-1);
          }
          assertEquals(values.size(), card);
        }
        UnsafeAccess.free(ptr);
      }

      private void dump(int[] prev, int total, int[] prev2, int total2) {
        // total2 > total
        System.err.println("total=" + total + " total2="+ total2);
        int i = 0;
        for (; i < total; i++) {
          System.err.println(prev[i] + " " + prev2[i]);
        }
        for (; i < total2; i++ ) {
          System.err.println("** " + prev2[i]);
        }
      }
    };
    Runnable get = new Runnable() {

      @Override
      public void run() {
        int read = 0;
        // Name is string int
        String name = Thread.currentThread().getName();
        int id = Integer.parseInt(name);
        Random r = new Random(setupTime + id);
        long ptr = UnsafeAccess.malloc(keySize);
        long buffer = UnsafeAccess.malloc(valueSize);
        byte[] buf = new byte[keySize];
        for (int i = 0; i < keysNumber; i++) {
          r.nextBytes(buf);
          UnsafeAccess.copy(buf, 0, ptr, keySize);
          for (Value v : values) {
            int res = Hashes.HGET(map, ptr, keySize, v.address, v.length, buffer, valueSize);
            assertEquals(valueSize, res);
            assertEquals(0, Utils.compareTo(v.address, v.length, buffer, valueSize));
            read++;
            if (read % 1000000 == 0) {
              System.out.println(Thread.currentThread().getName() + " read "+ read);
            }
          }
        }
        UnsafeAccess.free(ptr);
        UnsafeAccess.free(buffer);
      }
    };

    Runnable delete = new Runnable() {

      @Override
      public void run() {
        // Name is string int
        String name = Thread.currentThread().getName();
        int id = Integer.parseInt(name);
        Random r = new Random(setupTime + id);
        long ptr = UnsafeAccess.malloc(keySize);
        byte[] buf = new byte[keySize];

        for (int i = 0; i < keysNumber; i++) {
          r.nextBytes(buf);
          UnsafeAccess.copy(buf, 0, ptr, keySize);
          long card = (int) Hashes.HLEN(map, ptr, keySize);
          if (card != setSize) {
            Thread.dumpStack();
            System.exit(-1);
          }
          assertEquals(setSize, (int) card);
          boolean res = Hashes.DELETE(map, ptr, keySize);
          assertTrue(res);
          card = Hashes.HLEN(map, ptr, keySize);
          if (card != 0) {
            System.err.println("FAILED delete, card ="+ card);
            System.exit(-1);
          }
          assertEquals(0L, card);
        }
        UnsafeAccess.free(ptr);
      }
    };

    System.out.println("Loading data");
    Thread[] workers = new Thread[numThreads];

    long start = System.currentTimeMillis();
    for (int i = 0; i < numThreads; i++) {
      workers[i] = new Thread(load, Integer.toString(i));
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

    long end = System.currentTimeMillis();

    System.out.println("Loading " + (numThreads * keysNumber * setSize) + " elements os done in "
        + (end - start) + "ms");
    System.out.println("Reading data");
    start = System.currentTimeMillis();
    for (int i = 0; i < numThreads; i++) {
      workers[i] = new Thread(get, Integer.toString(i));
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

    end = System.currentTimeMillis();

    System.out.println("Reading " + (numThreads * keysNumber * setSize) + " elements os done in "
        + (end - start) + "ms");
    System.out.println("Deleting  data");
    start = System.currentTimeMillis();
    for (int i = 0; i < numThreads; i++) {
      workers[i] = new Thread(delete, Integer.toString(i));
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
    end = System.currentTimeMillis();
    System.out.println("Deleting of " + numThreads * keysNumber + " sets in " + (end - start)+"ms");
    assertEquals(0L, BigSortedMap.countRecords(map));
  }
}
