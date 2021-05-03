package org.bigbase.carrot.redis.sets;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.Key;
import org.bigbase.carrot.Value;
import org.bigbase.carrot.compression.Codec;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.redis.Commons;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Ignore;
import org.junit.Test;

public class SetsTest {
  BigSortedMap map;
  Key key;
  long buffer;
  int bufferSize = 64;
  int valSize = 16;
  long n = 1000000;
  List<Value> values;
  
  static {
    //UnsafeAccess.debug = true;
  }
  
  private List<Value> getValues(long n) {
    List<Value> values = new ArrayList<Value>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("VALUES SEED=" + seed);
    byte[] buf = new byte[valSize/2];
    for (int i=0; i < n; i++) {
      r.nextBytes(buf);
      long ptr = UnsafeAccess.malloc(valSize);
      UnsafeAccess.copy(buf, 0, ptr, buf.length);
      UnsafeAccess.copy(buf, 0, ptr + buf.length, buf.length);
      values.add(new Value(ptr, valSize));
    }
    return values;
  }
  
  private List<Value> getRandomValues(long n) {
    List<Value> values = new ArrayList<Value>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("VALUES SEED=" + seed);
    byte[] buf = new byte[valSize];
    for (int i=0; i < n; i++) {
      r.nextBytes(buf);
      long ptr = UnsafeAccess.malloc(valSize);
      UnsafeAccess.copy(buf, 0, ptr, buf.length);
      values.add(new Value(ptr, valSize));
    }
    return values;
  }
  private Key getKey() {
    long ptr = UnsafeAccess.malloc(valSize);
    byte[] buf = new byte[valSize];
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("KEY SEED=" + seed);
    r.nextBytes(buf);
    UnsafeAccess.copy(buf, 0, ptr, valSize);
    return key = new Key(ptr, valSize);
  }
  
  
  private void setUp() {
    map = new BigSortedMap(1000000000);
    buffer = UnsafeAccess.mallocZeroed(bufferSize); 
    values = getValues(n);
  }
  
  //@Ignore
  @Test
  public void runAllNoCompression() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    System.out.println();
    for (int i = 0; i < 1; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=NULL");
      allTests();
      BigSortedMap.printMemoryAllocationStats();      
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  @Ignore
  @Test
  public void runAllCompressionLZ4() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    System.out.println();
    for (int i = 0; i < 1; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=LZ4");
      allTests();
      BigSortedMap.printMemoryAllocationStats();      
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  @Ignore
  @Test
  public void runAllCompressionLZ4HC() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    System.out.println();
    for (int i = 0; i < 10; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=LZ4HC");
      allTests();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  private void allTests() throws IOException {
//    long start = System.currentTimeMillis();
//    setUp();
//    testSADDSISMEMBER();
//    tearDown();
//    setUp();
//    testAddRemove();
//    tearDown();
//    setUp();
//    testAddMultiDelete();
//    tearDown();
//    long end = System.currentTimeMillis();    
//    System.out.println("\nRUN in " + (end -start) + "ms");
//   testMemoryUsageForInts();
//   testCompressionSortedIntSet();
    testPerformance();
  }
  
  @Ignore
  public void testPerformance() {
    perfRun(1000000);
    perfRun(10000000);
    perfRun(100000000);
  }
  
  
  private void perfRun(int n) {
    System.out.println("Performance test run with " + n);
    map = new BigSortedMap(10000000000L);

    int toQuery = 1000000;
    int nn = n > 10000000? 10000000: n;
      
    values = getRandomValues(nn);
    Key key = getKey();
    long start = System.currentTimeMillis();
    int count = 0;
    for (Value v : values) {
      int res = Sets.SADD(map, key.address, key.length, v.address, v.length);
      assertEquals(1, res);
      count++;
      if (count % 1000000 == 0) {
        System.out.println("Loaded " + count);
      }
    }
    Random r = new Random();
    if (n > nn) {
      byte[] buf = new byte[valSize];
      for (int i = nn; i < n; i++) {
        r.nextBytes(buf);
        long ptr = UnsafeAccess.allocAndCopy(buf, 0, buf.length);
        int size = buf.length;
        Sets.SADD(map, key.address, key.length, ptr, size);
        UnsafeAccess.free(ptr);
        if ((i > nn) && (i % 1000000 == 0)) {
          System.out.println("Loaded " + (i));
        } 
      }
    }
    long end = System.currentTimeMillis();
    
    System.out.println(n + " items: load=" + ((double) n * 1000 / (end - start)) + " RPS");
    assertEquals(n, (int) Sets.SCARD(map, key.address, key.length));
 
    Runnable run = new Runnable() {
      public void run() {
        Random r = new Random();
        for (int i = 0; i < toQuery; i++) {
          int index = r.nextInt(values.size());
          Value v = values.get(index);
          int res = Sets.SISMEMBER(map, key.address, key.length, v.address, v.length);
          assertEquals(1, res);
        }
      }
    };
    runRead(1, run, toQuery);
    runRead(2, run, toQuery);
    runRead(4, run, toQuery);
    runRead(8, run, toQuery);
    System.out.println("Skip List Map Size=" + map.getMap().size());
    tearDown();
  }
  
  private void runRead(int numThreads, Runnable run, int toQuery) {

    long start = System.currentTimeMillis();
    Thread[] pool = new Thread[numThreads];    
    for (int i = 0; i < pool.length; i++) {
      pool[i] = new Thread(run);
      pool[i].start();
    }
    
    for (Thread t: pool) {
      try {
        t.join();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    long  end = System.currentTimeMillis();
    System.out.println(+ numThreads+ " threads READ perf=" + 
        ((double) numThreads * toQuery * 1000 / (end - start)) + " RPS");
  }
  
  @Ignore
  @Test
  public void testSADDSISMEMBER () {
    System.out.println("Test SADDSISMEMBER");
    Key key = getKey();
    long[] elemPtrs = new long[1];
    int[] elemSizes = new int[1];
    long start = System.currentTimeMillis();
    long count = 0;
    for (int i =0; i < n; i++) {
      elemPtrs[0] = values.get(i).address;
      elemSizes[0] = values.get(i).length;
      int num = Sets.SADD(map, key.address, key.length, elemPtrs, elemSizes);
      assertEquals(1, num);
      if (++count % 100000 == 0) {
        System.out.println("add " + count);
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("Total allocated memory ="+ BigSortedMap.getTotalAllocatedMemory() 
    + " for "+ n + " " + valSize+ " byte values. Overhead="+ 
        ((double)BigSortedMap.getTotalAllocatedMemory()/n - valSize)+
    " bytes per value. Time to load: "+(end -start)+"ms");
    
    BigSortedMap.printMemoryAllocationStats();
    
    assertEquals(n, Sets.SCARD(map, key.address, key.length));
    start = System.currentTimeMillis();
    count = 0;
    for (int i =0; i < n; i++) {
      int res = Sets.SISMEMBER(map, key.address, key.length, values.get(i).address, values.get(i).length);
      assertEquals(1, res);
      if (++count % 100000 == 0) {
        System.out.println("ismember " + count);
      }
    }
    end = System.currentTimeMillis();
    System.out.println("Time exist="+(end -start)+"ms");
    BigSortedMap.memoryStats();
    Sets.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Sets.SCARD(map, key.address, key.length));
 
  }
  
  @Ignore
  @Test
  public void testAddMultiDelete() throws IOException {
    System.out.println("Test Add Multi Delete");
    long[] elemPtrs = new long[1];
    int[] elemSizes = new int[1];
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      elemPtrs[0] = values.get(i).address;
      elemSizes[0] = values.get(i).length;
      int num = Sets.SADD(map, elemPtrs[0], elemSizes[0], elemPtrs, elemSizes);
      assertEquals(1, num);
      if (++count % 100000 == 0) {
        System.out.println("add " + count);
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("Total allocated memory ="+ BigSortedMap.getTotalAllocatedMemory() 
    + " for "+ n + " " + valSize+ " byte values. Overhead="+ 
        ((double)BigSortedMap.getTotalAllocatedMemory()/n - valSize)+
    " bytes per value. Time to load: "+(end -start)+"ms");
    
    System.out.println("Deleting keys ...");
    count = 0;
    start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      elemPtrs[0] = values.get(i).address;
      elemSizes[0] = values.get(i).length;
      Sets.DELETE(map, elemPtrs[0], elemSizes[0]);
      if (++count % 100000 == 0) {
        System.out.println("delete " + count);
      }
    }
    end = System.currentTimeMillis();
    long recc = Commons.countRecords(map);

    System.out.println("Deleted " + n + " in " + (end - start)+"ms. Count="+ recc);
    
    assertEquals(0, (int)recc);
    BigSortedMap.printMemoryAllocationStats();
  }
  
  @Ignore
  @Test
  public void testAddRemove() {
    System.out.println("Test Add Remove");
    Key key = getKey();
    long[] elemPtrs = new long[1];
    int[] elemSizes = new int[1];
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      elemPtrs[0] = values.get(i).address;
      elemSizes[0] = values.get(i).length;
      int num = Sets.SADD(map, key.address, key.length, elemPtrs, elemSizes);
      assertEquals(1, num);
      if (++count % 100000 == 0) {
        System.out.println("add " + count);
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("Total allocated memory ="+ BigSortedMap.getTotalAllocatedMemory() 
    + " for "+ n + " " + valSize+ " byte values. Overhead="+ 
        ((double)BigSortedMap.getTotalAllocatedMemory()/n - valSize)+
    " bytes per value. Time to load: "+(end -start)+"ms");
    
    BigSortedMap.printMemoryAllocationStats();
    
    assertEquals(n, Sets.SCARD(map, key.address, key.length));
    start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      int res = Sets.SREM(map, key.address, key.length, values.get(i).address, values.get(i).length);
      assertEquals(1, res);
    }
    end = System.currentTimeMillis();
    System.out.println("Time exist="+(end -start)+"ms");
    BigSortedMap.printMemoryAllocationStats();
    
    assertEquals(0, (int)BigSortedMap.countRecords(map));
    assertEquals(0, (int)Sets.SCARD(map, key.address, key.length));
    //TODO 
    Sets.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Sets.SCARD(map, key.address, key.length));
    BigSortedMap.printMemoryAllocationStats();

  }
  
  @Ignore
  @Test
  public void testMemoryUsageForInts() throws IOException {
    System.out.println("Test memory usage for ints");
    int n = 1000000;
    map = new BigSortedMap(100000000);
    long buffer = UnsafeAccess.malloc(Utils.SIZEOF_INT);
    long keyPtr = UnsafeAccess.allocAndCopy("key", 0, "key".length());
    int keySize = "key".length();
    Random r = new Random();
    int duplicates = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      int next = Math.abs(r.nextInt());
      UnsafeAccess.putInt(buffer,  next);
      int res = Sets.SADD(map, keyPtr, keySize, buffer, Utils.SIZEOF_INT);
      if (res == 0) {
        duplicates++;
        i--; continue;
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("Loaded in " + (end - start)+"ms. Mem usage for 1 int="+ 
       ((double)BigSortedMap.getTotalAllocatedMemory())/n+ " dups="+ duplicates);
    
    BigSortedMap.printMemoryAllocationStats();
    map.dumpStats();
    assertEquals(n, (int)Sets.SCARD(map, keyPtr, keySize));
    map.dispose();
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(buffer);
  }
  
  @Ignore
  @Test
  public void testCompressionSortedIntSet() throws IOException {
    System.out.println("Test compression sorted int set");
    int n = 1000000;
    List<Integer> list = new ArrayList<Integer>();
    Random r = new Random();
    while(list.size() < n) {
      int v = Math.abs(r.nextInt());
      //if (list.contains(v)) continue;
      list.add(v);
    }
    
    Collections.sort(list);
    int bufferSize = (Utils.SIZEOF_INT +  Utils.sizeUVInt(Utils.SIZEOF_INT)) * n;
    long buffer = UnsafeAccess.malloc(bufferSize);
    long ptr = buffer;
    for (int v: list) {
      Utils.writeUVInt(ptr, Utils.SIZEOF_INT);
      int mSizeSize = Utils.sizeUVInt(Utils.SIZEOF_INT);
      UnsafeAccess.putInt(ptr + mSizeSize, v);
      ptr += Utils.SIZEOF_INT + mSizeSize;
    }
   
    byte[] arr = new byte[bufferSize];
    UnsafeAccess.copy(buffer,  arr,  0, bufferSize);
    // Compress arr using LZ4 codec
    long cBuffer = UnsafeAccess.malloc(2 * bufferSize);
    Codec codec = CodecFactory.getInstance().getCodec(CodecType.LZ4HC);
    int size = codec.compress(buffer, bufferSize, cBuffer, 2 * bufferSize);
    System.out.println("Source size =" + bufferSize);
    System.out.println("LZ4HC  size =" + size);
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    GZIPOutputStream os = new GZIPOutputStream(baos);
    os.write(arr);
    os.close();
    System.out.println("GZIP   size =" + baos.toByteArray().length);
    
    Path path = Files.createTempFile("data", "raw");
    File f = path.toFile();
    
    System.out.println("File="+ f.getAbsolutePath());
    FileOutputStream fos = new FileOutputStream(f);
    fos.write(arr);
    fos.close();
    
  }
  
  private void tearDown() {
    // Dispose
    map.dispose();
    if (key != null) {
      UnsafeAccess.free(key.address);
      key = null;
    }
    for (Value v: values) {
      UnsafeAccess.free(v.address);
    }
    if (buffer > 0) {
      UnsafeAccess.free(buffer);
      buffer = 0;
    }
  }
}
