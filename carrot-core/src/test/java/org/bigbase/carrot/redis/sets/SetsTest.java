package org.bigbase.carrot.redis.sets;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.Key;
import org.bigbase.carrot.Value;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.redis.Commons;
import org.bigbase.carrot.util.UnsafeAccess;
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
    for (int i = 0; i < 10; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=NULL");
      allTests();
      BigSortedMap.printMemoryAllocationStats();      
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  //@Ignore
  @Test
  public void runAllCompressionLZ4() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    System.out.println();
    for (int i = 0; i < 10; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=LZ4");
      allTests();
      BigSortedMap.printMemoryAllocationStats();      
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  //@Ignore
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
    long start = System.currentTimeMillis();
    setUp();
    testSADDSISMEMBER();
    tearDown();
    setUp();
    testAddRemove();
    tearDown();
    setUp();
    testAddMultiDelete();
    tearDown();
    long end = System.currentTimeMillis();
    
    System.out.println("\nRUN in " + (end -start) + "ms");
    
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
    for (int i =0; i < n; i++) {
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
