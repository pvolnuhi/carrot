package org.bigbase.carrot.redis.hashes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
import org.bigbase.carrot.util.Utils;
import org.junit.Ignore;
import org.junit.Test;

public class HashesTest {
  BigSortedMap map;
  Key key;
  long buffer;
  int bufferSize = 64;
  int keySize = 8;
  int valSize = 8;
  long n = 200000;
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
    byte[] buf = new byte[valSize];
    for (int i=0; i < n; i++) {
      r.nextBytes(buf);
      long ptr = UnsafeAccess.malloc(valSize);
      UnsafeAccess.copy(buf, 0, ptr, valSize);
      values.add(new Value(ptr, valSize));
    }
    return values;
  }
  
  private Key getKey() {
    long ptr = UnsafeAccess.malloc(keySize);
    byte[] buf = new byte[keySize];
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("KEY SEED=" + seed);
    r.nextBytes(buf);
    UnsafeAccess.copy(buf, 0, ptr, keySize);
    return key = new Key(ptr, keySize);
  }
  
  private void setUp() {
    map = new BigSortedMap(1000000000);
    buffer = UnsafeAccess.mallocZeroed(bufferSize); 
    values = getValues(n);
  }
  

  @Ignore
  @Test
  public void runAllNoCompression() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    System.out.println();
    for (int i = 0; i < 100; i++) {
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
    for (int i = 0; i < 100; i++) {
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
    for (int i = 0; i < 100; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=LZ4HC");
      allTests();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  private void allTests() throws IOException {
    setUp();
    testSetExists();
    tearDown();
    setUp();
    testAddRemove();
    tearDown();
    setUp();
    testSetGet();
    tearDown();
  }
  
  @Ignore
  @Test
  public void testSetExists () throws IOException {
    System.out.println("\nTest Set - Exists");
    Key key = getKey();
    long elemPtr;
    int elemSize;
    long start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      elemPtr = values.get(i).address;
      elemSize = values.get(i).length;
      int num = Hashes.HSET(map, key.address, key.length, elemPtr, elemSize,  elemPtr, elemSize);
      assertEquals(1, num);
    }
    long end = System.currentTimeMillis();
    System.out.println("Total allocated memory ="+ BigSortedMap.getTotalAllocatedMemory() 
    + " for "+ n + " " + (keySize + valSize)+ " byte values. Overhead="+ 
        ((double)BigSortedMap.getTotalAllocatedMemory()/n - keySize - valSize)+
    " bytes per value. Time to load: "+(end -start)+"ms");

    assertEquals(n, Hashes.HLEN(map, key.address, key.length));
    start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      int res = Hashes.HEXISTS(map, key.address, key.length, values.get(i).address, values.get(i).length);
      assertEquals(1, res);
    }
    end = System.currentTimeMillis();
    System.out.println("Time exist="+(end -start)+"ms");
    System.out.println("Map.size =" + Commons.countRecords(map));
    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Hashes.HLEN(map, key.address, key.length));
  }
  
  @Ignore
  @Test
  public void testSetGet () throws IOException {
    System.out.println("\nTest Set - Get");
    Key key = getKey();
    long elemPtr;
    int elemSize;
    long start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      elemPtr = values.get(i).address;
      elemSize = values.get(i).length;
      int num = Hashes.HSET(map, key.address, key.length, elemPtr, elemSize,  elemPtr, elemSize);
      assertEquals(1, num);
    }
    long end = System.currentTimeMillis();
    System.out.println("Total allocated memory ="+ BigSortedMap.getTotalAllocatedMemory() 
    + " for "+ n + " " + (keySize + valSize)+ " byte values. Overhead="+ 
        ((double)BigSortedMap.getTotalAllocatedMemory()/n - (keySize + valSize))+
    " bytes per value. Time to load: "+(end - start)+"ms");
    
    assertEquals(n, Hashes.HLEN(map, key.address, key.length));
    start = System.currentTimeMillis();
    long buffer = UnsafeAccess.malloc(2 * valSize);
    int bufferSize = 2 * valSize;
    
    for (int i =0; i < n; i++) {
      int size = Hashes.HGET(map, key.address, key.length, values.get(i).address, values.get(i).length, 
        buffer, bufferSize);
      assertEquals(values.get(i).length, size);
      assertTrue(Utils.compareTo(values.get(i).address, values.get(i).length, buffer, size) == 0);
    }
    
    end = System.currentTimeMillis();
    System.out.println("Time get="+(end -start)+"ms");

    BigSortedMap.printMemoryAllocationStats();
    System.out.println("Map.size =" + Commons.countRecords(map));

    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Hashes.HLEN(map, key.address, key.length));
    UnsafeAccess.free(buffer);
  }
  
  
  @Ignore
  @Test
  public void testAddRemove() throws IOException {
    System.out.println("Test Add - Remove");
    Key key = getKey();
    long elemPtr;
    int elemSize;
    long start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      elemPtr = values.get(i).address;
      elemSize = values.get(i).length;
      int num = Hashes.HSET(map, key.address, key.length, elemPtr, elemSize,  elemPtr, elemSize);
      assertEquals(1, num);
    }
    long end = System.currentTimeMillis();
    System.out.println("Total allocated memory ="+ BigSortedMap.getTotalAllocatedMemory() 
    + " for "+ n + " " + (keySize + valSize) + " byte values. Overhead="+ 
        ((double)BigSortedMap.getTotalAllocatedMemory()/n - (keySize + valSize))+
    " bytes per value. Time to load: "+(end -start)+"ms");
    assertEquals(n, Hashes.HLEN(map, key.address, key.length));

    BigSortedMap.printMemoryAllocationStats();
    
//    start = System.currentTimeMillis();
//    for (int i =0; i < n; i++) {
//      int res = Hashes.HDEL(map, key.address, key.length, values.get(i).address, values.get(i).length);
//      assertEquals(1, res);
//    }
//    end = System.currentTimeMillis();
//    System.out.println("Time to delete="+(end -start)+"ms");
//
//    System.out.println("Map.size =" + Commons.countRecords(map));

//    assertEquals(0, (int)Hashes.HLEN(map, key.address, key.length));

    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Hashes.HLEN(map, key.address, key.length));
 
  }
  
  
  private void tearDown() {
    // Dispose
    map.dispose();
    UnsafeAccess.free(key.address);
    for (Value v: values) {
      UnsafeAccess.free(v.address);
    }
    UnsafeAccess.free(buffer);
  }
}
