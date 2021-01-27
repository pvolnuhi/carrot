package org.bigbase.carrot.redis.hashes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.Key;
import org.bigbase.carrot.Value;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
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
  long n = 2000;
  List<Value> values;
  
  static {
    UnsafeAccess.debug = true;
  }
  
  private List<Value> getValues(long n) {
    List<Value> values = new ArrayList<Value>();
    Random r = new Random();
    long seed = -5383581744249684084L;//r.nextLong();
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
    long seed = 2429934184943245518L;//r.nextLong();
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
  
  @Test
  public void runAllNoCompression() {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    System.out.println();
    for (int i = 0; i < 10; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=NULL");
      allTests();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  @Test
  public void runAllCompressionLZ4() {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    System.out.println();
    for (int i = 0; i < 10; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=LZ4");
      allTests();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  @Test
  public void runAllCompressionLZ4HC() {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    System.out.println();
    for (int i = 0; i < 10; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=LZ4HC");
      allTests();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  private void allTests() {
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
  public void testSetExists () {
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
    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Hashes.HLEN(map, key.address, key.length));
  }
  
  @Ignore
  @Test
  public void testSetGet () {
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
    + " for "+ n + " " + keySize+ " byte values. Overhead="+ 
        ((double)BigSortedMap.getTotalAllocatedMemory()/n - 2*keySize)+
    " bytes per value. Time to load: "+(end -start)+"ms");
    
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
    
    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Hashes.HLEN(map, key.address, key.length));
    UnsafeAccess.free(buffer);
  }
  
  
  @Ignore
  @Test
  public void testAddRemove() {
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
    + " for "+ n + " " + keySize+ " byte values. Overhead="+ 
        ((double)BigSortedMap.getTotalAllocatedMemory()/n - 2*keySize)+
    " bytes per value. Time to load: "+(end -start)+"ms");
    assertEquals(n, Hashes.HLEN(map, key.address, key.length));

    BigSortedMap.printMemoryAllocationStats();
    
    start = System.currentTimeMillis();
    //int count = 1;
    for (int i =0; i < n; i++) {
      //*DEBUG*/System.out.println("HDEL=" + count++);
      int res = Hashes.HDEL(map, key.address, key.length, values.get(i).address, values.get(i).length);
      assertEquals(1, res);
    }
    end = System.currentTimeMillis();
    System.out.println("Time to delete="+(end -start)+"ms");
    
    assertEquals(0, (int)Hashes.HLEN(map, key.address, key.length));

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
