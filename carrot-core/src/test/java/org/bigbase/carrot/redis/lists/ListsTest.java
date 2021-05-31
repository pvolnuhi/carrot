package org.bigbase.carrot.redis.lists;

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
import org.bigbase.carrot.redis.lists.Lists.Side;
import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Ignore;
import org.junit.Test;

public class ListsTest {
  BigSortedMap map;
  Key key;
  List<Value> values;
  long buffer;
  int bufferSize = 64;
  int keySize = 16;
  int valueSize = 8;
  int n = 100000;
  
  static {
    //UnsafeAccess.debug = true;
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
  
  private Key getAnotherKey() {
    long ptr = UnsafeAccess.malloc(keySize);
    byte[] buf = new byte[keySize];
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("ANOTHER KEY SEED=" + seed);
    r.nextBytes(buf);
    UnsafeAccess.copy(buf, 0, ptr, keySize);
    return new Key(ptr, keySize);
  }
  
  private List<Value> getValues(int n){
    byte[] buf = new byte[valueSize];
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("VALUES seed="+ seed);
    values = new ArrayList<Value>();
    for (int i = 0; i < n; i++) {
      r.nextBytes(buf);
      long ptr = UnsafeAccess.allocAndCopy(buf, 0, valueSize);
      values.add( new Value(ptr, valueSize));
    }
    return values;
  }
  
  private void setUp() {
    map = new BigSortedMap(100000000);
    buffer = UnsafeAccess.mallocZeroed(bufferSize); 
    values = getValues((int) n);
  }
  
  private void tearDown() {
    // Dispose
    map.dispose();
    UnsafeAccess.free(key.address);
    UnsafeAccess.free(buffer);
    values.stream().forEach( x -> UnsafeAccess.free(x.address));

    BigSortedMap.printMemoryAllocationStats();
    UnsafeAccess.mallocStats.printStats();
  }
  
  //@Ignore
  @Test
  public void runAllNoCompression() {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    System.out.println();
    for (int i = 0; i < 100; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=NULL");
      allTests();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  @Ignore
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
  
  @Ignore
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
    testLREM();
    tearDown();
    setUp();
    testLRANGE();
    tearDown();
    setUp();
    testLINSERT();
    tearDown();
    setUp();
    testLSET();
    tearDown();
    setUp();
    testRPOPLPUSH();
    tearDown();
    setUp();
    testLMOVE();
    tearDown();
    setUp();
    testLPUSHX();
    tearDown();
    setUp();
    testRPUSHX();
    tearDown();
    setUp();
    testLindexEdgeCases();
    tearDown();
    setUp();
    testLPUSHLINDEX();
    tearDown();
    setUp();
    testLPUSHLPOP();
    tearDown();
    setUp();
    testLPUSHRPOP();
    tearDown();
    setUp();
    testLRMIX();
    tearDown();
    setUp();
    testRPUSHLINDEX();
    tearDown();
    setUp();
    testRPUSHLPOP();
    tearDown();
    setUp();
    testRPUSHRPOP(); 
    tearDown();
  }
  
  @Ignore
  @Test
  public void testRPUSHX() {
    System.out.println("Test RPUSHX");
    Key key = getKey();
    
    // Try LPUSHX - no key yet
    Value v = values.get(0);
    long[] elemPtrs = new long[] {v.address};
    int[] elemSizes = new int[] {v.length};
    long len  = Lists.RPUSHX(map, key.address, key.length, elemPtrs, elemSizes);
    // Can not add
    assertEquals(-1, (int) len);
    
    // Add first element
    len  = Lists.RPUSH(map, key.address, key.length, elemPtrs, elemSizes);
    assertEquals(1, (int) len);
    
    // Now we are ready to go
    for (int i = 1; i < n; i++) {
      v = values.get(i);
      elemPtrs = new long[] {v.address};
      elemSizes = new int[] {v.length};
      len  = Lists.RPUSHX(map, key.address, key.length, elemPtrs, elemSizes);
      assertEquals(i + 1, (int) len);
    }
    
    System.out.println("Total allocated memory ="+ BigSortedMap.getTotalAllocatedMemory() 
    + " for "+ n + " " + valueSize + " byte values. Overhead="+ ((double)BigSortedMap.getTotalAllocatedMemory()/n - valueSize)+
    " bytes per value");
    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));
    
    for (int i = 0; i < n; i++) {
      int sz = Lists.RPOP(map, key.address, key.length, buffer, bufferSize);
      assertEquals(valueSize, (int)sz);
      v = values.get(n - 1 - i);
      assertTrue(Utils.compareTo(v.address, v.length, buffer, sz) == 0);
      assertEquals(n - i - 1, (int)Lists.LLEN(map, key.address, key.length));
    }
    
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Lists.LLEN(map, key.address, key.length));
    
  }
  
  @Ignore
  @Test
  public void testLPUSHX() {
    System.out.println("Test LPUSHX");
    Key key = getKey();
    
    // Try LPUSHX - no key yet
    Value v = values.get(0);
    long[] elemPtrs = new long[] {v.address};
    int[] elemSizes = new int[] {v.length};
    long len  = Lists.LPUSHX(map, key.address, key.length, elemPtrs, elemSizes);
    // Can not add
    assertEquals(-1, (int) len);
    
    // Add first element
    len  = Lists.LPUSH(map, key.address, key.length, elemPtrs, elemSizes);
    assertEquals(1, (int) len);
    
    // Now we are ready to go
    for (int i = 1; i < n; i++) {
      v = values.get(i);
      elemPtrs = new long[] {v.address};
      elemSizes = new int[] {v.length};
      len  = Lists.LPUSHX(map, key.address, key.length, elemPtrs, elemSizes);
      assertEquals(i + 1, (int) len);
    }
    
    System.out.println("Total allocated memory ="+ BigSortedMap.getTotalAllocatedMemory() 
    + " for "+ n + " " + valueSize + " byte values. Overhead="+ ((double)BigSortedMap.getTotalAllocatedMemory()/n - valueSize)+
    " bytes per value");
    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));
    
    for (int i = 0; i < n; i++) {
      int sz = Lists.LPOP(map, key.address, key.length, buffer, bufferSize);
      assertEquals(valueSize, (int)sz);
      v = values.get(n - 1 - i);
      assertTrue(Utils.compareTo(v.address, v.length, buffer, sz) == 0);
      assertEquals(n - i - 1, (int)Lists.LLEN(map, key.address, key.length));
    }
    
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Lists.LLEN(map, key.address, key.length));
    
  }
  
  @Ignore
  @Test
  public void testLPUSHLPOP () {
    System.out.println("Test LPUSHLPOP");
    Key key = getKey();
    
    for (int i = 0; i < n; i++) {
      //System.out.println(i);
      Value v = values.get(i);
      long[] elemPtrs = new long[] {v.address};
      int[] elemSizes = new int[] {v.length};
      long len  = Lists.LPUSH(map, key.address, key.length, elemPtrs, elemSizes);
      assertEquals(i + 1, (int) len);
    }
    
    System.out.println("Total allocated memory ="+ BigSortedMap.getTotalAllocatedMemory() 
    + " for "+ n + " " + valueSize + " byte values. Overhead="+ ((double)BigSortedMap.getTotalAllocatedMemory()/n - valueSize)+
    " bytes per value");
    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));
    
    for (int i = 0; i < n; i++) {
      int sz = Lists.LPOP(map, key.address, key.length, buffer, bufferSize);
      assertEquals(valueSize, (int)sz);
      Value v = values.get(n - 1 - i);
      assertTrue(Utils.compareTo(v.address, v.length, buffer, sz) == 0);
      assertEquals(n - i - 1, (int)Lists.LLEN(map, key.address, key.length));
    }
    
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Lists.LLEN(map, key.address, key.length));

  }
  
  @Ignore
  @Test
  public void testRPUSHRPOP () {
    System.out.println("\nTest RPUSHRPOP");
    Key key = getKey();
    
    for (int i = 0; i < n; i++) {
      Value v = values.get(i);
      long[] elemPtrs = new long[] {v.address};
      int[] elemSizes = new int[] {v.length};
      long len = Lists.RPUSH(map, key.address, key.length, elemPtrs, elemSizes);
      assertEquals(i + 1, (int) len);
    }
    
    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));
    
    for (int i = 0; i < n; i++) {
      int sz = Lists.RPOP(map, key.address, key.length, buffer, bufferSize);
      assertEquals(valueSize, (int)sz);
      Value v = values.get(n - 1 - i);
      assertTrue(Utils.compareTo(v.address, v.length, buffer, sz) == 0);
      assertEquals(n - i - 1, (int)Lists.LLEN(map, key.address, key.length));
    }
    
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Lists.LLEN(map, key.address, key.length));
 
  }
  
  @Ignore
  @Test
  public void testLPUSHRPOP () {
    System.out.println("\nTest LPUSHRPOP");
    Key key = getKey();
    for (int i = 0; i < n; i++) {
      Value v = values.get(i);
      long[] elemPtrs = new long[] {v.address};
      int[] elemSizes = new int[] {v.length};
      long len = Lists.LPUSH(map, key.address, key.length, elemPtrs, elemSizes);
      assertEquals(i + 1, (int) len);
    }
    
    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));
    
    for (int i =0; i < n; i++) {
      int sz = Lists.RPOP(map, key.address, key.length, buffer, bufferSize);
      Value v = values.get(i);
      assertTrue(Utils.compareTo(v.address, v.length, buffer, sz) == 0);
      assertEquals(valueSize, sz);
      assertEquals(n - i - 1, (int)Lists.LLEN(map, key.address, key.length));
    }
    
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Lists.LLEN(map, key.address, key.length));
 
  }
  
  @Ignore
  @Test
  public void testLRMIX () {
    System.out.println("\nTest LRMIX");
    Key key = getKey();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED="+ seed);
    for (int i = 0; i < n; i++) {
      Value v = values.get(i);
      long[] elemPtrs = new long[] {v.address};
      int[] elemSizes = new int[] {v.length};
      if (r.nextBoolean()) {
        Lists.LPUSH(map, key.address, key.length, elemPtrs, elemSizes);
      } else {
        Lists.RPUSH(map, key.address, key.length, elemPtrs, elemSizes);
      }
    }
    
    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));
    
    for (int i =0; i < n; i++) {      
      long sz = 0;
      if (r.nextBoolean()) {
        sz = Lists.RPOP(map, key.address, key.length, buffer, bufferSize);
      } else {
        sz = Lists.LPOP(map, key.address, key.length, buffer, bufferSize);
      }
      assertEquals(valueSize, (int)sz);
      assertEquals(n - i - 1, (int)Lists.LLEN(map, key.address, key.length));
    }
    
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int) Lists.LLEN(map, key.address, key.length));
 
  }
  
  @Ignore
  @Test
  public void testRPUSHLPOP () {
    System.out.println("\nTest RPUSHLPOP");
    Key key = getKey();
    for (int i = 0; i < n; i++) {
      Value v = values.get(i);
      long[] elemPtrs = new long[] {v.address};
      int[] elemSizes = new int[] {v.length};
      Lists.RPUSH(map, key.address, key.length, elemPtrs, elemSizes);
    }
    
    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));
    
    for (int i = 0; i < n; i++) {
      int sz = Lists.LPOP(map, key.address, key.length, buffer, bufferSize);
      assertEquals(valueSize, sz);
      Value v = values.get(i);
      assertTrue(Utils.compareTo(v.address, v.length, buffer, sz) == 0);
      assertEquals(n - i - 1, (int) Lists.LLEN(map, key.address, key.length));
    }
    
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Lists.LLEN(map, key.address, key.length));
 
  }
  
  @Ignore
  @Test
  public void testLPUSHLINDEX () {
    System.out.println("\nTest LPUSHLINDEX");
    Key key = getKey();
    for (int i = 0; i < n; i++) {
      Value v = values.get(i);
      long[] elemPtrs = new long[] {v.address};
      int[] elemSizes = new int[] {v.length};
      Lists.LPUSH(map, key.address, key.length, elemPtrs, elemSizes);
    }
    
    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));
    
    long start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      int sz = Lists.LINDEX(map, key.address, key.length, i, buffer, bufferSize);
      Value v = values.get(n - i - 1);
      assertTrue(Utils.compareTo(v.address, v.length, buffer, sz) == 0);
      assertEquals(valueSize, sz);
    }
    assertEquals(n, (int)Lists.LLEN(map, key.address, key.length));
    long end = System.currentTimeMillis();
    System.out.println("Time to index " + n+" from " + n + " long list=" + (end - start) + "ms");    
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Lists.LLEN(map, key.address, key.length));
 
  }
  
  @Ignore
  @Test
  public void testLindexEdgeCases () {
    System.out.println("\nTest LINDEX edge cases");
    Key key = getKey();
    for (int i = 0; i < n; i++) {
      Value v = values.get(i);
      long[] elemPtrs = new long[] {v.address};
      int[] elemSizes = new int[] {v.length};
      Lists.LPUSH(map, key.address, key.length, elemPtrs, elemSizes);
    }
    
    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));
    
    long start = System.currentTimeMillis();
    for (int i = 1; i <= n; i++) {
      int sz = Lists.LINDEX(map, key.address, key.length, -i, buffer, bufferSize);
      Value v = values.get(i - 1);
      assertTrue(Utils.compareTo(v.address, v.length, buffer, sz) == 0);
      assertEquals(valueSize, sz);
    }
    assertEquals(n, (int)Lists.LLEN(map, key.address, key.length));
    
    
    // check -n -1
    int sz = Lists.LINDEX(map, key.address, key.length, -n - 1, buffer, bufferSize);
    assertEquals(-1, sz);
    // Check n
    sz = Lists.LINDEX(map, key.address, key.length, n, buffer, bufferSize);
    assertEquals(-1, sz);

    
    long end = System.currentTimeMillis();
    System.out.println("Time to index " + n+" from " + n + " long list=" + (end - start) + "ms");    
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Lists.LLEN(map, key.address, key.length));
 
  }
  
  @Ignore
  @Test
  public void testRPUSHLINDEX () {
    System.out.println("Test RPUSHLINDEX");
    Key key = getKey();
    for (int i = 0; i < n; i++) {
      Value v = values.get(i);
      long[] elemPtrs = new long[] {v.address};
      int[] elemSizes = new int[] {v.length};
      Lists.RPUSH(map, key.address, key.length, elemPtrs, elemSizes);
    }
    
    assertEquals(n, (int) Lists.LLEN(map, key.address, key.length));
    
    long start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      int sz = Lists.LINDEX(map, key.address, key.length, i, buffer, bufferSize);
      assertEquals(valueSize, sz);
      Value v = values.get(i);
      assertTrue(Utils.compareTo(v.address, v.length, buffer, sz) == 0);
    }
    assertEquals(n, (int)Lists.LLEN(map, key.address, key.length));
    long end = System.currentTimeMillis();
    System.out.println("Time to index " + n+" from " + n + " long list=" + (end - start) + "ms");    
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Lists.LLEN(map, key.address, key.length));
 
  }
  
  @Ignore
  @Test
  public void testLMOVE() {
    System.out.println("\nTest LMOVE");
    Key key = getKey();
    Key key2 = getAnotherKey();
    
    // 1. Non-existent source LEFT-LEFT
    int size = Lists.LMOVE(map, key.address, key.length, key2.address, key2.length, Side.LEFT, Side.LEFT, buffer, bufferSize);
    assertEquals(-1, size);
    
    // 2. Non-existent source LEFT-RIGHT
    size = Lists.LMOVE(map, key.address, key.length, key2.address, key2.length, Side.LEFT, Side.RIGHT, buffer, bufferSize);
    assertEquals(-1, size);
    
    // 3. Non-existent source RIGHT-LEFT
    size = Lists.LMOVE(map, key.address, key.length, key2.address, key2.length, Side.RIGHT, Side.LEFT, buffer, bufferSize);
    assertEquals(-1, size);
    
    // 4. Non-existent source RIGHT-RIGHT
    size = Lists.LMOVE(map, key.address, key.length, key2.address, key2.length, Side.RIGHT, Side.RIGHT, buffer, bufferSize);
    assertEquals(-1, size);
    
    // Push 4 values to the source
    for (int i = 0; i < 4; i++) {
      Value v = values.get(i);
      int sz = (int) Lists.LPUSH(map, key.address, key.length, new long[] {v.address}, new int[] {v.length});
      assertEquals(i + 1, sz);
    }
    /*
     * SRC order of elements: 3,2,1,0
     */
    
    // Now repeat
    
    // 1. existent source LEFT-LEFT
    size = Lists.LMOVE(map, key.address, key.length, key2.address, key2.length, Side.LEFT, Side.LEFT, buffer, bufferSize);
    assertEquals(valueSize, size);
    // we moved element 3
    Value v = values.get(3);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    // DST = 3
    // SRC = 2, 1, 0
    // 2. existent source LEFT-RIGHT
    size = Lists.LMOVE(map, key.address, key.length, key2.address, key2.length, Side.LEFT, Side.RIGHT, buffer, bufferSize);
    assertEquals(valueSize, size);
    // we moved element 2
    v = values.get(2);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    // DST = 3, 2
    // SRC = 1, 0
    // 3. existent source RIGHT-LEFT
    size = Lists.LMOVE(map, key.address, key.length, key2.address, key2.length, Side.RIGHT, Side.LEFT, buffer, bufferSize);
    assertEquals(valueSize, size);
    // we moved element 0
    v = values.get(0);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    // DST = 0, 3, 2
    // SRC = 1
    // 4. existent source RIGHT-RIGHT
    size = Lists.LMOVE(map, key.address, key.length, key2.address, key2.length, Side.RIGHT, Side.RIGHT, buffer, bufferSize);
    assertEquals(valueSize, size);
    // we moved element 1
    v = values.get(1);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    // DST = 0, 3, 2, 1
    // SRC = empty
    
    assertEquals(0L, Lists.LLEN(map, key.address, key.length));
    assertEquals(4L, Lists.LLEN(map, key2.address, key2.length));

    // Now repeat from dst -> src
    
    // 1. existent source LEFT-LEFT
    size = Lists.LMOVE(map, key2.address, key2.length, key.address, key.length, Side.LEFT, Side.LEFT, buffer, bufferSize);
    assertEquals(valueSize, size);
    // we moved element 0
    v = values.get(0);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    // DST = 3, 2, 1
    // SRC = 0
    // 2. existent source LEFT-RIGHT
    size = Lists.LMOVE(map, key2.address, key2.length, key.address, key.length, Side.LEFT, Side.RIGHT, buffer, bufferSize);
    assertEquals(valueSize, size);
    // we moved element 3
    v = values.get(3);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    // DST = 2, 1
    // SRC = 0, 3
    // 3. existent source RIGHT-LEFT
    size = Lists.LMOVE(map, key2.address, key2.length, key.address, key.length, Side.RIGHT, Side.LEFT, buffer, bufferSize);
    assertEquals(valueSize, size);
    // we moved element 1
    v = values.get(1);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    // DST = 2
    // SRC = 1, 0, 3
    // 4. existent source RIGHT-RIGHT
    size = Lists.LMOVE(map, key2.address, key2.length, key.address, key.length, Side.RIGHT, Side.RIGHT, buffer, bufferSize);
    assertEquals(valueSize, size);
    // we moved element 2
    v = values.get(2);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    // DST = empty
    // SRC = 1, 0, 3, 2
    assertEquals(4L, Lists.LLEN(map, key.address, key.length));
    assertEquals(0L, Lists.LLEN(map, key2.address, key2.length));

    // Same key
    
    // 1. existent source - source LEFT-LEFT
    size = Lists.LMOVE(map, key.address, key.length, key.address, key.length, Side.LEFT, Side.LEFT, buffer, bufferSize);
    assertEquals(valueSize, size);
    // we moved element 1
    v = values.get(1);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    // DST = empty
    // SRC = 1, 0, 3, 2
    // 2. existent source - source LEFT-RIGHT
    size = Lists.LMOVE(map, key.address, key.length, key.address, key.length, Side.LEFT, Side.RIGHT, buffer, bufferSize);
    assertEquals(valueSize, size);
    // we moved element 1
    v = values.get(1);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    // DST = empty
    // SRC = 0, 3, 2, 1
    // 3. existent source-source RIGHT-LEFT
    size = Lists.LMOVE(map, key.address, key.length, key.address, key.length, Side.RIGHT, Side.LEFT, buffer, bufferSize);
    assertEquals(valueSize, size);
    // we moved element 1
    v = values.get(1);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    // DST = empty
    // SRC = 1, 0, 3, 2
    // 4. existent source-source RIGHT-RIGHT
    size = Lists.LMOVE(map, key.address, key.length, key.address, key.length, Side.RIGHT, Side.RIGHT, buffer, bufferSize);
    assertEquals(valueSize, size);
    // we moved element 2
    v = values.get(2);
    assertEquals(0, Utils.compareTo(v.address, v.length, buffer, size));
    assertEquals(4L, Lists.LLEN(map, key.address, key.length));
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Lists.LLEN(map, key.address, key.length));
    Lists.DELETE(map, key2.address, key2.length);
    assertEquals(0, (int)Lists.LLEN(map, key2.address, key2.length));
    // Dispose additional key
    UnsafeAccess.free(key2.address);
  }
  
  @Ignore
  @Test
  public void testRPOPLPUSH() {
    System.out.println("\nTest RPOPLPUSH");
    Key key = getKey();
    Key key2 = getAnotherKey();
    
    int size = Lists.RPOPLPUSH(map, key.address, key.length, key2.address, key2.length, buffer, bufferSize);
    assertEquals(-1, size);
    
    // Push 4 values to the source
    for (int i = 0; i < 4; i++) {
      Value v = values.get(i);
      int sz = (int) Lists.LPUSH(map, key.address, key.length, new long[] {v.address}, new int[] {v.length});
      assertEquals(i + 1, sz);
    }
    // Now we have SRC = 3, 2, 1, 0
    // Rotate single list
    for (int i = 0; i < 4; i++) {
      Value v = values.get(i);
      int sz = (int) Lists.RPOPLPUSH(map, key.address, key.length, key.address, key.length, buffer, bufferSize);
      assertEquals(valueSize, sz);
      assertEquals(0, Utils.compareTo(v.address, v.length, buffer, sz));
    }
    
    // Rotate two lists
    for (int i = 0; i < 4; i++) {
      Value v = values.get(i);
      int sz = (int) Lists.RPOPLPUSH(map, key.address, key.length, key2.address, key2.length, buffer, bufferSize);
      assertEquals(valueSize, sz);
      assertEquals(0, Utils.compareTo(v.address, v.length, buffer, sz));
    }
    assertEquals(0L, Lists.LLEN(map, key.address, key.length));
    assertEquals(4L, Lists.LLEN(map, key2.address, key2.length));
    
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Lists.LLEN(map, key.address, key.length));
    Lists.DELETE(map, key2.address, key2.length);
    assertEquals(0, (int)Lists.LLEN(map, key2.address, key2.length));
    // Dispose additional key
    UnsafeAccess.free(key2.address);
  }
  
  @Ignore
  @Test
  public void testBLMOVE() {
    System.out.println("BLMOVE - TODO");
    testLMOVE();
  }
  
  @Ignore
  @Test
  public void testBRPOPLPUSH() {
    System.out.println("BRPOPLPUSH - TODO");
    testRPOPLPUSH();
  }
  
  @Ignore
  @Test
  public void testLSET() {
    System.out.println("Test LSET operation");
    Key key = getKey();
    // load half of values
    int toLoad = n / 2;
    for (int i = 0; i < toLoad; i++) {
      Value v = values.get(i);
      int len = (int) Lists.LPUSH(map, key.address, key.length, new long[] {v.address}, new int[] {v.length});
      assertEquals(i + 1, len);
    }
    
    // Now overwrite
    for (int i = toLoad; i < n; i++) {
      Value v = values.get(i);
      int len = (int) Lists.LSET(map, key.address, key.length, i - toLoad, v.address, v.length);
      assertEquals(toLoad, len);
    }
    
    // Verify
    for (int i = 0; i < toLoad; i++) {
      Value v = values.get(i + toLoad);
      int sz = (int) Lists.LINDEX(map, key.address, key.length, i, buffer, bufferSize);
      assertEquals(valueSize, sz);
      assertEquals (0, Utils.compareTo(v.address, v.length, buffer, sz));
    }
    
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Lists.LLEN(map, key.address, key.length));
  }
  
  @Ignore
  @Test
  public void testLINSERT() {
    System.out.println("Test LINSERT operation");
    Key key = getKey();
    // load half of values
    for (int i = 0; i < n / 2; i++) {
      Value v = values.get(i);
      int len = (int) Lists.LPUSH(map, key.address, key.length, new long[] {v.address}, new int[] {v.length});
      assertEquals(i + 1, len);
    }
    
    // Test edge case first
    Value v = values.get(n / 2);
    long ls = Lists.LINSERT(map, key.address, key.length, true, buffer, bufferSize, v.address, v.length);
    assertEquals(-1L, ls);
    
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Test seed="+ seed);
    // Insert and Verify
    int listSize = n/2;
    for (int i = n/2; i < n/2 + 10000; i++) {
      int index = i;
      int insertPos = r.nextInt(listSize);
      boolean after = r.nextBoolean();
      int sz = Lists.LINDEX(map, key.address, key.length, insertPos, buffer, bufferSize);
      assertEquals(valueSize, sz);
      
      Value insert = values.get(index);      
      ls = Lists.LINSERT(map, key.address, key.length, after, buffer, sz, insert.address, insert.length);
      listSize++;
      assertEquals((int)ls, listSize);
      
      sz = (int) Lists.LINDEX(map, key.address, key.length, after? insertPos + 1 :insertPos, buffer, bufferSize);
      assertEquals(valueSize, sz);
      assertEquals(0, Utils.compareTo(insert.address, insert.length, buffer, sz));
      if (i % 1000 == 0) {
        System.out.println(i);
      }
    }
    
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Lists.LLEN(map, key.address, key.length));
  }

  @Ignore
  @Test
  public void testLRANGE() {
    System.out.println("Test LRANGE operation");
    Key key = getKey();
    // No exists yet
    
    long sz = Lists.LRANGE(map, key.address, key.length, 0, 1, buffer, bufferSize);
    assertEquals(-1L, sz);
    // load  values
    for (int i = 0; i < n; i++) {
      Value v = values.get(i);
      int len = (int) Lists.RPUSH(map, key.address, key.length, new long[] {v.address}, new int[] {v.length});
      assertEquals(i + 1, len);
    }
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Test seed="+ seed);
    
    // Test edge cases
    // 1. start >= n
    sz = Lists.LRANGE(map, key.address, key.length, n, n + 1, buffer, bufferSize);
    assertEquals(0L, sz);
    sz = Lists.LRANGE(map, key.address, key.length, -n - 1, - n - 1, buffer, bufferSize);
    assertEquals(0L, sz);
    // 1. start > end
    sz = Lists.LRANGE(map, key.address, key.length, 1, 0, buffer, bufferSize);
    assertEquals(0L, sz);
    
    // Test limited buffer
    
    int expNumber = (bufferSize - Utils.SIZEOF_INT) / (valueSize + Utils.sizeUVInt(valueSize));
    
    for (int i = 0; i < 1000; i++) {
      int i1 = r.nextInt(n);
      int i2 = r.nextInt(n);
      int start = i1 < i2? i1: i2;
      int end = i1 < i2? i2: i1;
      sz = Lists.LRANGE(map, key.address, key.length, start, end, buffer, bufferSize);
      end = Math.min(end, start + expNumber - 1);
      verifyRange(start, end, buffer, bufferSize);
    }
    
    int largeBufferSize = Utils.SIZEOF_INT + n * (valueSize + Utils.sizeUVInt(valueSize));
    long largeBuffer = UnsafeAccess.malloc (largeBufferSize);
    
    // Test large buffer
    
    for (int i = 0; i < 1000; i++) {
      int i1 = r.nextInt(n);
      int i2 = r.nextInt(n);
      int start = i1 < i2? i1: i2;
      int end = i1 < i2? i2: i1;
      sz = Lists.LRANGE(map, key.address, key.length, start, end, largeBuffer, largeBufferSize);
      verifyRange(start, end, largeBuffer, largeBufferSize);
    }
    
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Lists.LLEN(map, key.address, key.length));
    UnsafeAccess.free(largeBuffer);
    
    
  }
  
  /**
   * Verifies range of values
   * @param start start index (inclusive)
   * @param stop stop index (inclusive)
   * @param buffer buffer
   * @param bufferSize size
   */
  private void verifyRange(int start, int stop, long buffer, int bufferSize) {
    
    int total = UnsafeAccess.toInt(buffer);
    assertEquals(stop - start + 1, total);
    long ptr = buffer + Utils.SIZEOF_INT;

    for (int i = start; i <= stop; i++) {
      Value v = values.get(i);
      int sz = Utils.readUVInt(ptr);
      int ssz = Utils.sizeUVInt(sz);
      assertEquals(sz, v.length);
      assertEquals(0, Utils.compareTo(v.address, v.length, ptr + ssz, sz));
      ptr += sz + ssz;
    }
  }
  
  @Ignore
  @Test
  public void testLREM() {
    System.out.println("Test LREM operation");
    Key key = getKey();
    // No exists yet
    long removed = Lists.LREM(map, key.address, key.length, 0, values.get(0).address, values.get(0).length);
    assertEquals(0L, removed);
    
    removed = Lists.LREM(map, key.address, key.length, 10, values.get(0).address, values.get(0).length);
    assertEquals(0L, removed);
    
    removed = Lists.LREM(map, key.address, key.length, -10, values.get(0).address, values.get(0).length);
    assertEquals(0L, removed);
    
    // load  values (n-1)
    for (int i = 0; i < n - 1; i++) {
      Value v = values.get(i);
      int len = (int) Lists.RPUSH(map, key.address, key.length, new long[] {v.address}, new int[] {v.length});
      assertEquals(i + 1, len);
    }
    
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Test seed="+ seed);
    
    // Get the last one
    Value v = values.get(n - 1);
    // Remove direct
    for (int i = 0; i < 100; i++) {
      int toInsert = r.nextInt(100);
      long[] indexes = Utils.randomDistinctArray(n - 1, toInsert);
      for (int j = 0; j < indexes.length; j++) {
        Value vv = values.get((int)indexes[j]);
        long size = Lists.LINSERT(map, key.address, key.length, false, vv.address, vv.length, v.address, v.length);
        assertEquals(n + j, (int) size);
      }
      // Remove all
      removed = Lists.LREM(map, key.address, key.length, toInsert, v.address, v.length);

      assertEquals(toInsert, (int) removed);
      assertEquals(n - 1, (int) Lists.LLEN(map, key.address, key.length));
      if (i % 10 == 0) {
        System.out.println("direct ="+ i);
      }
    }
    
    // Remove direct more
    for (int i = 0; i < 100; i++) {
      int toInsert = r.nextInt(100);
      long[] indexes = Utils.randomDistinctArray(n - 1, toInsert);
      for (int j = 0; j < indexes.length; j++) {
        Value vv = values.get((int)indexes[j]);
        long size = Lists.LINSERT(map, key.address, key.length, false, vv.address, vv.length, v.address, v.length);
        assertEquals(n + j, (int) size);
      }
      // Remove all
      removed = Lists.LREM(map, key.address, key.length, 100, v.address, v.length);

      assertEquals(toInsert, (int) removed);
      assertEquals(n - 1, (int) Lists.LLEN(map, key.address, key.length));
      if (i % 10 == 0) {
        System.out.println("direct more="+ i);
      }
    }
    
    // Remove reverse
    for (int i = 0; i < 100; i++) {
      int toInsert = r.nextInt(100);
      long[] indexes = Utils.randomDistinctArray(n - 1, toInsert);
      for (int j = 0; j < indexes.length; j++) {
        Value vv = values.get((int)indexes[j]);
        long size = Lists.LINSERT(map, key.address, key.length, false, vv.address, vv.length, v.address, v.length);
        assertEquals(n + j, (int) size);
      }
      // Remove all
      removed = Lists.LREM(map, key.address, key.length, -toInsert, v.address, v.length);
      assertEquals(toInsert, (int) removed);
      assertEquals(n - 1, (int) Lists.LLEN(map, key.address, key.length));
      if (i % 10 == 0) {
        System.out.println("reverse ="+ i);
      }
    }
    
    // Remove reverse more
    for (int i = 0; i < 100; i++) {
      int toInsert = r.nextInt(100);
      long[] indexes = Utils.randomDistinctArray(n - 1, toInsert);
      for (int j = 0; j < indexes.length; j++) {
        Value vv = values.get((int)indexes[j]);
        long size = Lists.LINSERT(map, key.address, key.length, false, vv.address, vv.length, v.address, v.length);
        assertEquals(n + j, (int) size);
      }
      // Remove all
      removed = Lists.LREM(map, key.address, key.length, -100, v.address, v.length);
      assertEquals(toInsert, (int) removed);
      assertEquals(n - 1, (int) Lists.LLEN(map, key.address, key.length));
      if (i % 10 == 0) {
        System.out.println("reverse more="+ i);
      }
    }
    
    // Remove all
    for (int i = 0; i < 100; i++) {
      int toInsert = r.nextInt(100);
      long[] indexes = Utils.randomDistinctArray(n - 1, toInsert);
      for (int j = 0; j < indexes.length; j++) {
        Value vv = values.get((int)indexes[j]);
        long size = Lists.LINSERT(map, key.address, key.length, false, vv.address, vv.length, v.address, v.length);
        assertEquals(n + j, (int) size);
      }
      // Remove all
      removed = Lists.LREM(map, key.address, key.length, 0, v.address, v.length);
      assertEquals(toInsert, (int) removed);
      assertEquals(n - 1, (int) Lists.LLEN(map, key.address, key.length));
      if (i % 10 == 0) {
        System.out.println("all ="+ i);
      }
    }
    
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Lists.LLEN(map, key.address, key.length));
    
  }
  
}


