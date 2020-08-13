package org.bigbase.carrot.redis.lists;

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.Key;
import org.bigbase.carrot.util.UnsafeAccess;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class ListsTest {
  BigSortedMap map;
  Key key;
  long buffer;
  int bufferSize = 64;
  int keySize = 16;
  long n = 100000;
  static {
    UnsafeAccess.debug = true;
  }
  private Key getKey() {
    long ptr = UnsafeAccess.malloc(keySize);
    byte[] buf = new byte[keySize];
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED=" + seed);
    r.nextBytes(buf);
    UnsafeAccess.copy(buf, 0, ptr, keySize);
    return key = new Key(ptr, keySize);
  }
  
  @Before
  public void setUp() {
    map = new BigSortedMap(100000000);
    buffer = UnsafeAccess.mallocZeroed(bufferSize); 
  }
  
  @Test
  public void testLPUSHLPOP () {
    System.out.println("Test LPUSHLPOP");
    Key key = getKey();
    for (int i =0; i < n; i++) {
      //System.out.println(i);
      long[] elemPtrs = new long[] {key.address};
      int[] elemSizes = new int[] {key.length};
      Lists.LPUSH(map, key.address, key.length, elemPtrs, elemSizes);
    }
    System.out.println("Total allocated memory ="+ BigSortedMap.getTotalAllocatedMemory() 
    + " for "+ n + " 16 byte values. Overhead="+ ((double)BigSortedMap.getTotalAllocatedMemory()/n - 16)+
    " bytes per value");
    assertEquals(n, Lists.LLEN(map, key.address, key.length));
    
    for (int i =0; i < n; i++) {
      //System.out.println(i);
      long sz = Lists.LPOP(map, key.address, key.length, buffer, bufferSize);
      assertEquals(keySize, (int)sz);
      assertEquals(n-i-1, (long)Lists.LLEN(map, key.address, key.length));
    }
    
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Lists.LLEN(map, key.address, key.length));
 
  }
  
  @Test
  public void testRPUSHRPOP () {
    System.out.println("Test RPUSHRPOP");
    Key key = getKey();
    for (int i =0; i < n; i++) {
      //System.out.println(i);
      long[] elemPtrs = new long[] {key.address};
      int[] elemSizes = new int[] {key.length};
      Lists.RPUSH(map, key.address, key.length, elemPtrs, elemSizes);
    }
    
    assertEquals(n, Lists.LLEN(map, key.address, key.length));
    
    for (int i =0; i < n; i++) {
      //System.out.println(i);
      long sz = Lists.RPOP(map, key.address, key.length, buffer, bufferSize);
      assertEquals(keySize, (int)sz);
      assertEquals(n-i-1, (long)Lists.LLEN(map, key.address, key.length));
    }
    
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Lists.LLEN(map, key.address, key.length));
 
  }
  
  @Test
  public void testLPUSHRPOP () {
    System.out.println("Test LPUSHRPOP");
    Key key = getKey();
    for (int i =0; i < n; i++) {
      //System.out.println(i);
      long[] elemPtrs = new long[] {key.address};
      int[] elemSizes = new int[] {key.length};
      Lists.LPUSH(map, key.address, key.length, elemPtrs, elemSizes);
    }
    
    assertEquals(n, Lists.LLEN(map, key.address, key.length));
    
    for (int i =0; i < n; i++) {
      //System.out.println(i);
      long sz = Lists.RPOP(map, key.address, key.length, buffer, bufferSize);
      assertEquals(keySize, (int)sz);
      assertEquals(n-i-1, (long)Lists.LLEN(map, key.address, key.length));
    }
    
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Lists.LLEN(map, key.address, key.length));
 
  }
  
  @Test
  public void testLRMIX () {
    System.out.println("Test LRMIX");
    Key key = getKey();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED="+ seed);
    for (int i =0; i < n; i++) {
      //System.out.println(i);
      long[] elemPtrs = new long[] {key.address};
      int[] elemSizes = new int[] {key.length};
      if (r.nextBoolean()) {
        Lists.LPUSH(map, key.address, key.length, elemPtrs, elemSizes);
      } else {
        Lists.RPUSH(map, key.address, key.length, elemPtrs, elemSizes);
      }
    }
    
    assertEquals(n, Lists.LLEN(map, key.address, key.length));
    
    for (int i =0; i < n; i++) {
      //System.out.println(i);
      
      long sz = 0;
      if (r.nextBoolean()) {
        sz = Lists.RPOP(map, key.address, key.length, buffer, bufferSize);
      } else {
        sz = Lists.LPOP(map, key.address, key.length, buffer, bufferSize);
      }
      assertEquals(keySize, (int)sz);
      assertEquals(n-i-1, (long)Lists.LLEN(map, key.address, key.length));
    }
    
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Lists.LLEN(map, key.address, key.length));
 
  }
  
  @Test
  public void testRPUSHLPOP () {
    System.out.println("Test RPUSHLPOP");
    Key key = getKey();
    for (int i =0; i < n; i++) {
      //System.out.println(i);
      long[] elemPtrs = new long[] {key.address};
      int[] elemSizes = new int[] {key.length};
      Lists.RPUSH(map, key.address, key.length, elemPtrs, elemSizes);
    }
    
    assertEquals(n, Lists.LLEN(map, key.address, key.length));
    
    for (int i =0; i < n; i++) {
      //System.out.println(i);
      long sz = Lists.LPOP(map, key.address, key.length, buffer, bufferSize);
      assertEquals(keySize, (int)sz);
      assertEquals(n-i-1, (long)Lists.LLEN(map, key.address, key.length));
    }
    
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Lists.LLEN(map, key.address, key.length));
 
  }
  
  @Test
  public void testLPUSHLINDEX () {
    System.out.println("Test LPUSHLINDEX");
    Key key = getKey();
    for (int i =0; i < n; i++) {
      //System.out.println(i);
      long[] elemPtrs = new long[] {key.address};
      int[] elemSizes = new int[] {key.length};
      Lists.LPUSH(map, key.address, key.length, elemPtrs, elemSizes);
    }
    
    assertEquals(n, Lists.LLEN(map, key.address, key.length));
    
    long start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      //System.out.println(i);
      long sz = Lists.LINDEX(map, key.address, key.length, i, buffer, bufferSize);
      assertEquals(keySize, (int)sz);
    }
    assertEquals(n, (long)Lists.LLEN(map, key.address, key.length));
    long end = System.currentTimeMillis();
    System.out.println("Time to index " + n+" from "+n+ " long list="+(end -start)+"ms");    
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Lists.LLEN(map, key.address, key.length));
 
  }
  
  @Test
  public void testRPUSHLINDEX () {
    System.out.println("Test RPUSHLINDEX");
    Key key = getKey();
    for (int i =0; i < n; i++) {
      //System.out.println(i);
      long[] elemPtrs = new long[] {key.address};
      int[] elemSizes = new int[] {key.length};
      Lists.RPUSH(map, key.address, key.length, elemPtrs, elemSizes);
    }
    
    assertEquals(n, Lists.LLEN(map, key.address, key.length));
    
    long start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      //System.out.println(i);
      long sz = Lists.LINDEX(map, key.address, key.length, i, buffer, bufferSize);
      assertEquals(keySize, (int)sz);
    }
    assertEquals(n, (long)Lists.LLEN(map, key.address, key.length));
    long end = System.currentTimeMillis();
    System.out.println("Time to index " + n+" from "+n+ " long list="+(end -start)+"ms");    
    Lists.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Lists.LLEN(map, key.address, key.length));
 
  }
  
  @After
  public void tearDown() {
    // Dispose
    map.dispose();
    UnsafeAccess.free(key.address);
  }
}
