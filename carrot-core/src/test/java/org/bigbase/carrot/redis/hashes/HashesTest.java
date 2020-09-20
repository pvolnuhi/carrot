package org.bigbase.carrot.redis.hashes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.Key;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class HashesTest {
  BigSortedMap map;
  Key key;
  long buffer;
  int bufferSize = 64;
  int keySize = 8;
  long n = 2000;
  List<Key> values;
  
  static {
   // UnsafeAccess.debug = true;
  }
  
  private List<Key> getKeys(long n) {
    List<Key> keys = new ArrayList<Key>();
    Random r = new Random();
    long seed = 4391544250976551510L;//r.nextLong();
    r.setSeed(seed);
    System.out.println("KEYS SEED=" + seed);
    byte[] buf = new byte[keySize];
    for (int i=0; i < n; i++) {
      r.nextBytes(buf);
      long ptr = UnsafeAccess.malloc(keySize);
      UnsafeAccess.copy(buf, 0, ptr, keySize);
      keys.add(new Key(ptr, keySize));
    }
    return keys;
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
    map = new BigSortedMap(1000000000);
    buffer = UnsafeAccess.mallocZeroed(bufferSize); 
    values = getKeys(n);
  }
  
  @Test
  public void testSetExists () {
    System.out.println("Test Set - Exists");
    Key key = getKey();
    long elemPtr;
    int elemSize;
    long start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      elemPtr = values.get(i).address;
      elemSize = values.get(i).length;
      int num = Hashes.HSET(map, key.address, key.length, elemPtr, elemSize,  elemPtr, elemSize);
      assertEquals(1, num);
      if (i % 1000000 == 0) {
        System.out.println(i);
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("Total allocated memory ="+ BigSortedMap.getTotalAllocatedMemory() 
    + " for "+ n + " " + keySize+ " byte values. Overhead="+ 
        ((double)BigSortedMap.getTotalAllocatedMemory()/n - 2*keySize)+
    " bytes per value. Time to load: "+(end -start)+"ms");
    assertEquals(n, Hashes.HLEN(map, key.address, key.length));
    int notFound = 0;
    start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      //System.out.println(i);
      int res = Hashes.HEXISTS(map, key.address, key.length, values.get(i).address, values.get(i).length);
      assertEquals(1, res);
      notFound += 1 - res; 
    }
    end = System.currentTimeMillis();
    System.out.println("not found="+ notFound + " Time exist="+(end -start)+"ms");
    BigSortedMap.memoryStats();
    //map.dumpStats();
    //TODO 
    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Hashes.HLEN(map, key.address, key.length));
 
  }
  
  @Test
  public void testSetGet () {
    System.out.println("Test Set - Get");
    Key key = getKey();
    long elemPtr;
    int elemSize;
    long start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      elemPtr = values.get(i).address;
      elemSize = values.get(i).length;
      int num = Hashes.HSET(map, key.address, key.length, elemPtr, elemSize,  elemPtr, elemSize);
      assertEquals(1, num);
      if (i % 1000000 == 0) {
        System.out.println(i);
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("Total allocated memory ="+ BigSortedMap.getTotalAllocatedMemory() 
    + " for "+ n + " " + keySize+ " byte values. Overhead="+ 
        ((double)BigSortedMap.getTotalAllocatedMemory()/n - 2*keySize)+
    " bytes per value. Time to load: "+(end -start)+"ms");
    assertEquals(n, Hashes.HLEN(map, key.address, key.length));
    start = System.currentTimeMillis();
    long buffer = UnsafeAccess.malloc(2 * keySize);
    int bufferSize = 2 * keySize;
    
    for (int i =0; i < n; i++) {
      //System.out.println(i);
      int size = Hashes.HGET(map, key.address, key.length, values.get(i).address, values.get(i).length, 
        buffer, bufferSize);
      assertEquals(values.get(i).length, size);
      assertTrue(Utils.compareTo(values.get(i).address, values.get(i).length, buffer, size) == 0);
    }
    
    end = System.currentTimeMillis();
    System.out.println(" Time get="+(end -start)+"ms");
    BigSortedMap.memoryStats();
    //map.dumpStats();
    //TODO 
    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Hashes.HLEN(map, key.address, key.length));
 
  }
  
  
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
      if (i % 1000000 == 0) {
        System.out.println(i);
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("Total allocated memory ="+ BigSortedMap.getTotalAllocatedMemory() 
    + " for "+ n + " " + keySize+ " byte values. Overhead="+ 
        ((double)BigSortedMap.getTotalAllocatedMemory()/n - 2*keySize)+
    " bytes per value. Time to load: "+(end -start)+"ms");
    assertEquals(n, Hashes.HLEN(map, key.address, key.length));
    int notFound = 0;
    start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      //System.out.println(i);
      int res = Hashes.HDEL(map, key.address, key.length, values.get(i).address, values.get(i).length);
      assertEquals(1, res);
      notFound += 1 - res; 
    }
    end = System.currentTimeMillis();
    System.out.println("not found="+ notFound + " Time exist="+(end -start)+"ms");
    BigSortedMap.memoryStats();
    //map.dumpStats();
    //TODO 
    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Hashes.HLEN(map, key.address, key.length));
 
  }
  
  @After
  public void tearDown() {
    // Dispose
    map.dispose();
    UnsafeAccess.free(key.address);
    for (Key k: values) {
      UnsafeAccess.free(k.address);
    }
    UnsafeAccess.mallocStats.printStats();
    BigSortedMap.memoryStats();

  }
}
