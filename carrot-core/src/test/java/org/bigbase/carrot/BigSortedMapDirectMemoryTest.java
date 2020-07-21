package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.BeforeClass;
import org.junit.Test;

public class BigSortedMapDirectMemoryTest {

  static BigSortedMap map;
  static long totalLoaded;
  static long MAX_ROWS = 1000000;
  @BeforeClass 
  public static void setUp() throws IOException {
    BigSortedMap.setMaxBlockSize(4096);
    map = new BigSortedMap(100000000);
    totalLoaded = 0;
    long start = System.currentTimeMillis();
    while(totalLoaded < MAX_ROWS) {
      totalLoaded++;
      load(totalLoaded);
    }
    long end = System.currentTimeMillis();
    map.dumpStats();
    System.out.println("Time to load= "+ totalLoaded+" ="+(end -start)+"ms");
    long scanned = countRecords();
    System.out.println("Scanned="+ countRecords());
    System.out.println("Total memory="+BigSortedMap.getTotalAllocatedMemory());
    System.out.println("Total   data="+BigSortedMap.getTotalBlockDataSize());
    assertEquals(totalLoaded, scanned);
  }
  
  private static boolean load(long totalLoaded) {
    byte[] key = ("KEY"+ (totalLoaded)).getBytes();
    byte[] value = ("VALUE"+ (totalLoaded)).getBytes();
    long keyPtr = UnsafeAccess.malloc(key.length);
    UnsafeAccess.copy(key, 0, keyPtr, key.length);
    long valPtr = UnsafeAccess.malloc(value.length);
    UnsafeAccess.copy(value, 0, valPtr, value.length);
    boolean result = map.put(keyPtr, key.length, valPtr, value.length, 0);
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(valPtr);
    return result;
  }
  
  @Test
  public void testDeleteUndeleted() throws IOException {
    System.out.println("testDeleteUndeleted");
    List<byte[]> keys = delete(100);    
    assertEquals(totalLoaded - 100, countRecords());
    undelete(keys);
    assertEquals(totalLoaded, countRecords());

  }
  
  static long countRecords() throws IOException {
    BigSortedMapScanner scanner = map.getScanner(null, null);
    long counter = 0;
    while(scanner.hasNext()) {
      counter++;
      scanner.next();
    }
    scanner.close();
    return counter;
  }
  
  @Test
  public void testPutGet() {   
    System.out.println("testPutGet");

    long start = System.currentTimeMillis();    
    for(int i=1; i <= totalLoaded; i++) {
      byte[] key = ("KEY"+ (i)).getBytes();
      byte[] value = ("VALUE"+i).getBytes();
      long keyPtr = UnsafeAccess.malloc(key.length);
      UnsafeAccess.copy(key, 0, keyPtr, key.length);
      long valPtr = UnsafeAccess.malloc(value.length);
      
      try {
        long size = map.get(keyPtr,  key.length, valPtr, value.length, Long.MAX_VALUE) ;
        assertEquals(value.length, (int)size);
        assertTrue(Utils.compareTo(value, 0, value.length, valPtr, (int) size) == 0);
      } catch(Throwable t) {
        throw t;
      } finally {
        UnsafeAccess.free(keyPtr);
        UnsafeAccess.free(valPtr);
      }    
    }    
    long end = System.currentTimeMillis();   
    System.out.println("Time to get "+ totalLoaded+" ="+ (end - start)+"ms");    
    
  }
  
  @Test
  public void testExists() {   
    System.out.println("testExists");
  
    for(int i=1; i <= totalLoaded; i++) {
      byte[] key = ("KEY"+ (i)).getBytes();
      long keyPtr = UnsafeAccess.malloc(key.length);
      UnsafeAccess.copy(key, 0, keyPtr, key.length);
      boolean res = map.exists(keyPtr, key.length) ;
      UnsafeAccess.free(keyPtr);
      assertEquals(true, res);      
    }            
  }
  
  
  private List<byte[]> delete(int num) {
    Random r = new Random();
    int numDeleted = 0;
    long valPtr = UnsafeAccess.malloc(1);
    List<byte[]> list = new ArrayList<byte[]>();
    int collisions = 0;
    while (numDeleted < num) {
      int i = r.nextInt((int)totalLoaded) + 1;
      byte [] key = ("KEY"+ i).getBytes();
      long keyPtr = UnsafeAccess.malloc(key.length);
      UnsafeAccess.copy(key, 0, keyPtr, key.length);
      long len = map.get(keyPtr, key.length, valPtr, 1, Long.MAX_VALUE);
      if (len == DataBlock.NOT_FOUND) {
        collisions++;
        UnsafeAccess.free(keyPtr);
        continue;
      } else {
        boolean res = map.delete(keyPtr, key.length);
        assertTrue(res);
        numDeleted++;
        list.add(key);
        UnsafeAccess.free(keyPtr);
      }
    }
    UnsafeAccess.free(valPtr);
    System.out.println("Deleted="+ numDeleted +" collisions="+collisions);
    return list;
  }
  
  private void undelete(List<byte[]> keys) {
    for (byte[] key: keys) {
      byte[] value = ("VALUE"+ new String(key).substring(3)).getBytes();
      long keyPtr = UnsafeAccess.malloc(key.length);
      UnsafeAccess.copy(key, 0,  keyPtr, key.length);
      long valPtr = UnsafeAccess.malloc(value.length);
      UnsafeAccess.copy(value, 0, valPtr, value.length);
      boolean res = map.put(keyPtr, key.length, valPtr, value.length, 0);
      assertTrue(res);
    }
  }
  

  
  @Test
  public void testSequentialInsert() {
    System.out.println("testSequentialInsert");
    BigSortedMap.setMaxBlockSize(4096);

    BigSortedMap map = new BigSortedMap(1000);
    int counter = 0;
    while(true) {
      byte[] key = nextKeySeq(counter);
      byte[] value = nextValueSeq(counter);
      long keyPtr = UnsafeAccess.malloc(key.length);
      UnsafeAccess.copy(key,  0,  keyPtr, key.length);
      long valuePtr = UnsafeAccess.malloc(value.length);
      UnsafeAccess.copy(value, 0, valuePtr, value.length);
      if(map.put(keyPtr, key.length, valuePtr, value.length, 0)) {
        counter++;
      } else {
        counter--;
        break;
      }
    }
    System.out.println("SEQ: Inserted "+counter+" kvs");
  }
  
  @Test
  public void testNonSequentialInsert() {
    System.out.println("testNonSequentialInsert");
    BigSortedMap.setMaxBlockSize(4096);
    BigSortedMap map = new BigSortedMap(1000);
    int counter = 0;
    while(true) {
      byte[] key = nextKey(counter);
      byte[] value = nextValue(counter);
      long keyPtr = UnsafeAccess.malloc(key.length);
      UnsafeAccess.copy(key,  0,  keyPtr, key.length);
      long valuePtr = UnsafeAccess.malloc(value.length);
      UnsafeAccess.copy(value, 0, valuePtr, value.length);
      if(map.put(keyPtr, key.length, valuePtr, value.length, 0)) {
        counter++;
      } else {
        counter--;
        break;
      }
    }
    System.out.println("NON-SEQ: Inserted "+counter+" kvs");
  }
  
  private byte[] nextKeySeq (long n) {
    String s = format(n , 6);
    return ("KEY"+s).getBytes();
  }
  
  private byte[] nextValueSeq(long n) {
    String s = format(n , 6);
    return ("VALUE"+s).getBytes();
  }
  
  private byte[] nextKey(long n) {
    String s = formatReverse(n, 6);
    return ("KEY" + s).getBytes();
  }
  
  private byte[] nextValue(long n) {
    String s = formatReverse(n, 6);
    return ("VALUE"+s).getBytes();
  }
  
  private String format (long n, int pos) {
    String s = Long.toString(n);
    int len = s.length();
    for (int k=0; k < pos - len; k++) {
      s = "0" + s;
    }
    
    return s;
  }
  
  private String formatReverse (long n, int pos) {
    String s = Long.toString(n);
    int len = s.length();

    for (int k=0; k < pos - len; k++) {
      s = s + "0";
    }
    
    return s;
  }
}
