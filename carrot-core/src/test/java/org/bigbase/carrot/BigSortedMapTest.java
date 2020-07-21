package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.Utils;
import org.junit.BeforeClass;
import org.junit.Test;

public class BigSortedMapTest {

  static BigSortedMap map;
  static long totalLoaded;
  
  @BeforeClass 
  public static void setUp() throws IOException {
	  BigSortedMap.setMaxBlockSize(4096);
    map = new BigSortedMap(100000000L);
    totalLoaded = 0;
    long start = System.currentTimeMillis();
    while(totalLoaded < 1000000) {
      totalLoaded++;
      byte[] key = ("KEY"+ (totalLoaded)).getBytes();
      byte[] value = ("VALUE"+ (totalLoaded)).getBytes();
      map.put(key, 0, key.length, value, 0, value.length, 0);
      if (totalLoaded % 1000000 == 0) {
        System.out.println("Loaded = " + totalLoaded+" of "+ 100000000);
      }
    }
    long end = System.currentTimeMillis();
    map.dumpStats();
    System.out.println("Time to load= "+ totalLoaded+" ="+(end -start)+"ms");
    long scanned = countRecords();
    System.out.println("Scanned="+ countRecords());
    System.out.println("Total memory="+BigSortedMap.getTotalAllocatedMemory());
    System.out.println("Total   data="+BigSortedMap.getTotalBlockDataSize());
    System.out.println("Total  index=" + BigSortedMap.getTotalBlockIndexSize());
    assertEquals(totalLoaded, scanned);
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
    byte[] tmp = ("VALUE"+ totalLoaded).getBytes();
    for(int i=1; i <= totalLoaded; i++) {
      byte[] key = ("KEY"+ (i)).getBytes();
      byte[] value = ("VALUE"+i).getBytes();
      try {
        long size = map.get(key, 0, key.length, tmp, 0, Long.MAX_VALUE) ;
        assertEquals(value.length, (int)size);
        assertTrue(Utils.compareTo(value, 0, value.length, tmp, 0,(int) size) == 0);
      } catch(Throwable t) {
        throw t;
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
      boolean res = map.exists(key, 0, key.length) ;
      assertEquals(true, res);      
    }            
  }
  
  @Test
  public void testFirstKey() throws IOException {
    System.out.println("testFirstKey");

    byte[] firstKey = "KEY1".getBytes();
    byte[] secondKey = "KEY10".getBytes();
    byte[] key = map.getFirstKey();
    System.out.println(Bytes.toString(key));
    assertTrue(Utils.compareTo(key, 0, key.length, firstKey, 0, firstKey.length) == 0);
    boolean res = map.delete(firstKey, 0, firstKey.length);
    assertEquals ( true, res);
    key = map.getFirstKey();
    assertTrue(Utils.compareTo(key, 0, key.length, secondKey, 0, secondKey.length) == 0);
    
    byte[] value = "VALUE1".getBytes();
    
    res = map.put(firstKey, 0, firstKey.length, value, 0, value.length, 0);
    assertEquals(true, res);
    
  }
  
  
  private List<byte[]> delete(int num) {
    Random r = new Random();
    int numDeleted = 0;
    byte[] val = new byte[1];
    List<byte[]> list = new ArrayList<byte[]>();
    int collisions = 0;
    while (numDeleted < num) {
      int i = r.nextInt((int)totalLoaded) + 1;
      byte [] key = ("KEY"+ i).getBytes();
      long len = map.get(key, 0, key.length, val, 0, Long.MAX_VALUE);
      if (len == DataBlock.NOT_FOUND) {
        collisions++;
        continue;
      } else {
        boolean res = map.delete(key, 0, key.length);
        assertTrue(res);
        numDeleted++;
        list.add(key);
      }
    }
    System.out.println("Deleted="+ numDeleted +" collisions="+collisions);
    return list;
  }
  
  private void undelete(List<byte[]> keys) {
    for (byte[] key: keys) {
      byte[] value = ("VALUE"+ new String(key).substring(3)).getBytes();
      boolean res = map.put(key, 0, key.length, value, 0, value.length, 0);
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
      if(map.put(key, 0, key.length, value, 0, value.length, 0)) {
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
      if(map.put(key, 0, key.length, value, 0, value.length, 0)) {
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
