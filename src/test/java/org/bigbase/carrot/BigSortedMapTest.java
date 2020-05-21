package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.util.Utils;
import org.junit.BeforeClass;
import org.junit.Test;

public class BigSortedMapTest {

  static BigSortedMap map;
  static long totalLoaded;
  
  @BeforeClass 
  public static void setUp() {
	BigSortedMap.setMaxBlockSize(4096);
    map = new BigSortedMap(10000000);
    totalLoaded = 1;
    long start = System.currentTimeMillis();
    while(true) {
      byte[] key = ("KEY"+ (totalLoaded)).getBytes();
      byte[] value = ("VALUE"+ (totalLoaded)).getBytes();
      /*DEBUG*/System.out.println(totalLoaded);
      boolean res = map.put(key, 0, key.length, value, 0, value.length, 0);
      if (res == false) {
        totalLoaded--;
        break;
      }
      totalLoaded++;      
    }
    //dumpFirstKeys(map);
    long end = System.currentTimeMillis();
    map.dumpStats();
    System.out.println("Time to load= "+ totalLoaded+" ="+(end -start)+"ms");
    System.out.println("Scanned="+ countRecords());
    System.out.println("Total memory="+map.getMemoryAllocated());
    System.out.println("Total   data="+map.getTotalDataSize());
  }
    
  @Test
  public void testDeleteUndeleted() {
    System.out.println("testDeleteUndeleted");
    List<byte[]> keys = delete(100);    
    assertEquals(totalLoaded - 100, countRecords());
    undelete(keys);
    assertEquals(totalLoaded, countRecords());

  }
  
  static long countRecords() {
    BigSortedMapScanner scanner = map.getScanner(null, null);
    long counter = 0;
    while(scanner.hasNext()) {
      counter++;
      scanner.next();
    }
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
      long size = map.get(key, 0, key.length, tmp, 0, Long.MAX_VALUE) ;
      assertEquals(value.length, size);
      assertTrue(Utils.compareTo(value, 0, value.length, tmp, 0,(int) size) == 0);
      
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
  public void testFirstKey() {
    System.out.println("testFirstKey");

    byte[] firstKey = "KEY1".getBytes();
    byte[] secondKey = "KEY10".getBytes();
    byte[] key = map.getFirstKey();
    assertTrue(Utils.compareTo(key, 0, key.length, firstKey, 0, firstKey.length) == 0);
    boolean res = map.delete(firstKey, 0, firstKey.length);
    assertEquals ( true, res);
    key = map.getFirstKey();
    assertTrue(Utils.compareTo(key, 0, key.length, secondKey, 0, secondKey.length) == 0);
    
    byte[] value = "VALUE1".getBytes();
    
    res = map.put(firstKey, 0, firstKey.length, value, 0, value.length, 0);
    assertEquals(true, res);
    
  }
  
  
//  @Test
//  public void testLastKey() {
//    System.out.println("testLastKey");
//
//    byte[] lastKey = "KEY99999".getBytes();
//    byte[] secondKey = "KEY99998".getBytes();
//    byte[] key = map.getLastKey();
//    System.out.println( "Last key="+new String(key));
//
//    assertTrue(Utils.compareTo(key, 0, key.length, lastKey, 0, lastKey.length) == 0);
//    boolean res = map.delete(lastKey, 0, lastKey.length);
//    assertEquals ( true, res);
//    key = map.getLastKey();
//    assertTrue(Utils.compareTo(key, 0, key.length, secondKey, 0, secondKey.length) == 0);
//    
//    byte[] value = "VALUE99999".getBytes();
//    
//    res = map.put(lastKey, 0, lastKey.length, value, 0, value.length);
//    assertEquals(true, res);
//    
//  }
  
  @Test  
  public void testFullMapScanner() {
    System.out.println("testFullMap ");
    BigSortedMapScanner scanner = map.getScanner(null, null);
    long start = System.currentTimeMillis();
    long count = 0;
    byte[] value = new byte[("VALUE"+ totalLoaded).length()];
    byte[] prev = null;
    while(scanner.hasNext()) {
      count++;
      int keySize = scanner.keySize();
      int valSize = scanner.valueSize();
      byte[] cur = new byte[keySize];
      scanner.key(cur, 0);
      scanner.value(value, 0);
      if (prev != null) {
        assertTrue (Utils.compareTo(prev, 0, prev.length, cur, 0, cur.length) < 0);
      }
      prev = cur;
      //System.out.println( new String(cur, 0, keySize));
      boolean res = scanner.next();
    }   
    long end = System.currentTimeMillis();
    System.out.println("Scanned "+ count+" in "+ (end- start)+"ms");
    assertEquals(totalLoaded, count);

  }
  
  private List<byte[]> delete(int num) {
    Random r = new Random(2);
    int numDeleted = 0;
    byte[] val = new byte[1];
    List<byte[]> list = new ArrayList<byte[]>();
    while (numDeleted < num) {
      int i = r.nextInt((int)totalLoaded) + 1;
      byte [] key = ("KEY"+ i).getBytes();
      long len = map.get(key, 0, key.length, val, 0, Long.MAX_VALUE);
      if (len == DataBlock.NOT_FOUND) {
        continue;
      } else {
        boolean res = map.delete(key, 0, key.length);
        assertTrue(res);
        numDeleted++;
        list.add(key);
      }
    }
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
  public void testFullMapScannerWithDeletes() {
    System.out.println("testFullMapScannerWithDeletes ");
    int toDelete = 10000;
    List<byte[]> deletedKeys = delete(toDelete);
    BigSortedMapScanner scanner = map.getScanner(null, null);
    long start = System.currentTimeMillis();
    long count = 0;
    byte[] value = new byte[("VALUE"+ totalLoaded).length()];
    byte[] prev = null;
    while(scanner.hasNext()) {
      count++;
      int keySize = scanner.keySize();
      int valSize = scanner.valueSize();
      byte[] cur = new byte[keySize];
      scanner.key(cur, 0);
      scanner.value(value, 0);
      if (prev != null) {
        assertTrue (Utils.compareTo(prev, 0, prev.length, cur, 0, cur.length) < 0);
      }
      prev = cur;
      //System.out.println( new String(cur, 0, keySize));
      boolean res = scanner.next();
    }   
    long end = System.currentTimeMillis();
    System.out.println("Scanned "+ count+" in "+ (end- start)+"ms");
    assertEquals(totalLoaded - toDelete, count);
    undelete(deletedKeys);

  }
  
  @Test  
  public void testPartialScannerStartRow() {
    System.out.println("testPartialScannerStartRow ");
    byte[] startKey = "KEY999".getBytes();
    BigSortedMapScanner scanner = map.getScanner(startKey, null);
    long start = System.currentTimeMillis();
    long count = 0;
    byte[] value = new byte[("VALUE"+ totalLoaded).length()];
    byte[] prev = null;
    while(scanner.hasNext()) {
      count++;
      int keySize = scanner.keySize();
      int valSize = scanner.valueSize();
      byte[] cur = new byte[keySize];
      scanner.key(cur, 0);
      scanner.value(value, 0);
      if (prev != null) {
        assertTrue (Utils.compareTo(prev, 0, prev.length, cur, 0, cur.length) < 0);
      }
      prev = cur;
      //System.out.println( new String(cur, 0, keySize));
      boolean res = scanner.next();
      
    }   
    long end = System.currentTimeMillis();
    System.out.println("Scanned "+ count+" in "+ (end- start)+"ms");
    assertEquals(111, count);

  }
  
  @Test  
  public void testPartialScannerStopRow() {
    System.out.println("testPartialScannerStopRow ");
    byte[] stopKey = "KEY999".getBytes();
    BigSortedMapScanner scanner = map.getScanner(null, stopKey);
    long start = System.currentTimeMillis();
    long count = 0;
    byte[] value = new byte[("VALUE"+ totalLoaded).length()];
    byte[] prev = null;
    while(scanner.hasNext()) {
      count++;
      int keySize = scanner.keySize();
      int valSize = scanner.valueSize();
      byte[] cur = new byte[keySize];
      scanner.key(cur, 0);
      scanner.value(value, 0);
      if (prev != null) {
        assertTrue (Utils.compareTo(prev, 0, prev.length, cur, 0, cur.length) < 0);
      }
      prev = cur;
      //System.out.println( new String(cur, 0, keySize));
      boolean res = scanner.next();
      
    }   
    

    long end = System.currentTimeMillis();
    System.out.println("Scanned "+ count+" in "+ (end- start)+"ms");

    assertEquals(totalLoaded - 111, count);

  }
  
  @Test
  public void testPartialScannerStartStopRow() {
    System.out.println("testPartialScannerStartStopRow ");
    byte[] startKey = "KEY998".getBytes();

    byte[] stopKey = "KEY9989".getBytes();
    BigSortedMapScanner scanner = map.getScanner(startKey, stopKey);
    long start = System.currentTimeMillis();
    long count = 0;
    byte[] value = new byte[("VALUE"+ totalLoaded).length()];
    byte[] prev = null;
    while(scanner.hasNext()) {
      count++;
      int keySize = scanner.keySize();
      int valSize = scanner.valueSize();
      byte[] cur = new byte[keySize];
      scanner.key(cur, 0);
      scanner.value(value, 0);
      if (prev != null) {
        assertTrue (Utils.compareTo(prev, 0, prev.length, cur, 0, cur.length) < 0);
      }
      prev = cur;
      //System.out.println( new String(cur, 0, keySize));
      boolean res = scanner.next();
      
    }   
    long end = System.currentTimeMillis();
    System.out.println("Scanned "+ count+" in "+ (end- start)+"ms");
    assertEquals(100, count);

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
      //System.out.println(new String(key));
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
