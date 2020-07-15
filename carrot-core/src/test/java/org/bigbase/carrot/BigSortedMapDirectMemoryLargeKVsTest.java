package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Ignore;
import org.junit.Test;

public class BigSortedMapDirectMemoryLargeKVsTest {

  static long buffer = UnsafeAccess.malloc(64*1024);
  
  BigSortedMap map;
  long totalLoaded;
  List<Key> keys;
  static {
    UnsafeAccess.debug = true;
  }
  public  void setUp() throws IOException {
    BigSortedMap.setMaxBlockSize(4096);
    map = new BigSortedMap(100000000);
    totalLoaded = 0;
    long start = System.currentTimeMillis();
    keys = fillMap(map);
    System.out.println("Loaded");
    Utils.sortKeys(keys);
    totalLoaded = keys.size();
    // Delete 20% of keys to guarantee that we will be able
    // to delete than reinsert
    List<Key> deleted = delete((int)totalLoaded/5);
    keys.removeAll(deleted);
    // Update total loaded
    System.out.println("Adjusted size by "+ deleted.size() +" keys");    
    totalLoaded -= deleted.size();
    deallocate(deleted);

    long end = System.currentTimeMillis();
    map.dumpStats();
    System.out.println("Time to load= "+ totalLoaded+" ="+(end -start)+"ms");
    verifyGets(keys);
    BigSortedMapScanner scanner = map.getScanner(null, null);
    long scanned = verifyScanner(scanner, keys);
    scanner.close();
    System.out.println("Scanned="+ scanned);
    System.out.println("Total memory="+BigSortedMap.getTotalAllocatedMemory());
    System.out.println("Total   data="+BigSortedMap.getTotalBlockDataSize());
    System.out.println("Total  index=" + BigSortedMap.getTotalBlockIndexSize());
    assertEquals(totalLoaded, scanned);
  }
  
  void verifyGets(List<Key> keys) {
    int counter = 0;
    for(Key key: keys) {
      
      if (!map.exists(key.address, key.length)) {
        fail("FAILED index=" + counter+" key length=" + key.length+" key=" + key.address);
      }
      counter++;
    }
  }
  
  public void tearDown() {
    map.dispose();
    // Free keyes
    deallocate(keys);
  }
  
  private void deallocate(List<Key> keys) {
    for(Key key: keys) {
      UnsafeAccess.free(key.address);
    }
    keys.clear();
  }
  
  @Test
  public void testAll() throws IOException {
    
    for(int i=0; i < 1000; i++) {
      System.out.println("\n\n\n\n********* " + i+" **********\n\n\n\n");

      // First group does not undelete
      setUp();
      testGetAfterLoad();
      testExists();
      testFullMapScanner();
      testDeleteUndeleted();
      testFullMapScannerWithDeletes();
      testDirectMemoryAllRangesMapScanner();
      testDirectMemoryFullMapScanner();
      testDirectMemoryFullMapScannerWithDeletes();
      tearDown();
      UnsafeAccess.mallocStats();
      System.out.println("DataBlock large KV leak :" +DataBlock.largeKVs.get());
      System.out.println("IndexBlock large KV leak :" +IndexBlock.largeKVs.get());
      System.out.println("Total memory="+BigSortedMap.getTotalAllocatedMemory());
      System.out.println("Total   data="+BigSortedMap.getTotalBlockDataSize());
      System.out.println("Total  index=" + BigSortedMap.getTotalBlockIndexSize());
    }
  }
  
  private long verifyScanner(BigSortedMapScanner scanner, List<Key> keys) 
      throws IOException {
    int counter = 0;
    int delta = 0;
    while(scanner.hasNext()) {
      int keySize = scanner.keySize();
      if (keySize != keys.get(counter + delta).length) {
        System.out.println("counter="+counter+" expected key size="+ 
            keys.get(counter).length + " found="+keySize);
        delta ++;
      }
      long buf = UnsafeAccess.malloc(keySize);
      Key key = keys.get(counter + delta);
      scanner.key(buf, keySize);
      assertTrue(Utils.compareTo(buf, keySize, key.address, key.length) == 0);
      int size = scanner.value(buf, keySize);
      assertEquals(keySize, size);
      assertTrue(Utils.compareTo(buf, keySize, key.address, key.length) == 0);
      
      UnsafeAccess.free(buf);
      scanner.next();
      counter++;
    }
    return counter;
  }
  
  protected  ArrayList<Key> fillMap (BigSortedMap map) throws RetryOperationException {
    ArrayList<Key> keys = new ArrayList<Key>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("FILL SEED="+seed);
    int maxSize = 2048;
    boolean result = true;
    while(true) {
      int len = r.nextInt(maxSize-16) + 16;
      byte[] key = new byte[len];
      r.nextBytes(key);
      key = Bytes.toHex(key).getBytes(); 
      len = key.length;
      long keyPtr = UnsafeAccess.malloc(len);
      UnsafeAccess.copy(key, 0, keyPtr, len);
      result = map.put(keyPtr, len, keyPtr, len,  0);
      if(result) {
        keys.add(new Key(keyPtr, len));
      } else {
        UnsafeAccess.free(keyPtr);
        break;
      }
    }   
    return keys;
  }
  
  @Ignore
  @Test
  public void testDeleteUndeleted() throws IOException {
    System.out.println("testDeleteUndeleted");
    List<Key> keys = delete(100);    
    assertEquals(totalLoaded - 100, countRecords());
    undelete(keys);
    assertEquals(totalLoaded, countRecords());

  }
  
  long countRecords() throws IOException {
    BigSortedMapScanner scanner = map.getScanner(null, null);
    long counter = 0;
    while(scanner.hasNext()) {
//      int keySize = scanner.keySize();
//      byte[] buf = new byte[keySize];
//      scanner.key(buf,  0);
//      System.out.println(Bytes.toString(buf));
      counter++;
      scanner.next();
    }
    scanner.close();
    return counter;
  }
  
  @Ignore
  @Test
  public void testGetAfterLoad() {   
    System.out.println("testGetAfterLoad");

    long start = System.currentTimeMillis();    
    for(Key key: keys) {
      
      long valPtr = UnsafeAccess.malloc(key.length);
      
      try {
        long size = map.get(key.address,  key.length, valPtr, key.length, Long.MAX_VALUE) ;
        assertEquals(key.length, (int)size);
        assertTrue(Utils.compareTo(key.address, key.length, valPtr, (int) size) == 0);
      } catch(Throwable t) {
        throw t;
      } finally {
        UnsafeAccess.free(valPtr);
      }    
    }    
    long end = System.currentTimeMillis();   
    System.out.println("Time to get "+ totalLoaded+" ="+ (end - start)+"ms");    
    
  }
  
  @Ignore
  @Test
  public void testExists() {   
    System.out.println("testExists");
  
    long start = System.currentTimeMillis();    
    for(Key key: keys) {
      try {
        boolean result = map.exists(key.address,  key.length) ;
        assertTrue(result);
      } catch(Throwable t) {
        throw t;
      } 
    }    
    long end = System.currentTimeMillis();   
    System.out.println("Time to exist "+ totalLoaded+" ="+ (end - start)+"ms");    
  }
  
 
  
  @Ignore
  @Test  
  public void testFullMapScanner() throws IOException {
    System.out.println("testFullMap ");
    BigSortedMapScanner scanner = map.getScanner(null, null);
    long start = System.currentTimeMillis();
    long count = 0;

    while(scanner.hasNext()) {
      int keySize = scanner.keySize();
      int valSize = scanner.valueSize();
      long key = UnsafeAccess.malloc(keySize);
      long value = UnsafeAccess.malloc(valSize);
      scanner.key(key, keySize);
      scanner.value(value, valSize);
      Key kkey = keys.get((int)count);
      assertEquals(0, Utils.compareTo(kkey.address, kkey.length, key, keySize));
      assertEquals(0, Utils.compareTo(kkey.address, kkey.length, value, valSize));
      UnsafeAccess.free(key);
      UnsafeAccess.free(value);
      count++;
      scanner.next();
    }   
    
    long end = System.currentTimeMillis();
    System.out.println("Scanned "+ count+" in "+ (end- start)+"ms");
    assertEquals(keys.size(), (int)count);
    scanner.close();
  }
 
  @Ignore
  @Test  
  public void testDirectMemoryFullMapScanner() throws IOException {
    System.out.println("testDirectMemoryFullMapScanner ");
    BigSortedMapDirectMemoryScanner scanner = map.getScanner(0, 0, 0, 0);
    long start = System.currentTimeMillis();
    long count = 0;

    while(scanner.hasNext()) {
      int keySize = scanner.keySize();
      int valSize = scanner.valueSize();
      long key = UnsafeAccess.malloc(keySize);
      long value = UnsafeAccess.malloc(valSize);
      scanner.key(key, keySize);
      scanner.value(value, valSize);
      Key kkey = keys.get((int)count);
      assertEquals(0, Utils.compareTo(kkey.address, kkey.length, key, keySize));
      assertEquals(0, Utils.compareTo(kkey.address, kkey.length, value, valSize));
      UnsafeAccess.free(key);
      UnsafeAccess.free(value);
      count++;
      scanner.next();
    }   
    
    long end = System.currentTimeMillis();
    System.out.println("Scanned "+ count+" in "+ (end- start)+"ms");
    assertEquals(keys.size(), (int)count);
    scanner.close();
  }
  
  @Ignore
  @Test  
  public void testDirectMemoryAllRangesMapScanner() throws IOException {
    System.out.println("testDirectMemoryAllRangesMapScanner ");
    Random r = new Random();
    int startIndex = r.nextInt((int)totalLoaded);
    int stopIndex = r.nextInt((int)totalLoaded);
    
    if (startIndex > stopIndex) {
      int tmp = startIndex;
      startIndex = stopIndex;
      stopIndex = tmp;
    }    
    Key startKey = keys.get(startIndex); 
    Key stopKey  = keys.get(stopIndex);
    
    BigSortedMapDirectMemoryScanner scanner = 
        map.getScanner(0, 0, startKey.address, startKey.length);
    long count1 = countRows(scanner);
    scanner.close();
    scanner = 
        map.getScanner(startKey.address, startKey.length, stopKey.address, stopKey.length);
    long count2 = countRows(scanner);
    scanner.close();
    scanner = 
        map.getScanner(stopKey.address, stopKey.length, 0, 0);
    long count3 = countRows(scanner);
    scanner.close();
    assertEquals(totalLoaded, count1 + count2 + count3); 
  }
  
  private long countRows(BigSortedMapDirectMemoryScanner scanner) throws IOException {
    long start = System.currentTimeMillis();
    long count = 0;
    long prev = 0;
    int prevLen = 0;
  
    while(scanner.hasNext()) {
      count++;
      int keySize = scanner.keySize();
      long key = UnsafeAccess.malloc(keySize);      
      scanner.key(key, keySize);
      if (prev != 0) {
        assertTrue (Utils.compareTo(prev,  prevLen, key, keySize) < 0);
        UnsafeAccess.free(prev);
      }
      prev = key;
      scanner.next();
    }   
    if (prev != 0) {
      UnsafeAccess.free(prev);
    }
    long end = System.currentTimeMillis();
    System.out.println("Scanned "+ count+" in "+ (end- start)+"ms");
    return count;
  }
  
  private List<Key> delete(int num) {
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Delete seed ="+seed);
    int numDeleted = 0;
    long valPtr = UnsafeAccess.malloc(1);
    List<Key> list = new ArrayList<Key>();
    int collisions = 0;
    while (numDeleted < num) {
      int i = r.nextInt((int)totalLoaded);
      Key key = keys.get(i);
      long len = map.get(key.address, key.length, valPtr, 0, Long.MAX_VALUE);
      if (len == DataBlock.NOT_FOUND) {
        collisions++;
        continue;
      } else {
        boolean res = map.delete(key.address, key.length);
        assertTrue(res);
        numDeleted++;
        list.add(key);
      }
    }
    UnsafeAccess.free(valPtr);
    System.out.println("Deleted="+ numDeleted +" collisions="+collisions);
    return list;
  }
  
  /**
   * Delete X - Undelete X not always work, b/c our map is FULL
   * before deletion and there is no guarantee that insertion X deleted
   * rows back will succeed
   * @param keys
   */
  private void undelete(List<Key> keys) {
    /*DEBUG*/ System.out.println("Undelete "+ keys.size()+" keys");
    int count = 1;
    for (Key key: keys) {
      count++;
      boolean res = map.put(key.address, key.length, key.address, key.length, 0);
      if (res == false) {
        System.out.println("Count = "+count+" total="+ keys.size()+" memory ="+ 
      BigSortedMap.getTotalAllocatedMemory());
      }
      assertTrue(res);
    }
  }
  
  @Ignore
  @Test
  public void testFullMapScannerWithDeletes() throws IOException {
    System.out.println("testFullMapScannerWithDeletes ");
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    int toDelete = r.nextInt((int)totalLoaded);
    System.out.println("testFullMapScannerWithDeletes SEED="+ seed +
      " toDelete="+toDelete);
    List<Key> deletedKeys = delete(toDelete);
    BigSortedMapScanner scanner = map.getScanner(null, null);
    long start = System.currentTimeMillis();
    long count = 0;

    long prev = 0;
    int prevSize = 0;
    while(scanner.hasNext()) {
      count++;
      int keySize = scanner.keySize();
      long key = UnsafeAccess.malloc(keySize);
      scanner.key(key, keySize);
      if (prev != 0) {
        assertTrue (Utils.compareTo(prev, prevSize, key, keySize) < 0);
        UnsafeAccess.free(prev);
      }
      prev = key;
      prevSize = keySize;
      scanner.next();
    }   
    if (prev > 0) {
      UnsafeAccess.free(prev);
    }
    
    long end = System.currentTimeMillis();
    System.out.println("Scanned "+ count+" in "+ (end- start)+"ms");
    assertEquals(totalLoaded - toDelete, count);
    scanner.close();
    undelete(deletedKeys);

  }
    
  @Ignore
  @Test
  public void testDirectMemoryFullMapScannerWithDeletes() throws IOException {
    System.out.println("testDirectMemoryFullMapScannerWithDeletes ");
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    int toDelete = r.nextInt((int)totalLoaded);
    System.out.println("testDirectMemoryFullMapScannerWithDeletes SEED="+ seed +
      " toDelete="+toDelete);
    List<Key> deletedKeys = delete(toDelete);
    BigSortedMapDirectMemoryScanner scanner = map.getScanner(0, 0, 0, 0);
    long start = System.currentTimeMillis();
    long count = 0;

    long prev = 0;
    int prevSize = 0;
    while(scanner.hasNext()) {
      count++;
      int keySize = scanner.keySize();
      long key = UnsafeAccess.malloc(keySize);
      scanner.key(key, keySize);
      if (prev != 0) {
        assertTrue (Utils.compareTo(prev, prevSize, key, keySize) < 0);
        UnsafeAccess.free(prev);
      }
      prev = key;
      prevSize = keySize;
      scanner.next();
    }   
    if (prev > 0) {
      UnsafeAccess.free(prev);
    }
    
    long end = System.currentTimeMillis();
    System.out.println("Scanned "+ count+" in "+ (end- start)+"ms");
    assertEquals(totalLoaded - toDelete, count);
    scanner.close();
    undelete(deletedKeys);

  }
  

}
