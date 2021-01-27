package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Ignore;
import org.junit.Test;

public class BigSortedMapLargeKVsTest {

  BigSortedMap map;
  long totalLoaded;
  List<byte[]> keys;
  
  static {
    UnsafeAccess.debug = true;
  }
  
  public  void setUp() throws IOException {
    BigSortedMap.setMaxBlockSize(4096);
    map = new BigSortedMap(100000000);
    totalLoaded = 0;
    long start = System.currentTimeMillis();
    keys = fillMap(map);
    totalLoaded = keys.size();
    // Delete 20% to prevent OOM during test runs 
    List<byte[]> deleted = delete((int)totalLoaded/5);
    keys.removeAll(deleted);
    // Update total loaded
    System.out.println("Adjusted size by "+ deleted.size() +" keys");    
    totalLoaded -= deleted.size();
    Utils.sort(keys);
    totalLoaded = keys.size();
    long end = System.currentTimeMillis();
    //map.dumpStats();
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
  
  protected  ArrayList<byte[]> fillMap (BigSortedMap map) throws RetryOperationException {
    ArrayList<byte[]> keys = new ArrayList<byte[]>();
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
      result = map.put(key, 0, key.length, key, 0, key.length, 0);
      if(result) {
        keys.add(key);
      } else {
        break;
      }
    }   
    return keys;
  }
  
  @Test
  public void runAllNoCompression() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    for (int i = 0; i < 10; i++) {
      System.out.println("\n********* " + i+" ********** Codec = NULL\n");
      setUp();
      allTests();
      System.out.println("\nTotal memory     =" + BigSortedMap.getTotalAllocatedMemory());
      System.out.println("Total   data     =" + BigSortedMap.getTotalDataSize());
      System.out.println("Total  index     =" + BigSortedMap.getTotalBlockIndexSize());
      System.out.println("Compressed data  =" + BigSortedMap.getTotalCompressedDataSize());
      System.out.println("Compression ratio=" + ((float)BigSortedMap.getTotalDataSize())/
        BigSortedMap.getTotalAllocatedMemory());
      System.out.println();
      
      tearDown();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats();
    }
  }
  
  @Test
  public void runAllCompressionLZ4() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    for (int i = 0; i < 10; i++) {
      System.out.println("\n********* " + i+" ********** Codec = LZ4\n");
      setUp();
      allTests();
      System.out.println("\nTotal memory     =" + BigSortedMap.getTotalAllocatedMemory());
      System.out.println("Total   data     =" + BigSortedMap.getTotalDataSize());
      System.out.println("Total  index     =" + BigSortedMap.getTotalBlockIndexSize());
      System.out.println("Compressed data  =" + BigSortedMap.getTotalCompressedDataSize());
      System.out.println("Compression ratio=" + ((float)BigSortedMap.getTotalDataSize())/
        BigSortedMap.getTotalAllocatedMemory());
      System.out.println();
      
      tearDown();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats();
    }
  }
  
  @Test
  public void runAllCompressionLZ4HC() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    for (int i = 0; i < 10; i++) {
      System.out.println("\n********* " + i+" ********** Codec = LZ4HC\n");
      setUp();
      allTests();
      System.out.println("\nTotal memory     =" + BigSortedMap.getTotalAllocatedMemory());
      System.out.println("Total   data     =" + BigSortedMap.getTotalDataSize());
      System.out.println("Total  index     =" + BigSortedMap.getTotalBlockIndexSize());
      System.out.println("Compressed data  =" + BigSortedMap.getTotalCompressedDataSize());
      System.out.println("Compression ratio=" + ((float)BigSortedMap.getTotalDataSize())/
        BigSortedMap.getTotalAllocatedMemory());
      System.out.println();
      
      tearDown();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats();
    }
  }
  
  protected void allTests() throws IOException {
    testDeleteUndeleted();
    testGetAfterLoad();
    testExists();
    testFirstKey();
    testFullMapScanner();
    testFullMapScannerWithDeletes();
    testScannerSameStartStopRow();
    testAllScannerStartStopRow();
  }
  
  protected void tearDown() {
    map.dispose();
  }

  @Ignore
  @Test
  public void testDeleteUndeleted() throws IOException {
    System.out.println("testDeleteUndeleted");
    List<byte[]> keys = delete(100);    
    assertEquals(totalLoaded - 100, countRecords());
    undelete(keys);
    assertEquals(totalLoaded, countRecords());

  }
  
  long countRecords() throws IOException {
    BigSortedMapScanner scanner = map.getScanner(null, null);
    int counter = 0;
    while(scanner.hasNext()) {
      scanner.next();
      counter++;
    }
    scanner.close();
    return counter;
  }
  
  void verifyGets(List<byte[]> keys) {
    int counter = 0;
    int failed = 0;
    for(byte[] key: keys) {
      
      if (!map.exists(key, 0, key.length)) {
        fail("FAILED index=" + counter+" key length=" + key.length+" key=" + new String(key));
      }
      counter++;
    }
    System.out.println("Verify Gets failed="+ failed);
  }
  
  @Ignore
  @Test
  public void testGetAfterLoad() {   
    System.out.println("testGetAfterLoad");

    long start = System.currentTimeMillis();    
    for(byte[] key : keys) {
      byte[] value = new byte[key.length];
      try {
        long size = map.get(key, 0, key.length, value, 0, Long.MAX_VALUE) ;
        assertEquals(value.length, (int)size);
        assertTrue(Utils.compareTo(value, 0, value.length, key, 0,(int) size) == 0);
      } catch(Throwable t) {
        throw t;
      }
    }    
    long end = System.currentTimeMillis();   
    System.out.println("Time to get " + totalLoaded + " ="+ (end - start)+"ms");    
  }
  
  @Ignore
  @Test
  public void testExists() {   
    System.out.println("testExists");
  
    for(byte[] key: keys) {
      boolean res = map.exists(key, 0, key.length) ;
      assertEquals(true, res);      
    }            
  }
  
 
  @Ignore
  @Test
  public void testFirstKey() throws IOException {
    System.out.println("testFirstKey");

    byte[] firstKey = keys.get(0);
    byte[] secondKey = keys.get(1);
    byte[] key = map.getFirstKey();
    assertTrue(Utils.compareTo(key, 0, key.length, firstKey, 0, firstKey.length) == 0);
    boolean res = map.delete(firstKey, 0, firstKey.length);
    assertEquals ( true, res);
    key = map.getFirstKey();
    assertTrue(Utils.compareTo(key, 0, key.length, secondKey, 0, secondKey.length) == 0); 
    res = map.put(firstKey, 0, firstKey.length, firstKey, 0, firstKey.length, 0);
    assertEquals(true, res);
    
  }
  
  
  @Ignore
  @Test  
  public void testFullMapScanner() throws IOException {
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
      scanner.next();
    }   
    long end = System.currentTimeMillis();
    System.out.println("Scanned "+ count+" in "+ (end- start)+"ms");
    assertEquals(totalLoaded, count);
    scanner.close();
  }
 
  
  private List<byte[]> delete(int num) {
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Delete seed ="+seed);
    int numDeleted = 0;
    byte[] val = new byte[1];
    List<byte[]> list = new ArrayList<byte[]>();
    int collisions = 0;
    while (numDeleted < num) {
      int i = r.nextInt((int)totalLoaded);
      byte [] key = keys.get(i);
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
      boolean res = map.put(key, 0, key.length, key, 0, key.length, 0);
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
    System.out.println("testFullMapScannerWithDeletes SEED="+ seed+ " toDelete="+toDelete);

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
      scanner.next();
    }   
    long end = System.currentTimeMillis();
    System.out.println("Scanned "+ count+" in "+ (end- start)+"ms");
    assertEquals(totalLoaded - toDelete, count);
    undelete(deletedKeys);

  }
    
  @Ignore
  @Test
  public void testScannerSameStartStopRow () throws IOException
  {
    System.out.println("testScannerSameStartStopRow");
    Random r = new Random();
    int startIndex = r.nextInt((int)totalLoaded);
    byte[] startKey = keys.get(startIndex);

    BigSortedMapScanner scanner = map.getScanner(startKey, startKey);
    long count = countRows(scanner); 
    scanner.close();
    assertEquals(0, (int) count);
    startIndex = r.nextInt((int)totalLoaded);
    startKey = keys.get(startIndex);;
    scanner = map.getScanner(startKey, startKey);
    count = countRows(scanner); 
    scanner.close();
    assertEquals(0, (int) count);
  }
  
  
  @Ignore
  @Test
  public void testAllScannerStartStopRow() throws IOException {
    System.out.println("testAllScannerStartStopRow ");
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Test seed="+ seed);
    int startIndex = r.nextInt((int)totalLoaded);
    int stopIndex = r.nextInt((int)totalLoaded - startIndex) + startIndex;
    byte[] key1 = keys.get(startIndex);
    byte[] key2 = keys.get(stopIndex);
    byte[] startKey, stopKey;
    if (Utils.compareTo(key1, 0, key1.length, key2, 0, key2.length) > 0) {
      startKey = key2;
      stopKey = key1;
    } else {
      startKey = key1;
      stopKey = key2;
    }
    System.out.println("Total selected=" + (Math.abs(startIndex - stopIndex) + 1) +" of "+ totalLoaded);
    System.out.println("Min selected=" + (Math.min(startIndex, stopIndex)));
    System.out.println("Max selected=" + (Math.max(startIndex, stopIndex)));
    
    BigSortedMapScanner scanner = map.getScanner(null, startKey);
    long count1 = countRows(scanner); 
    scanner.close();
    scanner = map.getScanner(startKey, stopKey);
    long count2 = countRows(scanner);
    scanner.close();
    scanner = map.getScanner(stopKey, null);
    long count3 = countRows(scanner);
    scanner.close();
    System.out.println("Total scanned="+(count1 + count2+count3));
    assertEquals(totalLoaded, count1 + count2 + count3);

  }

  private long verifyScanner(BigSortedMapScanner scanner, List<byte[]> keys) 
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
      byte[] buf = new byte[keySize];
      byte[] key = keys.get(counter + delta);
      scanner.key(buf, 0);
      assertTrue(Utils.compareTo(buf, 0, buf.length, key, 0, key.length) == 0);
      int size = scanner.value(buf, 0);
      assertEquals(keySize, size);
      assertTrue(Utils.compareTo(buf, 0, buf.length, key, 0, key.length) == 0);
      scanner.next();
      counter++;
    }
    return counter;
  }
  
  protected long countRows(BigSortedMapScanner scanner) throws IOException {
    long start = System.currentTimeMillis();
    long count = 0;
    byte[] prev = null;
    while(scanner.hasNext()) {
      count++;
      int keySize = scanner.keySize();
      byte[] cur = new byte[keySize];
      byte[] value = new byte[keySize];
      
      scanner.key(cur, 0);
      scanner.value(value, 0);
      if (prev != null) {
        assertTrue (Utils.compareTo(prev, 0, prev.length, cur, 0, cur.length) < 0);
      }
      prev = cur;
      scanner.next();
    }   
    long end = System.currentTimeMillis();
    System.out.println("Scanned "+ count+" in "+ (end- start)+"ms");
    return count;
  }
  

}
