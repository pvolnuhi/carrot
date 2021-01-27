package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

public class BigSortedMapScannerTest {

  BigSortedMap map;
  long totalLoaded;
  long MAX_ROWS = 1000000;
  
  static {
    UnsafeAccess.debug = true;
  }
  
  private void setUp() throws IOException {
    BigSortedMap.setMaxBlockSize(4096);
    map = new BigSortedMap(100000000);
    totalLoaded = 0;
    long start = System.currentTimeMillis();
    while(totalLoaded < MAX_ROWS) {
      totalLoaded++;
      load(totalLoaded);
      if ((totalLoaded % 100000) == 0) {
        System.out.println("Loaded " + totalLoaded);
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("Time to load= " + totalLoaded + " =" + (end -start) + "ms");
    long scanned = countRecords();
    System.out.println("Scanned ="+ countRecords());
    System.out.println("\nTotal memory   =" + BigSortedMap.getTotalAllocatedMemory());
    System.out.println("Total   data     =" + BigSortedMap.getTotalDataSize());
    System.out.println("Compressed   data=" + BigSortedMap.getTotalCompressedDataSize());
    System.out.println("Compression ratio=" + ((float)BigSortedMap.getTotalDataSize())/
      BigSortedMap.getTotalAllocatedMemory());
    
    System.out.println("");
    assertEquals(totalLoaded, scanned);
  }
  
  private void tearDown() {
    map.dispose();
  }

  @Test
  public void runAllNoCompression() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    for (int i = 0; i < 1; i++) {
      System.out.println("\n********* " + i+" ********** Codec = NONE\n");
      setUp();
      allTests();
      tearDown();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats();
    }
  }
  
  @Test
  public void runAllCompressionLZ4() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    for (int i=0; i < 1; i++) {
      System.out.println("\n********* " + i+" ********** Codec = LZ4\n");
      setUp();
      allTests();
      tearDown();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats();
    }
  }
  
  @Test
  public void runAllCompressionLZ4HC() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    for (int i=0; i < 1; i++) {
      System.out.println("\n********* " + i+" ********** Codec = LZ4HC\n");
      setUp();
      allTests();
      tearDown();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats();
    }
  }
  
  protected void allTests() throws IOException {
    testFullMapScanner();
    testFullMapScannerWithDeletes();
    testScannerSameStartStopRow();
    testAllScannerStartStopRow();
  }
  
  @Ignore
  @Test  
  public void testFullMapScanner() throws IOException {
    System.out.println("testFullMap ");
    BigSortedMapScanner scanner = map.getScanner(null, null);
    long start = System.currentTimeMillis();
    long count = countRows(scanner);
    long end = System.currentTimeMillis();
    System.out.println("Scanned "+ count+" in "+ (end- start)+"ms");
    assertEquals(totalLoaded, count);
    scanner.close();
  }
   
  @Ignore
  @Test
  public void testFullMapScannerWithDeletes() throws IOException {
    System.out.println("testFullMapScannerWithDeletes ");
    int toDelete = 100000;
    List<byte[]> deletedKeys = delete(toDelete);
    BigSortedMapScanner scanner = map.getScanner(null, null);
    long start = System.currentTimeMillis();
    long count = 0;
    int vallen = ("VALUE" + totalLoaded).length();
    long value = UnsafeAccess.malloc(vallen);
    long prev = 0;
    int prevSize = 0;
    while(scanner.hasNext()) {
      count++;
      int keySize = scanner.keySize();
      int valSize = scanner.valueSize();
      long key = UnsafeAccess.malloc(keySize);
      scanner.key(key, keySize);
      scanner.value(value, vallen);
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
    UnsafeAccess.free(value);
    
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
    byte[] startKey = ("KEY" + startIndex).getBytes();

    BigSortedMapScanner scanner = map.getScanner(startKey, startKey);
    long count = countRows(scanner); 
    scanner.close();
    assertEquals(0, (int) count);
    startIndex = r.nextInt((int)totalLoaded);
    startKey = ("KEY" + startIndex).getBytes();
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
    int startIndex = r.nextInt((int)totalLoaded);
    int stopIndex = r.nextInt((int)totalLoaded - startIndex) + startIndex;
    byte[] key1 = ("KEY" + startIndex).getBytes();
    byte[] key2 = ("KEY" + stopIndex).getBytes();
    byte[] startKey, stopKey;
    if (Utils.compareTo(key1, 0, key1.length, key2, 0, key2.length) > 0) {
      startKey = key2;
      stopKey = key1;
    } else {
      startKey = key1;
      stopKey = key2;
    }
    System.out.println("Start="+ Bytes.toString(startKey) + " stop="+ Bytes.toString(stopKey));
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

  private long countRows(BigSortedMapScanner scanner) throws IOException {
    long start = System.currentTimeMillis();
    long count = 0;
    long prev = 0;
    int prevLen = 0;
    int vallen = ("VALUE"+ totalLoaded).length();
    long value = UnsafeAccess.malloc(vallen);
    
    while(scanner.hasNext()) {
      count++;
      int keySize = scanner.keySize();
      long key = UnsafeAccess.malloc(keySize);
      
      scanner.key(key, keySize);
      scanner.value(value, vallen);
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
    UnsafeAccess.free(value);
    long end = System.currentTimeMillis();
    System.out.println("Scanned "+ count+" in "+ (end- start)+"ms");
    return count;
  }  

  private boolean load(long totalLoaded) {
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
  
  long countRecords() throws IOException {
    BigSortedMapScanner scanner = map.getScanner(null, null);
    long counter = 0;
    while(scanner.hasNext()) {
      counter++;
      scanner.next();
    }
    scanner.close();
    return counter;
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
      UnsafeAccess.free(valPtr);
      UnsafeAccess.free(keyPtr);
    }
  }
}
