/**
 *    Copyright (C) 2021-present Carrot, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 */
package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Ignore;
import org.junit.Test;

public class BigSortedMapScannerTest {

  static {
    //UnsafeAccess.debug = true;
  }
  
  BigSortedMap map;
  long totalLoaded;
  long MAX_ROWS = 1000000;
  
  private void setUp() throws IOException {
    BigSortedMap.setMaxBlockSize(4096);
    map = new BigSortedMap(1000000000);
    totalLoaded = 0;
    long start = System.currentTimeMillis();
    while(totalLoaded < MAX_ROWS) {
      totalLoaded++;
      load(totalLoaded);
    }
    long end = System.currentTimeMillis();
    System.out.println("Time to load= "+ totalLoaded+" ="+(end -start)+"ms");
    start = System.currentTimeMillis();
    long scanned = countRecords();
    end = System.currentTimeMillis();
    System.out.println("Scanned="+ countRecords() + " in "+(end - start) + "ms");
    System.out.println("\nTotal memory      =" + BigSortedMap.getGlobalAllocatedMemory());
    System.out.println("Total   data      =" + BigSortedMap.getGlobalDataSize());
    System.out.println("Compression ratio =" + ((float)BigSortedMap.getGlobalDataSize())/
      BigSortedMap.getGlobalAllocatedMemory()+"\n");
    assertEquals(totalLoaded, scanned);
  }
  
  private void tearDown() {
    map.dispose();
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
    BigSortedMapScanner scanner = map.getScanner(0, 0, 0, 0);
    long counter = 0;
    while(scanner.hasNext()) {
      counter++;
      scanner.next();
    }
    scanner.close();
    return counter;
  }
 
  private void allTests() throws IOException {
    testDirectMemoryAllRangesMapScanner();
    testDirectMemoryAllRangesMapScannerReverse();
    testDirectMemoryFullMapScanner();
    testDirectMemoryAllRangesMapScannerReverse();
    testDirectMemoryFullMapScannerWithDeletes();
    testDirectMemoryFullMapScannerWithDeletesReverse();
    testDirectMemoryScannerSameStartStopRow();
    testDirectMemoryScannerSameStartStopRowReverse();
  
  }
  
  @Test
  public void runAllNoCompression() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    for (int i = 0; i < 1; i++) {
      System.out.println("\n********* " + i+" ********** Codec = NONE\n");
      setUp();
      allTests();
      tearDown();
      BigSortedMap.printGlobalMemoryAllocationStats();
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
      BigSortedMap.printGlobalMemoryAllocationStats();
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
      BigSortedMap.printGlobalMemoryAllocationStats();
      UnsafeAccess.mallocStats();
    }
  }
  
  @Ignore
  @Test  
  public void testDirectMemoryAllRangesMapScanner() throws IOException {
    System.out.println("testDirectMemoryAllRangesMapScanner ");
    Random r = new Random();
    int startIndex = r.nextInt((int)totalLoaded);
    int stopIndex = r.nextInt((int)totalLoaded);
    
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
    
    long startPtr = UnsafeAccess.malloc(startKey.length);
    UnsafeAccess.copy(startKey,  0, startPtr, startKey.length);
    int startLength = startKey.length;
    long stopPtr = UnsafeAccess.malloc(stopKey.length);
    UnsafeAccess.copy(stopKey,  0, stopPtr, stopKey.length);
    int stopLength = stopKey.length;
    
    BigSortedMapScanner scanner = 
        map.getScanner(0, 0, startPtr, startLength);
    long count1 = countRows(scanner);
    if (scanner != null) {
      scanner.close();
    }
    scanner = 
        map.getScanner(startPtr, startLength, stopPtr, stopLength);
    long count2 = countRows(scanner);
    if (scanner != null) {
      scanner.close();
    }
    scanner = 
        map.getScanner(stopPtr, stopLength, 0, 0);
    long count3 = countRows(scanner);
    if (scanner != null) {
      scanner.close();
    }
    assertEquals(totalLoaded, count1 + count2 + count3); 
    UnsafeAccess.free(startPtr);
    UnsafeAccess.free(stopPtr);
  }
  
  @Ignore
  @Test
  public void testDirectMemoryAllRangesMapScannerReverse() throws IOException {
    System.out.println("testDirectMemoryAllRangesMapScannerReverse");
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("testDirectMemoryAllRangesMapScannerReverse seed="+seed);
    int startIndex = r.nextInt((int)totalLoaded);
    int stopIndex = r.nextInt((int)totalLoaded);
    
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
    
    long startPtr = UnsafeAccess.malloc(startKey.length);
    UnsafeAccess.copy(startKey,  0, startPtr, startKey.length);
    int startLength = startKey.length;
    long stopPtr = UnsafeAccess.malloc(stopKey.length);
    UnsafeAccess.copy(stopKey,  0, stopPtr, stopKey.length);
    int stopLength = stopKey.length;
    
    BigSortedMapScanner scanner = 
        map.getScanner(0, 0, startPtr, startLength, false);
    long count1 = countRows(scanner);
    if (scanner != null) {
      scanner.close();
    }    
    scanner = map.getScanner(0, 0, startPtr, startLength, true);
    long count11 = countRowsReverse(scanner);
    assertEquals(count1, count11);
    if ( scanner != null) {
      scanner.close();
    }
    
    scanner = 
        map.getScanner(startPtr, startLength, stopPtr, stopLength, false);
    long count2 = countRows(scanner);
    if (scanner != null) {
      scanner.close();
    }
    
    scanner = 
        map.getScanner(startPtr, startLength, stopPtr, stopLength, true);
    long count22 = countRowsReverse(scanner);
    assertEquals(count2, count22);
    if (scanner != null) {
      scanner.close();
    }
    
    scanner = 
        map.getScanner(stopPtr, stopLength, 0, 0, false);
    long count3 = countRows(scanner);
    if (scanner != null) {
      scanner.close();
    }    
    scanner = 
        map.getScanner(stopPtr, stopLength, 0, 0, true);
    long count33 = countRowsReverse(scanner);
    assertEquals(count3, count33);
    if (scanner != null) {
      scanner.close();
    }
    assertEquals(totalLoaded, count1 + count2 + count3); 
    UnsafeAccess.free(startPtr);
    UnsafeAccess.free(stopPtr);
  }  
 
  @Ignore
  @Test  
  public void testDirectMemoryFullMapScanner() throws IOException {
    System.out.println("testDirectMemoryFullMapScanner ");
    BigSortedMapScanner scanner = map.getScanner(0,0,0,0);
    long start = System.currentTimeMillis();
    long count = countRows(scanner);
    long end = System.currentTimeMillis();
    System.out.println("Scanned "+ count+" in "+ (end- start)+"ms");
    assertEquals(totalLoaded, count);
    scanner.close();
  }
  
 @Ignore
  @Test  
  public void testDirectMemoryFullMapScannerReverse() throws IOException {
    System.out.println("testDirectMemoryFullMapScannerReverse ");
    BigSortedMapScanner scanner = map.getScanner(0,0,0,0, true);
    long start = System.currentTimeMillis();
    long count = countRowsReverse(scanner);
    long end = System.currentTimeMillis();
    System.out.println("Scanned "+ count+" in "+ (end- start)+"ms");
    assertEquals(totalLoaded, count);
    scanner.close();
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
  
  
  @Ignore
  @Test
  public void testDirectMemoryFullMapScannerWithDeletes() throws IOException {
    System.out.println("testDirectMemoryFullMapScannerWithDeletes ");
    int toDelete = 100000;
    List<byte[]> deletedKeys = delete(toDelete);
    BigSortedMapScanner scanner = map.getScanner(0, 0, 0, 0);
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
    scanner.close();
    
    if (prev > 0) {
      UnsafeAccess.free(prev);
    }
    UnsafeAccess.free(value);
    
    long end = System.currentTimeMillis();
    System.out.println("Scanned "+ count+" in "+ (end- start)+"ms");
    assertEquals(totalLoaded - toDelete, count);
    undelete(deletedKeys);

  }
  
  public void testDirectMemoryFullMapScannerWithDeletesReverse() throws IOException {
    System.out.println("testDirectMemoryFullMapScannerWithDeletesReverse ");
    int toDelete = 100000;
    List<byte[]> deletedKeys = delete(toDelete);
    BigSortedMapScanner scanner = map.getScanner(0, 0, 0, 0, true);
    long start = System.currentTimeMillis();
    long count = countRowsReverse(scanner);    
    long end = System.currentTimeMillis();
    System.out.println("Scanned "+ count+" in "+ (end- start)+"ms");
    scanner.close();
    assertEquals(totalLoaded - toDelete, count);
    
    undelete(deletedKeys);

  }
  @Ignore
  @Test
  public void testDirectMemoryScannerSameStartStopRow () throws IOException
  {
    System.out.println("testDirectMemoryScannerSameStartStopRow");
    Random r = new Random();
    int startIndex = r.nextInt((int)totalLoaded);
    byte[] startKey = ("KEY" + startIndex).getBytes();
    int length = startKey.length;
    long ptr = UnsafeAccess.malloc(length);
    UnsafeAccess.copy(startKey,  0, ptr, length);
    BigSortedMapScanner scanner = map.getScanner(ptr, length, ptr, length);
    long count = countRows(scanner); 
    if (scanner != null) {
      scanner.close();
    }
    assertEquals(0, (int) count);
    
    startIndex = r.nextInt((int)totalLoaded);
    startKey = ("KEY" + startIndex).getBytes();
    length = startKey.length;
    UnsafeAccess.free(ptr);
    
    ptr = UnsafeAccess.malloc(length);
    UnsafeAccess.copy(startKey,  0, ptr, length);
    scanner = map.getScanner(ptr, length, ptr, length);    
    count = countRows(scanner); 
    if (scanner != null) {
      scanner.close();
    }
    UnsafeAccess.free(ptr);

    assertEquals(0, (int) count);
  }

  @Ignore
  @Test
  public void testDirectMemoryScannerSameStartStopRowReverse () throws IOException
  {
    System.out.println("testDirectMemoryScannerSameStartStopRowReverse");
    Random r = new Random();
    int startIndex = r.nextInt((int)totalLoaded);
    byte[] startKey = ("KEY" + startIndex).getBytes();
    int length = startKey.length;
    long ptr = UnsafeAccess.malloc(length);
    UnsafeAccess.copy(startKey,  0, ptr, length);
    BigSortedMapScanner scanner = map.getScanner(ptr, length, ptr, length, true);
    long count = countRowsReverse(scanner); 
    if (scanner != null) {
      scanner.close();
    }
    assertEquals(0, (int) count);
    
    startIndex = r.nextInt((int)totalLoaded);
    startKey = ("KEY" + startIndex).getBytes();
    length = startKey.length;
    UnsafeAccess.free(ptr);
    
    ptr = UnsafeAccess.malloc(length);
    UnsafeAccess.copy(startKey,  0, ptr, length);
    scanner = map.getScanner(ptr, length, ptr, length, true);    
    count = countRowsReverse(scanner); 
    if (scanner != null) {
      scanner.close();
    }
    UnsafeAccess.free(ptr);

    assertEquals(0, (int) count);
  }
  
  private long countRows(BigSortedMapScanner scanner) throws IOException {
    if (scanner == null) return 0;
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
      prevLen = keySize;
      scanner.next();
    }   
    if (prev != 0) {
      UnsafeAccess.free(prev);
    }
    UnsafeAccess.free(value);
    long end = System.currentTimeMillis();
    System.out.println("Scanned direct "+ count+" in "+ (end- start)+"ms");
    return count;
  }
  
  
  private long countRowsReverse(BigSortedMapScanner scanner) throws IOException {
    if (scanner == null) return 0;
    long start = System.currentTimeMillis();
    long count = 0;
    long prev = 0;
    int prevLen = 0;
    int vallen = ("VALUE"+ totalLoaded).length();
    long value = UnsafeAccess.malloc(vallen);
    
    do {
      count++;
      int keySize = scanner.keySize();
      long key = UnsafeAccess.malloc(keySize);
      
      scanner.key(key, keySize);
      scanner.value(value, vallen);
      if (prev != 0) {
        assertTrue (Utils.compareTo(prev,  prevLen, key, keySize) > 0);
        UnsafeAccess.free(prev);
      }
      prev = key;
      prevLen = keySize;
    } while (scanner.previous());  
    if (prev != 0) {
      UnsafeAccess.free(prev);
    }
    UnsafeAccess.free(value);
    long end = System.currentTimeMillis();
    System.out.println("Scanned reversed "+ count+" in "+ (end- start)+"ms");
    return count;
  }

}
