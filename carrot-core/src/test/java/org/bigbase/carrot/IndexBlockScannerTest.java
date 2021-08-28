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
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.Key;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexBlockScannerTest {
  Logger LOG = LoggerFactory.getLogger(IndexBlockScannerTest.class);
  
  static {
    //UnsafeAccess.debug = true;
  }
  
  @Test
  public void testAll() throws IOException{
    for (int i=0; i < 1; i++) {
      System.out.println("\nRUN "+ i+"\n");
      testFullScan();
      testFullScanWithCompressionLZ4();
      testFullScanWithCompressionLZ4HC();
      testFullScanReverse();
      testFullScanReverseWithCompressionLZ4();
      testFullScanReverseWithCompressionLZ4HC();
      testOpenEndScan();
      testOpenEndScanWithCompressionLZ4();
      testOpenEndScanWithCompressionLZ4HC();
      testOpenEndScanReverse();
      testOpenEndScanReverseWithCompressionLZ4();
      testOpenEndScanReverseWithCompressionLZ4HC();
      testOpenStartScan();
      testOpenStartScanWithCompressionLZ4();
      testOpenStartScanWithCompressionLZ4HC();
      testOpenStartScanReverse();
      testOpenStartScanReverseWithCompressionLZ4();
      testOpenStartScanReverseWithCompressionLZ4HC();
      testSubScan();
      testSubScanWithCompressionLZ4();
      testSubScanWithCompressionLZ4HC();
      testSubScanReverse();
      testSubScanReverseWithCompressionLZ4();
      testSubScanReverseWithCompressionLZ4HC();
    }
    
    BigSortedMap.printGlobalMemoryAllocationStats();
    UnsafeAccess.mallocStats();
    
  }
  
  @Ignore
  @Test
  public void testFullScan() throws IOException {
    System.out.println("testFullScan");  
    IndexBlock ib = getIndexBlock(4096);
    List<Key> keys = fillIndexBlock(ib);
    Utils.sortKeys(keys);
    System.out.println("Loaded "+ keys.size()+" kvs");
    IndexBlockScanner scanner = 
        IndexBlockScanner.getScanner(ib, 0, 0, 0, 0, Long.MAX_VALUE);
    verifyScanner(scanner, keys);
    scanner.close();
    dispose(keys);
    ib.free();
  }
  
  @Ignore
  @Test
  public void testFullScanWithCompressionLZ4() throws IOException {
    System.out.println("testFullScanWithCompressionLZ4");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    testFullScan();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testFullScanWithCompressionLZ4HC() throws IOException {
    System.out.println("testFullScanWithCompressionLZ4HC");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    testFullScan();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testFullScanReverse() throws IOException {
    System.out.println("testFullScanReverse");  
    IndexBlock ib = getIndexBlock(4096);
    List<Key> keys = fillIndexBlock(ib);
    Utils.sortKeys(keys);
    System.out.println("Loaded "+ keys.size()+" kvs");
    // This creates reverse scanner
    IndexBlockScanner scanner = 
        IndexBlockScanner.getScanner(ib, 0, 0, 0, 0, Long.MAX_VALUE, null, true);
    verifyScannerReverse(scanner, keys);
    if (scanner != null) {
      scanner.close();
    }
    dispose(keys);
    ib.free(); 
  }
  
  @Ignore
  @Test
  public void testFullScanReverseWithCompressionLZ4() throws IOException {
    System.out.println("testFullScanReverseWithCompressionLZ4");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    testFullScanReverse();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testFullScanReverseWithCompressionLZ4HC() throws IOException {
    System.out.println("testFullScanReverseWithCompressionLZ4HC");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    testFullScanReverse();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  private void dispose(List<Key> keys) {
    for(Key key: keys) {
      UnsafeAccess.free(key.address);
    }
  }

  @Ignore
  @Test
  public void testOpenStartScan() throws IOException {
    System.out.println("testOpenStartScan");  
    IndexBlock ib = getIndexBlock(4096);
    List<Key> keys = fillIndexBlock(ib);
    Utils.sortKeys(keys);
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("seed="+seed);
    int stopRowIndex = r.nextInt(keys.size());
    Key stopRow = keys.get(stopRowIndex);
    System.out.println("Loaded "+ keys.size()+" kvs");
    List<Key> newkeys = keys.subList(0, stopRowIndex);
    System.out.println("Selected "+ newkeys.size()+" kvs");
    IndexBlockScanner scanner = 
        IndexBlockScanner.getScanner(ib, 0, 0, stopRow.address, stopRow.length, Long.MAX_VALUE);
    verifyScanner(scanner, newkeys);
    scanner.close();
    dispose(keys);
    ib.free();
  }
  
  @Ignore
  @Test
  public void testOpenStartScanWithCompressionLZ4() throws IOException {
    System.out.println("testOpenStartScanWithCompressionLZ4");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    testOpenStartScan();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testOpenStartScanWithCompressionLZ4HC() throws IOException {
    System.out.println("testOpenStartScanWithCompressionLZ4HC");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    testOpenStartScan();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  
  @Ignore
  @Test
  public void testOpenStartScanReverse() throws IOException {
    System.out.println("testOpenStartScanReverse");  
    IndexBlock ib = getIndexBlock(4096);
    List<Key> keys = fillIndexBlock(ib);
    Utils.sortKeys(keys);
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("testOpenStartScan seed="+ seed);    
    int stopRowIndex = r.nextInt(keys.size());
    Key stopRow = keys.get(stopRowIndex);
    System.out.println("Loaded "+ keys.size()+" kvs");
    List<Key> newkeys = keys.subList(0, stopRowIndex);
    System.out.println("Selected "+ newkeys.size()+" kvs");
    IndexBlockScanner scanner = 
        IndexBlockScanner.getScanner(ib, 0, 0, stopRow.address, 
          stopRow.length, Long.MAX_VALUE, null, true);
    verifyScannerReverse(scanner, newkeys);
    if (scanner != null) {
      scanner.close();
    }
    dispose(keys);
    ib.free();
  }
  
  @Ignore
  @Test
  public void testOpenStartScanReverseWithCompressionLZ4() throws IOException {
    System.out.println("testOpenStartScanReverseWithCompressionLZ4");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    testOpenStartScanReverse();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testOpenStartScanReverseWithCompressionLZ4HC() throws IOException {
    System.out.println("testOpenStartScanReverseWithCompressionLZ4HC");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    testOpenStartScanReverse();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  
  @Ignore
  @Test
  public void testOpenEndScan() throws IOException {
    System.out.println("testOpenEndScan");  
    IndexBlock ib = getIndexBlock(4096);
    List<Key> keys = fillIndexBlock(ib);
    Utils.sortKeys(keys);
    Random r = new Random();
    int startRowIndex = r.nextInt(keys.size());
    Key startRow = keys.get(startRowIndex);
    System.out.println("Loaded "+ keys.size()+" kvs");
    List<Key> newkeys = keys.subList(startRowIndex, keys.size());
    System.out.println("Selected "+ newkeys.size()+" kvs");
    IndexBlockScanner scanner = 
        IndexBlockScanner.getScanner(ib, startRow.address, 
          startRow.length,0 , 0, Long.MAX_VALUE);
    verifyScanner(scanner, newkeys);
    scanner.close();
    dispose(keys);
    ib.free();
  }
  
  @Ignore
  @Test
  public void testOpenEndScanWithCompressionLZ4() throws IOException {
    System.out.println("testOpenEndScanWithCompressionLZ4");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    testOpenEndScan();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testOpenEndScanWithCompressionLZ4HC() throws IOException {
    System.out.println("testOpenEndScanWithCompressionLZ4HC");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    testOpenEndScan();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  
  @Ignore
  @Test
  public void testOpenEndScanReverse() throws IOException {
    System.out.println("testOpenEndScanReverse");  
    IndexBlock ib = getIndexBlock(4096);
    List<Key> keys = fillIndexBlock(ib);
    Utils.sortKeys(keys);
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("testOpenEndScanReverse seed="+ seed);    

    int startRowIndex = r.nextInt(keys.size());
    Key startRow = keys.get(startRowIndex);
    System.out.println("Loaded "+ keys.size()+" kvs");
    List<Key> newkeys = keys.subList(startRowIndex, keys.size());
    System.out.println("Selected "+ newkeys.size()+" kvs");
    IndexBlockScanner scanner = 
        IndexBlockScanner.getScanner(ib, startRow.address, startRow.length,0 , 0, 
          Long.MAX_VALUE, null, true);
    verifyScannerReverse(scanner, newkeys);
    if (scanner != null) {
      scanner.close();
    }
    dispose(keys);
    ib.free();
  }
  
  @Ignore
  @Test
  public void testOpenEndScanReverseWithCompressionLZ4() throws IOException {
    System.out.println("testOpenEndScanReverseWithCompressionLZ4");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    testOpenEndScanReverse();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testOpenEndScanReverseWithCompressionLZ4HC() throws IOException {
    System.out.println("testOpenEndScanReverseWithCompressionLZ4HC");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    testOpenEndScan();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testSubScan() throws IOException {
    System.out.println("testSubScan");  
    IndexBlock ib = getIndexBlock(4096);
    List<Key> keys = fillIndexBlock(ib);
    Utils.sortKeys(keys);
    Random r = new Random();
    int startRowIndex = r.nextInt(keys.size());
    int stopRowIndex = r.nextInt(keys.size());
    int tmp = startRowIndex;
    if (startRowIndex > stopRowIndex) {
      startRowIndex = stopRowIndex;
      stopRowIndex = tmp;
    }
    Key startRow = keys.get(startRowIndex);
    Key stopRow = keys.get(stopRowIndex);
    System.out.println("Loaded "+ keys.size()+" kvs");
    List<Key> newkeys = keys.subList(startRowIndex, stopRowIndex);
    System.out.println("Selected "+ newkeys.size()+" kvs");
    IndexBlockScanner scanner = 
        IndexBlockScanner.getScanner(ib, startRow.address, startRow.length, 
          stopRow.address, stopRow.length, Long.MAX_VALUE);
    verifyScanner(scanner, newkeys);
    scanner.close();
    dispose(keys);
    ib.free();
  }
  
  @Ignore
  @Test
  public void testSubScanWithCompressionLZ4() throws IOException {
    System.out.println("testSubScanWithCompressionLZ4");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    testSubScan();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testSubScanWithCompressionLZ4HC() throws IOException {
    System.out.println("testSubScanWithCompressionLZ4HC");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    testSubScan();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testSubScanReverse() throws IOException {
    System.out.println("testSubScanReverse");  
    IndexBlock ib = getIndexBlock(4096);
    List<Key> keys = fillIndexBlock(ib);
    Utils.sortKeys(keys);
    
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("testSubScanReverse seed="+ seed);    
    
    int startRowIndex = r.nextInt(keys.size());
    int stopRowIndex = r.nextInt(keys.size());
    int tmp = startRowIndex;
    if (startRowIndex > stopRowIndex) {
      startRowIndex = stopRowIndex;
      stopRowIndex = tmp;
    }

    Key startRow = keys.get(startRowIndex);
    Key stopRow = keys.get(stopRowIndex);
    System.out.println("Loaded "+ keys.size()+" kvs");
    List<Key> newkeys = keys.subList(startRowIndex, stopRowIndex);
    System.out.println("Selected "+ keys.size()+" kvs");
    IndexBlockScanner scanner = 
        IndexBlockScanner.getScanner(ib, startRow.address, startRow.length, 
          stopRow.address, stopRow.length, Long.MAX_VALUE, null, true);
    verifyScannerReverse(scanner, newkeys);
    if (scanner != null) {
      scanner.close();
    }
    dispose(keys);
    ib.free();
  }
  
  @Ignore
  @Test
  public void testSubScanReverseWithCompressionLZ4() throws IOException {
    System.out.println("testSubScanReverseWithCompressionLZ4");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    testSubScanReverse();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testSubScanReverseWithCompressionLZ4HC() throws IOException {
    System.out.println("testSubScanReverseWithCompressionLZ4HC");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    testSubScanReverse();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  
  private void verifyScanner(IndexBlockScanner scanner, List<Key> keys) {
    int count = 0;
    DataBlockScanner dbscn = null;
    
    while ((dbscn = scanner.nextBlockScanner()) != null){
      while(dbscn.hasNext()) {
        count++;
        Key key = keys.get(count-1);
        int keySize = dbscn.keySize();
        int valSize = dbscn.valueSize();
        //System.out.println("expected size="+ key.length +" actual="+ keySize);
        assertEquals(key.length, keySize);
        assertEquals(key.length, valSize);
        byte[] buf = new byte[keySize];
        dbscn.key(buf, 0);
        assertTrue(Utils.compareTo(buf, 0, keySize, key.address, key.length) == 0);
        dbscn.value(buf, 0);
        assertTrue(Utils.compareTo(buf, 0, valSize, key.address, key.length) == 0);
        dbscn.next();
      }
    } 
    assertEquals(keys.size(), count);
  }
  
  private void verifyScannerReverse(IndexBlockScanner scanner, List<Key> keys) 
      throws IOException {
    if (scanner == null) {
      assertEquals(0, keys.size());
      return;
    }
    int count = 0;
    DataBlockScanner dbscn= scanner.lastBlockScanner();
    if (dbscn == null) {
      assertEquals(0, keys.size());
      return;
    }
    Collections.reverse(keys);
    do {
      do {
        count++;
        Key key = keys.get(count-1);
        int keySize = dbscn.keySize();
        int valSize = dbscn.valueSize();
        assertEquals(key.length, keySize);
        assertEquals(key.length, valSize);
        byte[] buf = new byte[keySize];
        dbscn.key(buf, 0);
        assertTrue(Utils.compareTo(buf, 0, keySize, key.address, key.length) == 0);
        dbscn.value(buf, 0);
        assertTrue(Utils.compareTo(buf, 0, valSize, key.address, key.length) == 0);
      } while(dbscn.previous());
      dbscn.close();
    } while((dbscn = scanner.previousBlockScanner()) != null);
    
    assertEquals(keys.size(), count);
  }
  
  private IndexBlock getIndexBlock(int size) {
    IndexBlock ib = new IndexBlock(null, size);
    ib.setFirstIndexBlock();
    return ib;
  }
  
  protected List<Key> fillIndexBlock (IndexBlock b) throws RetryOperationException {
    ArrayList<Key> keys = new ArrayList<Key>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("FILL seed="+ seed);
    int kvSize = 32;
    boolean result = true;
    while(result == true) {
      byte[] key = new byte[kvSize];
      r.nextBytes(key);
      long ptr = UnsafeAccess.malloc(kvSize);
      UnsafeAccess.copy(key,  0,  ptr, kvSize);
      result = b.put(ptr, kvSize, ptr, kvSize, 0, 0);
      if(result) {
        keys.add( new Key(ptr, kvSize));
      } else {
        UnsafeAccess.free(ptr);
      }
    }
    System.out.println("Number of data blocks="+b.getNumberOfDataBlock() + " "  + " index block data size =" + 
        b.getDataInBlockSize()+" num records=" + keys.size());
    return keys;
  }
  
}
