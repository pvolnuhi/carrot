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

public class DataBlockScannerTest {
  Logger LOG = LoggerFactory.getLogger(DataBlockScannerTest.class);
  
  protected DataBlock getDataBlock() {
    IndexBlock ib = new IndexBlock(4096);
    ib.setFirstIndexBlock();
    ib.firstBlock();
    return ib.firstBlock();
  } 
  
  @Test
  public void testFullScan() throws IOException {
    System.out.println("testFullScan");  
    DataBlock ib = getDataBlock();
    List<Key> keys = fillDataBlock(ib);
    Utils.sortKeys(keys);
    System.out.println("Loaded "+ keys.size()+" kvs");
    DataBlockScanner scanner = 
        DataBlockScanner.getScanner(ib, 0, 0, 0, 0, Long.MAX_VALUE);
    // Skip first system key
    scanner.next();
    verifyScanner(scanner, keys);
    scanner.close();
    dispose(keys);
  }
  
  
  @Test
  public void testFullScanCompressionDecompression() throws IOException {
    System.out.println("testFullScanCompressionDecompression");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));

    DataBlock ib = getDataBlock();
    List<Key> keys = fillDataBlock(ib);
    Utils.sortKeys(keys);
    System.out.println("Loaded "+ keys.size()+" kvs");
    ib.compressDataBlockIfNeeded();
    ib.decompressDataBlockIfNeeded();
    DataBlockScanner scanner = 
        DataBlockScanner.getScanner(ib, 0, 0, 0, 0, Long.MAX_VALUE);
    // Skip first system key
    scanner.next();
    verifyScanner(scanner, keys);
    scanner.close();
    dispose(keys);
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));

  }
  
  @Ignore
  @Test
  public void testReverseAll() throws IOException {
    for (int i=0; i < 100000; i++) {
      System.out.println("\n i="+i+"\n");
      testFullScanReverse();
      testOpenEndScanReverse();
      testOpenStartScanReverse();
      testSubScanReverse();
    }
  }
  
  @Test
  public void testFullScanReverseCompressionDecompression() throws IOException {
    System.out.println("testFullScanReverseCompressionDecompression");  
    // Enable data  block compression
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));

    DataBlock ib = getDataBlock();
    List<Key> keys = fillDataBlock(ib);
    Utils.sortKeys(keys);
    System.out.println("Loaded "+ keys.size()+" kvs");
    ib.compressDataBlockIfNeeded();
    ib.decompressDataBlockIfNeeded();
    DataBlockScanner scanner = 
        DataBlockScanner.getScanner(ib, 0, 0, 0, 0, Long.MAX_VALUE);
    // Skip first system key
    scanner.last();
    verifyScannerReverse(scanner, keys);
    scanner.close();
    dispose(keys);
    // Disable data  block compression
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Test
  public void testFullScanReverse() throws IOException {
    System.out.println("testFullScanReverse");  
    DataBlock ib = getDataBlock();
    List<Key> keys = fillDataBlock(ib);
    Utils.sortKeys(keys);
    System.out.println("Loaded "+ keys.size()+" kvs");
    DataBlockScanner scanner = 
        DataBlockScanner.getScanner(ib, 0, 0, 0, 0, Long.MAX_VALUE);
    // Skip first system key
    scanner.last();
    verifyScannerReverse(scanner, keys);
    scanner.close();
    dispose(keys);
  }
  
  private void dispose(List<Key> keys) {
    for(Key key: keys) {
      UnsafeAccess.free(key.address);
    }
  }
  
  @Test
  public void testOpenStartScan() throws IOException {
    System.out.println("testOpenStartScan");  
    DataBlock ib = getDataBlock();
    List<Key> keys = fillDataBlock(ib);
    Utils.sortKeys(keys);
    Random r = new Random();
    int stopRowIndex = r.nextInt(keys.size());
    Key stopRow = keys.get(stopRowIndex);
    System.out.println("Loaded "+ keys.size()+" kvs");
    keys = keys.subList(0, stopRowIndex);
    System.out.println("Selected "+ keys.size()+" kvs");
    DataBlockScanner scanner = 
        DataBlockScanner.getScanner(ib, 0, 0, stopRow.address, stopRow.length, Long.MAX_VALUE);
    // Skip first system key
    scanner.next();
    verifyScanner(scanner, keys);
    scanner.close();
    dispose(keys);

  }
  
  @Test
  public void testOpenStartScanReverse() throws IOException {
    System.out.println("testOpenStartScanReverse");  
    DataBlock ib = getDataBlock();
    List<Key> keys = fillDataBlock(ib);
    Utils.sortKeys(keys);
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("testOpenStartScanReverse seed="+ seed); 
    int stopRowIndex = r.nextInt(keys.size());
    Key stopRow = keys.get(stopRowIndex);
    System.out.println("Loaded "+ keys.size()+" kvs");
    keys = keys.subList(0, stopRowIndex);
    System.out.println("Selected "+ keys.size()+" kvs");
    if (keys.size() == 0) {
      System.out.println();
    }
    DataBlockScanner scanner = 
        DataBlockScanner.getScanner(ib, 0, 0, stopRow.address, stopRow.length, Long.MAX_VALUE);
    if (scanner != null) {
      if (scanner.last()) {
        verifyScannerReverse(scanner, keys);
      } else {
        assertEquals(0, keys.size());
      }
      scanner.close();
    } else {
      assertEquals(0, keys.size());
    }
    dispose(keys);

  }
  
  @Test
  public void testOpenEndScan() throws IOException {
    System.out.println("testOpenEndScan");  
    DataBlock ib = getDataBlock();
    List<Key> keys = fillDataBlock(ib);
    Utils.sortKeys(keys);
    Random r = new Random();
    int startRowIndex = r.nextInt(keys.size());
    Key startRow = keys.get(startRowIndex);
    System.out.println("Loaded "+ keys.size()+" kvs");
    keys = keys.subList(startRowIndex, keys.size());
    System.out.println("Selected "+ keys.size()+" kvs");
    DataBlockScanner scanner = 
        DataBlockScanner.getScanner(ib, startRow.address, startRow.length, 0, 0, Long.MAX_VALUE);
    verifyScanner(scanner, keys);
    scanner.close();
    dispose(keys);

  }
  
  
  @Test
  public void testOpenEndScanReverse() throws IOException {
    System.out.println("testOpenEndScanReverse");  
    DataBlock ib = getDataBlock();
    List<Key> keys = fillDataBlock(ib);
    Utils.sortKeys(keys);
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("testOpenEndScanReverse seed="+ seed); 
    int startRowIndex = r.nextInt(keys.size());
    Key startRow = keys.get(startRowIndex);
    System.out.println("Loaded "+ keys.size()+" kvs");
    keys = keys.subList(startRowIndex, keys.size());
    System.out.println("Selected "+ keys.size()+" kvs");
    DataBlockScanner scanner = 
        DataBlockScanner.getScanner(ib, startRow.address, startRow.length, 0, 0, Long.MAX_VALUE);
    if (scanner != null) {
      if (scanner.last()) {
        verifyScannerReverse(scanner, keys);
      } else {
        assertEquals(0, keys.size());
      }
      scanner.close();
    } else {
      assertEquals(0, keys.size());
    }
    dispose(keys);

  }
  @Test
  public void testSubScan() throws IOException {
    System.out.println("testSubScan");  
    DataBlock ib = getDataBlock();
    List<Key> keys = fillDataBlock(ib);
    Utils.sortKeys(keys);
    Random r = new Random();
    int startRowIndex = r.nextInt(keys.size());
    int stopRowIndex = r.nextInt(keys.size() - startRowIndex) +1 + startRowIndex;
    if (stopRowIndex >= keys.size()) {
      stopRowIndex = keys.size() -1;
    }
    Key startRow = keys.get(startRowIndex);
    Key stopRow = keys.get(stopRowIndex);
    System.out.println("Loaded "+ keys.size()+" kvs");
    keys = keys.subList(startRowIndex, stopRowIndex);
    System.out.println("Selected "+ keys.size()+" kvs");
    DataBlockScanner scanner = 
        DataBlockScanner.getScanner(ib, startRow.address, startRow.length, 
          stopRow.address, stopRow.length, Long.MAX_VALUE);
    if (scanner != null) { 
      verifyScanner(scanner, keys);
      scanner.close();
    } else {
      assertEquals(0, keys.size());
    }
    dispose(keys);
  }
  
  @Test
  public void testSubScanReverse() throws IOException {
    System.out.println("testSubScanReverse");  
    DataBlock ib = getDataBlock();
    List<Key> keys = fillDataBlock(ib);
    Utils.sortKeys(keys);
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("testSubScanReverse seed="+ seed); 
    int startRowIndex = r.nextInt(keys.size());
    int stopRowIndex = r.nextInt(keys.size() - startRowIndex) +1 + startRowIndex;
    if (stopRowIndex >= keys.size()) {
      stopRowIndex = keys.size() -1;
    }
    Key startRow = keys.get(startRowIndex);
    Key stopRow = keys.get(stopRowIndex);
    System.out.println("Loaded "+ keys.size()+" kvs");
    keys = keys.subList(startRowIndex, stopRowIndex);

    System.out.println("Selected "+ keys.size()+" kvs");
    // When start and stop rows are equals
    // scanner must be null
    DataBlockScanner scanner = 
        DataBlockScanner.getScanner(ib, startRow.address, startRow.length, 
          stopRow.address, stopRow.length, Long.MAX_VALUE);
    if (scanner != null) {
      if (scanner.last()) {
        verifyScannerReverse(scanner, keys);
      } else {
        assertEquals(0, keys.size());
      }
      scanner.close();
    } else {
      assertEquals(0, keys.size());
    }
    dispose(keys);
  }
  
  private void verifyScanner(DataBlockScanner scanner, List<Key> keys) {
    int count = 0;
    
      while(scanner.hasNext()) {
        count++;
        Key key = keys.get(count-1);
        int keySize = scanner.keySize();
        int valSize = scanner.valueSize();
        assertEquals(key.length, keySize);
        assertEquals(key.length, valSize);
        byte[] buf = new byte[keySize];
        scanner.key(buf, 0);
        assertTrue(Utils.compareTo(buf, 0, buf.length, key.address, key.length) == 0);
        scanner.value(buf, 0);
        assertTrue(Utils.compareTo(buf, 0, buf.length, key.address, key.length) == 0);
        scanner.next();
      } 
     
    assertEquals(keys.size(), count);
  }
  
  private void verifyScannerReverse(DataBlockScanner scanner, List<Key> keys) {
    int count = 0;
    
    Collections.reverse(keys);
      do {
        count++;
        Key key = keys.get(count-1);
        int keySize = scanner.keySize();
        int valSize = scanner.valueSize();
        assertEquals(key.length, keySize);
        assertEquals(key.length, valSize);
        byte[] buf = new byte[keySize];
        scanner.key(buf, 0);
        assertTrue(Utils.compareTo(buf, 0, buf.length, key.address, key.length) == 0);
        scanner.value(buf, 0);
        assertTrue(Utils.compareTo(buf, 0, buf.length, key.address, key.length) == 0);
      } while(scanner.previous());
      
      Collections.reverse(keys);
 
    assertEquals(keys.size(), count);
  }
  
  protected ArrayList<Key> fillDataBlock (DataBlock b) throws RetryOperationException {
    ArrayList<Key> keys = new ArrayList<Key>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Fill seed="+ seed);
    int length = 32;
    boolean result = true;
    while(result == true) {
      byte[] key = new byte[length];
      r.nextBytes(key);
      long ptr = UnsafeAccess.malloc(length);
      UnsafeAccess.copy(key, 0, ptr, length);
      result = b.put(ptr,  length, ptr, length, 0, 0);
      if(result) {
        keys.add( new Key(ptr, length));
      }
    }
    System.out.println(b.getNumberOfRecords() + " " + b.getNumberOfDeletedAndUpdatedRecords() + " " + b.getDataInBlockSize());
    return keys;
  }
  
}
