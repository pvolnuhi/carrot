package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

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
  public void testAll() throws IOException {
    for (int i=0; i < 10000; i++) {
      testFullScan();
      testFullScanReverse();
      testOpenEndScan();
      testOpenEndScanReverse();
      testOpenStartScan();
      testOpenStartScanReverse();
      testSubScan();
      testSubScanReverse();
    }
  }
  
  @Ignore
  @Test
  public void testFullScan() throws IOException {
    System.out.println("testFullScan");  
    DataBlock ib = getDataBlock();
    List<byte[]> keys = fillDataBlock(ib);
    Utils.sort(keys);
    System.out.println("Loaded "+ keys.size()+" kvs");
    DataBlockScanner scanner = DataBlockScanner.getScanner(ib, null, null, Long.MAX_VALUE);
    // Skip first system key
    scanner.next();
    verifyScanner(scanner, keys);
    scanner.close();
  }
  @Ignore
  @Test
  public void testFullScanReverse() throws IOException {
    System.out.println("testFullScanReverse");  
    DataBlock ib = getDataBlock();
    List<byte[]> keys = fillDataBlock(ib);
    Utils.sort(keys);
    System.out.println("Loaded "+ keys.size()+" kvs");
    //for (byte[] key: keys) {
    //  System.out.println(key.length);
    //}
    DataBlockScanner scanner = DataBlockScanner.getScanner(ib, null, null, Long.MAX_VALUE);
    // Skip first system key
    scanner.last();
    verifyScannerReverse(scanner, keys);
    scanner.close();
  }
  
  @Ignore
  @Test
  public void testOpenStartScan() throws IOException {
    System.out.println("testOpenStartScan");  
    DataBlock ib = getDataBlock();
    List<byte[]> keys = fillDataBlock(ib);
    Utils.sort(keys);
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    /*DEBUG*/ System.out.println("seed =" + seed);
    int stopRowIndex = r.nextInt(keys.size());
    byte[] stopRow = keys.get(stopRowIndex);
    System.out.println("Loaded "+ keys.size()+" kvs");
    keys = keys.subList(0, stopRowIndex);
    System.out.println("Selected "+ keys.size()+" kvs");
    DataBlockScanner scanner = DataBlockScanner.getScanner(ib, null, stopRow, Long.MAX_VALUE);
    // Skip first system key
    scanner.next();
    verifyScanner(scanner, keys);
    scanner.close();
  }
  
  @Ignore
  @Test
  public void testOpenStartScanReverse() throws IOException {
    System.out.println("testOpenStartScan");  
    DataBlock ib = getDataBlock();
    List<byte[]> keys = fillDataBlock(ib);
    Utils.sort(keys);
    Random r = new Random();
    int stopRowIndex = r.nextInt(keys.size());
    byte[] stopRow = keys.get(stopRowIndex);
    System.out.println("Loaded "+ keys.size()+" kvs");
    keys = keys.subList(0, stopRowIndex);
    System.out.println("Selected "+ keys.size()+" kvs");
    DataBlockScanner scanner = DataBlockScanner.getScanner(ib, null, stopRow, Long.MAX_VALUE);
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
  }
  
  @Ignore
  @Test
  public void testOpenEndScan() throws IOException {
    System.out.println("testOpenEndScan");  
    DataBlock ib = getDataBlock();
    List<byte[]> keys = fillDataBlock(ib);
    Utils.sort(keys);
    Random r = new Random();
    int startRowIndex = r.nextInt(keys.size());
    byte[] startRow = keys.get(startRowIndex);
    System.out.println("Loaded "+ keys.size()+" kvs");
    keys = keys.subList(startRowIndex, keys.size());
    System.out.println("Selected "+ keys.size()+" kvs");
    DataBlockScanner scanner = DataBlockScanner.getScanner(ib, startRow, null, Long.MAX_VALUE);
    verifyScanner(scanner, keys);
    scanner.close();
  }
  
  @Ignore
  @Test
  public void testOpenEndScanReverse() throws IOException {
    System.out.println("testOpenEndScanReverse");  
    DataBlock ib = getDataBlock();
    List<byte[]> keys = fillDataBlock(ib);
    Utils.sort(keys);
    Random r = new Random();
    int startRowIndex = r.nextInt(keys.size());
    byte[] startRow = keys.get(startRowIndex);
    System.out.println("Loaded "+ keys.size()+" kvs");
    keys = keys.subList(startRowIndex, keys.size());
    System.out.println("Selected "+ keys.size()+" kvs");
    DataBlockScanner scanner = DataBlockScanner.getScanner(ib, startRow, null, Long.MAX_VALUE);
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
  }
  
  @Ignore
  @Test
  public void testSubScan() throws IOException {
    System.out.println("testSubScan");  
    DataBlock ib = getDataBlock();
    List<byte[]> keys = fillDataBlock(ib);
    Utils.sort(keys);
    Random r = new Random();
    int startRowIndex = r.nextInt(keys.size());
    int stopRowIndex = r.nextInt(keys.size() - startRowIndex) +1 + startRowIndex;
    if (stopRowIndex >= keys.size()) {
      stopRowIndex = keys.size() -1;
    }
    byte[] startRow = keys.get(startRowIndex);
    byte[] stopRow = keys.get(stopRowIndex);
    System.out.println("Loaded "+ keys.size()+" kvs");
    keys = keys.subList(startRowIndex, stopRowIndex);
    System.out.println("Selected "+ keys.size()+" kvs");
    DataBlockScanner scanner = DataBlockScanner.getScanner(ib, startRow, stopRow, Long.MAX_VALUE);
    if (scanner != null) { 
      verifyScanner(scanner, keys);
      scanner.close();
    } else {
      assertEquals(0, keys.size());
    }
  }
  
  @Ignore
  @Test
  public void testSubScanReverse() throws IOException {
    System.out.println("testSubScanReverse");  
    DataBlock ib = getDataBlock();
    List<byte[]> keys = fillDataBlock(ib);
    Utils.sort(keys);
    Random r = new Random();
    int startRowIndex = r.nextInt(keys.size());
    int stopRowIndex = r.nextInt(keys.size() - startRowIndex) +1 + startRowIndex;
    if (stopRowIndex >= keys.size()) {
      stopRowIndex = keys.size() -1;
    }
    byte[] startRow = keys.get(startRowIndex);
    byte[] stopRow = keys.get(stopRowIndex);
    System.out.println("Loaded "+ keys.size()+" kvs");
    keys = keys.subList(startRowIndex, stopRowIndex);
    System.out.println("Selected "+ keys.size()+" kvs");
    DataBlockScanner scanner = DataBlockScanner.getScanner(ib, startRow, stopRow, Long.MAX_VALUE);
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
  }
  
  private void verifyScanner(DataBlockScanner scanner, List<byte[]> keys) {
    int count = 0;
    
      while(scanner.hasNext()) {
        count++;
        byte[] key = keys.get(count-1);
        int keySize = scanner.keySize();
        int valSize = scanner.valueSize();
        assertEquals(key.length, keySize);
        assertEquals(key.length, valSize);
        byte[] buf = new byte[keySize];
        scanner.key(buf, 0);
        assertTrue(Utils.compareTo(key, 0, key.length, buf, 0, buf.length) == 0);
        scanner.value(buf, 0);
        assertTrue(Utils.compareTo(key, 0, key.length, buf, 0, buf.length) == 0);
        scanner.next();
      } 
     
    assertEquals(keys.size(), count);
  }
  
  private void verifyScannerReverse(DataBlockScanner scanner, List<byte[]> keys) {
    int count = 0;
    
    //*DEBUG*/ System.out.println("Verify reverse");
    
    Collections.reverse(keys);
      do {
        count++;
        byte[] key = keys.get(count-1);
        int keySize = scanner.keySize();
        //*DEBUG*/ System.out.println(keySize);
        int valSize = scanner.valueSize();
        assertEquals(key.length, keySize);
        assertEquals(key.length, valSize);
        byte[] buf = new byte[keySize];
        scanner.key(buf, 0);
        assertTrue(Utils.compareTo(buf, 0, buf.length, key, 0, key.length) == 0);
        scanner.value(buf, 0);
        assertTrue(Utils.compareTo(buf, 0, buf.length, key, 0, key.length) == 0);
      } while(scanner.previous());
      
      Collections.reverse(keys);
 
    assertEquals(keys.size(), count);
  }
  
  protected ArrayList<byte[]> fillDataBlock (DataBlock b) throws RetryOperationException {
    ArrayList<byte[]> keys = new ArrayList<byte[]>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    /*DEBUG*/ System.out.println("fill seed =" + seed);
    boolean result = true;
    while(result == true) {
      byte[] key = new byte[32];
      r.nextBytes(key);
      result = b.put(key, 0, key.length, key, 0, key.length, 0, 0);
      if(result) {
        keys.add(key);
      }
    }
    System.out.println(b.getNumberOfRecords() + " " + b.getNumberOfDeletedAndUpdatedRecords() + " " + b.getDataInBlockSize());
    return keys;
  }
  
}
