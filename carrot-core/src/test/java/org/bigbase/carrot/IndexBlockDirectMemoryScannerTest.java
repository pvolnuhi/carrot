package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexBlockDirectMemoryScannerTest {
  Logger LOG = LoggerFactory.getLogger(IndexBlockDirectMemoryScannerTest.class);
  
  
  @Test
  public void testAll() throws IOException{
    for (int i=0; i < 1000; i++) {
      System.out.println("\nRUN "+ i+"\n");
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
    IndexBlock ib = getIndexBlock(4096);
    List<Key> keys = fillIndexBlock(ib);
    Utils.sortKeys(keys);
    System.out.println("Loaded "+ keys.size()+" kvs");
    IndexBlockDirectMemoryScanner scanner = 
        IndexBlockDirectMemoryScanner.getScanner(ib, 0, 0, 0, 0, Long.MAX_VALUE);
    verifyScanner(scanner, keys);
    scanner.close();
    dispose(keys);
    ib.free();
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
    IndexBlockDirectMemoryScanner scanner = 
        IndexBlockDirectMemoryScanner.getScanner(ib, 0, 0, 0, 0, Long.MAX_VALUE, null, true);
    verifyScannerReverse(scanner, keys);
    if (scanner != null) {
      scanner.close();
    }
    dispose(keys);
    ib.free(); 
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
    int stopRowIndex = 1;//r.nextInt(keys.size());
    Key stopRow = keys.get(stopRowIndex);
    System.out.println("Loaded "+ keys.size()+" kvs");
    keys = keys.subList(0, stopRowIndex);
    System.out.println("Selected "+ keys.size()+" kvs");
    IndexBlockDirectMemoryScanner scanner = 
        IndexBlockDirectMemoryScanner.getScanner(ib, 0, 0, stopRow.address, stopRow.length, Long.MAX_VALUE);
    verifyScanner(scanner, keys);
    scanner.close();
    dispose(keys);
    ib.free();
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
    keys = keys.subList(0, stopRowIndex);
    System.out.println("Selected "+ keys.size()+" kvs");
    IndexBlockDirectMemoryScanner scanner = 
        IndexBlockDirectMemoryScanner.getScanner(ib, 0, 0, stopRow.address, 
          stopRow.length, Long.MAX_VALUE, null, true);
    verifyScannerReverse(scanner, keys);
    if (scanner != null) {
      scanner.close();
    }
    dispose(keys);
    ib.free();
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
    keys = keys.subList(startRowIndex, keys.size());
    System.out.println("Selected "+ keys.size()+" kvs");
    IndexBlockDirectMemoryScanner scanner = 
        IndexBlockDirectMemoryScanner.getScanner(ib, startRow.address, 
          startRow.length,0 , 0, Long.MAX_VALUE);
    verifyScanner(scanner, keys);
    scanner.close();
    dispose(keys);
    ib.free();
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
    keys = keys.subList(startRowIndex, keys.size());
    System.out.println("Selected "+ keys.size()+" kvs");
    IndexBlockDirectMemoryScanner scanner = 
        IndexBlockDirectMemoryScanner.getScanner(ib, startRow.address, startRow.length,0 , 0, 
          Long.MAX_VALUE, null, true);
    verifyScannerReverse(scanner, keys);
    if (scanner != null) {
      scanner.close();
    }
    dispose(keys);
    ib.free();
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
    keys = keys.subList(startRowIndex, stopRowIndex);
    System.out.println("Selected "+ keys.size()+" kvs");
    IndexBlockDirectMemoryScanner scanner = 
        IndexBlockDirectMemoryScanner.getScanner(ib, startRow.address, startRow.length, 
          stopRow.address, stopRow.length, Long.MAX_VALUE);
    verifyScanner(scanner, keys);
    scanner.close();
    dispose(keys);
    ib.free();
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
    keys = keys.subList(startRowIndex, stopRowIndex);
    System.out.println("Selected "+ keys.size()+" kvs");
    IndexBlockDirectMemoryScanner scanner = 
        IndexBlockDirectMemoryScanner.getScanner(ib, startRow.address, startRow.length, 
          stopRow.address, stopRow.length, Long.MAX_VALUE, null, true);
    verifyScannerReverse(scanner, keys);
    if (scanner != null) {
      scanner.close();
    }
    dispose(keys);
    ib.free();
  }
  
  private void verifyScanner(IndexBlockDirectMemoryScanner scanner, List<Key> keys) {
    int count = 0;
    DataBlockDirectMemoryScanner dbscn=null;
    
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
  
  private void verifyScannerReverse(IndexBlockDirectMemoryScanner scanner, List<Key> keys) 
      throws IOException {
    if (scanner == null) {
      assertEquals(0, keys.size());
      return;
    }
    int count = 0;
    DataBlockDirectMemoryScanner dbscn= scanner.lastBlockScanner();
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
    IndexBlock ib = new IndexBlock(size);
    ib.setFirstIndexBlock();
    return ib;
  }
  
  protected List<Key> fillIndexBlock (IndexBlock b) throws RetryOperationException {
    ArrayList<Key> keys = new ArrayList<Key>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("FIIL seed="+ seed);
    int kvSize = 32;
    boolean result = true;
    while(true) {
      byte[] key = new byte[kvSize];
      r.nextBytes(key);
      long ptr = UnsafeAccess.malloc(kvSize);
      UnsafeAccess.copy(key,  0,  ptr, kvSize);
      result = b.put(ptr, kvSize, ptr, kvSize, 0, 0);
      if(result) {
        keys.add( new Key(ptr, kvSize));
      } else {
        break;
      }
    }
    System.out.println("Number of data blocks="+b.getNumberOfDataBlock() + " "  + " index block data size =" + 
        b.getDataInBlockSize()+" num records=" + keys.size());
    return keys;
  }
  
}
