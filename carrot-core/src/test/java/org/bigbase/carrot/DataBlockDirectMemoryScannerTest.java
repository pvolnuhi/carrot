package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataBlockDirectMemoryScannerTest {
  Logger LOG = LoggerFactory.getLogger(DataBlockDirectMemoryScannerTest.class);
  
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
    DataBlockDirectMemoryScanner scanner = 
        DataBlockDirectMemoryScanner.getScanner(ib, 0, 0, 0, 0, Long.MAX_VALUE);
    // Skip first system key
    scanner.next();
    verifyScanner(scanner, keys);
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
    DataBlockDirectMemoryScanner scanner = 
        DataBlockDirectMemoryScanner.getScanner(ib, 0, 0, stopRow.address, stopRow.size, Long.MAX_VALUE);
    // Skip first system key
    scanner.next();
    verifyScanner(scanner, keys);
    scanner.close();
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
    DataBlockDirectMemoryScanner scanner = 
        DataBlockDirectMemoryScanner.getScanner(ib, startRow.address, startRow.size, 0, 0, Long.MAX_VALUE);
    verifyScanner(scanner, keys);
    scanner.close();
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
    DataBlockDirectMemoryScanner scanner = 
        DataBlockDirectMemoryScanner.getScanner(ib, startRow.address, startRow.size, 
          stopRow.address, stopRow.size, Long.MAX_VALUE);
    verifyScanner(scanner, keys);
    scanner.close();
    dispose(keys);
  }
  
  
  private void verifyScanner(DataBlockDirectMemoryScanner scanner, List<Key> keys) {
    int count = 0;
    
      while(scanner.hasNext()) {
        count++;
        Key key = keys.get(count-1);
        int keySize = scanner.keySize();
        int valSize = scanner.valueSize();
        assertEquals(key.size, keySize);
        assertEquals(key.size, valSize);
        byte[] buf = new byte[keySize];
        scanner.key(buf, 0);
        assertTrue(Utils.compareTo(buf, 0, buf.length, key.address, key.size) == 0);
        scanner.value(buf, 0);
        assertTrue(Utils.compareTo(buf, 0, buf.length, key.address, key.size) == 0);
        scanner.next();
      } 
     
    assertEquals(keys.size(), count);
  }
  
  
  protected ArrayList<Key> fillDataBlock (DataBlock b) throws RetryOperationException {
    ArrayList<Key> keys = new ArrayList<Key>();
    Random r = new Random();
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
