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

public class IndexBlockDirectMemoryScannerTest {
  Logger LOG = LoggerFactory.getLogger(IndexBlockDirectMemoryScannerTest.class);
  
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
    
  }
  
  private void dispose(List<Key> keys) {
    for(Key key: keys) {
      UnsafeAccess.free(key.address);
    }
  }

  @Test
  public void testOpenStartScan() throws IOException {
    System.out.println("testOpenStartScan");  
    IndexBlock ib = getIndexBlock(4096);
    List<Key> keys = fillIndexBlock(ib);
    Utils.sortKeys(keys);
    Random r = new Random();
    int stopRowIndex = r.nextInt(keys.size());
    Key stopRow = keys.get(stopRowIndex);
    System.out.println("Loaded "+ keys.size()+" kvs");
    keys = keys.subList(0, stopRowIndex);
    System.out.println("Selected "+ keys.size()+" kvs");
    IndexBlockDirectMemoryScanner scanner = 
        IndexBlockDirectMemoryScanner.getScanner(ib, 0, 0, stopRow.address, stopRow.length, Long.MAX_VALUE);
    verifyScanner(scanner, keys);
    scanner.close();
    dispose(keys);

  }
  
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
        IndexBlockDirectMemoryScanner.getScanner(ib, startRow.address, startRow.length,0 , 0, Long.MAX_VALUE);
    verifyScanner(scanner, keys);
    scanner.close();
    dispose(keys);

  }
  
  @Test
  public void testSubScan() throws IOException {
    System.out.println("testSubScan");  
    IndexBlock ib = getIndexBlock(4096);
    List<Key> keys = fillIndexBlock(ib);
    Utils.sortKeys(keys);
    Random r = new Random();
    int startRowIndex = r.nextInt(keys.size());
    int stopRowIndex = r.nextInt(keys.size() - startRowIndex -1) +1 + startRowIndex;

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
  
  private IndexBlock getIndexBlock(int size) {
    IndexBlock ib = new IndexBlock(size);
    ib.setFirstIndexBlock();
    return ib;
  }
  
  protected List<Key> fillIndexBlock (IndexBlock b) throws RetryOperationException {
    ArrayList<Key> keys = new ArrayList<Key>();
    Random r = new Random();
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
