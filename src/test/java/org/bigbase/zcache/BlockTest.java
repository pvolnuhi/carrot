package org.bigbase.zcache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.RetryOperationException;
import org.bigbase.util.Utils;
import org.junit.Test;

public class BlockTest {

 // @Ignore
  @Test
  public void testBlockPutGet() throws RetryOperationException {
    System.out.println("testBlockPutGet");
    Block b = new Block (4096);
    ArrayList<byte[]> keys = fillBlock(b);
    Random r = new Random();

    System.out.println("Total inserted ="+ keys.size());
    int found = 0;
    long start = System.currentTimeMillis();
    for(int i = 0 ; i < 10000000; i++) {
      int index = r.nextInt(keys.size());
      byte[] key = keys.get(index);
      long off = b.get(key,  0,  key.length);
      if(off > 0) found++;
    }
    System.out.println("Total found ="+ found + " in "+(System.currentTimeMillis() - start) +"ms");
    System.out.println("Rate = "+(1000d * found)/(System.currentTimeMillis() - start) +" RPS");

  }

  @Test
  public void testScanAfterDelete() throws RetryOperationException
  {
    System.out.println("testScanAfterDelete");
    Block b = new Block (4096);
    ArrayList<byte[]> keys = fillBlock(b);
    scanAndVerify(b, keys);
    byte[] fk = b.getFirstKey();
    b.delete(fk, 0, fk.length);
    //Random r = new Random();
    //int toDelete = r.nextInt(keys.size());
    List<byte[]> newKeys = remove(keys, fk);//delete(b, keys, 1);
    System.out.println("Total deleted ="+ 1);

    scanAndVerify(b, newKeys);
    System.out.println("testScanAfterDelete DONE");

  }
  
  
  private List<byte[]> remove(ArrayList<byte[]> keys, byte[] fk) {
    List<byte[]> nkeys = new ArrayList<byte[]>(keys);
    int index = 0;
    for(byte[] key: keys) {
      if (Utils.compareTo(fk, 0, fk.length, key, 0, key.length) == 0) {
        break;
      }
      index++;
    }
    nkeys.remove(index);
    return nkeys;
  }

  @Test
  public void testBlockPutScan() throws RetryOperationException {
    System.out.println("testBlockPutScan");
    Block b = new Block (4096);
    ArrayList<byte[]> keys = fillBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    long start = System.currentTimeMillis();
    byte[] buffer = new byte[keys.get(0).length * 2];
    int N = 1000000;
    for(int i = 0 ; i < N; i++) {
      BlockScannerOld bs = BlockScannerOld.getScanner(b);
      int count =0;
      while(bs.hasNext()) {
        bs.keyValue(buffer, 0, buffer, buffer.length / 2);
        bs.next();
        count++;
      }
      assertEquals(keys.size(), count);

    }
    System.out.println("Rate = "+(1000d * N * keys.size())/(System.currentTimeMillis() - start) +" RPS");

  }
  
  //@Ignore
  @Test
  public void testBlockPutDelete() throws RetryOperationException {
    System.out.println("testBlockPutDelete");

    Block b = new Block (4096);
    ArrayList<byte[]> keys = fillBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    
    for (byte[] key: keys) {
      boolean result = b.delete(key, 0, key.length);
      assertEquals(true, result);
    }
    
    assertEquals(b.getNumDeletedRecords(), b.getNumRecords());
    for (byte[] key: keys) {
      long result = b.get(key, 0, key.length);
      assertEquals(Block.NOT_FOUND, result);
    }

  }
  
  @Test
  public void testBlockSplit() throws RetryOperationException {
    System.out.println("testBlockSplit");

    Block b = new Block (4096);
    ArrayList<byte[]> keys = fillBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    int totalKVs = keys.size();
    int totalDataSize = b.dataSize;
    Block bb = b.split(true);
    
    assertEquals(0, bb.getNumDeletedRecords());
    assertEquals(0, b.getNumDeletedRecords());
    
    assertEquals(totalKVs, bb.getNumRecords() + b.getNumRecords());
    assertEquals(totalDataSize, b.dataSize + bb.dataSize);
    byte[] f1 = b.getFirstKey(true);
    byte[] f2 = bb.getFirstKey(true);
    assertNotNull(f1); 
    assertNotNull(f2);
    assertTrue (Utils.compareTo(f1, 0, f1.length, f2, 0, f2.length) < 0);
    
    scanAndVerify(bb, keys.get(0).length);
    scanAndVerify(b, keys.get(0).length);

  }
  
  @Test
  public void testBlockMerge() throws RetryOperationException {
    System.out.println("testBlockMerge");

    Block b = new Block (4096);
    ArrayList<byte[]> keys = fillBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    int totalKVs = keys.size();
    int totalDataSize = b.dataSize;
    Block bb = b.split(true);
    
    assertEquals(0, bb.getNumDeletedRecords());
    assertEquals(0, b.getNumDeletedRecords());
    
    assertEquals(totalKVs, bb.getNumRecords() + b.getNumRecords());
    assertEquals(totalDataSize, b.dataSize + bb.dataSize);
    byte[] f1 = b.getFirstKey(true);
    byte[] f2 = bb.getFirstKey(true);
    assertNotNull(f1); 
    assertNotNull(f2);
    assertTrue (Utils.compareTo(f1, 0, f1.length, f2, 0, f2.length) < 0);
    b.merge(bb, true, true);
    
    assertEquals(0, b.getNumDeletedRecords());
    assertEquals(totalKVs, b.getNumRecords());
    assertEquals(totalDataSize, b.dataSize);
    
    scanAndVerify(b, keys.get(0).length);
    
  }
  
  
  @Test
  public void testCompactionFull() throws RetryOperationException {
    System.out.println("testCompactionFull");

    Block b = new Block (4096);
    ArrayList<byte[]> keys = fillBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    
    for (byte[] key: keys) {
      boolean result = b.delete(key, 0, key.length);
      assertEquals(true, result);
    }
    
    assertEquals(b.getNumDeletedRecords(), b.getNumRecords());
    assertEquals(keys.size(), b.getNumDeletedRecords());
    assertEquals(keys.size(), b.getNumRecords());
    
    for (byte[] key: keys) {
      long result = b.get(key, 0, key.length);
      assertEquals(Block.NOT_FOUND, result);
    }
    
    b.compact(false);
    
    assertEquals( 1, b.getNumRecords());
    assertEquals( 1, b.getNumDeletedRecords());
    assertTrue (b.getFirstKey(true) == null);    

  }
  
  @Test
  public void testCompactionPartial() throws RetryOperationException {
    System.out.println("testCompactionPartial");

    Block b = new Block (4096);
    ArrayList<byte[]> keys = fillBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    
    Random r = new Random();
    ArrayList<byte[]> deletedKeys = new ArrayList<byte[]>();
    for (byte[] key: keys) {
      if( r.nextDouble() < 0.5) {
        boolean result = b.delete(key, 0, key.length);
        assertEquals(true, result);
        deletedKeys.add(key);
      }
    }
    
    assertEquals(b.getNumDeletedRecords(), deletedKeys.size());
    assertEquals(keys.size(), b.getNumRecords());
    
    for (byte[] key: deletedKeys) {
      long result = b.get(key, 0, key.length);
      assertEquals(Block.NOT_FOUND, result);
    }
    
    b.compact(true);
    
    assertEquals( keys.size() - deletedKeys.size(), b.getNumRecords());
    assertEquals( 0, b.getNumDeletedRecords());

  }
  
  @Test
  public void testOrderedInsertion() throws RetryOperationException {
    System.out.println("testOrderedInsertion");
    Block b = new Block(4096);
    ArrayList<byte[]> keys = fillBlock(b);
    System.out.println("Total inserted =" + keys.size());
    scanAndVerify(b, keys.get(0).length);
  }
  
  private void scanAndVerify(Block b, int keyLength) throws RetryOperationException {
    byte[] buffer = new byte[keyLength];
    byte[] tmp = new byte[keyLength];

    BlockScannerOld bs = BlockScannerOld.getScanner(b);
    int count = 0;
    while (bs.hasNext()) {
      bs.key(buffer, 0);
      bs.next();
      count++;
      if (count > 1) {
        // compare
        int res = Utils.compareTo(tmp, 0, tmp.length, buffer, 0, buffer.length);
        assertTrue (res < 0);
      }
      System.arraycopy(buffer, 0, tmp, 0, tmp.length);
    }
    System.out.println("Scanned ="+ count);
  }
  
  private List<byte[]> delete(Block b, List<byte[]> keys, int num) {
    List<byte[]> list = new ArrayList<byte[]>(keys);
    Random r = new Random();
    for(int i = 0; i < num; i++) {
      //int n = r.nextInt(list.size());
      byte[] key = list.remove(0);
      boolean res = b.delete(key, 0, key.length);
      assertTrue(res);
    }
    return list;
  }
  
  private void scanAndVerify(Block b, List<byte[]> keys) throws RetryOperationException {
    int keyLength = keys.get(0).length;
    byte[] buffer = new byte[keyLength];
    byte[] tmp = new byte[keyLength];

    BlockScannerOld bs = BlockScannerOld.getScanner(b);
    int count = 0;
    while (bs.hasNext()) {
      bs.key(buffer, 0);
      assertTrue(contains(buffer, keys));
      bs.next();
      count++;
      if (count > 1) {
        // compare
        int res = Utils.compareTo(tmp, 0, tmp.length, buffer, 0, buffer.length);
        assertTrue (res < 0);
      }
      System.arraycopy(buffer, 0, tmp, 0, tmp.length);
    }
    assertEquals(keys.size(), count);
  }
  
  private boolean contains(byte[] key, List<byte[]> keys) {
    for (byte[] k : keys) {
      if (Utils.compareTo(k, 0, k.length, key, 0, key.length) == 0) {
        return true;
      }
    }
    return false;
  }
  
  //@Ignore
  @Test
  public void testBlockPutAfterDelete() throws RetryOperationException {
    System.out.println("testBlockPutAfterDelete");

    Block b = new Block (4096);
    ArrayList<byte[]> keys = fillBlock(b);
    
    byte[] key = new byte[32];
    Random r = new Random();
    r.nextBytes(key);
    
    // Try to insert
    boolean result = b.put(key, 0, key.length, key, 0, key.length);
    assertEquals(false, result);
    
    // Delete one record
    byte[] oneKey = keys.get(0);
    result = b.delete(oneKey, 0, oneKey.length);
    
    assertEquals(true, result);
    
    // Try insert one more time
    // Try to insert
    result = b.put(key, 0, key.length, key, 0, key.length);
    assertEquals(true, result);            

  }
  
  @Test
  public void testOverwriteOnUpdateEnabled() throws RetryOperationException {
    System.out.println("testOverwriteOnUpdateEnabled");

    Block b = new Block (4096);
    List<byte[]> keys = fillBlock(b);
    for( byte[] key: keys) {
      boolean res = b.put(key, 0, key.length, key, 0, key.length);
      assertTrue(res);
    }
    
    assertEquals(keys.size(), b.getNumRecords());
    assertEquals(0, b.getNumDeletedRecords());
    
    // Delete  5 first
    for (int i = 0; i < 5; i++) {
      byte[] key = keys.get(i);
      boolean res = b.delete(key, 0, key.length);
      assertTrue(res);

    }
    
    keys =  keys.subList(5, keys.size());
    assertTrue(keys.size() + 5 == b.getNumRecords());
    assertTrue(5 == b.getNumDeletedRecords());

    scanAndVerify(b, keys);

    ArrayList<byte[]> kkeys = new ArrayList<byte[]>();
    // Now insert existing keys with val/2
    for (int i = 0; i < 5; i++) {
      byte[] key = keys.get(i);
      b.put(key, 0, key.length, key, 0, key.length/2);
      kkeys.add(key);
    }
    assertEquals(keys.size(), b.getNumRecords());
    assertEquals(0, b.getNumDeletedRecords());
    
    scanAndVerify(b, keys);
    
    // Now insert existing keys with original value
        
    for (byte[] key: kkeys) {
      boolean res = b.put(key, 0, key.length, key, 0, key.length);
      assertTrue(res);
    }    
    assertEquals(keys.size(), b.getNumRecords());
    assertEquals(0, b.getNumDeletedRecords());
    scanAndVerify(b, keys);
    

  }
    
  
  @Test
  public void testFirstKey() {
    System.out.println("testFirstKey");

    Block b = new Block (4096);
    List<byte[]> keys = fillBlock(b);
    
    scanAndVerify(b, keys);
    
    BlockScannerOld scanner = BlockScannerOld.getScanner(b);    
    int keySize = scanner.keySize();    
    byte[] key = new byte[keySize];    
    scanner.key(key, 0);
    byte[] kkey = b.getFirstKey(true);
    assertTrue(Utils.compareTo(key, 0, key.length, kkey, 0, kkey.length) == 0);
    boolean res = b.delete(kkey, 0, kkey.length);
    assertEquals( true, res);
    kkey = b.getFirstKey(false);
    assertTrue(Utils.compareTo(key, 0, key.length, kkey, 0, kkey.length) == 0);   
    kkey = b.getFirstKey(true);   
    scanner = BlockScannerOld.getScanner(b);
    // It will skip deleted
    scanner.key(key, 0);    
    assertTrue(Utils.compareTo(key, 0, key.length, kkey, 0, kkey.length) == 0);    
  }
  
  @Test
  public void testLastKey() {
    System.out.println("testLastKey");

    Block b = new Block (4096);
    List<byte[]> keys = fillBlock(b);
    
    scanAndVerify(b, keys);
    
    BlockScannerOld scanner = BlockScannerOld.getScanner(b);    
    int keySize = scanner.keySize();    
    byte[] key = new byte[keySize];    
    byte[] kkey = b.getLastKey(true);
    
    while(scanner.hasNext()) {
      scanner.key(key, 0);
      assertTrue(Utils.compareTo(key, 0, key.length, kkey, 0, kkey.length) <= 0);
      scanner.next();
    }
    
    int num = b.getNumRecords();
    
    scanner = BlockScannerOld.getScanner(b);    

    for (int i = 0; i < num -2; i++) {
      boolean res = scanner.next();
      assertEquals(true, res);
    }
    // Delete 2 last
    while(scanner.hasNext()) {
      scanner.key(key, 0);
      b.delete(key, 0, key.length);
      scanner.next();
    }
    
    kkey = b.getLastKey(true);
    scanner = BlockScannerOld.getScanner(b);    
    while(scanner.hasNext()) {
      scanner.key(key, 0);
      assertTrue(Utils.compareTo(key, 0, key.length, kkey, 0, kkey.length) <= 0);
      scanner.next();
    }    
  }  
  
  
  private ArrayList<byte[]> fillBlock (Block b) throws RetryOperationException {
    ArrayList<byte[]> keys = new ArrayList<byte[]>();
    Random r = new Random();

    boolean result = true;
    while(result == true) {
      byte[] key = new byte[32];
      r.nextBytes(key);
      result = b.put(key, 0, key.length, key, 0, key.length);
      if(result) {
        keys.add(key);
      }
    }
    return keys;
  }
  
}
