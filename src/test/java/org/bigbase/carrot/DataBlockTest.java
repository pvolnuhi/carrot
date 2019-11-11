package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bigbase.carrot.RetryOperationException;
import org.bigbase.util.Utils;
import org.junit.Ignore;
import org.junit.Test;


public class DataBlockTest {
  Log LOG = LogFactory.getLog(DataBlockTest.class);

  static IndexBlock ib = new IndexBlock(4096);
  
  private DataBlock getDataBlock() {
    DataBlock b = new DataBlock(4096);
    b.register(ib, 0);
    return b;
  }
  
 // @Ignore
  @Test
  public void testDataBlockPutGet() throws RetryOperationException {
    System.out.println("testDataBlockPutGet");
    DataBlock b = getDataBlock();
    ArrayList<byte[]> keys = fillDataBlock(b);
    Random r = new Random();

    System.out.println("Total inserted ="+ keys.size());
    int found = 0;
    long start = System.currentTimeMillis();
    for(int i = 0 ; i < 10000000; i++) {
      int index = r.nextInt(keys.size());
      byte[] key = keys.get(index);
      long off = b.get(key,  0,  key.length, Long.MAX_VALUE);
      if(off > 0) found++;
    }
    System.out.println("Total found ="+ found + " in "+(System.currentTimeMillis() - start) +"ms");
    System.out.println("Rate = "+(1000d * found)/(System.currentTimeMillis() - start) +" RPS");

  }

  @Test
  public void testScanAfterDelete() throws RetryOperationException
  {
    System.out.println("testScanAfterDelete");
    DataBlock b = getDataBlock();
    ArrayList<byte[]> keys = fillDataBlock(b);
    scanAndVerify(b, keys);
    byte[] fk = b.getFirstKey();
    b.delete(fk, 0, fk.length, Long.MAX_VALUE);
    System.out.println("delete done");
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
  public void testDataBlockPutScan() throws RetryOperationException {
    System.out.println("testDataBlockPutScan");
    DataBlock b = getDataBlock();
    ArrayList<byte[]> keys = fillDataBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    long start = System.currentTimeMillis();
    byte[] buffer = new byte[keys.get(0).length * 2];
    int N = 1000000;
    for(int i = 0 ; i < N; i++) {
      DataBlockScanner bs = DataBlockScanner.getScanner(b, null, null, Long.MAX_VALUE);
      int count =0;
      while(bs.hasNext()) {
        bs.keyValue(buffer, 0);
        bs.next();
        count++;
      }
      assertEquals(keys.size(), count);

    }
    System.out.println("Rate = "+(1000d * N * keys.size())/(System.currentTimeMillis() - start) +" RPS");

  }
  
  //@Ignore
  @Test
  public void testDataBlockPutDelete() throws RetryOperationException {
    System.out.println("testDataBlockPutDelete");

    DataBlock b = getDataBlock();
    ArrayList<byte[]> keys = fillDataBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    
    for (byte[] key: keys) {
      OpResult result = b.delete(key, 0, key.length, Long.MAX_VALUE);
      assertEquals(OpResult.OK, result);
    }
    
    assertEquals(b.getNumberOfDeletedAndUpdatedRecords(), b.getNumberOfRecords());
    for (byte[] key: keys) {
      long result = b.get(key, 0, key.length, Long.MAX_VALUE);
      assertEquals(DataBlock.NOT_FOUND, result);
    }

  }
  
  @Test
  public void testDataBlockSplit() throws RetryOperationException {
    System.out.println("testDataBlockSplit");

    DataBlock b = getDataBlock();
    ArrayList<byte[]> keys = fillDataBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    int totalKVs = keys.size();
    int totalDataSize = b.getDataSize();
    DataBlock bb = b.split(true);
    // Register new DataBlock with a index block
    IndexBlock ib = new IndexBlock(4096);
    bb.register(ib ,  0);
    
    assertEquals(0, bb.numDeletedAndUpdatedRecords);
    assertEquals(0, b.getNumberOfDeletedAndUpdatedRecords());
    
    assertEquals(totalKVs, bb.numRecords + b.getNumberOfRecords());
    assertEquals(totalDataSize, b.getDataSize() + bb.dataSize);
    byte[] f1 = b.getFirstKey();
    byte[] f2 = bb.getFirstKey();
    assertNotNull(f1); 
    assertNotNull(f2);
    assertTrue (Utils.compareTo(f1, 0, f1.length, f2, 0, f2.length) < 0);
    
    scanAndVerify(bb, keys.get(0).length);
    scanAndVerify(b, keys.get(0).length);

  }
  
  
  @Test
  public void testDataBlockMerge() throws RetryOperationException {
    System.out.println("testDataBlockMerge");

    DataBlock b = getDataBlock();
    ArrayList<byte[]> keys = fillDataBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    int totalKVs = keys.size();
    int totalDataSize = b.getDataSize();
    DataBlock bb = b.split(true);
    
    IndexBlock ib = new IndexBlock(4096);
    bb.register(ib, 0);
    
    assertEquals(0, bb.getNumberOfDeletedAndUpdatedRecords());
    assertEquals(0, b.getNumberOfDeletedAndUpdatedRecords());
    
    assertEquals(totalKVs, bb.getNumberOfRecords() + b.getNumberOfRecords());
    assertEquals(totalDataSize, b.getDataSize() + bb.getDataSize());
    byte[] f1 = b.getFirstKey();
    byte[] f2 = bb.getFirstKey();
    assertNotNull(f1); 
    assertNotNull(f2);
    assertTrue (Utils.compareTo(f1, 0, f1.length, f2, 0, f2.length) < 0);
    b.merge(bb, true, true);
    
    assertEquals(0, b.getNumberOfDeletedAndUpdatedRecords());
    assertEquals(totalKVs, b.getNumberOfRecords());
    assertEquals(totalDataSize, b.getDataSize());
    
    scanAndVerify(b, keys.get(0).length);
    
  }
  
  
  @Test
  public void testCompactionFull() throws RetryOperationException {
    System.out.println("testCompactionFull");

    DataBlock b = getDataBlock();
    ArrayList<byte[]> keys = fillDataBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    
    for (byte[] key: keys) {
      OpResult result = b.delete(key, 0, key.length, Long.MAX_VALUE);
      assertEquals(OpResult.OK, result);
    }
    
    assertEquals(b.getNumberOfDeletedAndUpdatedRecords(), b.getNumberOfRecords());
    assertEquals(0, b.getNumberOfRecords());
    
    for (byte[] key: keys) {
      long result = b.get(key, 0, key.length, Long.MAX_VALUE);
      assertEquals(DataBlock.NOT_FOUND, result);
    }
    
    b.compact(false);
    
    assertTrue (b.getFirstKey() == null);    

  }
  
  @Test
  public void testCompactionPartial() throws RetryOperationException {
    System.out.println("testCompactionPartial");

    DataBlock b = getDataBlock();
    ArrayList<byte[]> keys = fillDataBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    
    Random r = new Random();
    ArrayList<byte[]> deletedKeys = new ArrayList<byte[]>();
    for (byte[] key: keys) {
      if( r.nextDouble() < 0.5) {
        OpResult result = b.delete(key, 0, key.length, Long.MAX_VALUE);
        assertEquals(OpResult.OK, result);
        deletedKeys.add(key);
      }
    }
    
    assertEquals(keys.size() - deletedKeys.size(), b.getNumberOfRecords());
    
    for (byte[] key: deletedKeys) {
      long result = b.get(key, 0, key.length, Long.MAX_VALUE);
      assertEquals(DataBlock.NOT_FOUND, result);
    }
    
    b.compact(true);
    
    assertEquals( keys.size() - deletedKeys.size(), b.getNumberOfRecords());
    assertEquals( 0, b.getNumberOfDeletedAndUpdatedRecords());

  }
  
  @Test
  public void testOrderedInsertion() throws RetryOperationException {
    System.out.println("testOrderedInsertion");
    DataBlock b = getDataBlock();
    ArrayList<byte[]> keys = fillDataBlock(b);
    System.out.println("Total inserted =" + keys.size());
    scanAndVerify(b, keys.get(0).length);
  }
  
  private void scanAndVerify(DataBlock b, int keyLength) throws RetryOperationException {
    byte[] buffer = new byte[keyLength];
    byte[] tmp = new byte[keyLength];

    DataBlockScanner bs = DataBlockScanner.getScanner(b, null, null, Long.MAX_VALUE);
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
  
  private List<byte[]> delete(DataBlock b, List<byte[]> keys, int num) {
    List<byte[]> list = new ArrayList<byte[]>(keys);
    Random r = new Random();
    for(int i = 0; i < num; i++) {
      //int n = r.nextInt(list.size());
      byte[] key = list.remove(0);
      OpResult res = b.delete(key, 0, key.length, Long.MAX_VALUE);
      assertTrue(res == OpResult.OK);
    }
    return list;
  }
  
  private void scanAndVerify(DataBlock b, List<byte[]> keys) throws RetryOperationException {
    int keyLength = keys.get(0).length;
    byte[] buffer = new byte[keyLength];
    byte[] tmp = new byte[keyLength];

    DataBlockScanner bs = DataBlockScanner.getScanner(b, null, null, Long.MAX_VALUE);
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
  public void testDataBlockPutAfterDelete() throws RetryOperationException {
    System.out.println("testDataBlockPutAfterDelete");

    DataBlock b = getDataBlock();
    ArrayList<byte[]> keys = fillDataBlock(b);
    
    byte[] key = new byte[32];
    Random r = new Random();
    r.nextBytes(key);
    
    // Try to insert
    boolean result = b.put(key, 0, key.length, key, 0, key.length, 0, 0);
    assertEquals(false, result);
    
    // Delete one record
    byte[] oneKey = keys.get(0);
    OpResult res = b.delete(oneKey, 0, oneKey.length, Long.MAX_VALUE);
    
    assertEquals(OpResult.OK, res);
    
    // Try insert one more time
    // Try to insert
    result = b.put(key, 0, key.length, key, 0, key.length, 0, 0);
    assertEquals(true, result);            

  }
  
  @Test
  public void testOverwriteOnUpdateEnabled() throws RetryOperationException {
    System.out.println("testOverwriteOnUpdateEnabled");

    DataBlock b = getDataBlock();
    List<byte[]> keys = fillDataBlock(b);
    for( byte[] key: keys) {
      boolean res = b.put(key, 0, key.length, key, 0, key.length, 0, 0);
      assertTrue(res);
    }
    
    assertEquals(keys.size(), b.getNumberOfRecords());
    assertEquals(0, b.getNumberOfDeletedAndUpdatedRecords());
    
    // Delete  5 first
    for (int i = 0; i < 5; i++) {
      byte[] key = keys.get(i);
      OpResult res = b.delete(key, 0, key.length, Long.MAX_VALUE);
      assertTrue(res == OpResult.OK);

    }
    
    keys =  keys.subList(5, keys.size());
    assertEquals(keys.size(), b.getNumberOfRecords());

    scanAndVerify(b, keys);

    ArrayList<byte[]> kkeys = new ArrayList<byte[]>();
    // Now insert existing keys with val/2
    for (int i = 0; i < 5; i++) {
      byte[] key = keys.get(i);
      b.put(key, 0, key.length, key, 0, key.length/2, 0, 0);
      kkeys.add(key);
    }
    assertEquals(keys.size(), b.getNumberOfRecords());
    
    scanAndVerify(b, keys);
    
    // Now insert existing keys with original value
        
    for (byte[] key: kkeys) {
      boolean res = b.put(key, 0, key.length, key, 0, key.length, 0, 0);
      assertTrue(res);
    }    
    assertEquals(keys.size(), b.getNumberOfRecords());
    //assertEquals(0, b.getNumberOfDeletedAndUpdatedRecords());
    scanAndVerify(b, keys);
    

  }
    
  
  @Test
  public void testFirstKey() {
    System.out.println("testFirstKey");

    DataBlock b = getDataBlock();
    List<byte[]> keys = fillDataBlock(b);
    
    scanAndVerify(b, keys);
    
    DataBlockScanner scanner = DataBlockScanner.getScanner(b, null, null, Long.MAX_VALUE);    
    int keySize = scanner.keySize();    
    byte[] key = new byte[keySize];    
    scanner.key(key, 0);
    byte[] kkey = b.getFirstKey();
    assertTrue(Utils.compareTo(key, 0, key.length, kkey, 0, kkey.length) == 0);
    OpResult res = b.delete(kkey, 0, kkey.length, Long.MAX_VALUE);
    assertEquals( OpResult.OK, res);
    //kkey = b.getFirstKey();
    //assertTrue(Utils.compareTo(key, 0, key.length, kkey, 0, kkey.length) == 0);   
    kkey = b.getFirstKey();   
    scanner = DataBlockScanner.getScanner(b, null, null, Long.MAX_VALUE);
    // It will skip deleted
    scanner.key(key, 0);    
    assertTrue(Utils.compareTo(key, 0, key.length, kkey, 0, kkey.length) == 0);    
  }
    
  
  private ArrayList<byte[]> fillDataBlock (DataBlock b) throws RetryOperationException {
    ArrayList<byte[]> keys = new ArrayList<byte[]>();
    Random r = new Random();

    boolean result = true;
    while(result == true) {
      byte[] key = new byte[32];
      r.nextBytes(key);
      result = b.put(key, 0, key.length, key, 0, key.length, 0, 0);
      if(result) {
        keys.add(key);
      }
    }
    System.out.println(b.getNumberOfRecords() + " " + b.getNumberOfDeletedAndUpdatedRecords() + " " + b.getDataSize());
    return keys;
  }
  
}
