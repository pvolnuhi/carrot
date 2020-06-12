package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bigbase.carrot.RetryOperationException;
import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.Utils;
import org.junit.Ignore;
import org.junit.Test;


public class DataBlockTest extends DataBlockTestBase{
  Log LOG = LogFactory.getLog(DataBlockTest.class);
  
  @Test
  public void testDataBlockPutGet() throws RetryOperationException {
    System.out.println("testDataBlockPutGet");
    DataBlock b = getDataBlock();
    ArrayList<byte[]> keys = fillDataBlock(b);
    Random r = new Random();

    System.out.println("Total inserted ="+ keys.size());
    int found = 0;
    long start = System.currentTimeMillis();
    for(int i = 0 ; i < 100000; i++) {
      int index = r.nextInt(keys.size());
      byte[] key = keys.get(index);
      byte[] value = new byte[key.length];
      long size = b.get(key, 0, key.length, value, 0, Long.MAX_VALUE);
      assertTrue(size == value.length);
      assertEquals(0, Utils.compareTo(key, 0, key.length, 
        value, 0, value.length));
      found++;
    }
    System.out.println("Total found ="+ found + " in "+(System.currentTimeMillis() - start) +"ms");
    System.out.println("Rate = "+(1000d * found)/(System.currentTimeMillis() - start) +" RPS");

  }
  
  
  @Test
  public void testScanAfterDelete() throws RetryOperationException, IOException
  {
    System.out.println("testScanAfterDelete");
    DataBlock b = getDataBlock();
    ArrayList<byte[]> keys = fillDataBlock(b);
    scanAndVerify(b, keys);
    byte[] fk = keys.get(0);
    OpResult res = b.delete(fk, 0, fk.length, Long.MAX_VALUE);
    assertEquals(OpResult.OK, res);
    System.out.println("delete done");
    List<byte[]> newKeys = remove(keys, fk);
    scanAndVerify(b, newKeys);
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
  public void testDataBlockPutScan() throws RetryOperationException, IOException {
    System.out.println("testDataBlockPutScan");
    DataBlock b = getDataBlock();
    ArrayList<byte[]> keys = fillDataBlock(b);
    long start = System.currentTimeMillis();
    byte[] buffer = new byte[keys.get(0).length * 2];
    int N = 100000;
    for(int i = 0 ; i < N; i++) {
      DataBlockScanner bs = DataBlockScanner.getScanner(b, null, null, Long.MAX_VALUE);
      int count =0;
      while(bs.hasNext()) {
        bs.keyValue(buffer, 0);
        bs.next();
        count++;
      }
      // First block has hidden system key {0} {0}
      assertEquals(keys.size() + 1, count);
      bs.close();

    }
    System.out.println("Rate = "+(1000d * N * keys.size())/(System.currentTimeMillis() - start) +" RPS");

  }
   
  @Test
  public void testDataBlockPutDelete() throws RetryOperationException {
    System.out.println("testDataBlockPutDelete");

    DataBlock b = getDataBlock();
    ArrayList<byte[]> keys = fillDataBlock(b);
    
    for (byte[] key: keys) {
      OpResult result = b.delete(key, 0, key.length, Long.MAX_VALUE);
      assertEquals(OpResult.OK, result);
    }
    // Only one system key left
    assertEquals(1, (int)b.getNumberOfRecords());
    for (byte[] key: keys) {
      long result = b.get(key, 0, key.length, Long.MAX_VALUE);
      assertEquals(DataBlock.NOT_FOUND, result);
    }

  }
  
   
  
  @Test
  public void testDataBlockSplit() throws RetryOperationException, IOException {
    System.out.println("testDataBlockSplit");

    DataBlock b = getDataBlock();
    ArrayList<byte[]> keys = fillDataBlock(b);
    int totalKVs = keys.size();
    int totalDataSize = b.getDataSize();
    DataBlock bb = b.split(true);
    // Register new DataBlock with a index block
    IndexBlock ib = new IndexBlock(4096);
    bb.register(ib ,  0);
    
    assertEquals(0, (int)bb.numDeletedAndUpdatedRecords);
    assertEquals(0, (int)b.getNumberOfDeletedAndUpdatedRecords());
    
    assertEquals(totalKVs +1, (int)(bb.numRecords + b.getNumberOfRecords()));
    assertEquals(totalDataSize, (int)(b.getDataSize() + bb.dataSize));
    byte[] f1 = b.getFirstKey();
    byte[] f2 = bb.getFirstKey();
    assertNotNull(f1); 
    assertNotNull(f2);
    assertTrue (Utils.compareTo(f1, 0, f1.length, f2, 0, f2.length) < 0);
    
    scanAndVerify(bb);
    scanAndVerify(b);

  }
  
   
  @Test
  public void testDataBlockMerge() throws RetryOperationException, IOException {
    System.out.println("testDataBlockMerge");

    DataBlock b = getDataBlock();
    ArrayList<byte[]> keys = fillDataBlock(b);
    int totalKVs = keys.size();
    int totalDataSize = b.getDataSize();
    DataBlock bb = b.split(true);
    
    IndexBlock ib = new IndexBlock(4096);
    bb.register(ib, 0);
    
    assertEquals(0, (int)bb.getNumberOfDeletedAndUpdatedRecords());
    assertEquals(0, (int)b.getNumberOfDeletedAndUpdatedRecords());
    
    assertEquals(totalKVs +1, (int)bb.getNumberOfRecords() + b.getNumberOfRecords());
    assertEquals(totalDataSize, (int)b.getDataSize() + bb.getDataSize());
    byte[] f1 = b.getFirstKey();
    byte[] f2 = bb.getFirstKey();
    assertNotNull(f1); 
    assertNotNull(f2);
    assertTrue (Utils.compareTo(f1, 0, f1.length, f2, 0, f2.length) < 0);
    b.merge(bb, true, true);
    
    assertEquals(0, (int)b.getNumberOfDeletedAndUpdatedRecords());
    assertEquals(totalKVs+1, (int)b.getNumberOfRecords());
    assertEquals(totalDataSize, (int)b.getDataSize());
    
    scanAndVerify(b);
    
  }
  
   
  @Test
  public void testCompactionFull() throws RetryOperationException {
    System.out.println("testCompactionFull");

    DataBlock b = getDataBlock();
    ArrayList<byte[]> keys = fillDataBlock(b);
    
    for (byte[] key: keys) {
      OpResult result = b.delete(key, 0, key.length, Long.MAX_VALUE);
      assertEquals(OpResult.OK, result);
    }
    
    assertEquals(1, (int)b.getNumberOfRecords());
    
    for (byte[] key: keys) {
      long result = b.get(key, 0, key.length, Long.MAX_VALUE);
      assertEquals(DataBlock.NOT_FOUND, result);
    }    
    b.compact(false);
    assertTrue (Bytes.compareTo(new byte[] {0}, b.getFirstKey()) == 0);    

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
    
    assertEquals(keys.size() - deletedKeys.size() +1, (int)b.getNumberOfRecords());
    
    for (byte[] key: deletedKeys) {
      long result = b.get(key, 0, key.length, Long.MAX_VALUE);
      assertEquals(DataBlock.NOT_FOUND, result);
    }
    
    b.compact(true);
    
    assertEquals( keys.size() - deletedKeys.size() +1, (int)b.getNumberOfRecords());
    assertEquals( 0, (int) b.getNumberOfDeletedAndUpdatedRecords());

  }
  
   
  @Test
  public void testOrderedInsertion() throws RetryOperationException, IOException {
    System.out.println("testOrderedInsertion");
    DataBlock b = getDataBlock();
    ArrayList<byte[]> keys = fillDataBlock(b);
    scanAndVerify(b, keys);
  }
  
  
  protected void scanAndVerify(DataBlock b, List<byte[]> keys) 
      throws RetryOperationException, IOException {
    byte[] buffer = null;
    byte[] tmp = null;
    try (DataBlockScanner bs = DataBlockScanner.getScanner(b, null, null, Long.MAX_VALUE);) {
      int count = 0;
      // skip first system
      if (b.isFirstBlock()) {
        bs.next();
      }
      while (bs.hasNext()) {
        int len = bs.keySize();
        buffer = new byte[len];
        bs.key(buffer, 0);
        
        boolean result = contains(buffer, keys);
        assertTrue(result);
        bs.next();
        count++;
        if (count > 1) {
          // compare
          int res = Utils.compareTo(tmp, 0, tmp.length, buffer, 0, buffer.length);
          assertTrue(res < 0);
        }
        tmp = new byte[len];
        System.arraycopy(buffer, 0, tmp, 0, tmp.length);
      }
      assertEquals(keys.size(), count);
    }
  }
  
  private boolean contains(byte[] key, List<byte[]> keys) {
    for (byte[] k : keys) {
      if (Utils.compareTo(k, 0, k.length, key, 0, key.length) == 0) {
        return true;
      }
    }
    return false;
  }
  
     
  @Test
  public void testDataBlockPutAfterDelete() throws RetryOperationException {
    System.out.println("testDataBlockPutAfterDelete");

    DataBlock b = getDataBlock();
    ArrayList<byte[]> keys = fillDataBlock(b);
    
    int dataSize = b.getDataSize();
    int blockSize = b.getBlockSize();
    int avail = blockSize - dataSize - DataBlock.RECORD_TOTAL_OVERHEAD;
    if (avail >= blockSize/2) {
      System.out.println("Skip test");
      return;
    } else if (avail < 0) {
      avail = 62;
    }
    byte[] key = new byte[avail/2 +1];
    Random r = new Random();
    r.nextBytes(key);
    
    // Try to insert
    boolean result = b.put(key, 0, key.length, key, 0, key.length, 0, 0);
    assertEquals(false, result);
    
    // Delete one record
    byte[] oneKey = keys.get(0);
        
    OpResult res = b.delete(oneKey, 0, oneKey.length, Long.MAX_VALUE);
    dataSize = b.getDataSize();
    blockSize = b.getBlockSize();
    avail = blockSize - dataSize;
    assertEquals(OpResult.OK, res);
    
    // Try insert one more time
    // Try to insert
    int reqSize = DataBlock.mustStoreExternally(key.length, key.length)? DataBlock.RECORD_TOTAL_OVERHEAD + 12:
      2*key.length + DataBlock.RECORD_TOTAL_OVERHEAD;
    boolean expResult = reqSize < avail ? true: false;
    result = b.put(key, 0, key.length, key, 0, key.length, 0, 0);
    assertEquals(expResult, result);            

  }
  
   
  @Test
  public void testOverwriteOnUpdateSmallerEnabled() throws RetryOperationException, IOException {
    System.out.println("testOverwriteOnUpdateSmallerEnabled");

    DataBlock b = getDataBlock();
    List<byte[]> keys = fillDataBlock(b);
    for( byte[] key: keys) {
      boolean res = b.put(key, 0, key.length, key, 0, key.length, 0, 0);
      assertTrue(res);
    }
    
    assertEquals(keys.size()+1, (int)b.getNumberOfRecords());
    assertEquals(0, (int)b.getNumberOfDeletedAndUpdatedRecords());

    // Delete some
    int numRecords = b.getNumberOfRecords();
    int toDelete = numRecords/2;
    if (toDelete == 0) {
      // bypass test
      return;
    }
    for (int i = 0; i < toDelete; i++) {
      byte[] key = keys.get(i);
      OpResult res = b.delete(key, 0, key.length, Long.MAX_VALUE);
      assertTrue(res == OpResult.OK);

    }

    keys =  keys.subList(toDelete, keys.size());
    assertEquals(keys.size()+1, (int)b.getNumberOfRecords());

    scanAndVerify(b, keys);

    ArrayList<byte[]> kkeys = new ArrayList<byte[]>();
    // Now insert existing keys with val/2
    for (int i = 0; i < keys.size(); i++) {
      byte[] key = keys.get(i);
      long addr = b.get(key, 0, key.length, Long.MAX_VALUE);
      int oldValLen = DataBlock.valueLength(addr);

      boolean res = b.put(key, 0, key.length, key, 0, key.length/2, 0, 0);
      // With large K-Vs we not always able to insert all k-v
      if (res == false) {
        if (isValidFailure(b, key, key.length, key.length/2, oldValLen)) {
          continue;
        } else {
          fail("FAIL");
        }
      }
      byte[] val = new byte[key.length/2];
      long size = b.get(key, 0, key.length, val, 0, Long.MAX_VALUE);
      assertEquals(val.length, (int)size);
      verifyGets(b, keys);
      scanAndVerify(b, keys);
      kkeys.add(key);
    }
    assertEquals(keys.size()+1, (int)b.getNumberOfRecords());
    scanAndVerify(b, keys);
    // Now insert existing keys with original value
    for (byte[] key: kkeys) {
      long addr = b.get(key, 0, key.length, Long.MAX_VALUE);
      assertTrue(addr > 0);
      int oldValLen = DataBlock.valueLength(addr);

      boolean res = b.put(key, 0, key.length, key, 0, key.length, 0, 0);
      if (res == false) {
        if (isValidFailure(b, key, key.length, key.length, oldValLen)) {
          continue;
        } else {
          fail("FAIL");
        }
      }
      byte[] val = new byte[key.length];
      long size = b.get(key,  0,  key.length, val, 0, Long.MAX_VALUE);
      assertEquals(val.length, (int)size);
      assertTrue(res);
    }    

    assertEquals(keys.size()+1, (int)b.getNumberOfRecords());
    scanAndVerify(b, keys);    

  }
  
  @Test
  public void testOverwriteSameValueSize() throws RetryOperationException, IOException {
    System.out.println("testOverwriteSameValueSize");
    Random r = new Random();
    DataBlock b = getDataBlock();
    List<byte[]> keys = fillDataBlock(b);
    for( byte[] key: keys) {
      byte[] value = new byte[key.length];
      r.nextBytes(value);
      byte[] buf = new byte[value.length];
      boolean res = b.put(key, 0, key.length, value, 0, value.length, 0, 0);
      assertTrue(res);
      long size = b.get(key, 0, key.length, buf, 0, Long.MAX_VALUE);
      assertEquals(value.length, (int)size);
      assertTrue(Utils.compareTo(buf, 0, buf.length, value, 0, value.length) == 0);
    }
    assertEquals(keys.size()+1, (int)b.getNumberOfRecords());
    assertEquals(0, (int)b.getNumberOfDeletedAndUpdatedRecords());

   
    scanAndVerify(b, keys);    

  }
  
  @Test
  public void testOverwriteSmallerValueSize() throws RetryOperationException, IOException {
    System.out.println("testOverwriteSmallerValueSize");
    Random r = new Random();
    DataBlock b = getDataBlock();
    List<byte[]> keys = fillDataBlock(b);
    for( byte[] key: keys) {
      byte[] value = new byte[key.length-2];
      r.nextBytes(value);
      byte[] buf = new byte[value.length];
      boolean res = b.put(key, 0, key.length, value, 0, value.length, 0, 0);
      assertTrue(res);
      long size = b.get(key, 0, key.length, buf, 0, Long.MAX_VALUE);
      assertEquals(value.length, (int)size);
      assertTrue(Utils.compareTo(buf, 0, buf.length, value, 0, value.length) == 0);
    }
    assertEquals(keys.size()+1, (int)b.getNumberOfRecords());
    assertEquals(0, (int)b.getNumberOfDeletedAndUpdatedRecords());

   
    scanAndVerify(b, keys);    

  }
  
  @Test
  public void testOverwriteLargerValueSize() throws RetryOperationException, IOException {
    System.out.println("testOverwriteLargerValueSize");
    Random r = new Random();
    DataBlock b = getDataBlock();
    List<byte[]> keys = fillDataBlock(b);
    // Delete half keys
    int toDelete = keys.size()/2;
    for(int i=0; i < toDelete; i++) {
      byte[] key = keys.remove(0);
      OpResult res = b.delete(key, 0, key.length, Long.MAX_VALUE);
      assertEquals(OpResult.OK, res);
    }
    assertEquals(keys.size()+1, (int)b.getNumberOfRecords());

    for( byte[] key: keys) {
      byte[] value = new byte[key.length+2];
      r.nextBytes(value);
      byte[] buf = new byte[value.length];
      boolean res = b.put(key, 0, key.length, value, 0, value.length, 0, 0);
      assertTrue(res);
      long size = b.get(key, 0, key.length, buf, 0, Long.MAX_VALUE);
      assertEquals(value.length, (int)size);
      assertTrue(Utils.compareTo(buf, 0, buf.length, value, 0, value.length) == 0);
    }
    assertEquals(keys.size()+1, (int)b.getNumberOfRecords());
    assertEquals(0, (int)b.getNumberOfDeletedAndUpdatedRecords());
   
    scanAndVerify(b, keys);    

  }
  
  private void verifyGets(DataBlock b, List<byte[]> keys) {
    for(byte[] key: keys) {
      long address = b.get(key, 0, key.length, Long.MAX_VALUE);
      
      assertTrue(address>0);
      int klen = DataBlock.keyLength(address);
      int vlen = DataBlock.valueLength(address);
      assertEquals(key.length, klen);
      byte[] buf = new byte[vlen];
      long size = b.get(key, 0, key.length, buf, 0, Long.MAX_VALUE);
      assertEquals(vlen, (int)size);
    }
  }
  
  private boolean isValidFailure(DataBlock b, byte[] key, int keyLen, int valLen, int oldValLen) {
    int dataSize = b.getDataSize();
    int blockSize = b.getBlockSize();
    int newRecSize = keyLen + valLen + DataBlock.RECORD_TOTAL_OVERHEAD;
    if (DataBlock.mustStoreExternally(keyLen, valLen)) {
      newRecSize = 12 + DataBlock.RECORD_TOTAL_OVERHEAD;
    }
    int oldRecSize = keyLen + oldValLen + DataBlock.RECORD_TOTAL_OVERHEAD;
    if (DataBlock.mustStoreExternally(keyLen, oldValLen)) {
      oldRecSize = 12 + DataBlock.RECORD_TOTAL_OVERHEAD;
    }
    
    return dataSize + newRecSize -oldRecSize > blockSize;
  }
 
   
  @Test
  public void testFirstKey() throws IOException {
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
    // Close scanner - this will release read lock on a block
    scanner.close();
    
    OpResult res = b.delete(kkey, 0, kkey.length, Long.MAX_VALUE);
    // We can't delete first system key
    assertEquals( OpResult.NOT_FOUND, res);
    kkey = b.getFirstKey();   
    scanner = DataBlockScanner.getScanner(b, null, null, Long.MAX_VALUE);
    // Skip system key
    scanner.next();
    keySize = scanner.keySize();    
    key = new byte[keySize];   
    scanner.key(key, 0);  
    scanner.close();
    res = b.delete(key, 0, key.length, Long.MAX_VALUE);
    // We can delete next key
    assertEquals( OpResult.OK, res);    
  }
    
  
  protected ArrayList<byte[]> fillDataBlock (DataBlock b) throws RetryOperationException {
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
