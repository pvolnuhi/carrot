package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Ignore;
import org.junit.Test;

public class DataBlockDirectMemoryTest {

  class Key {
    long address;
    int size;
    
    Key(long address, int size){
      this.address = address;
      this.size = size;
    }
  }
  
  static IndexBlock ib = new IndexBlock(4096);
  
  private DataBlock getDataBlock() {
    DataBlock b = new DataBlock(4096);
    b.register(ib, 0);
    return b;
  }
  @Test
  public void testBlockPutGet() throws RetryOperationException {
    System.out.println("testBlockPutGet");
    DataBlock b = getDataBlock();
    ArrayList<Key> keys = fillBlock(b);
    Random r = new Random();

    System.out.println("Total inserted ="+ keys.size());
    int found = 0;
    long start = System.currentTimeMillis();
    int n = 1000000;
    for(int i = 0 ; i < n; i++) {
      int index = r.nextInt(keys.size());
      long keyPtr = keys.get(index).address;
      int keyLength = keys.get(index).size;
      long off = b.get(keyPtr,  keyLength, Long.MAX_VALUE);
      int res = Utils.compareTo(DataBlock.keyAddress(off), keyLength, keyPtr, keyLength);
      assertTrue(res == 0);
      if(off > 0) found++;
    }
    assertEquals(n, found);
    System.out.println("Total found ="+ found + " in "+(System.currentTimeMillis() - start) +"ms");
    System.out.println("Rate = "+(1000d * found)/(System.currentTimeMillis() - start) +" RPS");
    b.free();
    System.out.println("testBlockPutGet DONE");


  }

  private final void toBytes(long val, byte[] b, int off) {
      
      for (int i = 7; i > 0; i--) {
        b[i + off] = (byte) val;
        val >>>= 8;
      }
      b[off] = (byte) val;
    }
  
  private final void getKey(byte[] key, long v) {
  	toBytes(v, key, 11);
  }
  
  private final void getValue(byte[] value, long v) {
  	toBytes(v, value, 5);

  }
  
  byte[] key;
  byte[] value;
  
  void initKeyValue() {
  	key = new byte[19];
      byte[] tnBytes = "Thread-1".getBytes();
      
      System.arraycopy(tnBytes, 0, key, 0, 8);
      key[8] = (byte) 'K';
      key[9] = (byte) 'E';
      key[10] = (byte) 'Y';
      
      value = new byte[13];
      value[0] = (byte) 'V';
      value[1] = (byte) 'A';
      value[2] = (byte) 'L';
      value[3] = (byte) 'U';
      value[4] = (byte) 'E';
  }
  
  @Test
  public void testPutGet128Bug() {
    initKeyValue();
    getKey(key, 128);
    getValue(value, 128);
    DataBlock b = getDataBlock();

    boolean res = b.put(key, 0, key.length, value, 0, value.length, 0, 0);
    assertTrue(res);
    byte[] tmp = new byte[13];
    long size = b.get(key, 0, key.length, tmp, 0, Long.MAX_VALUE);
    System.out.println(size);
    assertTrue(size == value.length);

  }
  
  @Test
  public void testBlockPutScan() throws RetryOperationException, IOException {
    System.out.println("testBlockPutScan");
    DataBlock b = getDataBlock();
    ArrayList<Key> keys = fillBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    long start = System.currentTimeMillis();
    
    long buffer = UnsafeAccess.malloc(16384);
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
      bs.close();

    }
    b.free();
    System.out.println("Rate = "+(1000d * N * keys.size())/(System.currentTimeMillis() - start) +" RPS");
    System.out.println("testBlockPutScan DONE");

  }
  
  //@Ignore
  @Test
  public void testBlockPutDelete() throws RetryOperationException {
    System.out.println("testBlockPutDelete");

    DataBlock b = getDataBlock();
    ArrayList<Key> keys = fillBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    
    for (Key key: keys) {
      OpResult result = b.delete(key.address, key.size, Long.MAX_VALUE);
      assertEquals(OpResult.OK, result);
    }
    
    assertEquals(0, (int)b.getNumberOfRecords());
    for (Key key: keys) {
      long result = b.get(key.address, key.size, Long.MAX_VALUE);
      assertEquals(DataBlock.NOT_FOUND, result);
    }
    b.free();
    System.out.println("testBlockPutDelete DONE");

  }
  
  //@Ignore
  @Test
  public void testBlockSplit() throws RetryOperationException, IOException {
    System.out.println("testBlockSplit");

    DataBlock b = getDataBlock();
    ArrayList<Key> keys = fillBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    int totalKVs = keys.size();
    int totalDataSize = b.getDataSize();
    DataBlock bb = b.split(true);
    
    IndexBlock ib = new IndexBlock(4096);
    bb.register(ib, 0);
    
    assertEquals(0, (int)bb.getNumberOfDeletedAndUpdatedRecords());
    assertEquals(0, (int)b.getNumberOfDeletedAndUpdatedRecords());
    
    assertEquals(totalKVs, (int)bb.getNumberOfRecords() + b.getNumberOfRecords());
    assertEquals(totalDataSize, (int)b.getDataSize() + bb.getDataSize());
    byte[] f1 = b.getFirstKey();
    byte[] f2 = bb.getFirstKey();
    assertNotNull(f1); 
    assertNotNull(f2);
    assertTrue (Utils.compareTo(f1, 0, f1.length, f2, 0, f2.length) < 0);
    
    scanAndVerify(bb);
    scanAndVerify(b);
    b.free();
    bb.free();
    System.out.println("testBlockSplit DONE");

  }
  
  //@Ignore
  @Test
  public void testBlockMerge() throws RetryOperationException, IOException {
    System.out.println("testBlockMerge");

    DataBlock b = getDataBlock();
    ArrayList<Key> keys = fillBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    int totalKVs = keys.size();
    int totalDataSize = b.getDataSize();
    DataBlock bb = b.split(true);
    IndexBlock ib = new IndexBlock(4096);
    bb.register(ib, 0);
    
    assertEquals(0, (int)bb.getNumberOfDeletedAndUpdatedRecords());
    assertEquals(0, (int)b.getNumberOfDeletedAndUpdatedRecords());
    
    assertEquals(totalKVs, (int)bb.getNumberOfRecords() + b.getNumberOfRecords());
    assertEquals(totalDataSize, (int)b.getDataSize() + bb.getDataSize());
    byte[] f1 = b.getFirstKey();
    byte[] f2 = bb.getFirstKey();
    assertNotNull(f1); 
    assertNotNull(f2);
    assertTrue (Utils.compareTo(f1, 0, f1.length, f2, 0, f2.length) < 0);
    b.merge(bb, true, true);
    
    assertEquals(0, (int)b.getNumberOfDeletedAndUpdatedRecords());
    assertEquals(totalKVs, (int)b.getNumberOfRecords());
    assertEquals(totalDataSize, (int)b.getDataSize());
    
    scanAndVerify(b);
    System.out.println("testBlockMerge after scanAndVerify");

    b.free();
    System.out.println("testBlockMerge FREE b");

    //TODO: fix this later
//    bb.free();
    System.out.println("testBlockMerge FREE bb");

    System.out.println("testBlockMerge DONE");

    
  }
  
  
  @Test
  public void testCompactionFull() throws RetryOperationException {
    System.out.println("testCompactionFull");

    DataBlock b = getDataBlock();
    ArrayList<Key> keys = fillBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    
    for (Key key: keys) {
      OpResult result = b.delete(key.address, key.size, Long.MAX_VALUE);
      assertEquals(OpResult.OK, result);
    }
    
    assertEquals(0, (int)b.getNumberOfRecords());
    
    for (Key key: keys) {
      long result = b.get(key.address, key.size, Long.MAX_VALUE);
      assertEquals(DataBlock.NOT_FOUND, result);
    }
    
    b.compact(false);
    
    assertEquals( 0, (int)b.getNumberOfRecords());
    assertEquals( 0, (int)b.getNumberOfDeletedAndUpdatedRecords());
    assertTrue (b.getFirstKey() == null);  
    b.free();
    System.out.println("testCompactionFull DONE");

  }
  
  @Test
  public void testCompactionPartial() throws RetryOperationException {
    System.out.println("testCompactionPartial");

    DataBlock b = getDataBlock();
    ArrayList<Key> keys = fillBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    
    Random r = new Random();
    ArrayList<Key> deletedKeys = new ArrayList<Key>();
    for (Key key: keys) {
      if( r.nextDouble() < 0.5) {
        OpResult result = b.delete(key.address, key.size, Long.MAX_VALUE);
        assertEquals(OpResult.OK, result);
        deletedKeys.add(key);
      }
    }
    
    assertEquals(keys.size() - deletedKeys.size(), (int)b.getNumberOfRecords());
    
    for (Key key: deletedKeys) {
      long result = b.get(key.address, key.size, Long.MAX_VALUE);
      assertEquals(DataBlock.NOT_FOUND, result);
    }
    
    b.compact(true);
    
    assertEquals( keys.size() - deletedKeys.size(), (int)b.getNumberOfRecords());
    assertEquals( 0, (int)b.getNumberOfDeletedAndUpdatedRecords());
    b.free();
    System.out.println("testCompactionPartial DONE");


  }
  
  @Test
  public void testOrderedInsertion() throws RetryOperationException, IOException {
    System.out.println("testOrderedInsertion");
    DataBlock b = getDataBlock();
    ArrayList<Key> keys = fillBlock(b);
    System.out.println("Total inserted =" + keys.size());
    scanAndVerify(b, keys);
    b.free();
    System.out.println("testOrderedInsertion DONE");

  }
  
  private void scanAndVerify(DataBlock b) throws RetryOperationException, IOException {
    long buffer ;
    long tmp =0;

    DataBlockScanner bs = DataBlockScanner.getScanner(b, null, null, Long.MAX_VALUE);
    int prevKeySize=0;
    int count = 0;
    while (bs.hasNext()) {
      int keySize = bs.keySize();
      buffer = UnsafeAccess.malloc(keySize);
      bs.key(buffer, keySize);
      bs.next();
      count++;
      if (count > 1) {
        // compare
        int res = Utils.compareTo(tmp, prevKeySize, buffer, keySize);
        assertTrue (res < 0);
        UnsafeAccess.free(tmp);
      }
      tmp = buffer;
      prevKeySize = keySize;
    }
    UnsafeAccess.free(tmp);
    bs.close();
    System.out.println("Scanned ="+ count);
  }

  private void scanAndVerify(DataBlock b, List<Key> keys) 
      throws RetryOperationException, IOException {
    long buffer = 0;
    long tmp = 0;

    DataBlockScanner bs = DataBlockScanner.getScanner(b, null, null, Long.MAX_VALUE);
    int count = 0;
    int prevKeySize = 0;
    while (bs.hasNext()) {
      int keySize = bs.keySize();
      buffer = UnsafeAccess.malloc(keySize);
      bs.key(buffer, keySize);
      assertTrue(contains(buffer,keySize, keys));
      bs.next();
      count++;
      if (count > 1) {
        // compare
        int res = Utils.compareTo(tmp, prevKeySize, buffer, keySize);
        assertTrue (res < 0);
      }
      tmp = buffer;
      prevKeySize = keySize;
    }
    UnsafeAccess.free(tmp);
    bs.close();
  }
  
  private boolean contains(long key, int keySize, List<Key> keys) {
    for (Key k : keys) {
      if (Utils.compareTo(k.address, k.size, key, keySize) == 0) {
        return true;
      }
    }
    return false;
  }
  
  //@Ignore
  @Test
  public void testBlockPutAfterDelete() throws RetryOperationException {
    System.out.println("testBlockPutAfterDelete");

    DataBlock b = getDataBlock();
    ArrayList<Key> keys = fillBlock(b);
    
    int dataSize = b.getDataSize();
    int blockSize = b.getBlockSize();
    int avail = blockSize - dataSize - DataBlock.RECORD_TOTAL_OVERHEAD;
    if (avail >= blockSize/2) {
      System.out.println("Skip test");
      return;
    } else if (avail < 0) {
      avail = 62;
    }
    long key = UnsafeAccess.malloc(avail/2 +1);
    int size = avail/2 + 1;
    
    // Try to insert
    boolean result = b.put(key, size, key, size, 0, 0);
    assertEquals(false, result);
    
    // Delete one record
    Key oneKey = keys.get(0);
        
    OpResult res = b.delete(oneKey.address, oneKey.size, Long.MAX_VALUE);
    dataSize = b.getDataSize();
    blockSize = b.getBlockSize();
    avail = blockSize - dataSize;
    assertEquals(OpResult.OK, res);
    
    // Try insert one more time
    // Try to insert
    int reqSize = DataBlock.mustStoreExternally(size, size)? DataBlock.RECORD_TOTAL_OVERHEAD + 12:
      2 * size + DataBlock.RECORD_TOTAL_OVERHEAD;
    boolean expResult = reqSize < avail ? true: false;
    result = b.put(key, size, key, size, 0, 0);
    assertEquals(expResult, result); 
    b.free();

  }
  
  @Ignore
  @Test
  public void loopNext() throws RetryOperationException, IOException {
    for(int i=1; i < 100000; i++) {
      testOverwriteOnUpdateEnabled();
    }
  }
  
  
  @Test
  public void testOverwriteOnUpdateEnabled() throws RetryOperationException, IOException {
    System.out.println("testOverwriteOnUpdateEnabled");

    DataBlock b = getDataBlock();
    List<Key> keys = fillBlock(b);
    for( Key key: keys) {
      boolean res = b.put(key.address, key.size, key.address, key.size, Long.MAX_VALUE, 0);
      assertTrue(res);
    }
    
    assertEquals(keys.size(), (int)b.getNumberOfRecords());
    assertEquals(0, (int)b.getNumberOfDeletedAndUpdatedRecords());
    
    int toDelete = keys.size()/2;
    
    // Delete  first
    for (int i = 0; i < toDelete; i++) {
      Key key = keys.get(i);
      OpResult res = b.delete(key.address, key.size, Long.MAX_VALUE);
      assertTrue(res == OpResult.OK);

    }
    keys =  keys.subList(toDelete, keys.size());
    assertTrue(keys.size() == b.getNumberOfRecords());
    scanAndVerify(b, keys);
    // Now insert existing keys with val/2
    for (int i = 0; i < keys.size(); i++) {
      Key key = keys.get(i);
      long addr = b.get(key.address, key.size, Long.MAX_VALUE);
      int oldValLen = DataBlock.valueLength(addr);
      boolean res = b.put(key.address, key.size, key.address, key.size/2, Long.MAX_VALUE, 0);
      if (res == false) {
        if (isValidFailure(b, key, key.size/2, oldValLen)) {
          continue;
        } else {
          fail("FAIL");
        }
      }
    }
    verifyGets(b, keys);
    scanAndVerify(b, keys);
    assertEquals(keys.size(), (int)b.getNumberOfRecords());
    assertEquals(0, (int)b.getNumberOfDeletedAndUpdatedRecords());
    // Now insert existing keys with original value
    for (Key key: keys) {
      long addr = b.get(key.address, key.size, Long.MAX_VALUE);
      assertTrue(addr > 0);
      int oldValLen = DataBlock.valueLength(addr);
      boolean res = b.put(key.address, key.size, key.address, key.size, Long.MAX_VALUE, 0);
      if (res == false) {
        if (isValidFailure(b, key, key.size, oldValLen)) {
          continue;
        } else {
          fail("FAIL");
        }
      }
      assertTrue(res);
    }    
    assertEquals(keys.size(), (int)b.getNumberOfRecords());
    assertEquals(0, (int)b.getNumberOfDeletedAndUpdatedRecords());
    scanAndVerify(b, keys);
    b.free();

  }
    
  protected ArrayList<Key> fillBlock (DataBlock b) throws RetryOperationException {
    ArrayList<Key> keys = new ArrayList<Key>();
    Random r = new Random();
    int keyLength = 32;
    boolean result = true;
    while(result == true) {
      byte[] key = new byte[keyLength];
      r.nextBytes(key);
      long ptr = UnsafeAccess.malloc(keyLength);
      UnsafeAccess.copy(key, 0, ptr, keyLength);
      result = b.put(key, 0, key.length, key, 0, key.length, Long.MAX_VALUE, 0);
      if(result) {
        keys.add(new Key(ptr, keyLength));
      }
    }
    System.out.println("M: "+ DataBlock.getTotalAllocatedMemory() +" D:"+DataBlock.getTotalDataSize());
    return keys;
  }
  
  private void verifyGets(DataBlock b, List<Key> keys) {
    for(Key key: keys) {
      long address = b.get(key.address, key.size, Long.MAX_VALUE);
      
      assertTrue(address > 0);
      int klen = DataBlock.keyLength(address);
      int vlen = DataBlock.valueLength(address);
      assertEquals(key.size, klen);
      long buf = UnsafeAccess.malloc(vlen);
      long size = b.get(key.address, key.size, buf, vlen, Long.MAX_VALUE);
      assertEquals(vlen, (int)size);
    }
  }
  private boolean isValidFailure(DataBlock b,  Key key, int valLen, int oldValLen) {
    int dataSize = b.getDataSize();
    int blockSize = b.getBlockSize();
    int newRecSize = key.size + valLen + DataBlock.RECORD_TOTAL_OVERHEAD;
    if (DataBlock.mustStoreExternally(key.size, valLen)) {
      newRecSize = 12 + DataBlock.RECORD_TOTAL_OVERHEAD;
    }
    int oldRecSize = key.size + oldValLen + DataBlock.RECORD_TOTAL_OVERHEAD;
    if (DataBlock.mustStoreExternally(key.size, oldValLen)) {
      oldRecSize = 12 + DataBlock.RECORD_TOTAL_OVERHEAD;
    }
    
    return dataSize + newRecSize -oldRecSize > blockSize;
  }
}
