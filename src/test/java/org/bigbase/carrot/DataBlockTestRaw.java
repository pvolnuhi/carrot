package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Test;

public class DataBlockTestRaw {

  int keyLength = 32;
  int valueLength = 32;
  
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
    ArrayList<Long> keys = fillBlock(b);
    Random r = new Random();

    System.out.println("Total inserted ="+ keys.size());
    int found = 0;
    long start = System.currentTimeMillis();
    int n = 10000000;
    for(int i = 0 ; i < n; i++) {
      int index = r.nextInt(keys.size());
      long keyPtr = keys.get(index);
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
  public void testBlockPutScan() throws RetryOperationException {
    System.out.println("testBlockPutScan");
    DataBlock b = getDataBlock();
    ArrayList<Long> keys = fillBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    long start = System.currentTimeMillis();
    long buffer = UnsafeAccess.malloc(keyLength + valueLength);
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
    b.free();
    System.out.println("Rate = "+(1000d * N * keys.size())/(System.currentTimeMillis() - start) +" RPS");
    System.out.println("testBlockPutScan DONE");

  }
  
  //@Ignore
  @Test
  public void testBlockPutDelete() throws RetryOperationException {
    System.out.println("testBlockPutDelete");

    DataBlock b = getDataBlock();
    ArrayList<Long> keys = fillBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    
    for (Long key: keys) {
      OpResult result = b.delete(key, keyLength, Long.MAX_VALUE);
      assertEquals(OpResult.OK, result);
    }
    
    assertEquals(0, (int)b.getNumberOfRecords());
    for (Long key: keys) {
      long result = b.get(key, keyLength, Long.MAX_VALUE);
      assertEquals(DataBlock.NOT_FOUND, result);
    }
    b.free();
    System.out.println("testBlockPutDelete DONE");

  }
  
  //@Ignore
  @Test
  public void testBlockSplit() throws RetryOperationException {
    System.out.println("testBlockSplit");

    DataBlock b = getDataBlock();
    ArrayList<Long> keys = fillBlock(b);
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
    
    scanAndVerify(bb, keyLength);
    scanAndVerify(b, keyLength);
    b.free();
    bb.free();
    System.out.println("testBlockSplit DONE");

  }
  
  //@Ignore
  @Test
  public void testBlockMerge() throws RetryOperationException {
    System.out.println("testBlockMerge");

    DataBlock b = getDataBlock();
    ArrayList<Long> keys = fillBlock(b);
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
    
    scanAndVerify(b, keyLength);
    System.out.println("testBlockMerge after scanAndVerify");

    b.free();
    System.out.println("testBlockMerge FREE b");

    //TODO: fix this later
    bb.free();
    System.out.println("testBlockMerge FREE bb");

    System.out.println("testBlockMerge DONE");

    
  }
  
  
  @Test
  public void testCompactionFull() throws RetryOperationException {
    System.out.println("testCompactionFull");

    DataBlock b = getDataBlock();
    ArrayList<Long> keys = fillBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    
    for (Long key: keys) {
      OpResult result = b.delete(key, keyLength, Long.MAX_VALUE);
      assertEquals(OpResult.OK, result);
    }
    
    assertEquals(0, (int)b.getNumberOfRecords());
    
    for (Long key: keys) {
      long result = b.get(key, keyLength, Long.MAX_VALUE);
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
    ArrayList<Long> keys = fillBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    
    Random r = new Random();
    ArrayList<Long> deletedKeys = new ArrayList<Long>();
    for (Long key: keys) {
      if( r.nextDouble() < 0.5) {
        OpResult result = b.delete(key, keyLength, Long.MAX_VALUE);
        assertEquals(OpResult.OK, result);
        deletedKeys.add(key);
      }
    }
    
    assertEquals(keys.size() - deletedKeys.size(), (int)b.getNumberOfRecords());
    
    for (Long key: deletedKeys) {
      long result = b.get(key, keyLength, Long.MAX_VALUE);
      assertEquals(DataBlock.NOT_FOUND, result);
    }
    
    b.compact(true);
    
    assertEquals( keys.size() - deletedKeys.size(), (int)b.getNumberOfRecords());
    assertEquals( 0, (int)b.getNumberOfDeletedAndUpdatedRecords());
    b.free();
    System.out.println("testCompactionPartial DONE");


  }
  
  @Test
  public void testOrderedInsertion() throws RetryOperationException {
    System.out.println("testOrderedInsertion");
    DataBlock b = getDataBlock();
    ArrayList<Long> keys = fillBlock(b);
    System.out.println("Total inserted =" + keys.size());
    scanAndVerify(b, keyLength);
    b.free();
    System.out.println("testOrderedInsertion DONE");

  }
  
  private void scanAndVerify(DataBlock b, int keyLength) throws RetryOperationException {
    long buffer = UnsafeAccess.malloc(keyLength);
    long tmp = UnsafeAccess.malloc(keyLength);

    DataBlockScanner bs = DataBlockScanner.getScanner(b, null, null, Long.MAX_VALUE);
    int count = 0;
    while (bs.hasNext()) {
      bs.key(buffer, keyLength);
      bs.next();
      count++;
      if (count > 1) {
        // compare
        int res = Utils.compareTo(tmp, keyLength, buffer, keyLength);
        assertTrue (res < 0);
      }
      UnsafeAccess.copy(buffer, tmp, keyLength);
    }
    UnsafeAccess.free(buffer);
    UnsafeAccess.free(tmp);
    
    System.out.println("Scanned ="+ count);
  }
  
  private void dumpAddress(long addr, int n) {
    for (int i=0; i < n; i++) {
      System.out.print(UnsafeAccess.toByte(addr + i) +" ");
    }
    System.out.println();
  }
  
  private void scanAndVerify(DataBlock b, List<Long> keys) throws RetryOperationException {
    long buffer = UnsafeAccess.malloc(keyLength);
    long tmp = UnsafeAccess.malloc(keyLength);

    DataBlockScanner bs = DataBlockScanner.getScanner(b, null, null, Long.MAX_VALUE);
    int count = 0;
    while (bs.hasNext()) {
      bs.key(buffer, keyLength);
      assertTrue(contains(buffer, keys));
      bs.next();
      count++;
      if (count > 1) {
        // compare
        int res = Utils.compareTo(tmp, keyLength, buffer, keyLength);
        assertTrue (res < 0);
      }
      UnsafeAccess.copy(buffer, tmp, keyLength);
    }
    UnsafeAccess.free(buffer);
    UnsafeAccess.free(tmp);
  }
  
  private boolean contains(long key, List<Long> keys) {
    for (Long k : keys) {
      if (Utils.compareTo(k, keyLength, key, keyLength) == 0) {
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
    ArrayList<Long> keys = fillBlock(b);
    
    byte[] key = new byte[32];
    Random r = new Random();
    r.nextBytes(key);
    long tmp = UnsafeAccess.malloc(keyLength);
    UnsafeAccess.copy(key, 0, tmp, keyLength);
    // Try to insert
    boolean result = b.put(tmp, keyLength, tmp, keyLength, Long.MAX_VALUE, 0);
    assertEquals(false, result);
    
    // Delete one record
    long oneKey = keys.get(0);
    OpResult res = b.delete(oneKey, keyLength, Long.MAX_VALUE);
    
    assertEquals(OpResult.OK, res);
    
    // Try insert one more time
    // Try to insert
    result = b.put(tmp, keyLength, tmp, keyLength, Long.MAX_VALUE, 0 );
    assertEquals(true, result);      
    b.free();
    System.out.println("testBlockPutAfterDelete DONE");

  }
  
  @Test
  public void testOverwriteOnUpdateEnabled() throws RetryOperationException {
    System.out.println("testOverwriteOnUpdateEnabled");

    DataBlock b = getDataBlock();
    List<Long> keys = fillBlock(b);
    for( long key: keys) {
      boolean res = b.put(key, keyLength, key, keyLength, Long.MAX_VALUE, 0);
      assertTrue(res);
    }
    
    assertEquals(keys.size(), (int)b.getNumberOfRecords());
    assertEquals(0, (int)b.getNumberOfDeletedAndUpdatedRecords());
    
    // Delete  5 first
    for (int i = 0; i < 5; i++) {
      long key = keys.get(i);
      OpResult res = b.delete(key, keyLength, Long.MAX_VALUE);
      assertTrue(res == OpResult.OK);

    }
    keys =  keys.subList(5, keys.size());
    assertTrue(keys.size() == b.getNumberOfRecords());
    scanAndVerify(b, keys);
    ArrayList<Long> kkeys = new ArrayList<Long>();
    // Now insert existing keys with val/2
    for (int i = 0; i < 5; i++) {
      long key = keys.get(i);
      b.put(key, keyLength, key, keyLength/2, Long.MAX_VALUE, 0);
      kkeys.add(key);
    }
    assertEquals(keys.size(), (int)b.getNumberOfRecords());
    assertEquals(0, (int)b.getNumberOfDeletedAndUpdatedRecords());
    scanAndVerify(b, keys);
    // Now insert existing keys with original value
    for (long key: kkeys) {
      boolean res = b.put(key, keyLength, key, keyLength, Long.MAX_VALUE, 0);
      assertTrue(res);
    }    
    assertEquals(keys.size(), (int)b.getNumberOfRecords());
    assertEquals(0, (int)b.getNumberOfDeletedAndUpdatedRecords());
    scanAndVerify(b, keys);
    b.free();
    System.out.println("testOverwriteOnUpdateEnabled DONE");

  }
    
  private ArrayList<Long> fillBlock (DataBlock b) throws RetryOperationException {
    ArrayList<Long> keys = new ArrayList<Long>();
    Random r = new Random();

    boolean result = true;
    while(result == true) {
      byte[] key = new byte[32];
      r.nextBytes(key);
      long ptr = UnsafeAccess.malloc(keyLength);
      UnsafeAccess.copy(key, 0, ptr, keyLength);
      result = b.put(key, 0, key.length, key, 0, key.length, Long.MAX_VALUE, 0);
      if(result) {
        keys.add(ptr);
      }
    }
    System.out.println("M: "+ DataBlock.getTotalAllocatedMemory() +" D:"+DataBlock.getTotalDataSize());
    return keys;
  }
  
}
