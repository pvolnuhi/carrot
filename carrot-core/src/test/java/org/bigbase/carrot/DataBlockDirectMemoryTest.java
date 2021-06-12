package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Ignore;
import org.junit.Test;

public class DataBlockDirectMemoryTest extends DataBlockTestBase {

 
  @Test
  public void testBlockPutGet() throws RetryOperationException {
    System.out.println("testBlockPutGet");
    DataBlock b = getDataBlock();
    ArrayList<Key> keys = fillDataBlock(b);
    Random r = new Random();

    System.out.println("Total inserted ="+ keys.size());
    int found = 0;
    long start = System.currentTimeMillis();
    int n = 1000000;
    for(int i = 0 ; i < n; i++) {
      int index = r.nextInt(keys.size());
      long keyPtr = keys.get(index).address;
      int keyLength = keys.get(index).length;
      long off = b.get(keyPtr,  keyLength, Long.MAX_VALUE);
      int res = Utils.compareTo(DataBlock.keyAddress(off), keyLength, keyPtr, keyLength);
      assertTrue(res == 0);
      if(off > 0) found++;
    }
    assertEquals(n, found);
    System.out.println("Total found ="+ found + " in "+(System.currentTimeMillis() - start) +"ms");
    System.out.println("Rate = "+(1000d * found)/(System.currentTimeMillis() - start) +" RPS");
    //b.free();
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
    ArrayList<Key> keys = fillDataBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    long start = System.currentTimeMillis();
    
    long buffer = UnsafeAccess.malloc(16384);
    int N = 1000000;
    for(int i = 0 ; i < N; i++) {
      DataBlockScanner bs = 
          DataBlockScanner.getScanner(b, 0, 0, 0, 0, Long.MAX_VALUE);
      int count =0;
      while(bs.hasNext()) {
        bs.keyValue(buffer, 0);
        bs.next();
        count++;
      }
      // First block has one system record {0}{0}
      assertEquals(keys.size()+1, count);
      bs.close();

    }
    //b.free();
    System.out.println("Rate = "+(1000d * N * keys.size())/(System.currentTimeMillis() - start) +" RPS");
    System.out.println("testBlockPutScan DONE");

  }
  
  @Test
  public void testBlockPutDelete() throws RetryOperationException {
    System.out.println("testBlockPutDelete");

    DataBlock b = getDataBlock();
    ArrayList<Key> keys = fillDataBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    
    for (Key key: keys) {
      OpResult result = b.delete(key.address, key.length, Long.MAX_VALUE);
      assertEquals(OpResult.OK, result);
    }
    
    // We still expect 1 record left - its system {0}{0}
    assertEquals(1, (int)b.getNumberOfRecords());
    for (Key key: keys) {
      long result = b.get(key.address, key.length, Long.MAX_VALUE);
      assertEquals(DataBlock.NOT_FOUND, result);
    }
    //b.free();
    System.out.println("testBlockPutDelete DONE");

  }
  
  @Test
  public void testBlockSplit() throws RetryOperationException, IOException {
    System.out.println("testBlockSplit");

    DataBlock b = getDataBlock();
    ArrayList<Key> keys = fillDataBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    int totalKVs = keys.size();
    int totalDataSize = b.getDataInBlockSize();
    DataBlock bb = b.split(true);
    
    IndexBlock ib = new IndexBlock(4096);
    bb.register(ib, 0);
    
    assertEquals(0, (int)bb.getNumberOfDeletedAndUpdatedRecords());
    assertEquals(0, (int)b.getNumberOfDeletedAndUpdatedRecords());
    
    assertEquals(totalKVs +1, (int)bb.getNumberOfRecords() + b.getNumberOfRecords());
    assertEquals(totalDataSize, (int)b.getDataInBlockSize() + bb.getDataInBlockSize());
    byte[] f1 = b.getFirstKey();
    byte[] f2 = bb.getFirstKey();
    assertNotNull(f1); 
    assertNotNull(f2);
    assertTrue (Utils.compareTo(f1, 0, f1.length, f2, 0, f2.length) < 0);
    
    scanAndVerify(bb);
    scanAndVerify(b);
    //b.free();
    //bb.free();
    System.out.println("testBlockSplit DONE");

  }
  
  
  @Test
  public void testBlockMerge() throws RetryOperationException, IOException {
    System.out.println("testBlockMerge");

    DataBlock b = getDataBlock();
    ArrayList<Key> keys = fillDataBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    int totalKVs = keys.size();
    int totalDataSize = b.getDataInBlockSize();
    DataBlock bb = b.split(true);
    IndexBlock ib = new IndexBlock(4096);
    bb.register(ib, 0);
    
    assertEquals(0, (int)bb.getNumberOfDeletedAndUpdatedRecords());
    assertEquals(0, (int)b.getNumberOfDeletedAndUpdatedRecords());
    
    // +1 is system key in a first block
    assertEquals(totalKVs +1, (int)bb.getNumberOfRecords() + b.getNumberOfRecords());
    assertEquals(totalDataSize, (int)b.getDataInBlockSize() + bb.getDataInBlockSize());
    byte[] f1 = b.getFirstKey();
    byte[] f2 = bb.getFirstKey();
    assertNotNull(f1); 
    assertNotNull(f2);
    assertTrue (Utils.compareTo(f1, 0, f1.length, f2, 0, f2.length) < 0);
    b.merge(bb, true);
    
    assertEquals(0, (int)b.getNumberOfDeletedAndUpdatedRecords());
    assertEquals(totalKVs +1, (int)b.getNumberOfRecords());
    assertEquals(totalDataSize, (int)b.getDataInBlockSize());
    
    scanAndVerify(b);
    System.out.println("testBlockMerge after scanAndVerify");

   // b.free();
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
    ArrayList<Key> keys = fillDataBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    
    for (Key key: keys) {
      OpResult result = b.delete(key.address, key.length, Long.MAX_VALUE);
      assertEquals(OpResult.OK, result);
    }
    
    assertEquals(1, (int)b.getNumberOfRecords());
    
    for (Key key: keys) {
      long result = b.get(key.address, key.length, Long.MAX_VALUE);
      assertEquals(DataBlock.NOT_FOUND, result);
    }
    
    b.compact(false);
    
    assertEquals( 1, (int)b.getNumberOfRecords());
    assertEquals( 0, (int)b.getNumberOfDeletedAndUpdatedRecords());
    // First key is a system key
    assertTrue (Bytes.compareTo(new byte[] {0}, b.getFirstKey()) == 0);  
    //b.free();
    System.out.println("testCompactionFull DONE");

  }
  
  @Test
  public void testCompactionPartial() throws RetryOperationException {
    System.out.println("testCompactionPartial");

    DataBlock b = getDataBlock();
    ArrayList<Key> keys = fillDataBlock(b);
    System.out.println("Total inserted ="+ keys.size());
    
    Random r = new Random();
    ArrayList<Key> deletedKeys = new ArrayList<Key>();
    for (Key key: keys) {
      if( r.nextDouble() < 0.5) {
        OpResult result = b.delete(key.address, key.length, Long.MAX_VALUE);
        assertEquals(OpResult.OK, result);
        deletedKeys.add(key);
      }
    }
    
    assertEquals(keys.size() - deletedKeys.size() +1, (int)b.getNumberOfRecords());
    
    for (Key key: deletedKeys) {
      long result = b.get(key.address, key.length, Long.MAX_VALUE);
      assertEquals(DataBlock.NOT_FOUND, result);
    }
    
    b.compact(true);
    
    assertEquals( keys.size() - deletedKeys.size() +1, (int)b.getNumberOfRecords());
    assertEquals( 0, (int)b.getNumberOfDeletedAndUpdatedRecords());
    //b.free();
    System.out.println("testCompactionPartial DONE");


  }
  
  @Test
  public void testOrderedInsertion() throws RetryOperationException, IOException {
    System.out.println("testOrderedInsertion");
    DataBlock b = getDataBlock();
    ArrayList<Key> keys = fillDataBlock(b);
    System.out.println("Total inserted =" + keys.size());
    scanAndVerify(b, keys);
    b.free();
    System.out.println("testOrderedInsertion DONE");

  }
  

  
  @Test
  public void testBlockPutAfterDelete() throws RetryOperationException {
    System.out.println("testBlockPutAfterDelete");

    DataBlock b = getDataBlock();
    ArrayList<Key> keys = fillDataBlock(b);
    
    int dataSize = b.getDataInBlockSize();
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
        
    OpResult res = b.delete(oneKey.address, oneKey.length, Long.MAX_VALUE);
    dataSize = b.getDataInBlockSize();
    blockSize = b.getBlockSize();
    avail = blockSize - dataSize;
    assertEquals(OpResult.OK, res);
    
    // Try insert one more time
    // Try to insert
    int reqSize = DataBlock.mustStoreExternally(size, size)? DataBlock.RECORD_TOTAL_OVERHEAD + 12:
      2 * size + DataBlock.RECORD_TOTAL_OVERHEAD;
    boolean expResult = reqSize <= avail ? true: false;
    result = b.put(key, size, key, size, 0, 0);
    assertEquals(expResult, result); 
    //b.free();

  }
  
  @Ignore
  @Test
  public void loopNext() throws RetryOperationException, IOException {
    for(int i=1; i < 100000; i++) {
      testOverwriteOnUpdateEnabled();
    }
  }
  
  @Test
  public void testCompressionDecompression() throws RetryOperationException, IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    System.out.println("testCompression");

    DataBlock b = getDataBlock();
    List<Key> keys = fillDataBlock(b);
    b.compressDataBlockIfNeeded();
    b.decompressDataBlockIfNeeded();
    scanAndVerify(b, keys);
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    
  }
  
  @Test
  public void testFirstKey() throws IOException {
    System.out.println("testFirstKey");

    DataBlock b = getDataBlock();
    List<Key> keys = fillDataBlock(b);
    
    scanAndVerify(b, keys);
    
    DataBlockScanner scanner = 
        DataBlockScanner.getScanner(b, 0, 0, 0, 0, Long.MAX_VALUE);    
    int keySize = scanner.keySize();    
    byte[] key = new byte[keySize];    
    scanner.key(key, 0);
    byte[] kkey = b.getFirstKey();
    assertTrue(Utils.compareTo(key, 0, key.length, kkey, 0, kkey.length) == 0);
    // Close scanner - this will release read lock on a block
    scanner.close();
    
    
    long addr = UnsafeAccess.malloc(keySize);
    UnsafeAccess.copy(key, 0, addr, keySize);
    OpResult res = b.delete(addr, keySize, Long.MAX_VALUE);
    // We can't delete first system key
    assertEquals( OpResult.NOT_FOUND, res);
    scanner = DataBlockScanner.getScanner(b, 0, 0, 0, 0, Long.MAX_VALUE);
    // Skip system key
    scanner.next();
    keySize = scanner.keySize();    
    addr = UnsafeAccess.malloc(keySize);   
    scanner.key(addr, keySize);  
    scanner.close();
    res = b.delete(addr, keySize, Long.MAX_VALUE);
    // We can delete next key
    assertEquals( OpResult.OK, res);    
  }
  
  @Test
  public void testOverwriteSameValueSize() throws RetryOperationException, IOException {
    System.out.println("testOverwriteSameValueSize");
    Random r = new Random();
    DataBlock b = getDataBlock();
    List<Key> keys = fillDataBlock(b);
    for( Key key: keys) {
      byte[] value = new byte[key.length];
      r.nextBytes(value);
      long bufPtr = UnsafeAccess.malloc(value.length);
      long valuePtr = UnsafeAccess.allocAndCopy(value, 0, value.length);
      boolean res = b.put(key.address, key.length, valuePtr, value.length, 0, 0);
      assertTrue(res);
      long size = b.get(key.address, key.length, bufPtr, value.length, Long.MAX_VALUE);
      assertEquals(value.length, (int)size);
      assertTrue(Utils.compareTo(bufPtr, value.length,  valuePtr, value.length) == 0);
      UnsafeAccess.free(valuePtr);
      UnsafeAccess.free(bufPtr);
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
    List<Key> keys = fillDataBlock(b);
    for( Key key: keys) {
      byte[] value = new byte[key.length-2]; // smaller values
      r.nextBytes(value);
      long bufPtr = UnsafeAccess.malloc(value.length);
      long valuePtr = UnsafeAccess.allocAndCopy(value, 0, value.length);
      boolean res = b.put(key.address, key.length, valuePtr, value.length, 0, 0);
      assertTrue(res);
      long size = b.get(key.address, key.length, bufPtr, value.length, Long.MAX_VALUE);
      assertEquals(value.length, (int)size);
      assertTrue(Utils.compareTo(bufPtr, value.length,  valuePtr, value.length) == 0);
      UnsafeAccess.free(valuePtr);
      UnsafeAccess.free(bufPtr);
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
    List<Key> keys = fillDataBlock(b);
    // Delete half keys
    int toDelete = keys.size()/2;
    for(int i=0; i < toDelete; i++) {
      Key key = keys.remove(0);
      OpResult res = b.delete(key.address, key.length, Long.MAX_VALUE);
      assertEquals(OpResult.OK, res);
    }
    assertEquals(keys.size()+1, (int)b.getNumberOfRecords());

    for( Key key: keys) {
      byte[] value = new byte[key.length+2]; // large values
      r.nextBytes(value);
      long bufPtr = UnsafeAccess.malloc(value.length);
      long valuePtr = UnsafeAccess.allocAndCopy(value, 0, value.length);
      boolean res = b.put(key.address,  key.length, valuePtr, value.length, 0, 0);
      assertTrue(res);
      long size = b.get(key.address, key.length, bufPtr, value.length, Long.MAX_VALUE);
      assertEquals(value.length, (int)size);
      assertTrue(Utils.compareTo(bufPtr, value.length, valuePtr, value.length) == 0);
    }
    assertEquals(keys.size()+1, (int)b.getNumberOfRecords());
    assertEquals(0, (int)b.getNumberOfDeletedAndUpdatedRecords());
   
    scanAndVerify(b, keys);    

  }
  
  @Test
  public void testOverwriteOnUpdateEnabled() throws RetryOperationException, IOException {
    System.out.println("testOverwriteOnUpdateEnabled");

    DataBlock b = getDataBlock();
    List<Key> keys = fillDataBlock(b);
    for( Key key: keys) {
      boolean res = b.put(key.address, key.length, key.address, key.length, Long.MAX_VALUE, 0);
      assertTrue(res);
    }
    //Again system record +1
    assertEquals(keys.size() +1, (int)b.getNumberOfRecords());
    assertEquals(0, (int)b.getNumberOfDeletedAndUpdatedRecords());
    
    int toDelete = keys.size()/2;
    
    // Delete  first
    for (int i = 0; i < toDelete; i++) {
      Key key = keys.get(i);
      OpResult res = b.delete(key.address, key.length, Long.MAX_VALUE);
      assertTrue(res == OpResult.OK);

    }
    keys =  keys.subList(toDelete, keys.size());
    assertTrue((keys.size() +1) == b.getNumberOfRecords());
    scanAndVerify(b, keys);
    // Now insert existing keys with val/2
    for (int i = 0; i < keys.size(); i++) {
      Key key = keys.get(i);
      long addr = b.get(key.address, key.length, Long.MAX_VALUE);
      int oldValLen = DataBlock.valueLength(addr);
      boolean res = b.put(key.address, key.length, key.address, key.length/2, Long.MAX_VALUE, 0);
      if (res == false) {
        if (isValidFailure(b, key, key.length/2, oldValLen)) {
          continue;
        } else {
          fail("FAIL");
        }
      }
    }
    verifyGets(b, keys);
    scanAndVerify(b, keys);
    assertEquals(keys.size() +1, (int)b.getNumberOfRecords());
    assertEquals(0, (int)b.getNumberOfDeletedAndUpdatedRecords());
    // Now insert existing keys with original value
    for (Key key: keys) {
      long addr = b.get(key.address, key.length, Long.MAX_VALUE);
      assertTrue(addr > 0);
      int oldValLen = DataBlock.valueLength(addr);
      boolean res = b.put(key.address, key.length, key.address, key.length, Long.MAX_VALUE, 0);
      if (res == false) {
        if (isValidFailure(b, key, key.length, oldValLen)) {
          continue;
        } else {
          fail("FAIL");
        }
      }
      assertTrue(res);
    }    
    assertEquals(keys.size() +1, (int)b.getNumberOfRecords());
    assertEquals(0, (int)b.getNumberOfDeletedAndUpdatedRecords());
    scanAndVerify(b, keys);
    //b.free();

  }
    
  protected ArrayList<Key> fillDataBlock (DataBlock b) throws RetryOperationException {
    ArrayList<Key> keys = new ArrayList<Key>();
    Random r = new Random();
    int keyLength = 32;
    boolean result = true;
    while(result == true) {
      byte[] key = new byte[keyLength];
      r.nextBytes(key);
      long ptr = UnsafeAccess.malloc(keyLength);
      UnsafeAccess.copy(key, 0, ptr, keyLength);
      result = b.put(key, 0, key.length, key, 0, key.length, 0, 0);
      if(result) {
        keys.add(new Key(ptr, keyLength));
      }
    }
    System.out.println("M: "+ BigSortedMap.getTotalAllocatedMemory() +" D:"+BigSortedMap.getTotalDataSize());
    return keys;
  }
  
  protected void scanAndVerify(DataBlock b, List<Key> keys) 
      throws RetryOperationException, IOException {
    long buffer = 0;
    long tmp = 0;

    DataBlockScanner bs = 
        DataBlockScanner.getScanner(b, 0, 0, 0, 0, Long.MAX_VALUE);
    int count = 0;
    int prevKeySize = 0;
    // skip first system key-value
    bs.next();
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
  
  protected void verifyGets(DataBlock b, List<Key> keys) {
    for(Key key: keys) {
      long address = b.get(key.address, key.length, Long.MAX_VALUE);
      
      assertTrue(address > 0);
      int klen = DataBlock.keyLength(address);
      int vlen = DataBlock.valueLength(address);
      assertEquals(key.length, klen);
      long buf = UnsafeAccess.malloc(vlen);
      long size = b.get(key.address, key.length, buf, vlen, Long.MAX_VALUE);
      assertEquals(vlen, (int)size);
    }
  }
}
