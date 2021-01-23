package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Test;
import org.junit.Ignore;


public class IndexBlockDirectMemoryTest {
  
  static {
    UnsafeAccess.debug = true;
  }
  
  @Test
  public void testAll() throws RetryOperationException, IOException {

    for (int i = 0; i < 100; i++) {
      System.out.println("\nRun " + (i+1)+"\n");
      testPutGet();
      testPutGetWithCompressionLZ4();
      testPutGetWithCompressionLZ4HC();
      testPutGetDeleteFull();
      testPutGetDeleteFullWithCompressionLZ4();
      testPutGetDeleteFullWithCompressionLZ4HC();
      testPutGetDeletePartial();
      testPutGetDeletePartialWithCompressionLZ4();
      testPutGetDeletePartialWithCompressionLZ4HC();
      testAutomaticDataBlockMerge();
      testAutomaticDataBlockMergeWithCompressionLZ4();
      testAutomaticDataBlockMergeWithCompressionLZ4HC();
      testOverwriteSameValueSize();
      testOverwriteSameValueSizeWithCompressionLZ4();
      testOverwriteSameValueSizeWithCompressionLZ4HC();
      testOverwriteSmallerValueSize();
      testOverwriteSmallerValueSizeWithCompressionLZ4();
      testOverwriteSmallerValueSizeWithCompressionLZ4HC();
      testOverwriteLargerValueSize();
      testOverwriteLargerValueSizeWithCompressionLZ4();
      testOverwriteLargerValueSizeWithCompressionLZ4HC();
    }
    BigSortedMap.printMemoryAllocationStats();
    UnsafeAccess.mallocStats();
  }
  
  protected void freeKeys(ArrayList<Key> keys) {
    for(Key key: keys) {
      UnsafeAccess.free(key.address);
    }
  }
  
  @Ignore
  @Test
  public void testPutGet() {
    System.out.println("testPutGet");  
    IndexBlock ib = getIndexBlock(4096);
    ArrayList<Key> keys = fillIndexBlock(ib);
    for(Key key: keys) {
      long valuePtr = UnsafeAccess.malloc(key.length);
      long size = ib.get(key.address, key.length, valuePtr, key.length, Long.MAX_VALUE);
      assertTrue(size == key.length);
      int res = Utils.compareTo(key.address, key.length, valuePtr, key.length);
      assertEquals(0, res);
      UnsafeAccess.free(valuePtr);
    }
    ib.free();
    freeKeys(keys);
  }
  
  @Ignore
  @Test
  public void testPutGetWithCompressionLZ4() {
    System.out.println("testPutGetWithCompressionLZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    testPutGet();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }

  @Ignore
  @Test
  public void testPutGetWithCompressionLZ4HC() {
    System.out.println("testPutGetWithCompressionLZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    testPutGet();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test 
  public void testAutomaticDataBlockMerge() {
    System.out.println("testAutomaticDataBlockMerge");  
    IndexBlock ib = getIndexBlock(4096);
    ArrayList<Key> keys = fillIndexBlock(ib);
    Utils.sortKeys(keys);

    int before = ib.getNumberOfDataBlock();
    
    // Delete half of records
    
    List<Key> toDelete = keys.subList(0, keys.size()/2);
    for(Key key : toDelete) {
      OpResult res = ib.delete(key.address,  key.length, Long.MAX_VALUE);
      assertTrue(res == OpResult.OK);
    }
    int after = ib.getNumberOfDataBlock();
    System.out.println("Before =" + before + " After=" + after);
    assertTrue(before > after);
    ib.free();
    freeKeys(keys);

  }
  
  @Ignore
  @Test
  public void testAutomaticDataBlockMergeWithCompressionLZ4() {
    System.out.println("testAutomaticDataBlockMergeWithCompressionLZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    testAutomaticDataBlockMerge();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }

  @Ignore
  @Test
  public void testAutomaticDataBlockMergeWithCompressionLZ4HC() {
    System.out.println("testAutomaticDataBlockMergeWithCompressionLZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    testAutomaticDataBlockMerge();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testPutGetDeleteFull() {
    System.out.println("testPutGetDeleteFull");  

    IndexBlock ib = getIndexBlock(4096);
    ArrayList<Key> keys = fillIndexBlock(ib);

    for(Key key: keys) {
      long valuePtr = UnsafeAccess.malloc(key.length);

      long size = ib.get(key.address, key.length, valuePtr, key.length, Long.MAX_VALUE);
      assertTrue(size == key.length);
      int res = Utils.compareTo(key.address, key.length, valuePtr, key.length);
      assertEquals(0, res);
      UnsafeAccess.free(valuePtr);
    }
    
    // now delete all
    List<Key> splitRequires = new ArrayList<Key>();
    //int count = 0;
    for(Key key: keys) {
      //*DEBUG*/System.out.println(count++);
      OpResult result = ib.delete(key.address, key.length, Long.MAX_VALUE);
      if (result == OpResult.SPLIT_REQUIRED) {
        splitRequires.add(key);
        continue;
      }
      assertEquals(OpResult.OK, result);
      // try again
      result = ib.delete(key.address, key.length, Long.MAX_VALUE);
      assertEquals(OpResult.NOT_FOUND, result);
    }
    // Now try get them
    for(Key key: keys) {
      long valuePtr = UnsafeAccess.malloc(key.length);

      long size = ib.get(key.address, key.length, valuePtr, key.length, Long.MAX_VALUE);
      if (splitRequires.contains(key)) {
        assertTrue(size > 0);
      } else {
        assertTrue(size == DataBlock.NOT_FOUND);
      }
      UnsafeAccess.free(valuePtr);
    }
    ib.free();
    freeKeys(keys);

  } 
  
  @Ignore
  @Test
  public void testPutGetDeleteFullWithCompressionLZ4() {
    System.out.println("testPutGetDeleteFullWithCompressionLZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    testPutGetDeleteFull();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  @Ignore
  @Test
  public void testPutGetDeleteFullWithCompressionLZ4HC() {
    System.out.println("testPutGetDeleteFullWithCompressionLZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    testPutGetDeleteFull();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testPutGetDeletePartial() {
    System.out.println("testPutGetDeletePartial");  

    IndexBlock ib = getIndexBlock(4096);
    ArrayList<Key> keys = fillIndexBlock(ib);

    for(Key key: keys) {
      long valuePtr = UnsafeAccess.malloc(key.length);
      long size = ib.get(key.address, key.length, valuePtr, key.length, Long.MAX_VALUE);
      assertTrue(size == key.length);
      int res = Utils.compareTo(key.address, key.length, valuePtr, key.length);
      assertEquals(0, res);
      UnsafeAccess.free(valuePtr);
    }
    
    // now delete some
    List<Key> toDelete = keys.subList(0, keys.size()/2);
    List<Key> splitRequires = new ArrayList<Key>();

    for(Key key: toDelete) {
      
      OpResult result = ib.delete(key.address, key.length, Long.MAX_VALUE);
      if (result == OpResult.SPLIT_REQUIRED) {
        splitRequires.add(key);
        continue;
      }
      assertEquals(OpResult.OK, result);
      // try again
      result = ib.delete(key.address, key.length, Long.MAX_VALUE);
      assertEquals(OpResult.NOT_FOUND, result);
    }
    // Now try get them
    for(Key key: toDelete) {
      long valuePtr = UnsafeAccess.malloc(key.length);
      long size = ib.get(key.address, key.length, valuePtr, key.length, Long.MAX_VALUE);
      if (splitRequires.contains(key)) {
        assertTrue(size > 0);
      } else {
        assertTrue(size == DataBlock.NOT_FOUND);
      }      
      UnsafeAccess.free(valuePtr);
    }
    // Now get the rest
    for(Key key: keys.subList(keys.size()/2, keys.size())) {
      long valuePtr = UnsafeAccess.malloc(key.length);
      long size = ib.get(key.address, key.length, valuePtr, key.length, Long.MAX_VALUE);
      assertTrue(size == key.length);
      int res = Utils.compareTo(key.address, key.length, valuePtr, key.length);
      assertEquals(0, res);
      UnsafeAccess.free(valuePtr);
    }
    
    ib.free();
    freeKeys(keys);

  } 
  
  @Ignore
  @Test
  public void testPutGetDeletePartialWithCompressionLZ4() {
    System.out.println("testPutGetDeletePartialWithCompressionLZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    testPutGetDeletePartial();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testPutGetDeletePartialWithCompressionLZ4HC() {
    System.out.println("testPutGetDeletePartiallWithCompressionLZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    testPutGetDeletePartial();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testOverwriteSameValueSize() throws RetryOperationException, IOException {
    System.out.println("testOverwriteSameValueSize");
    Random r = new Random();
    IndexBlock ib = getIndexBlock(4096);
    ArrayList<Key> keys = fillIndexBlock(ib);
    for( Key key: keys) {
      byte[] value = new byte[key.length];
      r.nextBytes(value);
      long valuePtr = UnsafeAccess.allocAndCopy(value, 0, value.length);
      long buf = UnsafeAccess.malloc(value.length);
      boolean res = ib.put(key.address, key.length, valuePtr, value.length, Long.MAX_VALUE, 0);
      assertTrue(res);
      long size = ib.get(key.address, key.length, buf, value.length, Long.MAX_VALUE);
      assertEquals(value.length, (int)size);
      assertTrue(Utils.compareTo(buf, value.length, valuePtr, value.length) == 0);
      UnsafeAccess.free(valuePtr);
      UnsafeAccess.free(buf);
    }
    scanAndVerify(ib, keys);  
    ib.free();
    freeKeys(keys);

  }
  
  @Ignore
  @Test
  public void testOverwriteSameValueSizeWithCompressionLZ4()
      throws RetryOperationException, IOException {
    System.out.println("testOverwriteSameValueSizeWithCompressionLZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    testOverwriteSameValueSize();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testOverwriteSameValueSizeWithCompressionLZ4HC()
      throws RetryOperationException, IOException {
    System.out.println("testOverwriteSameValueSizeWithCompressionLZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    testOverwriteSameValueSize();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testOverwriteSmallerValueSize() throws RetryOperationException, IOException {
    System.out.println("testOverwriteSmallerValueSize");
    Random r = new Random();
    IndexBlock ib = getIndexBlock(4096);
    ArrayList<Key> keys = fillIndexBlock(ib);
    for( Key key: keys) {
      byte[] value = new byte[key.length - 2];
      r.nextBytes(value);
      long valuePtr = UnsafeAccess.allocAndCopy(value, 0, value.length);
      long buf = UnsafeAccess.malloc(value.length);
      boolean res = ib.put(key.address, key.length, valuePtr, value.length, Long.MAX_VALUE, 0);
      assertTrue(res);
      long size = ib.get(key.address, key.length, buf, value.length, Long.MAX_VALUE);
      assertEquals(value.length, (int)size);
      assertTrue(Utils.compareTo(buf, value.length, valuePtr, value.length) == 0);
      UnsafeAccess.free(valuePtr);
      UnsafeAccess.free(buf);
    }
    scanAndVerify(ib, keys);    
    ib.free();
    freeKeys(keys);

  }
  
  @Ignore
  @Test
  public void testOverwriteSmallerValueSizeWithCompressionLZ4()
      throws RetryOperationException, IOException {
    System.out.println("testOverwriteSmallerValueSizeWithCompressionLZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    testOverwriteSmallerValueSize();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }

  @Ignore
  @Test
  public void testOverwriteSmallerValueSizeWithCompressionLZ4HC()
      throws RetryOperationException, IOException {
    System.out.println("testOverwriteSmallerValueSizeWithCompressionLZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    testOverwriteSmallerValueSize();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testOverwriteLargerValueSize() throws RetryOperationException, IOException {
    System.out.println("testOverwriteLargerValueSize");
    Random r = new Random();
    IndexBlock ib = getIndexBlock(4096);
    ArrayList<Key> keys = fillIndexBlock(ib);
    // Delete half keys
    int toDelete = keys.size()/2;
    for(int i=0; i < toDelete; i++) {
      Key key = keys.remove(0);
      OpResult res = ib.delete(key.address, key.length, Long.MAX_VALUE);
      assertEquals(OpResult.OK, res);
      UnsafeAccess.free(key.address);
    }
    for( Key key: keys) {
      byte[] value = new byte[key.length + 2];
      r.nextBytes(value);      
      long valuePtr = UnsafeAccess.allocAndCopy(value, 0, value.length);
      long buf = UnsafeAccess.malloc(value.length);
      boolean res = ib.put(key.address, key.length, valuePtr, value.length, Long.MAX_VALUE, 0);
      assertTrue(res);
      long size = ib.get(key.address, key.length, buf, value.length, Long.MAX_VALUE);
      assertEquals(value.length, (int)size);
      assertTrue(Utils.compareTo(buf, value.length, valuePtr, value.length) == 0);
      UnsafeAccess.free(valuePtr);
      UnsafeAccess.free(buf);
    }
    scanAndVerify(ib, keys);   
    ib.free();
    freeKeys(keys);
  }

  @Ignore
  @Test
  public void testOverwriteLargerValueSizeWithCompressionLZ4()
      throws RetryOperationException, IOException {
    System.out.println("testOverwriteLargerValueSizeWithCompressionLZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    testOverwriteLargerValueSize();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }

  @Ignore
  @Test
  public void testOverwriteLargerValueSizeWithCompressionLZ4HC()
      throws RetryOperationException, IOException {
    System.out.println("testOverwriteLargerValueSizeWithCompressionLZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    testOverwriteLargerValueSize();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  protected void scanAndVerify(IndexBlock b, List<Key> keys)
      throws RetryOperationException, IOException {
    long buffer = 0;
    long tmp = 0;
    int prevLength = 0;
    IndexBlockScanner is = IndexBlockScanner.getScanner(b, null, null, Long.MAX_VALUE);
    DataBlockScanner bs = null;
    int count = 0;
    while ((bs = is.nextBlockScanner()) != null) {
      while (bs.hasNext()) {
        int len = bs.keySize();
        buffer = UnsafeAccess.malloc(len);
        bs.key(buffer, len);

        boolean result = contains(buffer, len, keys);
        assertTrue(result);
        bs.next();
        count++;
        if (count > 1) {
          // compare
          int res = Utils.compareTo(tmp,  prevLength, buffer, len);
          assertTrue(res < 0);
          UnsafeAccess.free(tmp);
        }
        tmp = buffer;
        prevLength = len;
      }
      bs.close();
    }
    UnsafeAccess.free(tmp);
    is.close();
    assertEquals(keys.size(), count);

  }
  private boolean contains(long key, int size, List<Key> keys) {
    for (Key k : keys) {
      if (Utils.compareTo(k.address, k.length, key, size) == 0) {
        return true;
      }
    }
    return false;
  }
  
  protected IndexBlock getIndexBlock(int size) {
    IndexBlock ib = new IndexBlock(size);
    ib.setFirstIndexBlock();
    return ib;
  }
  
  
  protected ArrayList<Key> fillIndexBlock (IndexBlock b) throws RetryOperationException {
    ArrayList<Key> keys = new ArrayList<Key>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Fill seed=" + seed);
    int kvSize = 32;
    boolean result = true;
    while(result == true) {
      byte[] key = new byte[kvSize];
      r.nextBytes(key);
      long ptr = UnsafeAccess.malloc(kvSize);
      UnsafeAccess.copy(key, 0, ptr, kvSize);
      result = b.put(ptr, kvSize, ptr, kvSize, 0, 0);
      if(result) {
        keys.add(new Key(ptr, kvSize));
      } else {
        UnsafeAccess.free(ptr);
      }
    }
    System.out.println("Number of data blocks="+b.getNumberOfDataBlock() + " "  + " index block data size =" + 
        b.getDataInBlockSize()+" num records=" + keys.size());
    return keys;
  }
}
