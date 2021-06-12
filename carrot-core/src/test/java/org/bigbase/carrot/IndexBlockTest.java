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
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexBlockTest {
  Logger LOG = LoggerFactory.getLogger(IndexBlockTest.class);
  IndexBlock ib ;
  
  static {
    UnsafeAccess.debug = true;
  }
    
  //@Ignore
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

  @Ignore
  @Test
  public void testPutGet() {
    System.out.println("testPutGet");
    ib = getIndexBlock(4096);
    ArrayList<byte[]> keys = fillIndexBlock(ib);
    Utils.sort(keys);
    int count = 0;
    byte[] value = null;
    for (byte[] key : keys) {
      value = new byte[key.length];
      long size = ib.get(key, 0, key.length, value, 0, Long.MAX_VALUE);
      assertEquals(value.length, (int) size);
      int res = Utils.compareTo(key, 0, key.length, value, 0, value.length);
      assertEquals(0, res);
      count ++;
    }
    System.out.println("Verified "+ count+" records");

    ib.free();

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
    ib = getIndexBlock(4096);
    ArrayList<byte[]> keys = fillIndexBlock(ib);
    Utils.sort(keys);

    int before = ib.getNumberOfDataBlock();

    // Delete half of records

    List<byte[]> toDelete = keys.subList(0, keys.size() / 2);
    for (byte[] key : toDelete) {
      OpResult res = ib.delete(key, 0, key.length, Long.MAX_VALUE);
      assertTrue(res == OpResult.OK);
    }
    int after = ib.getNumberOfDataBlock();
    System.out.println("Before =" + before + " After=" + after);
    assertTrue(before > after);
    ib.free();

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
    ib = getIndexBlock(4096);
    ArrayList<byte[]> keys = fillIndexBlock(ib);
    Utils.sort(keys);
    byte[] value = null;
    for (byte[] key : keys) {
      value = new byte[key.length];
      long size = ib.get(key, 0, key.length, value, 0, Long.MAX_VALUE);
      assertTrue(size == value.length);
      int res = Utils.compareTo(key, 0, key.length, value, 0, value.length);
      assertEquals(0, res);

    }

    // now delete all
    List<byte[]> splitRequired = new ArrayList<byte[]>();
    for (byte[] key : keys) {

      OpResult result = ib.delete(key, 0, key.length, Long.MAX_VALUE);

      assertTrue(result != OpResult.NOT_FOUND);
      if (result == OpResult.SPLIT_REQUIRED) {
        // Index block split may be required if key being deleted is a first key in
        // a data block and next key is greater in size
        // TODO: verification of a failure
        splitRequired.add(key);
        continue;
      }
      assertEquals(OpResult.OK, result);
      // try again
      result = ib.delete(key, 0, key.length, Long.MAX_VALUE);
      assertEquals(OpResult.NOT_FOUND, result);
    }
        
    // Now try get them
    for (byte[] key : keys) {
      long size = ib.get(key, 0, key.length, value, 0, Long.MAX_VALUE);
      if (splitRequired.contains(key)) {
        assertTrue(size > 0);
      } else {
        assertTrue(size == DataBlock.NOT_FOUND);
      }
    }
    System.out.println("testPutGetDeleteFull OK");
    ib.free();
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

    ib = getIndexBlock(4096);
    ArrayList<byte[]> keys = fillIndexBlock(ib);
    Utils.sort(keys);
    byte[] value = null;
    System.out.println("INDEX_BLOCK DUMP:");

    for (byte[] key : keys) {
      value = new byte[key.length];
      long size = ib.get(key, 0, key.length, value, 0, Long.MAX_VALUE);
      assertTrue(size == value.length);
      int res = Utils.compareTo(key, 0, key.length, value, 0, value.length);
      assertEquals(0, res);
    }

    // now delete some
    List<byte[]> toDelete = keys.subList(0, keys.size() / 2);
    List<byte[]> splitRequired = new ArrayList<byte[]>();

    for (byte[] key : toDelete) {
      OpResult result = ib.delete(key, 0, key.length, Long.MAX_VALUE);
      assertTrue(result != OpResult.NOT_FOUND);
      if (result == OpResult.SPLIT_REQUIRED) {
        // Index block split may be required if key being deleted is a first key in
        // a data block and next key is greater in size
        // TODO: verification of a failure
        splitRequired.add(key);
        continue;
      }
      assertEquals(OpResult.OK, result);
      // try again
      result = ib.delete(key, 0, key.length, Long.MAX_VALUE);
      assertEquals(OpResult.NOT_FOUND, result);
    }
    // Now try get them
    for (byte[] key : toDelete) {
      value = new byte[key.length];
      long size = ib.get(key, 0, key.length, value, 0, Long.MAX_VALUE);
      if (splitRequired.contains(key)) {
        assertTrue(size > 0);
      } else {
        assertTrue(size == DataBlock.NOT_FOUND);
      }
    }
    // Now get the rest
    for (byte[] key : keys.subList(keys.size() / 2, keys.size())) {
      value = new byte[key.length];

      long size = ib.get(key, 0, key.length, value, 0, Long.MAX_VALUE);
      assertTrue(size == value.length);
      int res = Utils.compareTo(key, 0, key.length, value, 0, value.length);
      assertEquals(0, res);
    }
    ib.free();

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
    ib = getIndexBlock(4096);
    List<byte[]> keys = fillIndexBlock(ib);
    for (byte[] key : keys) {
      byte[] value = new byte[key.length];
      r.nextBytes(value);
      byte[] buf = new byte[value.length];
      boolean res = ib.put(key, 0, key.length, value, 0, value.length, 0, 0);
      assertTrue(res);
      long size = ib.get(key, 0, key.length, buf, 0, Long.MAX_VALUE);
      assertEquals(value.length, (int) size);
      assertTrue(Utils.compareTo(buf, 0, buf.length, value, 0, value.length) == 0);
    }
    scanAndVerify(ib, keys);
    ib.free();

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
    ib = getIndexBlock(4096);
    List<byte[]> keys = fillIndexBlock(ib);
    for (byte[] key : keys) {
      byte[] value = new byte[key.length - 2];
      r.nextBytes(value);
      byte[] buf = new byte[value.length];
      boolean res = ib.put(key, 0, key.length, value, 0, value.length, 0, 0);
      assertTrue(res);
      long size = ib.get(key, 0, key.length, buf, 0, Long.MAX_VALUE);
      assertEquals(value.length, (int) size);
      assertTrue(Utils.compareTo(buf, 0, buf.length, value, 0, value.length) == 0);
    }
    scanAndVerify(ib, keys);
    ib.free();

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
    ib = getIndexBlock(4096);
    List<byte[]> keys = fillIndexBlock(ib);
    // Delete half keys
    int toDelete = keys.size() / 2;
    for (int i = 0; i < toDelete; i++) {
      byte[] key = keys.remove(0);
      OpResult res = ib.delete(key, 0, key.length, Long.MAX_VALUE);
      assertEquals(OpResult.OK, res);
    }

    for (byte[] key : keys) {
      byte[] value = new byte[key.length + 2];
      r.nextBytes(value);
      byte[] buf = new byte[value.length];
      boolean res = ib.put(key, 0, key.length, value, 0, value.length, 0, 0);
      assertTrue(res);
      long size = ib.get(key, 0, key.length, buf, 0, Long.MAX_VALUE);
      assertEquals(value.length, (int) size);
      assertTrue(Utils.compareTo(buf, 0, buf.length, value, 0, value.length) == 0);
    }
    scanAndVerify(ib, keys);
    ib.free();

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

  protected IndexBlock getIndexBlock(int size) {
    IndexBlock ib = new IndexBlock(size);
    ib.setFirstIndexBlock();
    return ib;
  }

  protected ArrayList<byte[]> fillIndexBlock(IndexBlock b) throws RetryOperationException {
    ArrayList<byte[]> keys = new ArrayList<byte[]>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED=" + seed);
    int kvSize = 32;
    boolean result = true;
    while (true) {
      byte[] key = new byte[kvSize];
      r.nextBytes(key);
      result = b.put(key, 0, key.length, key, 0, key.length, 0, 0);
      if (result) {
        keys.add(key);
      } else {
        break;
      }
    }
    System.out.println("Number of data blocks=" + b.getNumberOfDataBlock() + " "
        + " index block data size =" + b.getDataInBlockSize() + " num records=" + keys.size());
    return keys;
  }

  protected void scanAndVerify(IndexBlock b, List<byte[]> keys)
      throws RetryOperationException, IOException {
    byte[] buffer = null;
    byte[] tmp = null;
    IndexBlockDirectMemoryScanner is = IndexBlockDirectMemoryScanner.getScanner(b, 0, 0, 0, 0, Long.MAX_VALUE);
    DataBlockDirectMemoryScanner bs = null;
    int count = 0;
    while ((bs = is.nextBlockScanner()) != null) {
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
      bs.close();
    }
    is.close();
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
}
