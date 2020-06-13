package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.util.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexBlockTest {
  Logger LOG = LoggerFactory.getLogger(IndexBlockTest.class);
  
  
  @Test
  public void testPutGet() {
    System.out.println("testPutGet");  
    IndexBlock ib = getIndexBlock(4096);
    ArrayList<byte[]> keys = fillIndexBlock(ib);
    Utils.sort(keys);
    byte[] value = null;
    for(byte[] key: keys) {
      value = new byte[key.length];
      long size = ib.get(key, 0, key.length, value, 0, Long.MAX_VALUE);
      assertTrue(size == value.length);
      int res = Utils.compareTo(key, 0, key.length, value, 0, value.length);
      assertEquals(0, res);
    }
  }

  @Test
  public void testPutGetDeleteFull() {
    System.out.println("testPutGetDeleteFull");  
    IndexBlock ib = getIndexBlock(4096);
    ArrayList<byte[]> keys = fillIndexBlock(ib);
    Utils.sort(keys);
    byte[] value = null;
    int found = 0;
    for(byte[] key: keys) {
      value = new byte[key.length];
      long size = ib.get(key, 0, key.length, value, 0, Long.MAX_VALUE);
      assertTrue(size == value.length);
      int res = Utils.compareTo(key, 0, key.length, value, 0, value.length);
      assertEquals(0, res);

    }
    
    // now delete all

    found =0;
    List<byte[]> splitRequired = new ArrayList<byte[]>();
    for(byte[] key: keys) {
      
      String shortKey = new String(key);
      shortKey = shortKey.substring(0, Math.min(16, shortKey.length()));
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
    for(byte[] key: keys) {
      long size = ib.get(key, 0, key.length, value, 0, Long.MAX_VALUE);
      if (splitRequired.contains(key)) {
        assertTrue (size > 0);
      } else {
        assertTrue(size == DataBlock.NOT_FOUND);
      }
    }
    System.out.println("testPutGetDeleteFull OK");
  } 
  
  @Test
  public void testPutGetDeletePartial() {
    System.out.println("testPutGetDeletePartial");  

    IndexBlock ib = getIndexBlock(4096);
    ArrayList<byte[]> keys = fillIndexBlock(ib);
    Utils.sort(keys);
    byte[] value = null;
    System.out.println("INDEX_BLOCK DUMP:");  

    for(byte[] key: keys) {
      value = new byte[key.length];
      long size = ib.get(key, 0, key.length, value, 0, Long.MAX_VALUE);
      assertTrue(size == value.length);
      int res = Utils.compareTo(key, 0, key.length, value, 0, value.length);
      assertEquals(0, res);
    }
    
    // now delete some
    List<byte[]> toDelete = keys.subList(0, keys.size()/2);
    List<byte[]> splitRequired = new ArrayList<byte[]>();

    for(byte[] key: toDelete) {
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
    for(byte[] key: toDelete) {
      value = new byte[key.length];
      long size = ib.get(key, 0, key.length, value, 0, Long.MAX_VALUE);
      if (splitRequired.contains(key)) {
        assertTrue (size > 0);
      } else {
        assertTrue(size == DataBlock.NOT_FOUND);
      }
    }
    // Now get the rest
    for(byte[] key: keys.subList(keys.size()/2, keys.size())) {
      value = new byte[key.length];

      long size = ib.get(key, 0, key.length, value, 0, Long.MAX_VALUE);
      assertTrue(size == value.length);
      int res = Utils.compareTo(key, 0, key.length, value, 0, value.length);
      assertEquals(0, res);
    }
  } 
  
  @Test
  public void testOverwriteSameValueSize() throws RetryOperationException, IOException {
    System.out.println("testOverwriteSameValueSize");
    Random r = new Random();
    IndexBlock b = getIndexBlock(4096);
    List<byte[]> keys = fillIndexBlock(b);
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
    
   
    scanAndVerify(b, keys);    

  }
  
  @Test
  public void testOverwriteSmallerValueSize() throws RetryOperationException, IOException {
    System.out.println("testOverwriteSmallerValueSize");
    Random r = new Random();
    IndexBlock b = getIndexBlock(4096);
    List<byte[]> keys = fillIndexBlock(b);
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
    scanAndVerify(b, keys);    

  }
  
  @Test
  public void testOverwriteLargerValueSize() throws RetryOperationException, IOException {
    System.out.println("testOverwriteLargerValueSize");
    Random r = new Random();
    IndexBlock b = getIndexBlock(4096);
    List<byte[]> keys = fillIndexBlock(b);
    // Delete half keys
    int toDelete = keys.size()/2;
    for(int i=0; i < toDelete; i++) {
      byte[] key = keys.remove(0);
      OpResult res = b.delete(key, 0, key.length, Long.MAX_VALUE);
      assertEquals(OpResult.OK, res);
    }

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
    
    scanAndVerify(b, keys);    

  }
  protected IndexBlock getIndexBlock(int size) {
    IndexBlock ib = new IndexBlock(size);
    ib.setFirstIndexBlock();
    return ib;
  }
  
  protected ArrayList<byte[]> fillIndexBlock (IndexBlock b) throws RetryOperationException {
    ArrayList<byte[]> keys = new ArrayList<byte[]>();
    Random r = new Random();
    int kvSize = 32;
    boolean result = true;
    while(true) {
      byte[] key = new byte[kvSize];
      r.nextBytes(key);
      result = b.put(key, 0, key.length, key, 0, key.length, 0, 0);
      if(result) {
        keys.add(key);
      } else {
        break;
      }
    }
    System.out.println("Number of data blocks="+b.getNumberOfDataBlock() + " "  + " index block data size =" + 
        b.getDataSize()+" num records=" + keys.size());
    return keys;
  }
  
  protected void scanAndVerify(IndexBlock b, List<byte[]> keys)
      throws RetryOperationException, IOException {
    byte[] buffer = null;
    byte[] tmp = null;
    IndexBlockScanner is = IndexBlockScanner.getScanner(b, null, null, Long.MAX_VALUE);
    DataBlockScanner bs = null;
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
