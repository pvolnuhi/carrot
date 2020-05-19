package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.util.Bytes;
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
    //System.out.println("INDEX_BLOCK DUMP:");  
    for(byte[] key: keys) {
      value = new byte[key.length];
      //System.out.println("Get found"+(++found) +" "+ Bytes.toHex(key));  

      long size = ib.get(key, 0, key.length, value, 0, Long.MAX_VALUE);
      assertTrue(size == value.length);
      int res = Utils.compareTo(key, 0, key.length, value, 0, value.length);
      assertEquals(0, res);

    }
    
    // now delete all

    found =0;
    for(byte[] key: keys) {
      
      OpResult result = ib.delete(key, 0, key.length, Long.MAX_VALUE);
      if (result != OpResult.OK) {
        System.err.println("Not found for delete: "+(++found) +" "+ Bytes.toHex(key));  
        long size = ib.get(key, 0, key.length, value, 0, Long.MAX_VALUE);
        if (size != key.length) {
          fail("Get failed with return=" + size);
        } else {
          System.err.println("Get OK with return=" + size);
        }
        result = ib.delete(key, 0, key.length, Long.MAX_VALUE);
      }
      assertEquals(OpResult.OK, result);
      //System.out.println("Deleted "+(++found) +" "+ Bytes.toHex(key));  

      // try again
      result = ib.delete(key, 0, key.length, Long.MAX_VALUE);
      assertEquals(OpResult.NOT_FOUND, result);
    }
    // Now try get them
    for(byte[] key: keys) {
      long size = ib.get(key, 0, key.length, value, 0, Long.MAX_VALUE);
      assertTrue(size == DataBlock.NOT_FOUND);
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
    
    for(byte[] key: toDelete) {
      OpResult result = ib.delete(key, 0, key.length, Long.MAX_VALUE);
      assertEquals(OpResult.OK, result);
      // try again
      result = ib.delete(key, 0, key.length, Long.MAX_VALUE);
      assertEquals(OpResult.NOT_FOUND, result);
    }
    // Now try get them
    for(byte[] key: toDelete) {
      value = new byte[key.length];
      long size = ib.get(key, 0, key.length, value, 0, Long.MAX_VALUE);
      assertTrue(size == DataBlock.NOT_FOUND);
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
  
  
  private IndexBlock getIndexBlock(int size) {
    IndexBlock ib = new IndexBlock(size);
    byte[] kk = new byte[] { (byte) 0};
    ib.put(kk, 0, kk.length, kk, 0, kk.length, Long.MAX_VALUE, Op.DELETE.ordinal());
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
  
}
