package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Test;

public class IndexBlockTestRaw {
  
  int kvSize = 32;
  
  @Test
  public void testPutGet() {
    System.out.println("testPutGet");  

    IndexBlock ib = getIndexBlock(4096);
    ArrayList<Long> keys = fillIndexBlock(ib);

    long valuePtr = UnsafeAccess.malloc(kvSize);

    System.out.println("INDEX_BLOCK");  

    for(long keyPtr: keys) {
      long size = ib.get(keyPtr, kvSize, valuePtr, kvSize, Long.MAX_VALUE);
      assertTrue(size == kvSize);
      int res = Utils.compareTo(keyPtr, kvSize, valuePtr, kvSize);
      assertEquals(0, res);
    }
    System.out.println("testPutGet OK");  
    
  }

  @Test
  public void testPutGetDeleteFull() {
    System.out.println("testPutGetDeleteFull");  

    IndexBlock ib = getIndexBlock(4096);
    ArrayList<Long> keys = fillIndexBlock(ib);

    long valuePtr = UnsafeAccess.malloc(kvSize);

    System.out.println("INDEX_BLOCK DUMP:");  

    for(long keyPtr: keys) {
      long size = ib.get(keyPtr, kvSize, valuePtr, kvSize, Long.MAX_VALUE);
      assertTrue(size == kvSize);
      int res = Utils.compareTo(keyPtr, kvSize, valuePtr, kvSize);
      assertEquals(0, res);
    }
    
    // now delete all

    for(long keyPtr: keys) {
      
      OpResult result = ib.delete(keyPtr, kvSize, Long.MAX_VALUE);
      assertEquals(OpResult.OK, result);
      // try again
      result = ib.delete(keyPtr, kvSize, Long.MAX_VALUE);
      assertEquals(OpResult.NOT_FOUND, result);
    }
    // Now try get them
    for(long keyPtr: keys) {
      long size = ib.get(keyPtr, kvSize, valuePtr, kvSize, Long.MAX_VALUE);
      assertTrue(size == DataBlock.NOT_FOUND);
    }
    System.out.println("testPutGetDeleteFull OK");
  } 
  
  @Test
  public void testPutGetDeletePartial() {
    System.out.println("testPutGetDeletePartial");  

    IndexBlock ib = getIndexBlock(4096);
    ArrayList<Long> keys = fillIndexBlock(ib);

    long valuePtr = UnsafeAccess.malloc(kvSize);

    System.out.println("INDEX_BLOCK DUMP:");  

    for(long keyPtr: keys) {
      long size = ib.get(keyPtr, kvSize, valuePtr, kvSize, Long.MAX_VALUE);
      assertTrue(size == kvSize);
      int res = Utils.compareTo(keyPtr, kvSize, valuePtr, kvSize);
      assertEquals(0, res);
    }
    
    // now delete some
    List<Long> toDelete = keys.subList(0, keys.size()/2);
    
   for(long keyPtr: toDelete) {
      
      OpResult result = ib.delete(keyPtr, kvSize, Long.MAX_VALUE);
      assertEquals(OpResult.OK, result);
      // try again
      result = ib.delete(keyPtr, kvSize, Long.MAX_VALUE);
      assertEquals(OpResult.NOT_FOUND, result);
    }
    // Now try get them
    for(long keyPtr: toDelete) {
      long size = ib.get(keyPtr, kvSize, valuePtr, kvSize, Long.MAX_VALUE);
      assertTrue(size == DataBlock.NOT_FOUND);
    }
    // Now get the rest
    for(long keyPtr: keys.subList(keys.size()/2, keys.size())) {
      long size = ib.get(keyPtr, kvSize, valuePtr, kvSize, Long.MAX_VALUE);
      assertTrue(size == kvSize);
      int res = Utils.compareTo(keyPtr, kvSize, valuePtr, kvSize);
      assertEquals(0, res);
    }
    System.out.println("testPutGetDeletePartial OK");
  } 
  
  
  private IndexBlock getIndexBlock(int size) {
    IndexBlock ib = new IndexBlock(size);
    byte[] kk = new byte[] { (byte) 0};
    ib.put(kk, 0, kk.length, kk, 0, kk.length, Long.MAX_VALUE, Op.DELETE.ordinal());
    return ib;
  }
  
  
  private ArrayList<Long> fillIndexBlock (IndexBlock b) throws RetryOperationException {
    ArrayList<Long> keys = new ArrayList<Long>();
    Random r = new Random();

    boolean result = true;
    while(result == true) {
      byte[] key = new byte[32];
      r.nextBytes(key);
      long ptr = UnsafeAccess.malloc(kvSize);
      UnsafeAccess.copy(key, 0, ptr, kvSize);
      result = b.put(key, 0, key.length, key, 0, key.length, Long.MAX_VALUE, 0);
      if(result) {
        keys.add(ptr);
      }
    }
    System.out.println("Number of data blocks="+b.getNumberOfDataBlock() + " "  + " index block data size =" + 
        b.getDataSize()+" num records=" + keys.size());
    return keys;
  }
}
