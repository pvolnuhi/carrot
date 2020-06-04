package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Test;

public class IndexBlockDirectMemoryTest {
  
  @Test
  public void testPutGet() {
    System.out.println("testPutGet");  

    IndexBlock ib = getIndexBlock(4096);
    ArrayList<Key> keys = fillIndexBlock(ib);

    for(Key key: keys) {
      long valuePtr = UnsafeAccess.malloc(key.size);
      long size = ib.get(key.address, key.size, valuePtr, key.size, Long.MAX_VALUE);
      assertTrue(size == key.size);
      int res = Utils.compareTo(key.address, key.size, valuePtr, key.size);
      assertEquals(0, res);
      UnsafeAccess.free(valuePtr);
    }
    
  }

  @Test
  public void testPutGetDeleteFull() {
    System.out.println("testPutGetDeleteFull");  

    IndexBlock ib = getIndexBlock(4096);
    ArrayList<Key> keys = fillIndexBlock(ib);

    for(Key key: keys) {
      long valuePtr = UnsafeAccess.malloc(key.size);

      long size = ib.get(key.address, key.size, valuePtr, key.size, Long.MAX_VALUE);
      assertTrue(size == key.size);
      int res = Utils.compareTo(key.address, key.size, valuePtr, key.size);
      assertEquals(0, res);
      UnsafeAccess.free(valuePtr);
    }
    
    // now delete all
    List<Key> splitRequires = new ArrayList<Key>();
    for(Key key: keys) {
      
      OpResult result = ib.delete(key.address, key.size, Long.MAX_VALUE);
      if (result == OpResult.SPLIT_REQUIRED) {
        splitRequires.add(key);
        continue;
      }
      assertEquals(OpResult.OK, result);
      // try again
      result = ib.delete(key.address, key.size, Long.MAX_VALUE);
      assertEquals(OpResult.NOT_FOUND, result);
    }
    // Now try get them
    for(Key key: keys) {
      long valuePtr = UnsafeAccess.malloc(key.size);

      long size = ib.get(key.address, key.size, valuePtr, key.size, Long.MAX_VALUE);
      if (splitRequires.contains(key)) {
        assertTrue(size > 0);
      } else {
        assertTrue(size == DataBlock.NOT_FOUND);
      }
      UnsafeAccess.free(valuePtr);
    }
  } 
  
  @Test
  public void testPutGetDeletePartial() {
    System.out.println("testPutGetDeletePartial");  

    IndexBlock ib = getIndexBlock(4096);
    ArrayList<Key> keys = fillIndexBlock(ib);

    for(Key key: keys) {
      long valuePtr = UnsafeAccess.malloc(key.size);
      long size = ib.get(key.address, key.size, valuePtr, key.size, Long.MAX_VALUE);
      assertTrue(size == key.size);
      int res = Utils.compareTo(key.address, key.size, valuePtr, key.size);
      assertEquals(0, res);
      UnsafeAccess.free(valuePtr);
    }
    
    // now delete some
    List<Key> toDelete = keys.subList(0, keys.size()/2);
    List<Key> splitRequires = new ArrayList<Key>();

    for(Key key: toDelete) {
      
      OpResult result = ib.delete(key.address, key.size, Long.MAX_VALUE);
      if (result == OpResult.SPLIT_REQUIRED) {
        splitRequires.add(key);
        continue;
      }
      assertEquals(OpResult.OK, result);
      // try again
      result = ib.delete(key.address, key.size, Long.MAX_VALUE);
      assertEquals(OpResult.NOT_FOUND, result);
    }
    // Now try get them
    for(Key key: toDelete) {
      long valuePtr = UnsafeAccess.malloc(key.size);
      long size = ib.get(key.address, key.size, valuePtr, key.size, Long.MAX_VALUE);
      if (splitRequires.contains(key)) {
        assertTrue(size > 0);
      } else {
        assertTrue(size == DataBlock.NOT_FOUND);
      }      
      UnsafeAccess.free(valuePtr);
    }
    // Now get the rest
    for(Key key: keys.subList(keys.size()/2, keys.size())) {
      long valuePtr = UnsafeAccess.malloc(key.size);
      long size = ib.get(key.address, key.size, valuePtr, key.size, Long.MAX_VALUE);
      assertTrue(size == key.size);
      int res = Utils.compareTo(key.address, key.size, valuePtr, key.size);
      assertEquals(0, res);
      UnsafeAccess.free(valuePtr);
    }
  } 
  
  
  private IndexBlock getIndexBlock(int size) {
    IndexBlock ib = new IndexBlock(size);
    ib.setFirstIndexBlock();
    return ib;
  }
  
  
  protected ArrayList<Key> fillIndexBlock (IndexBlock b) throws RetryOperationException {
    ArrayList<Key> keys = new ArrayList<Key>();
    Random r = new Random();
    int kvSize = 32;
    boolean result = true;
    while(result == true) {
      byte[] key = new byte[kvSize];
      r.nextBytes(key);
      long ptr = UnsafeAccess.malloc(kvSize);
      UnsafeAccess.copy(key, 0, ptr, kvSize);
      result = b.put(key, 0, key.length, key, 0, key.length, Long.MAX_VALUE, 0);
      if(result) {
        keys.add(new Key(ptr, kvSize));
      }
    }
    System.out.println("Number of data blocks="+b.getNumberOfDataBlock() + " "  + " index block data size =" + 
        b.getDataSize()+" num records=" + keys.size());
    return keys;
  }
}
