package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
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
  
  @Test
  public void testOverwriteSameValueSize() throws RetryOperationException, IOException {
    System.out.println("testOverwriteSameValueSize");
    Random r = new Random();
    IndexBlock b = getIndexBlock(4096);
    List<Key> keys = fillIndexBlock(b);
    for( Key key: keys) {
      byte[] value = new byte[key.size];
      r.nextBytes(value);
      long valuePtr = UnsafeAccess.allocAndCopy(value, 0, value.length);
      long buf = UnsafeAccess.malloc(value.length);
      boolean res = b.put(key.address, key.size, valuePtr, value.length, Long.MAX_VALUE, 0);
      assertTrue(res);
      long size = b.get(key.address, key.size, buf, value.length, Long.MAX_VALUE);
      assertEquals(value.length, (int)size);
      assertTrue(Utils.compareTo(buf, value.length, valuePtr, value.length) == 0);
      UnsafeAccess.free(valuePtr);
      UnsafeAccess.free(buf);
    }
    scanAndVerify(b, keys);    
  }
  
  @Test
  public void testOverwriteSmallerValueSize() throws RetryOperationException, IOException {
    System.out.println("testOverwriteSmallerValueSize");
    Random r = new Random();
    IndexBlock b = getIndexBlock(4096);
    List<Key> keys = fillIndexBlock(b);
    for( Key key: keys) {
      byte[] value = new byte[key.size-2];
      r.nextBytes(value);
      long valuePtr = UnsafeAccess.allocAndCopy(value, 0, value.length);
      long buf = UnsafeAccess.malloc(value.length);
      boolean res = b.put(key.address, key.size, valuePtr, value.length, Long.MAX_VALUE, 0);
      assertTrue(res);
      long size = b.get(key.address, key.size, buf, value.length, Long.MAX_VALUE);
      assertEquals(value.length, (int)size);
      assertTrue(Utils.compareTo(buf, value.length, valuePtr, value.length) == 0);
      UnsafeAccess.free(valuePtr);
      UnsafeAccess.free(buf);
    }
    scanAndVerify(b, keys);    

  }
  
  @Test
  public void testOverwriteLargerValueSize() throws RetryOperationException, IOException {
    System.out.println("testOverwriteLargerValueSize");
    Random r = new Random();
    IndexBlock b = getIndexBlock(4096);
    List<Key> keys = fillIndexBlock(b);
    // Delete half keys
    int toDelete = keys.size()/2;
    for(int i=0; i < toDelete; i++) {
      /*DEBUG*/ System.out.println(i);
      Key key = keys.remove(0);
      OpResult res = b.delete(key.address, key.size, Long.MAX_VALUE);
      assertEquals(OpResult.OK, res);
    }

    for( Key key: keys) {
      byte[] value = new byte[key.size + 2];
      r.nextBytes(value);      
      long valuePtr = UnsafeAccess.allocAndCopy(value, 0, value.length);
      long buf = UnsafeAccess.malloc(value.length);
      boolean res = b.put(key.address, key.size, valuePtr, value.length, Long.MAX_VALUE, 0);
      assertTrue(res);
      long size = b.get(key.address, key.size, buf, value.length, Long.MAX_VALUE);
      assertEquals(value.length, (int)size);
      assertTrue(Utils.compareTo(buf, value.length, valuePtr, value.length) == 0);
      UnsafeAccess.free(valuePtr);
      UnsafeAccess.free(buf);
    }
    scanAndVerify(b, keys);    
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
      if (Utils.compareTo(k.address, k.size, key, size) == 0) {
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
