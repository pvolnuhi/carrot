/**
 *    Copyright (C) 2021-present Carrot, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 */
package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.bigbase.carrot.util.Key;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Ignore;
import org.junit.Test;

public class IndexBlockLargeKVsTest extends IndexBlockTest{

  @Ignore
  @Test 
  public void testAutomaticDataBlockMerge() {
  
  }
  /**
   * 
   * 1. K & V are both in data block - we do not test this
   * 2. K & V both external, reducing value size should keep them both external
   * 3. K  is in data block , V  is external, reducing V size to 12 bytes and less will guarantee
   *    that there will be no overflow in a data block and new V will be kept in a data block 
   */
  @Ignore
  @Test
  public void testOverwriteSmallerValueSize() throws RetryOperationException, IOException {
    System.out.println("testOverwriteSmallerValueSize - LargeKVs");
    for (int i = 0; i < 1; i++) {
      Random r = new Random();
      IndexBlock b = getIndexBlock(4096);
      ArrayList<Key> keys = fillIndexBlock(b);
      System.out.println("KEYS ="+ keys.size());
      for (Key key : keys) {
        int keySize = key.length;
        int valueSize = 0;
        DataBlock.AllocType type = DataBlock.getAllocType(keySize, keySize);
        if (type == DataBlock.AllocType.EMBEDDED) {
          continue;
        } else if (type == DataBlock.AllocType.EXT_KEY_VALUE) {
          valueSize = keySize/2 ;
        } else { // DataBlock.AllocType.EXT_VALUE
          valueSize = 12;
        }
        byte[] value = new byte[valueSize];
        r.nextBytes(value);
        long valuePtr = UnsafeAccess.allocAndCopy(value, 0, valueSize);
        long buf = UnsafeAccess.malloc(valueSize);
        boolean res = b.put(key.address, keySize, valuePtr, valueSize, Long.MAX_VALUE, 0);
        assertTrue(res);
        long size = b.get(key.address, keySize, buf, valueSize, Long.MAX_VALUE);
        assertEquals(valueSize, (int) size);
        assertTrue(Utils.compareTo(buf, valueSize, valuePtr,  valueSize) == 0);
        UnsafeAccess.free(valuePtr);
        UnsafeAccess.free(buf);
      }
      scanAndVerify(b, keys);
      b.free();
      freeKeys(keys);
    }

  }
  /**
   * 
   * 1. K & V are both in data block and > 12 - push V out of data block
   * 2. K & V both external, increasing value size should keep them both external
   * 3. K  is in data block , V  is external, increasing V size is safe 
   */
  @Ignore
  @Test
  public void testOverwriteLargerValueSize() throws RetryOperationException, IOException {
    System.out.println("testOverwriteLargerValueSize- LargeKVs");
    for (int i = 0; i < 1; i++) {
      Random r = new Random();
      
      IndexBlock b = getIndexBlock(4096);
      ArrayList<Key> keys = fillIndexBlock(b);
      for (Key key : keys) {
        int keySize = key.length;
        int valueSize = 0;
        DataBlock.AllocType type = DataBlock.getAllocType(keySize, keySize);
        if (type == DataBlock.AllocType.EMBEDDED) {
          if (keySize < 12) {
            continue;
          } else {
            valueSize = 2067;
          }
          
        } else  { // VALUE is outside data block, increasing it will keep it outside
          valueSize = keySize * 2 ;
        } 
        byte[] value = new byte[valueSize];
        r.nextBytes(value);
        long valuePtr = UnsafeAccess.allocAndCopy(value, 0, valueSize);
        long buf = UnsafeAccess.malloc(valueSize);
        boolean res = b.put(key.address, keySize, valuePtr, valueSize, Long.MAX_VALUE, 0);
        assertTrue(res);
        long size = b.get(key.address, keySize, buf, valueSize, Long.MAX_VALUE);
        assertEquals(valueSize, (int) size);
        assertTrue(Utils.compareTo(buf, valueSize, valuePtr, valueSize) == 0);
        UnsafeAccess.free(valuePtr);
        UnsafeAccess.free(buf);
      }
      scanAndVerify(b, keys);
      b.free();
      freeKeys(keys);
    }
  }
  
  
  protected ArrayList<Key> fillIndexBlock (IndexBlock b) throws RetryOperationException {
    ArrayList<Key> keys = new ArrayList<Key>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    /*DEBUG*/ System.out.println("FILL SEED="  + seed);
    int maxSize = 4096;
    boolean result = true;
    int count = 0;
    while(result == true) {
      /*DEBUG*/ System.out.println(count++);
      byte[] key = new byte[r.nextInt(maxSize) + 2];
      r.nextBytes(key);
      long ptr = UnsafeAccess.malloc(key.length);
      UnsafeAccess.copy(key, 0, ptr, key.length);
      result = b.put(key, 0, key.length, key, 0, key.length, -1);
      if(result) {
        keys.add(new Key(ptr, key.length));
      } else {
        UnsafeAccess.free(ptr);
      }
    }
    System.out.println("Number of data blocks="+b.getNumberOfDataBlock() + " "  + " index block data size =" + 
        b.getDataInBlockSize()+" num records=" + keys.size());
    return keys;
  }
}
