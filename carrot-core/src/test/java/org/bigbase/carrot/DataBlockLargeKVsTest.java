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
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.util.Key;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Test;

public class DataBlockLargeKVsTest extends DataBlockTest{

  
  protected ArrayList<Key> fillDataBlock (DataBlock b) throws RetryOperationException {
    ArrayList<Key> keys = new ArrayList<Key>();
    Random r = new Random();
    int maxSize = 4096;
    boolean result = true;
    while(result == true) {
      int len = r.nextInt(maxSize) + 1;
      byte[] key = new byte[len];
      r.nextBytes(key);
      long ptr = UnsafeAccess.malloc(len);
      UnsafeAccess.copy(key, 0, ptr, len);
      result = b.put(key, 0, key.length, key, 0, key.length,  -1);
      if(result) {
        keys.add(new Key(ptr, len));
      }
    }
    System.out.println("M: "+ BigSortedMap.getTotalAllocatedMemory() +" D:"+BigSortedMap.getTotalDataSize());
    return keys;
  }
  
  /**
   * 
   * 1. K & V are both in data block - we do not test this
   * 2. K & V both external, reducing value size should keep them both external
   * 3. K  is in data block , V  is external, reducing V size to 12 bytes and less will guarantee
   *    that there will be no overflow in a data block and new V will be kept in a data block 
   */
  @Test
  public void testOverwriteSmallerValueSize() throws RetryOperationException, IOException {
    System.out.println("testOverwriteSmallerValueSize- Large KVs");
    for (int i = 0; i < 1000; i++) {
      Random r = new Random();
      DataBlock b = getDataBlock();
      List<Key> keys = fillDataBlock(b);
      for (Key key : keys) {
        int keySize = key.length;
        int valueSize = 0;
        DataBlock.AllocType type = DataBlock.getAllocType(keySize, keySize);
        if (type == DataBlock.AllocType.EMBEDDED) {
          continue;
        } else if (type == DataBlock.AllocType.EXT_KEY_VALUE) {
          valueSize = keySize / 2;
        } else { // DataBlock.AllocType.EXT_VALUE
          valueSize = 12;
        }
        byte[] value = new byte[valueSize]; // smaller values
        r.nextBytes(value);
        long bufPtr = UnsafeAccess.malloc(value.length);
        long valuePtr = UnsafeAccess.allocAndCopy(value, 0, value.length);
        boolean res = b.put(key.address, key.length, valuePtr, value.length, 0, 0);
        assertTrue(res);
        long size = b.get(key.address, key.length, bufPtr, value.length, Long.MAX_VALUE);
        assertEquals(value.length, (int) size);
        assertTrue(Utils.compareTo(bufPtr, value.length, valuePtr, value.length) == 0);
        UnsafeAccess.free(valuePtr);
        UnsafeAccess.free(bufPtr);
      }
      assertEquals(keys.size() + 1, (int) b.getNumberOfRecords());
      scanAndVerify(b, keys);
      b.free();
    }
  }
  /**
   * 
   * 1. K & V are both in data block and > 12 - push V out of data block
   * 2. K & V both external, increasing value size should keep them both external
   * 3. K  is in data block , V  is external, increasing V size is safe 
   */
  @Test
  public void testOverwriteLargerValueSize() throws RetryOperationException, IOException {
    System.out.println("testOverwriteLargerValueSize- Large KVs");
    
    for (int i = 0; i < 1000; i++) {
      Random r = new Random();
      DataBlock b = getDataBlock();
      List<Key> keys = fillDataBlock(b);
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
        } else { // VALUE is outside data block, increasing it will keep it outside
          valueSize = keySize * 2;
        }
        byte[] value = new byte[valueSize]; // large values
        r.nextBytes(value);
        long bufPtr = UnsafeAccess.malloc(value.length);
        long valuePtr = UnsafeAccess.allocAndCopy(value, 0, value.length);
        boolean res = b.put(key.address, key.length, valuePtr, value.length, 0, 0);
        assertTrue(res);
        long size = b.get(key.address, key.length, bufPtr, value.length, Long.MAX_VALUE);
        assertEquals(value.length, (int) size);
        assertTrue(Utils.compareTo(bufPtr, value.length, valuePtr, value.length) == 0);
      }
      assertEquals(keys.size() + 1, (int) b.getNumberOfRecords());
      scanAndVerify(b, keys);
      b.free();
    }
  }
}
