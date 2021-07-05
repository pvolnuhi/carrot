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

import java.util.ArrayList;
import java.util.Random;

import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.Key;
import org.bigbase.carrot.util.UnsafeAccess;

public class IndexBlockScannerLargeKVsTest extends IndexBlockScannerTest{

  protected ArrayList<Key> fillIndexBlock (IndexBlock b) throws RetryOperationException {
    ArrayList<Key> keys = new ArrayList<Key>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("FILL seed=" + seed);
    int maxSize = 2048;
    boolean result = true;
    while(true) {
      int len = r.nextInt(maxSize - 2) + 2;
      byte[] key = new byte[len];
      r.nextBytes(key);
      key = Bytes.toHex(key).getBytes();
      len = key.length;
      long ptr = UnsafeAccess.malloc(len);
      UnsafeAccess.copy(key, 0,  ptr, len);
      result = b.put(ptr, len, ptr, len, 0, 0);
      if(result) {
        keys.add( new Key(ptr, len));        
      } else {
        UnsafeAccess.free(ptr);
        break;
      }
    }
    System.out.println("Number of data blocks="+b.getNumberOfDataBlock()  + " index block data size =" + 
        b.getDataInBlockSize()+" num records=" + keys.size());
    //b.dumpIndexBlockExt();
    return keys;
  }
}
