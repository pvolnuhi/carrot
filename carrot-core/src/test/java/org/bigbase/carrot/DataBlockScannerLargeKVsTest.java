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

import org.bigbase.carrot.util.UnsafeAccess;

public class DataBlockScannerLargeKVsTest extends DataBlockScannerTest{

  protected ArrayList<Key> fillDataBlock (DataBlock b) throws RetryOperationException {
    ArrayList<Key> keys = new ArrayList<Key>();
    Random r = new Random();
    int maxSize = 2048;
    boolean result = true;
    while(result == true) {
      int len = r.nextInt(maxSize) +2;
      byte[] key = new byte[len];
      r.nextBytes(key);
      long ptr = UnsafeAccess.malloc(len);
      UnsafeAccess.copy(key, 0, ptr, len);
      result = b.put(ptr, len, ptr, len, 0, 0);
      if(result) {
        keys.add( new Key(ptr, len));
      }
    }
    System.out.println("records="+b.getNumberOfRecords()  + "  data size=" + b.getDataInBlockSize());
    return keys;
  }
}
