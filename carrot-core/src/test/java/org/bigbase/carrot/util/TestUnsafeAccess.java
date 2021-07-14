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

package org.bigbase.carrot.util;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.junit.Test;

public class TestUnsafeAccess {

  
  @Test
  public void testCopyByteBuffer() {
    
    ByteBuffer bbuf = ByteBuffer.allocate(4096);
    long ptr = UnsafeAccess.malloc(4096);
    String testStr = "new string to test";
    bbuf.put(testStr.getBytes(), 0, testStr.length());
    bbuf.rewind();
    UnsafeAccess.copy(bbuf, ptr, testStr.length());
    
    String str = Utils.toString(ptr, testStr.length());
    assertEquals(testStr, str);

    bbuf.position(0);
    UnsafeAccess.copy(ptr, bbuf, testStr.length());
    byte[] arr = bbuf.array();
    str = new String(arr, 0, testStr.length());
    assertEquals(testStr, str);
    
  }
  
  @Test
  public void testCopyByteBufferRaw() {
    
    ByteBuffer bbuf = ByteBuffer.allocateDirect(4096);
    long ptr = UnsafeAccess.malloc(4096);
    String testStr = "new string to test";
    bbuf.put(testStr.getBytes(), 0, testStr.length());
    bbuf.rewind();
    UnsafeAccess.copy(bbuf, ptr, testStr.length());
    
    String str = Utils.toString(ptr, testStr.length());
    assertEquals(testStr, str);

    bbuf.position(0);
    UnsafeAccess.copy(ptr, bbuf, testStr.length());
    long bptr = UnsafeAccess.address(bbuf);
    str = Utils.toString(bptr, testStr.length());
    assertEquals(testStr, str);
    
  }
}
