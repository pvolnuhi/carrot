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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Ignore;
import org.junit.Test;

public class TestUtils {

  public static byte[] greaterThan(byte[] arr) {
    byte[] buf = new byte[arr.length];
    System.arraycopy(arr, 0, buf, 0, arr.length);
    for (int i = buf.length -1; i >= 0; i--) {
      int v = buf[i];
      if (v == -1) {
        continue;
      } else if (v >= 0){
        buf[i] = (byte) (v + 1);
        return buf;
      } else {
        v += 256;
        buf[i] = (byte) (v + 1);
        return buf;
      }
    }
    buf = new byte[arr.length +1];
    System.arraycopy(arr, 0, buf, 0, arr.length);
    return buf;
  }
  
  public static byte[] lessThan(byte[] arr) {
    byte[] buf = new byte[arr.length];
    System.arraycopy(arr, 0, buf, 0, arr.length);
    for (int i = buf.length -1; i >= 0; i--) {
      int v = buf[i];
      if (v == 0) {
        continue;
      } else if (v >= 0){
        buf[i] = (byte) (v - 1);
        return buf;
      } else {
        v += 256;
        buf[i] = (byte) (v - 1);
        return buf;
      }
    }
    buf = new byte[arr.length - 1];
    System.arraycopy(arr, 0, buf, 0, arr.length -1);
    return buf;
  }
  
  @Ignore
  @Test 
  public void testAll() {
    for (int i = 0; i < 100; i++) {
      testDirectBufferAddress();
    }
  }
    
  @Test
  public void testDirectBufferAddress() {
    ByteBuffer buf = ByteBuffer.allocateDirect(16);
    long addr = UnsafeAccess.address(buf);
    System.out.println("addr=" + addr);
    addr = UnsafeAccess.address(buf);
    System.out.println("addr=" + addr);
    int N = 1000000;
    long[] ptrs = new long[N];
    
    long total = 0;
    for (int i = 0; i < N; i++) {
      buf = ByteBuffer.allocateDirect(16);
      long start = System.nanoTime();
      ptrs[i] = UnsafeAccess.address(buf);
      long end = System.nanoTime();
      total += (end - start);
    }
    System.out.println("Time for "+ N +" address() ="+ total);
  }
  
  @Test
  public void testGreaterThan() {
    byte[] in = new byte[] {0,0,0};
    byte[] out  = new byte[] {0,0,1};
    assertTrue(Utils.compareTo(TestUtils.greaterThan(in), 0, in.length, out, 0, out.length) == 0);
    
    in = new byte[] {0, 0,(byte)255};
    out  = new byte[] {0,1,(byte)255};
    assertTrue(Utils.compareTo(TestUtils.greaterThan(in), 0, in.length, out, 0, out.length) == 0);
    
    in = new byte[] {0, (byte) 255,(byte)255};
    out  = new byte[] {1,(byte) 255,(byte)255};
    assertTrue(Utils.compareTo(TestUtils.greaterThan(in), 0, in.length, out, 0, out.length) == 0);
    
    in = new byte[] {(byte)255, (byte)255,(byte)255};
    out  = new byte[] {(byte)255,(byte)255,(byte)255, 0};
    assertTrue(Utils.compareTo(TestUtils.greaterThan(in), 0, out.length, out, 0, out.length) == 0);
  }
  
  @Test
  public void testLessThan() {
    byte[] in = new byte[] {1,1,1};
    byte[] out  = new byte[] {1,1,0};
    assertTrue(Utils.compareTo(TestUtils.lessThan(in), 0, out.length, out, 0, out.length) == 0);
    
    in = new byte[] {1,1,0};
    out  = new byte[] {1,0,0};
    assertTrue(Utils.compareTo(TestUtils.lessThan(in), 0, out.length, out, 0, out.length) == 0);    
    
    in = new byte[] {1,0,0};
    out  = new byte[] {0,0,0};
    assertTrue(Utils.compareTo(TestUtils.lessThan(in), 0, out.length, out, 0, out.length) == 0);
    
    in = new byte[] {0,0,0};
    out  = new byte[] {0,0};
    assertTrue(Utils.compareTo(TestUtils.lessThan(in), 0, out.length, out, 0, out.length) == 0);

  }  
  
  
  @Test
  public void testDoubleConversions() {
    System.out.println("testDoubleConversions");
    long ptr = UnsafeAccess.malloc(30);
    int size = 30;
    
    Random r = new Random();
    long start = System.currentTimeMillis();
    double total = 0;
    for(int i=0; i < 100; i++) {
      double d = r.nextDouble() - 0.5d;
      int len = Utils.doubleToStr(d, ptr, size);
      double dd = Utils.strToDouble(ptr, len);
      System.out.println(d+" " + dd);
      total += dd;
    }
    long end = System.currentTimeMillis();
    System.out.println ("Time =" + (end-start) + " total="+total);
  }
  
  @Test
  public void testLongConversions() {
    System.out.println("testLongConversions");
    long ptr = UnsafeAccess.malloc(30);
    int size = 30;
    
    Random r = new Random();
    long start = System.currentTimeMillis();
    long total = 0;
    for(int i=0; i < 10000000; i++) {
      long d = r.nextLong();
      int len = Utils.longToStr(d, ptr, size);
      long dd = Utils.strToLong(ptr, len);
      assertEquals(d, dd);
      total += dd;
    }
    long end = System.currentTimeMillis();
    System.out.println ("Time =" + (end-start) + " total="+total);
  }
  
  @Test
  public void testLongConversionsBytes() {
    System.out.println("testLongConversions byte arrays");
    byte[] buf = new byte[30];
    
    Random r = new Random();
    long start = System.currentTimeMillis();
    long total = 0;
    for(int i=0; i < 10000000; i++) {
      long d = r.nextLong();
      int len = Utils.longToStr(d, buf, 3);
      long dd = Utils.strToLong(buf, 3, len);
      assertEquals(d, dd);
      total += dd;
    }
    long end = System.currentTimeMillis();
    System.out.println ("Time =" + (end-start) + " total="+total);
  }
  
  @Test
  public void testLongConversionsBuffers() {
    System.out.println("testLongConversions byte buffers");
    ByteBuffer buf = ByteBuffer.allocate(30);
    
    Random r = new Random();
    long start = System.currentTimeMillis();
    long total = 0;
    for(int i=0; i < 10000000; i++) {
      long d = r.nextLong();
      int len = Utils.longToStr(d, buf, 3);
      long dd = Utils.strToLong(buf, 3, len);
      assertEquals(d, dd);
      total += dd;
    }
    long end = System.currentTimeMillis();
    System.out.println ("Time =" + (end-start) + " total="+total);
  }
  
  @Test
  public void testUnsignedVariableInt() {
    int [] values = new int[1000];
    fillRandom(values, 1 << 7);
    verify(values);
    fillRandom(values, 1 << 14);
    verify(values);
    fillRandom(values, 1 << 21);
    verify(values);
    fillRandom(values, 1 << 28);
    verify(values);
  }
  
  private void verify(int[] values) {
    long ptr = UnsafeAccess.malloc(4);
    for(int i=0; i < values.length; i++) {
      // clear
      UnsafeAccess.putInt(ptr,  0);
      int size = Utils.writeUVInt(ptr, values[i]);
      int v = Utils.readUVInt(ptr);
      assertEquals(values[i], v);
      assertEquals(size, Utils.sizeUVInt(v));
    }
    UnsafeAccess.free(ptr);
  }

  private void fillRandom(int[] arr, int maxValue) {
    Random r = new Random();
    for(int i=0; i < arr.length; i++) {
      arr[i] = r.nextInt(maxValue);
    }
  }
  
  @Test
  public void testDoubleToLex() {
    Random r = new Random();
    int N = 100;
    double[] arr = new double[N];
    long ptr = UnsafeAccess.malloc( N * Utils.SIZEOF_LONG);
    for(int i = 0; i < N ; i++) {
      double d = r.nextDouble();
      arr[i] = d * r.nextInt();
      Utils.doubleToLex(ptr + i * Utils.SIZEOF_LONG, arr[i]);
    }
    
    for (int i=0; i < N ; i++) {
      double d = Utils.lexToDouble(ptr + i * Utils.SIZEOF_LONG);
      assertEquals(arr[i], d);
    }
    
    ArrayList<Key> keys = new ArrayList<Key>(N);
    for(int i =0; i < N; i++) {
      keys.add( new Key(ptr + i * Utils.SIZEOF_LONG, Utils.SIZEOF_LONG));
    }
    
    Utils.sortKeys(keys);
    Arrays.sort(arr);
    for (int i=0; i < arr.length; i++) {
      double d = Utils.lexToDouble(keys.get(i).address);
      assertEquals(arr[i], d);
    }
   keys.stream().map(x-> Utils.lexToDouble(x.address)).forEach(System.out::println);
    
  }
  
  
  @Test
  public void testLongOrdering() {
    Random r = new Random();
    List<Key> list = new ArrayList<Key>();
    for (int i = 0; i < 20; i++) {
      long ptr = UnsafeAccess.malloc(Utils.SIZEOF_LONG);
      long v = Math.abs(r.nextLong());
      UnsafeAccess.putLong(ptr, Long.MAX_VALUE - v);
      list.add(new Key(ptr, Utils.SIZEOF_LONG));
    }
    
    Utils.sortKeys(list);
    
    for(Key k : list) {
      System.out.println(Long.MAX_VALUE - UnsafeAccess.toLong(k.address));
    }
  }
  
  @Ignore
  @Test
  public void testRandomDistinctArray() {
    long max = Long.MAX_VALUE / 2;
    int count = 1000;
    while(max > count) {
      long[] arr = Utils.randomDistinctArray(max, count);
      System.out.println("max=" + max + " count="+ count);
      verifyUnique(arr);
      max >>>= 1;
    }
  }
  
  
  private void verifyUnique(long[] arr) {
    Arrays.sort(arr);
    
    for (int i = 1; i < arr.length; i++) {
      if (arr[i-1] == arr[i]) {
        fail("Failed");
      }
    }
  }
  
  @Test
  public void testBitSetUnSetAPI() {
    
    // Test 64-bit set
    long vl = 1L;
    long ptr = UnsafeAccess.malloc(Utils.SIZEOF_LONG);
    for (int i = 0; i <= 64; i++) {
      UnsafeAccess.putLong(ptr, vl);
      int pos = UnsafeAccess.firstBitSetLong(ptr);
      assertEquals(63 - i, pos);
      vl <<= 1;
    }
    
    vl = 0xffffffffffffffffL;
    
    for (int i = 0; i <= 64; i++) {
      UnsafeAccess.putLong(ptr, vl);
      int pos = UnsafeAccess.firstBitUnSetLong(ptr);
      if (i == 0) {
        assertEquals(-1, pos);
      } else {
        assertEquals(64 - i, pos);
      }
      vl <<= 1;
    }
    
    int vi = 1;
    
    for (int i = 0; i <= 32; i++) {
      UnsafeAccess.putInt(ptr, vi);
      int pos = UnsafeAccess.firstBitSetInt(ptr);
      assertEquals(31 - i, pos);
      vi <<= 1;
    }
    
    vi = 0xffffffff;
    
    for (int i = 0; i <= 32; i++) {
      UnsafeAccess.putInt(ptr, vi);
      int pos = UnsafeAccess.firstBitUnSetInt(ptr);
      if (i == 0) {
        assertEquals(-1, pos);
      } else {
        assertEquals(32 - i, pos);
      }
      vi <<= 1;
    }
    
    short vs = 1;
    
    for (int i = 0; i <= 16; i++) {
      UnsafeAccess.putShort(ptr, vs);
      int pos = UnsafeAccess.firstBitSetShort(ptr);
      assertEquals(15 - i, pos);
      vs <<= 1;
    }
    
    vs = (short) 0xffff;
    
    for (int i = 0; i <= 16; i++) {
      UnsafeAccess.putShort(ptr, vs);
      int pos = UnsafeAccess.firstBitUnSetShort(ptr);
      if (i == 0) {
        assertEquals(-1, pos);
      } else {
        assertEquals(16 - i, pos);
      }
      vs <<= 1;
    }
    
    byte vb = 1;
    
    for (int i = 0; i <= 8; i++) {
      UnsafeAccess.putByte(ptr, vb);
      int pos = UnsafeAccess.firstBitSetByte(ptr);
      assertEquals(7 - i, pos);
      vb <<= 1;
    }
    
    vb = (byte) 0xff;
    
    for (int i = 0; i <= 8; i++) {
      UnsafeAccess.putByte(ptr, vb);
      int pos = UnsafeAccess.firstBitUnSetByte(ptr);
      if (i == 0) {
        assertEquals(-1, pos);
      } else {
        assertEquals(8 - i, pos);
      }
      vb <<= 1;
    }
    
    UnsafeAccess.free(ptr);
  }
  
  @Test
  public void testStringByteBufferConversion() {
    ByteBuffer heap = ByteBuffer.allocate(64);
    ByteBuffer nativ = ByteBuffer.allocateDirect(64);
    
    String s = "ABRACADABRAABRACADABRA";
    
    Utils.strToByteBuffer(s, heap);
    //heap.flip();
    String result = Utils.byteBufferToString(heap);
    assertEquals(s, result);
    
    Utils.strToByteBuffer(s, nativ);
    //nativ.flip();
    result = Utils.byteBufferToString(nativ);
    assertEquals(s, result);
    
  }
}
