package org.bigbase.xcache.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bigbase.util.UnsafeAccess;
import org.bigbase.util.Utils;
import org.junit.Test;

public class TestUtils {
  private static final Log LOG = LogFactory.getLog(TestUtils.class);

  @Test
  public void testCompareToArrayNative() {
    
    // Positive
    byte[] arr = new byte[113];
    long address = UnsafeAccess.theUnsafe.allocateMemory(2*113);
    Random r = new Random();
    r.nextBytes(arr);
    
    UnsafeAccess.copy(arr, 0, address, arr.length);
    
    int off = 0;
    int len  = arr.length;
    
    for(; off < len; off++) {
      System.out.println(off);
      assertTrue(Utils.compareTo(arr, off, len - off, address + off, len - off) == 0);
    }
    
    // Negative
    off = 0;
    for(; off < len -1; off++) {
      assertTrue(Utils.compareTo(arr, off, len - off, address + off -1, len - off -1) != 0);
    }
    
    UnsafeAccess.theUnsafe.freeMemory(address);
  }
  
  @Test
  public void testCompareToNativeNative() {
    // Positive
    byte[] arr = new byte[113];
    Random r = new Random();
    r.nextBytes(arr);
    long address1 = UnsafeAccess.theUnsafe.allocateMemory(2*113);    
    UnsafeAccess.copy(arr, 0, address1, arr.length);
    long address2 = UnsafeAccess.theUnsafe.allocateMemory(2*113);    
    UnsafeAccess.copy(arr, 0, address2, arr.length);
    
    
    int off = 0;
    int len  = arr.length;
    
    for(; off < len; off++) {
      assertTrue(Utils.compareTo(address1 + off, len - off, address2 + off, len - off) == 0);
    }
    
    // Negative
    off = 0;
    for(; off < len -1; off++) {
      assertTrue(Utils.compareTo(address1 + off, len - off, address2 + off -1, len - off -1) != 0);
    }
    
    UnsafeAccess.theUnsafe.freeMemory(address1);
    UnsafeAccess.theUnsafe.freeMemory(address2);


  }
  
  @Test
  public void testPrefixByteArrayByteArray() {
    
    byte[] b1 = "xfskdghkljfqokkbvxjw;k".getBytes();
    byte[] b2 = "dhfjgjhlk;'l;'l;ghkklj;lk;khugy".getBytes();
    byte[] b3 = "xfskdghkljfqokkbvxjw;kyghdljkj".getBytes();
    byte[] b4 = "dhfjgjhlk;'l;'l;ghkklkkkkkkkkkkk".getBytes();
    
    assertEquals(0, Utils.prefix(b1, 0, b1.length, b2, 0, b2.length));
    assertEquals(0, Utils.prefix(b2, 0, b2.length, b1, 0, b1.length));
    
    assertEquals(b1.length, Utils.prefix(b1, 0, b1.length, b3, 0, b3.length));
    assertEquals(b1.length, Utils.prefix(b3, 0, b3.length, b1, 0, b1.length));
    
    assertEquals(21, Utils.prefix(b2, 0, b2.length, b4, 0, b4.length));
    assertEquals(21, Utils.prefix(b4, 0, b4.length, b2, 0, b2.length));    
    
  }
  
  @Test
  public void testPrefixByteArrayMemory() {
    
    byte[] b1 = "xfskdghkljfqokkbvxjw;k".getBytes();
    byte[] b2 = "dhfjgjhlk;'l;'l;ghkklj;lk;khugy".getBytes();
    byte[] b3 = "xfskdghkljfqokkbvxjw;kyghdljkj".getBytes();
    byte[] b4 = "dhfjgjhlk;'l;'l;ghkklkkkkkkkkkkk".getBytes();
    
    long m1 = UnsafeAccess.malloc(b1.length);
    UnsafeAccess.copy(b1, 0, m1, b1.length);
    
    long m2 = UnsafeAccess.malloc(b2.length);
    UnsafeAccess.copy(b2, 0, m2, b2.length);
    
    long m3 = UnsafeAccess.malloc(b3.length);
    UnsafeAccess.copy(b3, 0, m3, b3.length);
    
    long m4 = UnsafeAccess.malloc(b4.length);
    UnsafeAccess.copy(b4, 0, m4, b4.length);
    
    assertEquals(0, Utils.prefix(b1, 0, b1.length, m2, b2.length));
    
    assertEquals(b1.length, Utils.prefix(b1, 0, b1.length, m3, b3.length));
    
    assertEquals(21, Utils.prefix(b2, 0, b2.length, m4,  b4.length));
    UnsafeAccess.free(m1);
    UnsafeAccess.free(m2);
    UnsafeAccess.free(m3);
    UnsafeAccess.free(m4);
  }
  
  @Test
  public void testPrefixMemoryMemory() {
    
    byte[] b1 = "xfskdghkljfqokkbvxjw;k".getBytes();
    byte[] b2 = "dhfjgjhlk;'l;'l;ghkklj;lk;khugy".getBytes();
    byte[] b3 = "xfskdghkljfqokkbvxjw;kyghdljkj".getBytes();
    byte[] b4 = "dhfjgjhlk;'l;'l;ghkklkkkkkkkkkkk".getBytes();
    
    long m1 = UnsafeAccess.malloc(b1.length);
    UnsafeAccess.copy(b1, 0, m1, b1.length);
    
    long m2 = UnsafeAccess.malloc(b2.length);
    UnsafeAccess.copy(b2, 0, m2, b2.length);
    
    long m3 = UnsafeAccess.malloc(b3.length);
    UnsafeAccess.copy(b3, 0, m3, b3.length);
    
    long m4 = UnsafeAccess.malloc(b4.length);
    UnsafeAccess.copy(b4, 0, m4, b4.length);
    assertEquals(0, Utils.prefix(m1, b1.length, m2, b2.length));
    assertEquals(b1.length, Utils.prefix(m1, b1.length, m3, b3.length));
    assertEquals(21, Utils.prefix(m2, b2.length, m4,  b4.length));
    UnsafeAccess.free(m1);
    UnsafeAccess.free(m2);
    UnsafeAccess.free(m3);
    UnsafeAccess.free(m4);
    
  }
  
}
