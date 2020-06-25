package org.bigbase.carrot.util;

import static org.junit.Assert.assertTrue;

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
  
}
