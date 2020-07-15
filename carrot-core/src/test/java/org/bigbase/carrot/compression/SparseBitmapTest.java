package org.bigbase.carrot.compression;

import java.util.Random;

import org.bigbase.carrot.util.UnsafeAccess;
import org.junit.Test;

public class SparseBitmapTest {
  int size = 4096;

  
  @Test
  
  public void runTest() {
    CodecFactory factory = CodecFactory.getInstance();
    Codec codec = factory.getCodec(CodecType.LZ4);
    long src = UnsafeAccess.mallocZeroed(size);
    long dst = UnsafeAccess.mallocZeroed(2* size);
    
    double pct = 0;
    Random r = new Random();
    for (int i = 1; i <=100; i++) {
      pct += 0.01;
      UnsafeAccess.setMemory(src, size, (byte)0);
      fill (src, pct, r);
      int compressedSize = codec.compress(src, size, dst, 2 * size);
      System.out.println("Sparsiness " + i +"% comp ratio=" + (((float)size)/compressedSize));
    }
  }

  private void fill(long src, double pct, Random r) {
    // TODO Auto-generated method stub
    int max = size * 8;
    for (int i =0 ; i < max; i++) {
      double d = r.nextDouble();
      if (d < pct) {
        setBit(src, i);
      }
    }
  }
  
  private void setBit(long src, long offset) {
    int n = (int)(offset/8);
    int pos = (int)(offset - n * 8);
    byte b = UnsafeAccess.toByte(src + n);
    b |= 1 << (7-pos);
    UnsafeAccess.putByte(src + n, b);
    
  }
}
