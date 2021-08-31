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
package org.bigbase.carrot.compression;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.bigbase.carrot.util.UnsafeAccess;

/**
 * 
 * Custom bitmap codec
 * Input stream (bit stream):
 * 
 * 00011010000100
 * 
 * Format of encoded stream:
 * 
 * Code Scheme - 1 byte
 * Segment +
 * 
 * Scheme:
 * 
 * 16 = (15,1)  15 bits for dominant bit 1 - bit for other
 * 15 = (14,1)
 * 14 = (13,1)
 * 13 = (12,1)
 * 12 = (11,1)
 * 11 = (10,1)
 * 10 = (9,1)
 * 9 = (8,1)
 * 8 = (7,1)
 * 7 = (6,1)
 * 6 = (5,1)
 * 5 = (4,1)
 * 4 = (3,1)
 * Segment:
 * 
 * X bits for dominant
 * Y bits for other
 * Example of coding:
 * 
 * 0 - dominant bit, 1 - non-dominant
 * 
 * (15,1) followed by n 1's 
 *  
 * (1010,1)1110 - decodes to 00000000001111   (10 0' followed by four 1's ) Compression 14/9 = 1.56
 * (1010,1)0    - decodes to 00000000001      (10 0's followed by 1)  Compression 11/6 = 1.83
 * (1111,0)     - decoded to 000000000000000  (15 0's)   Compression 15 / 5 = 3
 * (1111,1)0    - decoded to 0000000000000001 (15 0's followed by one 1) Compression 15/6 = 2.5
 * 
 *
 */

public class BitmapCodec implements Codec {
  
  static enum Scheme {
    S15_1, S14_1, S13_1, S12_1, S11_1, S10_1, S9_1, S8_1, S7_1, 
    S6_1, S5_1, S4_1, S3_1; 
  }
  
  /** Dominant bit*/
  int bit = 0;
  int popCount;
  /**
   * Constructor
   * @param bit dominant bit in a bitmap (0 or 1)
   */
  public BitmapCodec(int bit) {
    this.bit = bit;
  }
  
  public void setPopCount(int popCount) {
    this.popCount = popCount;
  }
  
  @Override
  public int compress(ByteBuffer src, ByteBuffer dst) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }
  
  @Override
  public int decompress(ByteBuffer src, ByteBuffer dst) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int compress(long src, int srcSize, long dst, int dstCapacity) {
    Scheme scheme = getCompressionScheme();
    int bits1 = getFirstBitNumber(scheme);
    int bits2 = getSecBitNumber(scheme);
    return compress(src, srcSize, dst, dstCapacity, bits1, bits2);
  }

  private Scheme getCompressionScheme() {
    // TODO Auto-generated method stub
    return null;
  }

  private int getFirstBitNumber(Scheme s) {
    int ord = s.ordinal();
    return 15 - ord;
  }
  
  private int getSecBitNumber(Scheme s) {
    // TODO 
    return 1;
  }
  
  /**
   * Compress using a given scheme
   * 
   * @param src source address
   * @param srcSize source size 
   * @param dst destination address
   * @param dstCapacity destination capacity
   * @param bits1 dominant bits number (scheme)
   * @param bits2 non-dominant bits number (scheme)
   * @return compressed size
   */
  private int compress(long src, int srcSize, long dst, int dstCapacity, int bits1, int bits2) {
    long r1 = 0, r2 = 0; // r1 - for input, r2 - for output
    int num8 = srcSize >>> 3;
    int num4 = srcSize & 4;
    int num2 = srcSize & 2;
    int num1 = srcSize & 1;
    int max1 = 1 << bits1;
    int max2 = 1 << bits2; // actually - 2
    int mask1 = 0xff << bits1;
    long read_off = 0, write_off = 0;
    int written = 0; // written to r2
    int nlz0 = 0, nlz1 = 0;
    for (int i=0; i < num8; i++, read_off += 8) {
      r1 = UnsafeAccess.toLong(src + read_off);
      int read = 0;
      while (read < 64) {
        int _nlz0 = Long.numberOfLeadingZeros(r1);
        if (_nlz0 == 64 - read) {
          nlz0 += _nlz0;
          // long series of 0's
          break;
        }
        nlz0 += _nlz0;
        if (nlz0  >= max1) {
          nlz0 -= max1;
          // bits1 + bits2
          int v = 0xffff >>> (32 - bits1 - bits2) - 1;// 1 - is OK when bits2 = 1;
          if (64 - written >= bits1 + bits2) {
            int n = 64 - written - bits1 - bits2;
            v <<= n;
            r2 += v;
            written += bits1 + bits2; 
            if (written == 64) {
              UnsafeAccess.putLong(dst + write_off, r2);
              r2 = 0;
              written = 0;
              write_off += 8;
            }
          } else {
            int n = bits1 + bits2 + written - 64;
            r2 += v >>> n;
            UnsafeAccess.putLong(dst + write_off, r2);
            r2 = ((-1) >> (32 - n)) & v;
            written = n;    
            write_off += 8;
          }
        } else {
        
          // Read 1 s
          int  _nlz1 = Long.numberOfLeadingZeros(~r1); 
          
        }
      }
      
      
      // TODO
      
      // write encoded
      
      UnsafeAccess.putLong(dst + write_off, r2);
      
      
    }
    
    return 0;
  }
  
  @Override
  public int decompress(long src, int srcSize, long dst, int dstCapacity) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public CodecType getType() {
    return CodecType.BITMAP;
  }
  
  /**
   * TO IGNORE
   */
  
  @Override
  public int getCompressionThreshold() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setCompressionThreshold(int val) {
    // Ignore

  }


  @Override
  public double getAvgCompressionRatio() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getTotalProcessed() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setLevel(int level) {
    // TODO Auto-generated method stub

  }

  @Override
  public int getLevel() {
    // TODO Auto-generated method stub
    return 0;
  }

  
  public static void main(String[] args) {
    
    // Calculate time for
    long totalTime = 0; // ns 
    long totalZ = 0;
    Random r = new Random();
    long[] arr = new long[100000];
    final int numCycles = 1000;
    for (int j=0; j < numCycles; j++) {
      for(int i = 0; i < arr.length; i++) {
        arr[i] = r.nextInt(Integer.MAX_VALUE);
      }
      
      long t1 = System.nanoTime();
      totalZ += timeIt(arr);
      long t2 = System.nanoTime();
      totalTime += (t2-t1);
    }
    
    System.out.println("TZ=" + totalZ + " time per byte (ns)=" + 
        ((double)totalTime) / (8 * numCycles * arr.length));
  }
  
  private static long timeIt(long[] arr) {
    long total = 0;
    for(int i=0; i < arr.length; i++) {
      total += Long.numberOfLeadingZeros(arr[i]);
    }
    return total;
  }
}
