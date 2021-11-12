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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.DoubleStream;

import org.junit.Test;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

public class TestBitmapCompression {
  
  @Test
  public void testBitmapCodecs() throws IOException {
    
    String[] names = new String[] {"Roaring", "16bit", "16bit diff", "8bit", "8bit diff", "4bit", "2bit"};
    double[][] results = new double[names.length][];
    results[0] = mainLoop(TestBitmapCompression::calculateRoaring, "\nROARING BITMAP\n");
    results[1] = mainLoop(TestBitmapCompression::calculateSize16, "\n16-bit codec\n");
    results[2] = mainLoop(TestBitmapCompression::calculateSize16Diff, "\n16-bit DIFF codec\n");
    results[3] = mainLoop(TestBitmapCompression::calculateSize8, "\n8-bit codec\n");
    results[4] = mainLoop(TestBitmapCompression::calculateSize8Diff, "\n8-bit DIFF codec\n");
    results[5] = mainLoop(TestBitmapCompression::calculateSize4, "\n4-bit codec\n");
    results[6] = mainLoop(TestBitmapCompression::calculateSize2, "\n2-bit codec\n");
    
    System.out.printf("\n%-20s%-20s%-20s%-20s\n\n","Density","Compression","Codec", "XRoaring");
    int n = results[0].length;
    int i = 0;
    for(; i < n ; i++) {
      int index = maxIndex(results, i);
      if (index >=0) {
        System.out.printf("%-20f%-20f%-20s%-20f\n", ((double)(i + 1)/1000), results[index][i], 
          names[index], results[index][i] / results[0][i]);
      } else {
        break;
      }
    }
    
    System.out.printf("\n%-15s%-15s%-15s%-15s%-15s%-15s%-15s%-15s\n\n", "Density",
      names[0], names[1], names[2], names[3], names[4], names[5], names[6]);
    
    for(int k = 0; k < i ; k++) {      
      System.out.printf("%-15f%-15f%-15f%-15f%-15f%-15f%-15f%-15f\n", ((double)(k + 1)/1000),
        results[0][k], results[1][k], results[2][k], results[3][k], results[4][k], results[5][k], results[6][k]);
    }
  }
  
  static int maxIndex(double[][] results, int i) {
    int index = -1;
    double max = 0;
    for (int j = 0; j< results.length; j++) {
      if (results[j][i] <= 1.) continue;
      if (results[j][i] > max) {
        index = j; max = results[j][i];
      }
    }
    return index;
  }
  
  
  private double[] mainLoop(Function<byte[], Integer> f, String header) {
    System.out.println(header);

    int n = 300, loops = 200;
    double[] result = new double[n];
    double[] fractions = new double[n];
    // Initialize fraction array
    for (int i = 0; i < fractions.length; i++) {
      fractions[i] = (double)(i + 1)/ 1000;
    }
    
    for (int k = 0; k < loops; k++) {
      for (int i = 0; i < fractions.length; i++) {
        byte[] arr = new byte[256 * 256];
        populate(arr, fractions[i]);
        int size = f.apply(arr);
        result[i] += size;
      }
    }
    // Normalize result
    AtomicInteger index = new AtomicInteger(0);
    DoubleStream ds = Arrays.stream(result).map( x -> (256 * 256) / (x / loops));
    double[] dd = ds.toArray();
    Arrays.stream(dd).forEach( x -> System.out.println(fractions[index.getAndIncrement()] + 
      " : " + x));
    return dd;
  }
  
  private  static int calculateRoaring(byte[] arr)  {
    MutableRoaringBitmap rr = MutableRoaringBitmap.bitmapOf();
    
    for (int i = 0; i < arr.length; i++) {
      if (arr[i] > 0) {
        rr.add(i);
      }
    }
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    try {
      rr.serialize(dos);
      dos.close();
    } catch (IOException e) {
      // tootoo
    }
    return bos.toByteArray().length * 8;
  }
  
  
  private static int calculateSize2(byte[] arr) {
    // Calculate by chunk of size 4 - 2 bits
    int size = 0;
    for (int i = 0; i < arr.length; i += 4) {
      int sz = bitCount(arr, i, 4) == 0? 1: 5;
      size += sz;
    }
    return size; 
  }
  
  private static int calculateSize4(byte[] arr) {
    // Calculate by chunk of size 16 - 4 bits
    int size = 0;
    for (int i = 0; i < arr.length; i += 16) {
      int bitCount = bitCount(arr, i, 16);
      // when bitCount == 0, we encode chunk into just 1 bit - 0
      // when bitCount > 0, first bit is always 1, followed by 2 bits:
      // 00 - 1 bit set
      // 01 - 2 bits set
      // 10 - 3 bits set
      // 11 - raw 16 bits chunk
      int sz = bitCount * 4 + (bitCount == 0? 1: 3); /* to keep # bits*/;
      if (sz >= 16 + 3) {
        size += 16 + 3;
      } else {
        size += sz;
      }
    }
    return size;
  }
   
  /**
   * For testing only array size = 64K
   * @param arr array represents bits: 0, 1
   * @return compressed size
   */
  private static int calculateSize8(byte[] arr) {
    // Calculate by chunk of size 256 - 8 bits
    int size = 0;
    int maxBitCount = 0;
    for (int i = 0; i < arr.length; i += 256) {
      int bitCount = bitCount(arr, i, 256);
      if (bitCount > maxBitCount) {
        maxBitCount = bitCount;
      }
      int sz = bitCount * 8 + (bitCount == 0? 1: 6) /* we need only 5 bits to keep max elements in a compressed 
      256 bits chunk - 31; first bit is always 1, total 5+1 = 6 bits*/;
      if (sz >= 256 + 6) {
        size += 256 + 6;
      } else {
        size += sz;
      }
    }
    return size;
  }
  
  /**
   * For testing only array size = 64K
   * @param arr array represents bits: 0, 1
   * @return compressed size
   */
  private static int calculateSize8Diff(byte[] arr) {
    // Calculate by chunk of size 256 - 8 bits
    int size = 0;
    int maxBits = 0;
    int maxBitCount = 0;
    for (int i = 0; i < arr.length; i += 256) {
      int[] index = indexArray(arr, i, 256);
      if (index.length > 0) {
        int[] diffArray = diffArray(index);
        int max = max(diffArray);
        int lead = Integer.numberOfLeadingZeros(max);
        int bits = 32  - lead;
        if (bits > maxBits) {
          maxBits = bits;
        }
        
        // For 256 bit slices we use 6 bit to keep number of elements ( 1's)
        // We do not use DIFF encoding unless we have at least 4 bits
        int numBits = diffArray.length;
        int sz;
        if (numBits > maxBitCount) {
          maxBitCount = numBits;
        }
        if (numBits >= 4) {
        // 7 bits - is 1xxxxxx, last 6 bits  are total bit count (bits set)
        // in a 256 bit block set
        // first bit == 1, when bit count > 0   
          
          sz = 7 + 3 /*keeps bits, max = 8 */ /*+ Utils.sizeUVInt(diffArray[0])*/ + 
          (diffArray.length) * bits;
        } else {
          sz = 7 + diffArray.length * 8;// 
        }
        if ( sz > 256 + 7) {
          sz = 256 + 7;
        }
        size += sz;
      } else {
        size += 1; // just length = 0, one bit = 0 
      }
    }
    return size;
  }
  
  /**
   * For testing only array size = 64K
   * @param arr array represents bits: 0, 1
   * @return compressed size in bits
   */
  private static int calculateSize16(byte[] arr) {
    // 16 bits
    int bitCount = bitCount(arr, 0, arr.length);
    int size = bitCount * 16 + 12 /* max bits in compressed block is 4096 */;
    if (size > arr.length) {
      size = arr.length;
    }
    return size;
  }
  
  
  private static int bitCount(byte[] arr, int offset, int chunkSize) {
    int count = 0;
    for (int i = offset; i < offset + chunkSize; i++) {
      count += arr[i];
    }
    return count;
  }
  
  private static int calculateSize16Diff(byte[] arr) {
     int[] index = indexArray(arr, 0, arr.length);
     int[] diffArray = diffArray(index);
     int max = max(diffArray);
     int lead = Integer.numberOfLeadingZeros(max);
     int bits = 32 - lead;
     int size = 14 /*number of elements*/ + 
          /* first element*/ + 4 /* bits number */ + (diffArray.length) * bits;
     if (size > arr.length) {
       return arr.length;
     }
     return size;
  }
  
  private static int[] diffArray(int[] arr) {
    int[] diff = new int[arr.length];
    diff[0] = arr[0];
    for (int i = 1; i < arr.length; i++) {
      diff[i] = arr[i] - arr[i - 1];
    }
    return diff;
  }
  
  private static int[] indexArray(byte[] arr, int offset, int chunkSize) {
    int count  = bitCount(arr, offset, chunkSize);
    int[] index = new int[count];
    int j = 0;
    for (int i = offset; i < offset + chunkSize; i++) {
      if (arr[i] == 0) continue;
      index[j++] = i - offset;
    }
    return index;
  }
  
  static int max(int[] arr) {
    /// skip first element
    int max = 0;
    for (int i = 0; i < arr.length; i++) {
      if (arr[i] > max) max = arr[i];
    }
    return max;
  }
  
  /**
   * Populates byte array for a given bit fraction
   * @param arr array to populate
   * @param bitFraction fraction of 1's
   */
  private static void populate(byte[] arr, double bitFraction) {
    Random r = new Random();
    for (int i = 0; i < arr.length; i++) {
      double d = r.nextDouble();
      if (d < bitFraction) {
        arr[i] = 1;
      }
    }
  }
}
