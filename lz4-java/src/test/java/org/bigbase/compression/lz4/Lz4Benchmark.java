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
 */

package org.bigbase.compression.lz4;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Random;

import org.junit.Test;

import sun.misc.Unsafe;

public class Lz4Benchmark {
  int blockSize = 4096;
  long numBlocks = 500000;
  Random r = new Random();
  static Unsafe unsafe;
  static {
    unsafe = (Unsafe) AccessController.doPrivileged(new PrivilegedAction<Object>() {
      @Override
      public Object run() {
        try {
          Field f = Unsafe.class.getDeclaredField("theUnsafe");
          f.setAccessible(true);
          return f.get(null);
        } catch (Throwable e) {
          System.err.println("sun.misc.Unsafe is not accessible: " + e.getMessage());
        }
        return null;
      }
    });
  }

  @Test
  public void testBlocksCompressDecompressSequentially() {
    
    System.out.println("testBlocksCompressDecompressSequentially");
    long address = unsafe.allocateMemory(blockSize * numBlocks);
    fillBlocks(address);
    long buf = unsafe.allocateMemory(blockSize);
    for (int k = 0; k < 10; k++) {
      System.out.println("iteration "+(k+1));
      long startTime = System.currentTimeMillis();
      for (int i=0; i < numBlocks; i++) {        
        long src = address + i * blockSize;
        int r = LZ4.compressDirectAddress(src, blockSize, buf, blockSize);
        int size = LZ4.decompressDirectAddress(buf, r, src, blockSize);
      }
      long endTime = System.currentTimeMillis();
      System.out.println(numBlocks * 1000/ (endTime -startTime) +" block comp/decomp per sec");
    }
    unsafe.freeMemory(address);
    unsafe.freeMemory(buf);
  }
  
  @Test
  public void testBlocksCompressDecompressRandomly() {
    System.out.println("testBlocksCompressDecompressRandomly");

    long address = unsafe.allocateMemory(blockSize * numBlocks);
    fillBlocks(address);
    long buf = unsafe.allocateMemory(blockSize);
    for (int k = 0; k < 10; k++) {
      System.out.println("iteration "+(k+1));
      long startTime = System.currentTimeMillis();
      for (int i=0; i < numBlocks; i++) {
        int n =  r.nextInt((int)numBlocks);
        long src = address + n * blockSize;
        int r = LZ4.compressDirectAddress(src, blockSize, buf, blockSize);
        int size = LZ4.decompressDirectAddress(buf, r, src, blockSize);
      }
      long endTime = System.currentTimeMillis();
      System.out.println(numBlocks * 1000/ (endTime -startTime) +" block comp/decomp per sec");
    }
    unsafe.freeMemory(address);
    unsafe.freeMemory(buf);
  }
  
  private void fillBlocks(long startPtr) {
    // Block size is 'blockSize'
    System.out.println("fill start");
    long ptr = startPtr;
    long endPtr = startPtr + blockSize * numBlocks;
    
    while (ptr < endPtr) {
      long l1 = r.nextLong();
      long l2 = r.nextLong();
      unsafe.putLong(ptr, l1);
      unsafe.putLong(ptr + 8, l2);
      unsafe.putLong(ptr + 16, 0L);
      unsafe.putLong(ptr + 24, 0L);
      ptr += 32;
    }
    System.out.println("fill finished");

  }
  
}
