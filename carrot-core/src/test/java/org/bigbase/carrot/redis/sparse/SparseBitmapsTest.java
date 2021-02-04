package org.bigbase.carrot.redis.sparse;

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.Key;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.redis.Commons;
import org.bigbase.carrot.util.UnsafeAccess;
import org.junit.Ignore;
import org.junit.Test;

public class SparseBitmapsTest {
  BigSortedMap map;
  Key key;
  long buffer;
  int bufferSize = 64;
  int keySize = 8;
  int N = 100000;
  int delta = 100;
  double dencity = 0.01;
  
  static {
    UnsafeAccess.debug = true;
  }
    
  private Key getKey() {
    long ptr = UnsafeAccess.malloc(keySize);
    byte[] buf = new byte[keySize];
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED=" + seed);
    r.nextBytes(buf);
    UnsafeAccess.copy(buf, 0, ptr, keySize);
    return new Key(ptr, keySize);
  }
  
  private void setUp() {
    map = new BigSortedMap(10000000);
    buffer = UnsafeAccess.mallocZeroed(bufferSize); 
    key = getKey();
  }
  
  private void tearDown() {
    map.dispose();

    UnsafeAccess.free(key.address);
    UnsafeAccess.free(buffer);
    UnsafeAccess.mallocStats.printStats();
    BigSortedMap.memoryStats();
  }

  //@Ignore
  @Test
  public void runAllNoCompression() {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    System.out.println();
    for (int i = 0; i < 10; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=NULL");
      allTests();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  //@Ignore
  @Test
  public void runAllCompressionLZ4() {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    System.out.println();
    for (int i = 0; i < 10; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=LZ4");
      allTests();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  //@Ignore
  @Test
  public void runAllCompressionLZ4HC() {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    System.out.println();
    for (int i = 0; i < 10; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=LZ4HC");
      allTests();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  private void allTests() {
    setUp();
    testSetBitGetBitLoop();
    tearDown();
    //setUp();
    //testPerformance();
    //tearDown();
    setUp();
    testSparseLength();
    tearDown();
  }
  
  @Ignore
  @Test
  public void testSetBitGetBitLoop() {
    
    System.out.println("\nTest SetBitGetBitLoop\n");
    long offset= 0;

    long start = System.currentTimeMillis();
    for (int i = 0; i < N ; i++) {
      offset += delta ;
      int bit = SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 1);
      if (bit != 0) {
        System.out.println("i="+ i +" offset =" + offset);
      }
      assertEquals(0, bit);
      bit = SparseBitmaps.SGETBIT(map, key.address, key.length, offset);
      assertEquals(1, bit);
    }
    long count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(N, (int)count);

    /*DEBUG*/ System.out.println("\nTotal RAM=" + UnsafeAccess.getAllocatedMemory()+"\n");
    
    BigSortedMap.printMemoryAllocationStats();
    
    long end  = System.currentTimeMillis();
    
    System.out.println("Time for " + N + " new SetBit/GetBit/CountBits =" + (end - start) + "ms");
    
    offset= 0;
    start = System.currentTimeMillis();
    for (int i = 0; i < N ; i++) {
      offset += delta ;
      int bit = SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 0);
      assertEquals(1, bit);
      bit = SparseBitmaps.SGETBIT(map, key.address, key.length, offset);
      assertEquals(0, bit);
    }
    count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(0, (int)count);

    end  = System.currentTimeMillis();
    
    System.out.println("Time for " + N + " existing SetBit/GetBit/CountBits =" + (end - start) + "ms");
  }
  
  
  @Ignore
  @Test
  public void testPerformance() {
    
    System.out.println("\nTest Performance\n");
    long offset= 0;
    long MAX =  (long)(N / dencity);
    Random r = new Random();
    
    long start = System.currentTimeMillis();
    long expected = N;
    for (int i = 0; i < N ; i++) {
      offset = Math.abs(r.nextLong()) % MAX;
      int bit = SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 1); 
      if (bit == 1) {
        expected--;
      }
    }
    long end  = System.currentTimeMillis();    
    long memory = UnsafeAccess.getAllocatedMemory();
    /*DEBUG*/ System.out.println("Total RAM=" + memory+"\n");
    
    long count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(expected, count);
    
    System.out.println("Time for " + N + " dencity="+ dencity+ " new SetBit=" + (end - start) + "ms");
    System.out.println("Compression ratio="+( ((double)MAX) / (8 * memory)));
    BigSortedMap.printMemoryAllocationStats();

  }
  
  @Ignore
  @Test
  public void testSparseLength() {
    System.out.println("\nTest testSparseLength\n");

    long offset = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < N ; i++) {
      offset += delta ;
      SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 1);
      long count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG);
      assertEquals( i + 1, (int)count);
      long len = SparseBitmaps.SSTRLEN(map, key.address, key.length);
      long expectedlength = (offset / SparseBitmaps.BITS_PER_CHUNK) * SparseBitmaps.BYTES_PER_CHUNK
          + SparseBitmaps.BYTES_PER_CHUNK;
      assertEquals(expectedlength, len);
      if (i % 10000 == 0 && i > 0) {
        System.out.println(i);
      }
    }
    long end  = System.currentTimeMillis();

    System.out.println("\nTotal RAM=" + UnsafeAccess.getAllocatedMemory()+"\n");
    BigSortedMap.printMemoryAllocationStats();

    long count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(N, (int)count);
    System.out.println("Time for " + N + " SetBit/BitCount/StrLength =" + (end - start) + "ms");
    
  }
}
