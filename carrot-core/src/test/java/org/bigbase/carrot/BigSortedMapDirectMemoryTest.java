package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Ignore;
import org.junit.Test;


//TODO: MEMORY LEAK
public class BigSortedMapDirectMemoryTest {

  BigSortedMap map;
  long totalLoaded;
  long MAX_ROWS = 1000000;

  static {
    //UnsafeAccess.debug = true;
  }

  long countRecords() throws IOException {
    BigSortedMapDirectMemoryScanner scanner = map.getScanner(0, 0, 0, 0);
    long counter = 0;
    while (scanner.hasNext()) {
      counter++;
      scanner.next();
    }
    scanner.close();
    return counter;
  }

  private boolean load(long totalLoaded) {
    byte[] key = ("KEY" + (totalLoaded)).getBytes();
    byte[] value = ("VALUE" + (totalLoaded)).getBytes();
    long keyPtr = UnsafeAccess.malloc(key.length);
    UnsafeAccess.copy(key, 0, keyPtr, key.length);
    long valPtr = UnsafeAccess.malloc(value.length);
    UnsafeAccess.copy(value, 0, valPtr, value.length);
    boolean result = map.put(keyPtr, key.length, valPtr, value.length, 0);
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(valPtr);
    return result;
  }

  public void setUp() throws IOException {
    BigSortedMap.setMaxBlockSize(4096);
    map = new BigSortedMap(100000000);
    totalLoaded = 0;
    long start = System.currentTimeMillis();
    while (totalLoaded < MAX_ROWS) {
      totalLoaded++;
      load(totalLoaded);
      if (totalLoaded % 100000 == 0) {
        System.out.println("Loaded " + totalLoaded);
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("Time to load= " + totalLoaded + " =" + (end - start) + "ms");
    long scanned = countRecords();
    System.out.println("Scanned=" + countRecords());
    System.out.println("\nTotal memory     =" + BigSortedMap.getTotalAllocatedMemory());
    System.out.println("Total   data       =" + BigSortedMap.getTotalDataSize());
    System.out.println("Compressed size    =" + BigSortedMap.getTotalCompressedDataSize());
    System.out.println("Compression  ratio ="
        + ((float) BigSortedMap.getTotalDataSize()) / BigSortedMap.getTotalAllocatedMemory());
    System.out.println();
    assertEquals(totalLoaded, scanned);
  }

  public void tearDown() {
    map.dispose();
  }

  private void allTests() throws IOException {
    testDeleteUndeleted();
    testExists();
    testPutGet();
  }

  @Test
  public void runAllNoCompression() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    for (int i = 0; i < 1; i++) {
      System.out.println("\n********* " + i + " ********** Codec = NONE\n");
      setUp();
      allTests();
      tearDown();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats();
    }
  }

  @Test
  public void runAllCompressionLZ4() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    for (int i = 0; i < 1; i++) {
      System.out.println("\n********* " + i + " ********** Codec = LZ4\n");
      setUp();
      allTests();
      tearDown();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats();
    }
  }

  @Test
  public void runAllCompressionLZ4HC() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    for (int i = 0; i < 1; i++) {
      System.out.println("\n********* " + i + " ********** Codec = LZ4HC\n");
      setUp();
      allTests();
      tearDown();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats();
    }
  }

  @Ignore
  @Test
  public void testDeleteUndeleted() throws IOException {
    System.out.println("testDeleteUndeleted");
    List<byte[]> keys = delete(100);
    assertEquals(totalLoaded - 100, countRecords());
    undelete(keys);
    assertEquals(totalLoaded, countRecords());

  }

  @Ignore
  @Test
  public void testPutGet() {
    System.out.println("testPutGet");

    long start = System.currentTimeMillis();
    for (int i = 1; i <= totalLoaded; i++) {
      byte[] key = ("KEY" + (i)).getBytes();
      byte[] value = ("VALUE" + i).getBytes();
      long keyPtr = UnsafeAccess.malloc(key.length);
      UnsafeAccess.copy(key, 0, keyPtr, key.length);
      long valPtr = UnsafeAccess.malloc(value.length);

      try {
        long size = map.get(keyPtr, key.length, valPtr, value.length, Long.MAX_VALUE);
        assertEquals(value.length, (int) size);
        assertTrue(Utils.compareTo(value, 0, value.length, valPtr, (int) size) == 0);
      } catch (Throwable t) {
        throw t;
      } finally {
        UnsafeAccess.free(keyPtr);
        UnsafeAccess.free(valPtr);
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("Time to get " + totalLoaded + " =" + (end - start) + "ms");

  }

  @Ignore
  @Test
  public void testExists() {
    System.out.println("testExists");

    for (int i = 1; i <= totalLoaded; i++) {
      byte[] key = ("KEY" + (i)).getBytes();
      long keyPtr = UnsafeAccess.malloc(key.length);
      UnsafeAccess.copy(key, 0, keyPtr, key.length);
      boolean res = map.exists(keyPtr, key.length);
      UnsafeAccess.free(keyPtr);
      assertEquals(true, res);
    }
  }

  private List<byte[]> delete(int num) {
    Random r = new Random();
    int numDeleted = 0;
    long valPtr = UnsafeAccess.malloc(1);
    List<byte[]> list = new ArrayList<byte[]>();
    int collisions = 0;
    while (numDeleted < num) {
      int i = r.nextInt((int) totalLoaded) + 1;
      byte[] key = ("KEY" + i).getBytes();
      long keyPtr = UnsafeAccess.malloc(key.length);
      UnsafeAccess.copy(key, 0, keyPtr, key.length);
      long len = map.get(keyPtr, key.length, valPtr, 1, Long.MAX_VALUE);
      if (len == DataBlock.NOT_FOUND) {
        collisions++;
        UnsafeAccess.free(keyPtr);
        continue;
      } else {
        boolean res = map.delete(keyPtr, key.length);
        assertTrue(res);
        numDeleted++;
        list.add(key);
        UnsafeAccess.free(keyPtr);
      }
    }
    UnsafeAccess.free(valPtr);
    System.out.println("Deleted=" + numDeleted + " collisions=" + collisions);
    return list;
  }

  private void undelete(List<byte[]> keys) {
    for (byte[] key : keys) {
      byte[] value = ("VALUE" + new String(key).substring(3)).getBytes();
      long keyPtr = UnsafeAccess.malloc(key.length);
      UnsafeAccess.copy(key, 0, keyPtr, key.length);
      long valPtr = UnsafeAccess.malloc(value.length);
      UnsafeAccess.copy(value, 0, valPtr, value.length);
      boolean res = map.put(keyPtr, key.length, valPtr, value.length, 0);
      UnsafeAccess.free(valPtr);
      UnsafeAccess.free(keyPtr);
      assertTrue(res);
    }
  }

}
