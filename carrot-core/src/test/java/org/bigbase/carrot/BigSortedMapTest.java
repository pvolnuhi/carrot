package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

import org.junit.Ignore;
import org.junit.Test;

//TODO: MEMORY LEAK

public class BigSortedMapTest {

  BigSortedMap map;
  long totalLoaded;
  
  static {
    UnsafeAccess.debug = true;
  }
  
  public void setUp() throws IOException {
    BigSortedMap.setMaxBlockSize(4096);
    map = new BigSortedMap(100000000L);
    totalLoaded = 0;
    long start = System.currentTimeMillis();
    while(totalLoaded < 1000000) {
      totalLoaded++;
      byte[] key = ("KEY"+ (totalLoaded)).getBytes();
      byte[] value = ("VALUE"+ (totalLoaded)).getBytes();
      map.put(key, 0, key.length, value, 0, value.length, 0);
      if (totalLoaded % 100000 == 0) {
        System.out.println("Loaded = " + totalLoaded+" of "+ 100000000);
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("Time to load= "+ totalLoaded+" ="+(end -start)+"ms");
    long scanned = countRecords();
    System.out.println("Scanned="+ countRecords());
    System.out.println("\nTotal memory      =" + BigSortedMap.getTotalAllocatedMemory());
    System.out.println("Total  index      =" + BigSortedMap.getTotalBlockIndexSize());
    System.out.println("Total   data      =" + BigSortedMap.getTotalDataSize());
    System.out.println("Compressed size   =" + BigSortedMap.getTotalCompressedDataSize());
    System.out.println("Compression ratio =" + ((float)BigSortedMap.getTotalDataSize())/
      BigSortedMap.getTotalAllocatedMemory());
    System.out.println();

    assertEquals(totalLoaded, scanned);
  }
   
  public void tearDown() {
    map.dispose();
  }
  
  @Test
  public void runAllNoCompression() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    for (int i = 0; i < 1; i++) {
      System.out.println("\n********* " + i+" ********** Codec = NONE\n");
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
    for (int i=0; i < 1; i++) {
      System.out.println("\n********* " + i+" ********** Codec = LZ4\n");
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
    for (int i=0; i < 1; i++) {
      System.out.println("\n********* " + i+" ********** Codec = LZ4HC\n");
      setUp();
      allTests();
      tearDown();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats();
    }
  }
  
  private void allTests() throws IOException {
    testDeleteUndeleted();
    testExists();
    testFirstKey();
    testPutGet();
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
  
  long countRecords() throws IOException {
    BigSortedMapScanner scanner = map.getScanner(null, null);
    long counter = 0;
    while(scanner.hasNext()) {
      counter++;
      scanner.next();
    }
    scanner.close();
    return counter;
  }
  
  @Ignore
  @Test
  public void testPutGet() {   
    System.out.println("testPutGet");

    long start = System.currentTimeMillis();    
    byte[] tmp = ("VALUE"+ totalLoaded).getBytes();
    for(int i=1; i <= totalLoaded; i++) {
      byte[] key = ("KEY"+ (i)).getBytes();
      byte[] value = ("VALUE"+i).getBytes();
      try {
        long size = map.get(key, 0, key.length, tmp, 0, Long.MAX_VALUE) ;
        assertEquals(value.length, (int)size);
        assertTrue(Utils.compareTo(value, 0, value.length, tmp, 0,(int) size) == 0);
      } catch(Throwable t) {
        throw t;
      }      
    }    
    long end = System.currentTimeMillis();   
    System.out.println("Time to get "+ totalLoaded+" ="+ (end - start)+"ms");    
    
  }
  
  @Ignore
  @Test
  public void testExists() {   
    System.out.println("testExists");
  
    for(int i=1; i <= totalLoaded; i++) {
      byte[] key = ("KEY"+ (i)).getBytes();
      boolean res = map.exists(key, 0, key.length) ;
      assertEquals(true, res);      
    }            
  }
  
  @Ignore
  @Test
  public void testFirstKey() throws IOException {
    System.out.println("testFirstKey");

    byte[] firstKey = "KEY1".getBytes();
    byte[] secondKey = "KEY10".getBytes();
    byte[] key = map.getFirstKey();
    System.out.println(Bytes.toString(key));
    assertTrue(Utils.compareTo(key, 0, key.length, firstKey, 0, firstKey.length) == 0);
    boolean res = map.delete(firstKey, 0, firstKey.length);
    assertEquals ( true, res);
    key = map.getFirstKey();
    assertTrue(Utils.compareTo(key, 0, key.length, secondKey, 0, secondKey.length) == 0);
    
    byte[] value = "VALUE1".getBytes();
    
    res = map.put(firstKey, 0, firstKey.length, value, 0, value.length, 0);
    assertEquals(true, res);
    
  }
  
  
  private List<byte[]> delete(int num) {
    Random r = new Random();
    int numDeleted = 0;
    byte[] val = new byte[1];
    List<byte[]> list = new ArrayList<byte[]>();
    int collisions = 0;
    while (numDeleted < num) {
      int i = r.nextInt((int)totalLoaded) + 1;
      byte [] key = ("KEY"+ i).getBytes();
      long len = map.get(key, 0, key.length, val, 0, Long.MAX_VALUE);
      if (len == DataBlock.NOT_FOUND) {
        collisions++;
        continue;
      } else {
        boolean res = map.delete(key, 0, key.length);
        assertTrue(res);
        numDeleted++;
        list.add(key);
      }
    }
    System.out.println("Deleted="+ numDeleted +" collisions="+collisions);
    return list;
  }
  
  private void undelete(List<byte[]> keys) {
    for (byte[] key: keys) {
      byte[] value = ("VALUE"+ new String(key).substring(3)).getBytes();
      boolean res = map.put(key, 0, key.length, value, 0, value.length, 0);
      assertTrue(res);
    }
  }
  
}
