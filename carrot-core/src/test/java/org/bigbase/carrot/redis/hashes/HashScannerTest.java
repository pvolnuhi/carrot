package org.bigbase.carrot.redis.hashes;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.Key;
import org.bigbase.carrot.KeyValue;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Ignore;
import org.junit.Test;

public class HashScannerTest {
  BigSortedMap map;
  int valSize = 8;
  int fieldSize = 8;
  static {
    //UnsafeAccess.debug = true;
  }
  
  private List<KeyValue> getKeyValues(long n) {
    List<KeyValue> values = new ArrayList<KeyValue>();
    
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("VALUES SEED=" + seed);
    byte[] vbuf = new byte[valSize];
    byte[] fbuf = new byte[fieldSize];
    for (int i=0; i < n; i++) {
      r.nextBytes(fbuf);
      r.nextBytes(vbuf);
      long vptr = UnsafeAccess.malloc(valSize);
      long fptr = UnsafeAccess.malloc(fieldSize);
      UnsafeAccess.copy(vbuf, 0, vptr, vbuf.length);
      UnsafeAccess.copy(fbuf, 0, fptr, fbuf.length);
      values.add(new KeyValue(fptr, fieldSize, vptr, valSize));
    }
    return values;
  }
  
  private Key getKey() {
    long ptr = UnsafeAccess.malloc(valSize);
    byte[] buf = new byte[valSize];
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("KEY SEED=" + seed);
    r.nextBytes(buf);
    UnsafeAccess.copy(buf, 0, ptr, valSize);
    return new Key(ptr, valSize);
  }
  
    
  private void setUp() {
    map = new BigSortedMap(1000000000);
  }
  
  //@Ignore
  @Test
  public void runAllNoCompression() throws IOException {
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
  public void runAllCompressionLZ4() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    System.out.println();
    for (int i = 0; i < 10; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=LZ4");
      allTests();
      BigSortedMap.printMemoryAllocationStats();      
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  @Ignore
  @Test
  public void runAllCompressionLZ4HC() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    System.out.println();
    for (int i = 0; i < 10; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=LZ4HC");
      allTests();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  private void allTests() throws IOException {
    long start = System.currentTimeMillis();
    setUp();
    testSingleFullScanner();
    tearDown();
    setUp();
    testSinglePartialScanner();
    tearDown();
    long end = System.currentTimeMillis();    
    System.out.println("\nRUN in " + (end -start) + "ms");
    
  } 

  private void loadData(Key key, List<KeyValue> values) {
    int loaded = Hashes.HSET(map, key, values);
    assertEquals(values.size(), loaded);
  }
  
  @Ignore
  @Test
  public void testSingleFullScanner() throws IOException {
    long n = 1000000;

    System.out.println("Test single full scanner - one key "+ n + " elements");
    Key key = getKey();
    List<KeyValue> values = getKeyValues(n);
    List<KeyValue> copy = copy(values);    
    long start = System.currentTimeMillis();
    
    loadData(key, values);
    
    long end = System.currentTimeMillis();
    System.out.println("Total allocated memory ="+ BigSortedMap.getTotalAllocatedMemory() 
    + " for "+ n + " " + (fieldSize + valSize) + " byte field-values. Overhead="+ 
        ((double)BigSortedMap.getTotalAllocatedMemory()/n - fieldSize - valSize)+
    " bytes per value. Time to load: "+(end -start)+"ms");
    
    BigSortedMap.printMemoryAllocationStats();
    
    assertEquals(n, Hashes.HLEN(map, key.address, key.length));
    
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Test seed=" + seed);
    
    long card = 0;
    while ((card = Hashes.HLEN(map, key.address, key.length)) > 0) {      
      assertEquals(copy.size(), (int) card);
      /*DEBUG*/ System.out.println("Set size=" + copy.size());
      deleteRandom(map, key.address, key.length, copy, r);
      HashScanner scanner = Hashes.getHashScanner(map, key.address, key.length, 0,0,0,0, false, false);
      int expected = copy.size();
      int cc = 0;
      while(scanner.hasNext()) {
        cc++;
        scanner.next();
      }
      scanner.close();
      assertEquals(expected, cc);
    }

    assertEquals(0, (int)BigSortedMap.countRecords(map));
    assertEquals(0, (int)Hashes.HLEN(map, key.address, key.length));
    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Hashes.HLEN(map, key.address, key.length));
    BigSortedMap.printMemoryAllocationStats();
    // Free memory
    UnsafeAccess.free(key.address);
    values.stream().forEach(x -> { UnsafeAccess.free(x.keyPtr); UnsafeAccess.free(x.valuePtr);});
  }
  
  @Ignore
  @Test
  public void testSinglePartialScanner() throws IOException {
    long n = 1000000;

    System.out.println("Test single partial scanner - one key "+ n + " elements");
    Key key = getKey();
    List<KeyValue> values = getKeyValues(n);
    long start = System.currentTimeMillis();
    loadData(key, values);
    long end = System.currentTimeMillis();
    Utils.sortKeyValues(values);
    List<KeyValue> copy = copy(values);    
    
    System.out.println("Total allocated memory ="+ BigSortedMap.getTotalAllocatedMemory() 
    + " for "+ n + " " + (fieldSize + valSize) + " byte values. Overhead="+ 
        ((double)BigSortedMap.getTotalAllocatedMemory()/n - fieldSize - valSize)+
    " bytes per value. Time to load: "+(end -start)+"ms");
    
    BigSortedMap.printMemoryAllocationStats();
    
    assertEquals(n, Hashes.HLEN(map, key.address, key.length));
    
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Test seed=" + seed);
    
    long card = 0;
    while ((card = Hashes.HLEN(map, key.address, key.length)) > 0) {      
      assertEquals(copy.size(), (int) card);
      /*DEBUG*/ System.out.println("Hash size=" + copy.size());
      deleteRandom(map, key.address, key.length, copy, r);
      if (copy.size() == 0) break;
      int startIndex = r.nextInt(copy.size());
      int endIndex = r.nextInt(copy.size() - startIndex) + startIndex;
      
      long startPtr = copy.get(startIndex).keyPtr;
      int startSize = copy.get(startIndex).keySize;
      long endPtr = copy.get(endIndex).keyPtr;
      int endSize = copy.get(endIndex).keySize;

      int expected = (int)(endIndex - startIndex);
      HashScanner scanner = Hashes.getHashScanner(map, key.address, key.length,
        startPtr, startSize, endPtr, endSize, false, false);
      if (scanner == null) {
        continue;
      }
      int cc = 0;
      while(scanner.hasNext()) {
        cc++;
        scanner.next();
      }
      scanner.close();
      assertEquals(expected, cc);
    }

    assertEquals(0, (int)BigSortedMap.countRecords(map));
    assertEquals(0, (int)Hashes.HLEN(map, key.address, key.length));
    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Hashes.HLEN(map, key.address, key.length));
    BigSortedMap.printMemoryAllocationStats();
    // Free memory
    UnsafeAccess.free(key.address);
    values.stream().forEach(x -> {UnsafeAccess.free(x.keyPtr); UnsafeAccess.free(x.valuePtr);});

  }
  
  private <T> List<T> copy(List<T> src) {
    List<T> copy = new ArrayList<T>();
    for (T t: src) {
      copy.add(t);
    }
    return copy;
  }
  
  private void deleteRandom(BigSortedMap map, long keyPtr, int keySize, List<KeyValue> copy, Random r) {
    int toDelete = copy.size() < 10? copy.size(): r.nextInt( (int) copy.size() / 2);
    for (int i= 0; i < toDelete; i++) {
      int n = r.nextInt(copy.size());
      KeyValue v = copy.remove(n);
      int count = Hashes.HDEL(map, keyPtr, keySize, v.keyPtr, v.keySize);
      assertEquals(1, count);
    }
  }
  
  private void tearDown() {
    // Dispose
    map.dispose();
    UnsafeAccess.mallocStats.printStats();
  }
}
