package org.bigbase.carrot.redis.hashes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
  long n = 100000;

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
    for (int i = 0; i < 100; i++) {
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
    for (int i = 0; i < 100; i++) {
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
    testDirectScannerPerformance();
    tearDown();
    setUp();
    testReverseScannerPerformance();
    tearDown();
//    setUp();
//    testEdgeConditions();
//    tearDown();
//    setUp();
//    testSingleFullScanner();
//    tearDown();
//    setUp();
//    testSingleFullScannerReverse();
//    tearDown();
//    setUp();
//    testSinglePartialScanner();
//    tearDown();
//    setUp();
//    testSinglePartialScannerReverse();
//    tearDown();
//    setUp();
//    testSinglePartialScannerOpenStart();
//    tearDown();
//    setUp();
//    testSinglePartialScannerOpenEnd();
//    tearDown(); 
//    setUp();
//    testSinglePartialScannerReverseOpenStart();
//    tearDown();
//    setUp();
//    testSinglePartialScannerReverseOpenEnd();
//    tearDown();
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
      HashScanner scanner = Hashes.getScanner(map, key.address, key.length, 0, 0, 0, 0, false, false);
      int expected = copy.size();
      if (scanner == null) {
        assertEquals(0, expected);
        break;
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
    values.stream().forEach(x -> { UnsafeAccess.free(x.keyPtr); UnsafeAccess.free(x.valuePtr);});
  }
  
  @Ignore
  @Test
  public void testEdgeConditions() throws IOException {
    
    byte[] zero1 = new byte[] {0};
    byte[] zero2 = new byte[] {0,0};
    byte[] max1  = new byte[] { (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, 
                                (byte)0xff, (byte)0xff, (byte)0xff, (byte)0};
    byte[] max2  = new byte[] { (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, 
                                (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff,
                                (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, 
                                    (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff};
    long zptr1 = UnsafeAccess.allocAndCopy(zero1, 0, zero1.length);
    int zptrSize1 = zero1.length;
    long zptr2 = UnsafeAccess.allocAndCopy(zero2, 0, zero2.length);
    int zptrSize2 = zero2.length;
    long mptr1 = UnsafeAccess.allocAndCopy(max1, 0, max1.length);
    int mptrSize1 = max1.length;
    long mptr2 = UnsafeAccess.allocAndCopy(max2, 0, max2.length);
    int mptrSize2 = max2.length;
    
    System.out.println("Test edge conditions "+ n + " elements");
    Key key = getKey();
    List<KeyValue> values = getKeyValues(n);
    long start = System.currentTimeMillis();
    loadData(key, values);
    long end = System.currentTimeMillis();

    Utils.sortKeyValues(values);

    System.out.println("Total allocated memory ="+ BigSortedMap.getTotalAllocatedMemory() 
    + " for "+ n + " " + valSize+ " byte values. Overhead="+ 
        ((double)BigSortedMap.getTotalAllocatedMemory()/n - valSize)+
    " bytes per value. Time to load: "+(end -start)+"ms");
        
    // Direct
    HashScanner scanner = Hashes.getScanner(map, key.address, key.length, zptr1, zptrSize1, 
      zptr2, zptrSize2, false, false);
    assertTrue (scanner.hasNext() == false);
    scanner.close();
    
    // Reverse
    scanner = Hashes.getScanner(map, key.address, key.length, zptr1, zptrSize1, 
      zptr2, zptrSize2, false, true);
    assertTrue (scanner == null);
    
    // Direct
    scanner = Hashes.getScanner(map, key.address, key.length, mptr1, mptrSize1, 
      mptr2, mptrSize2, false, false);
    assertTrue (scanner.hasNext() == false);
    scanner.close();

    // Reverse
    scanner = Hashes.getScanner(map, key.address, key.length, mptr1, mptrSize1, 
      mptr2, mptrSize2, false, true);
    assertTrue (scanner == null);
    
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Test seed=" + seed);
    
    int index = r.nextInt(values.size());
    int expected = index;
    KeyValue v = values.get(index);
    // Direct
    scanner = Hashes.getScanner(map, key.address, key.length, zptr1, zptrSize1, 
      v.keyPtr, v.keySize, false, false);
    
    if (expected == 0) {
      assertTrue(scanner.hasNext() == false);
    } else {
      assertEquals(expected, Utils.count(scanner));
    }
    scanner.close();
    
    // Reverse
    scanner = Hashes.getScanner(map, key.address, key.length, zptr1, zptrSize1, 
      v.keyPtr, v.keySize, false, true);
    
    if (expected == 0) {
      assertTrue(scanner == null);
    } else {
      assertEquals(expected, Utils.countReverse(scanner));
      scanner.close();
    }
    // Always close ALL scanners

    index = r.nextInt(values.size());
    expected = values.size() - index;
    v = values.get(index);
    // Direct
    scanner = Hashes.getScanner(map, key.address, key.length,  
      v.keyPtr, v.keySize, mptr2, mptrSize2, false, false);
    
    if (expected == 0) {
      assertTrue(scanner.hasNext() == false);
    } else {
      assertEquals(expected, Utils.count(scanner));
    }
    scanner.close();

    // Reverse
    scanner = Hashes.getScanner(map, key.address, key.length,  
      v.keyPtr, v.keySize, mptr2, mptrSize2, false, true);
    
    if (expected == 0) {
      assertTrue(scanner == null);
    } else {
      assertEquals(expected, Utils.countReverse(scanner));
      scanner.close();
    }

    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Hashes.HLEN(map, key.address, key.length));
    BigSortedMap.printMemoryAllocationStats();
    // Free memory
    UnsafeAccess.free(key.address);
    values.stream().forEach(x -> {UnsafeAccess.free(x.keyPtr);UnsafeAccess.free(x.valuePtr);});

  }
    
  @Ignore
  @Test
  public void testSingleFullScannerReverse() throws IOException {

    System.out.println("Test single full scanner reverse - one key "+ n + " elements");
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
      HashScanner scanner = Hashes.getScanner(map, key.address, key.length, 0,0,0,0, false, true);
      
      int expected = copy.size();
      if (scanner == null && expected == 0) {
        break;
      } else if (scanner == null) {
        fail("Scanner is null, but expected=" + expected);
      }
      int cc = 0;
      do {
        cc++;
      } while(scanner.previous());
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
      HashScanner scanner = Hashes.getScanner(map, key.address, key.length,
        startPtr, startSize, endPtr, endSize, false, false);
      if (scanner == null) {
        assertEquals(0, expected);
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
  
  @Ignore
  @Test
  public void testSinglePartialScannerOpenStart() throws IOException {

    System.out.println("Test single partial scanner open start - one key "+ n + " elements");
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
      int startIndex = 0;
      int endIndex = r.nextInt(copy.size() - startIndex) + startIndex;
      
      long startPtr = 0;//copy.get(startIndex).keyPtr;
      int startSize = 0;//copy.get(startIndex).keySize;
      long endPtr = copy.get(endIndex).keyPtr;
      int endSize = copy.get(endIndex).keySize;

      int expected = (int)(endIndex - startIndex);
      HashScanner scanner = Hashes.getScanner(map, key.address, key.length,
        startPtr, startSize, endPtr, endSize, false, false);
      if (scanner == null) {
        assertEquals(0, expected);
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
  
  @Ignore
  @Test
  public void testSinglePartialScannerOpenEnd() throws IOException {

    System.out.println("Test single partial scanner open end - one key "+ n + " elements");
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
      int endIndex = copy.size();
      
      long startPtr = copy.get(startIndex).keyPtr;
      int startSize = copy.get(startIndex).keySize;
      long endPtr = 0;
      int endSize = 0;

      int expected = (int)(endIndex - startIndex);
      HashScanner scanner = Hashes.getScanner(map, key.address, key.length,
        startPtr, startSize, endPtr, endSize, false, false);
      if (scanner == null) {
        assertEquals(0, expected);
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
  
  @Ignore
  @Test
  public void testSinglePartialScannerReverse() throws IOException {

    System.out.println("Test single partial scanner reverse - one key "+ n + " elements");
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
      HashScanner scanner = Hashes.getScanner(map, key.address, key.length,
        startPtr, startSize, endPtr, endSize, false, true);
      if (scanner == null && expected == 0) {
        continue;
      } else if (scanner == null) {
        fail("Scanner is null, but expected="+ expected);
      }
      int cc = 0;
      do {
        cc++;
      } while(scanner.previous());
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
  
  
  @Ignore
  @Test
  public void testSinglePartialScannerReverseOpenStart() throws IOException {

    System.out.println("Test single partial scanner reverse open start - one key "+ n + " elements");
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
      int startIndex = 0;//r.nextInt(copy.size());
      int endIndex = r.nextInt(copy.size() - startIndex) + startIndex;
      
      long startPtr = 0;//copy.get(startIndex).keyPtr;
      int startSize = 0;//copy.get(startIndex).keySize;
      long endPtr = copy.get(endIndex).keyPtr;
      int endSize = copy.get(endIndex).keySize;

      int expected = (int)(endIndex - startIndex);
      HashScanner scanner = Hashes.getScanner(map, key.address, key.length,
        startPtr, startSize, endPtr, endSize, false, true);
      if (scanner == null && expected == 0) {
        continue;
      } else if (scanner == null) {
        fail("Scanner is null, but expected="+ expected);
      }
      int cc = 0;
      do {
        cc++;
      } while(scanner.previous());
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
  
  @Ignore
  @Test
  public void testSinglePartialScannerReverseOpenEnd() throws IOException {

    System.out.println("Test single partial scanner reverse open end - one key "+ n + " elements");
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
      int endIndex = copy.size();//r.nextInt(copy.size() - startIndex) + startIndex;
      
      long startPtr = copy.get(startIndex).keyPtr;
      int startSize = copy.get(startIndex).keySize;
      long endPtr = 0;//copy.get(endIndex).keyPtr;
      int endSize = 0;//copy.get(endIndex).keySize;

      int expected = (int)(endIndex - startIndex);
      HashScanner scanner = Hashes.getScanner(map, key.address, key.length,
        startPtr, startSize, endPtr, endSize, false, true);
      if (scanner == null && expected == 0) {
        continue;
      } else if (scanner == null) {
        fail("Scanner is null, but expected="+ expected);
      }
      int cc = 0;
      do {
        cc++;
      } while(scanner.previous());
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
  @Ignore
  @Test
  public void testDirectScannerPerformance() throws IOException {
    int n = 5000000; // 5M elements
    System.out.println("Test direct scanner performance "+ n + " elements");
    Key key = getKey();
    List<KeyValue> values = getKeyValues(n);
    long start = System.currentTimeMillis();
    loadData(key, values);
    long end = System.currentTimeMillis();
    
    System.out.println("Total allocated memory ="+ BigSortedMap.getTotalAllocatedMemory() 
    + " for "+ n + " " + valSize+ " byte values. Overhead="+ 
        ((double)BigSortedMap.getTotalAllocatedMemory()/n - valSize)+
    " bytes per value. Time to load: "+(end -start)+"ms");
    
    HashScanner scanner = Hashes.getScanner(map, key.address, key.length, 0, 0, 0, 0, false, false);
    
    start = System.currentTimeMillis();
    long count = 0;
    while(scanner.hasNext()) {
      count++;
      scanner.next();
    }
    scanner.close();
    assertEquals(count, (long) n);
    end = System.currentTimeMillis();
    System.out.println("Scanned "+ n+" elements in "+ (end-start)+"ms");
    // Free memory
    UnsafeAccess.free(key.address);
    values.stream().forEach(x -> {UnsafeAccess.free(x.keyPtr); UnsafeAccess.free(x.valuePtr);});
  }  
  
  @Ignore
  @Test
  public void testReverseScannerPerformance() throws IOException {
    int n = 5000000; // 5M elements
    System.out.println("Test reverse scanner performance "+ n + " elements");
    Key key = getKey();
    List<KeyValue> values = getKeyValues(n);
    long start = System.currentTimeMillis();
    loadData(key, values);
    long end = System.currentTimeMillis();
    
    System.out.println("Total allocated memory ="+ BigSortedMap.getTotalAllocatedMemory() 
    + " for "+ n + " " + valSize+ " byte values. Overhead="+ 
        ((double)BigSortedMap.getTotalAllocatedMemory()/n - valSize)+
    " bytes per value. Time to load: "+(end -start)+"ms");
    
    HashScanner scanner = Hashes.getScanner(map, key.address, key.length, 0, 0, 0, 0, false, true);
    
    start = System.currentTimeMillis();
    long count = 0;
    
    do {
      count++;
    } while(scanner.previous());
    scanner.close();
    assertEquals(count, (long) n);
    
    end = System.currentTimeMillis();
    System.out.println("Scanned (reversed) "+ n+" elements in "+ (end-start)+"ms");
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
