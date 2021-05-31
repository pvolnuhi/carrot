package org.bigbase.carrot.redis.strings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.Key;
import org.bigbase.carrot.KeyValue;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.ops.OperationFailedException;
import org.bigbase.carrot.redis.Commons;
import org.bigbase.carrot.redis.MutationOptions;
import org.bigbase.carrot.redis.sets.Sets;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Ignore;
import org.junit.Test;

public class StringsTest {
  BigSortedMap map;
  Key key;
  long buffer;
  int bufferSize = 512;
  long n = 100000;
  List<KeyValue> keyValues;
  
  static {
    //UnsafeAccess.debug = true;
  }
  
  private List<KeyValue> getKeyValues(long n) {
    List<KeyValue> keyValues = new ArrayList<KeyValue>();
    for (int i=0; i < n; i++) {
      // key
      byte[] key = ("user:" + i).getBytes();
      long keyPtr = UnsafeAccess.malloc(key.length);
      int keySize = key.length;
      UnsafeAccess.copy(key, 0, keyPtr, keySize);
      
      // value
      FakeUserSession session = FakeUserSession.newSession(i);
      byte[] value = session.toString().getBytes();
      int valueSize = value.length;
      long valuePtr = UnsafeAccess.malloc(valueSize);
      UnsafeAccess.copy(value, 0, valuePtr, valueSize);
      keyValues.add(new KeyValue(keyPtr, keySize, valuePtr, valueSize));
    }
    return keyValues;
  }
  
  //@Ignore
  @Test
  public void runAllNoCompression() throws OperationFailedException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    System.out.println();
    for (int i = 0; i < 10; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=NULL");
      allTests();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  @Ignore
  @Test
  public void runAllCompressionLZ4() throws OperationFailedException {
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
  public void runAllCompressionLZ4HC() throws OperationFailedException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    System.out.println();
    for (int i = 0; i < 10; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=LZ4HC");
      allTests();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  private void allTests() throws OperationFailedException {
    setUp();
    testSetIfNotExists();
    tearDown();
    setUp();
    testSetIfExists();
    tearDown();
    setUp();
    testSetWithTTL();
    tearDown();
    setUp();
    testSetGetWithTTL();
    tearDown();    
    setUp();
    testGetExpire();
    tearDown();
    setUp();
    testIncrementLongWrongFormat();
    tearDown();
    setUp();
    testIncrementDoubleWrongFormat();
    tearDown();
    setUp();
    testIncrementLong();
    tearDown();
    setUp();
    testIncrementDouble();
    tearDown();
    setUp();
    testSetGet();
    tearDown();
    setUp();
    testSetRemove();
    tearDown();
    setUp();
    testAppend();
    tearDown();
    setUp();
    testGetDelete();
    tearDown();
    setUp();
    testGetEx();
    tearDown();
    setUp();
    testGetSet();
    tearDown();
    setUp();
    testStrLength();
    tearDown();
    setUp();
    testSetEx();
    tearDown();
    setUp();
    testPSetEx();
    tearDown();
    setUp();
    testMget();
    tearDown();
    
    setUp();
    testMSet();
    tearDown();
    
    setUp();
    testMSetNX();
    tearDown();    
    setUp();
    testSetGetBit();
    tearDown();
    setUp();
    testGetRange();
    tearDown();
    setUp();
    testSetRange();
    tearDown();
    setUp();
    testBitCount();
    tearDown();
    
    setUp();
    testBitPosition();
    tearDown();
    
  }
  
  
  private void setUp() {
    map = new BigSortedMap(1000000000);
    buffer = UnsafeAccess.mallocZeroed(bufferSize); 
    keyValues = getKeyValues(n);
  }
  
  @Ignore
  @Test
  public void testGetExpire() {
    System.out.println("Test Get expire");
    KeyValue kv = keyValues.get(0);
    
    Random r = new Random();
    long exp = 0;
    
    boolean res = Strings.SET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp, MutationOptions.NONE, false);
    assertTrue(res);
    long size = Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    assertEquals(kv.valueSize, (int) size);
    assertTrue(Utils.compareTo(kv.valuePtr, kv.valueSize, buffer, (int)size) == 0);
    
    long expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);
    
    exp = Math.abs(r.nextLong());
    res = Strings.SET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp, MutationOptions.NONE, false);
    assertTrue(res);

    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);
    
    exp = Math.abs(r.nextLong());
    res = Strings.SET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp, MutationOptions.NONE, false);
    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);

    assertEquals(exp, expire);
    
    exp = Math.abs(r.nextLong());
    res = Strings.SET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp, MutationOptions.NONE, false);
    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);

    assertEquals(exp, expire);
    
    exp = Math.abs(r.nextLong());
    res = Strings.SET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp, MutationOptions.NONE, false);
    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);

    assertEquals(exp, expire);
    
  }
  
  @Ignore
  @Test
  public void testSetIfNotExists() {
    System.out.println("Test SETNX with options");    
    KeyValue kv = keyValues.get(0);
    long exp = 10;
    
    boolean res = Strings.SETNX(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp);
    assertTrue(res);
     
    res = Strings.SETNX(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp);
    assertFalse(res);
    
    long expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);
    
  }
  
  @Ignore
  @Test
  public void testSetIfExists() {
    System.out.println("Test SETXX with options");    
    KeyValue kv = keyValues.get(0);
    long exp = 10;
    
    boolean res = Strings.SETXX(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp);
    assertFalse(res);
     
    res = Strings.SETNX(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp);
    assertTrue(res);
    
    exp = 100;
    res = Strings.SETXX(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp);
    assertTrue(res);
    
    long expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);
    
  }
  
  @Ignore
  @Test
  public void testSetWithTTL() {
    System.out.println("Test SET with TTL options");    
    KeyValue kv = keyValues.get(0);
    long exp = 10;
    
    boolean res = Strings.SET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp, MutationOptions.NONE, true);
    assertTrue(res);
     
    res = Strings.SET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp + 10, MutationOptions.NONE, true);
    assertTrue(res);    
    
    // Check that expire did not change
    long expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);
    
    res = Strings.DELETE(map, kv.keyPtr, kv.keySize);
    assertTrue(res);
    
    res = Strings.SET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp, MutationOptions.XX, true);
    assertFalse(res);
    
    res = Strings.SET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp, MutationOptions.NONE, true);
    assertTrue(res);
    
    exp += 10;
    res = Strings.SET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp, MutationOptions.XX, false);
    assertTrue(res);
    
    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);
    
    res = Strings.SET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp + 10, MutationOptions.XX, true);
    assertTrue(res);
    
    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);
    
 
    res = Strings.SET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp, MutationOptions.NX, true);
    assertFalse(res);
    
    res = Strings.DELETE(map, kv.keyPtr, kv.keySize);
    assertTrue(res);
    
    
    res = Strings.SET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp, MutationOptions.NX, true);
    assertTrue(res);
    
    exp += 10;
    res = Strings.SET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp, MutationOptions.XX, false);
    assertTrue(res);
    
    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);
    
    res = Strings.SET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp + 10, MutationOptions.XX, true);
    assertTrue(res);
    
    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);
    
  }
  
  @Ignore
  @Test
  public void testSetGetWithTTL() {
    System.out.println("Test SETGET with TTL options");    
    KeyValue kv = keyValues.get(0);
    KeyValue kv1 = keyValues.get(1);
    KeyValue kv2 = keyValues.get(2);
    long exp = 10; 
    long SET_FAILED = -2;
    long GET_FAILED = -1;
    
    long res = Strings.SETGET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, 
      exp, MutationOptions.NONE, true, buffer, bufferSize);
    assertEquals(GET_FAILED, res);
     
    res = Strings.SETGET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, 
      exp + 10, MutationOptions.NONE, true, buffer, bufferSize);
    assertEquals(kv.valueSize, (int) res);   
    assertTrue(Utils.compareTo(kv.valuePtr, kv.valueSize, buffer, (int)res) == 0);
    
    // Check that expire did not change
    long expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);
    
    boolean result = Strings.DELETE(map, kv.keyPtr, kv.keySize);
    assertTrue(result);
    
    res = Strings.SETGET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, 
      exp, MutationOptions.XX, true, buffer, bufferSize);
    assertEquals(SET_FAILED, res);
    
    result = Strings.SET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, exp, MutationOptions.NONE, true);
    assertTrue(result);
    
    exp += 10;
    res = Strings.SETGET(map, kv.keyPtr, kv.keySize, kv1.valuePtr, kv1.valueSize, 
      exp, MutationOptions.XX, false, buffer, bufferSize);
    assertEquals(kv.valueSize, (int) res);
    
    assertTrue(Utils.compareTo(kv.valuePtr, kv.valueSize, buffer, (int)res) == 0);

    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);
    
    res = Strings.SETGET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, 
      exp + 10, MutationOptions.XX, true, buffer, bufferSize);
    assertEquals(kv1.valueSize, (int) res);
    assertTrue(Utils.compareTo(kv1.valuePtr, kv1.valueSize, buffer, (int)res) == 0);

    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);
    
    res = Strings.SETGET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, 
      exp, MutationOptions.NX, true, buffer, bufferSize);
    assertEquals(SET_FAILED,  res);
    
    result = Strings.DELETE(map, kv.keyPtr, kv.keySize);
    assertTrue(result);
        
    res = Strings.SETGET(map, kv.keyPtr, kv.keySize, kv2.valuePtr, kv2.valueSize, 
      exp, MutationOptions.NX, true, buffer, bufferSize);
    assertEquals(GET_FAILED, res);
    
    exp += 10;
    res = Strings.SETGET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, 
      exp, MutationOptions.XX, false, buffer, bufferSize);
    assertEquals(kv2.valueSize, (int) res);
    
    assertTrue(Utils.compareTo(kv2.valuePtr, kv2.valueSize, buffer, (int)res) == 0);

    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(exp, expire);
    
  }
  
  @Ignore
  @Test 
  public void testIncrementLongWrongFormat() {
    System.out.println("Test Increment Long wrong format");
    KeyValue kv = keyValues.get(0);
    Strings.SET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, 0, MutationOptions.NONE, false);
    try {
      Strings.INCRBY(map, kv.keyPtr, kv.keySize, 1);
    } catch (OperationFailedException e) {
      return;
    }
    fail("Test failed");
  }
  
  @Ignore
  @Test
  public void testIncrementDoubleWrongFormat() {
    System.out.println("Test Increment Double wrong format");
    KeyValue kv = keyValues.get(0);
    Strings.SET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, 0, MutationOptions.NONE, false);
    try {
      Strings.INCRBYFLOAT(map, kv.keyPtr, kv.keySize, 1d);
    } catch (OperationFailedException e) {
      return;
    }
    fail("Test failed");
  }
  
  @Ignore
  @Test
  public void testIncrementLong() throws OperationFailedException {
    System.out.println("Test Increment Long ");
    
    KeyValue kv = keyValues.get(0);
    
    Strings.INCRBY(map, kv.keyPtr, kv.keySize, 0);
    int size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    long value = Utils.strToLong(buffer, size);
    assertEquals(0L, value);
    
    Strings.INCRBY(map, kv.keyPtr, kv.keySize, -11110);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToLong(buffer, size);
    String svalue = Utils.toString(buffer, size);
    assertEquals(-11110L, value);
    
    Strings.INCRBY(map, kv.keyPtr, kv.keySize, 11110);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToLong(buffer, size);
    assertEquals(0L, value);
    
    Strings.INCRBY(map, kv.keyPtr, kv.keySize, 10);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToLong(buffer, size);
    assertEquals(10L, value);
 
    Strings.INCRBY(map, kv.keyPtr, kv.keySize, 100);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToLong(buffer, size);
    assertEquals(110L, value);

    Strings.INCRBY(map, kv.keyPtr, kv.keySize, 1000);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToLong(buffer, size);
    assertEquals(1110L, value);
    
    
    Strings.INCRBY(map, kv.keyPtr, kv.keySize, 10000);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToLong(buffer, size);
    assertEquals(11110L, value);
    
    Strings.INCRBY(map, kv.keyPtr, kv.keySize, Long.MIN_VALUE);
    long newValue = value + Long.MIN_VALUE;
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToLong(buffer, size);
    
    assertEquals(newValue, value);

  }
  
  @Ignore
  @Test
  public void testIncrementDouble() throws OperationFailedException {
    System.out.println("Test Increment Double ");
    
    KeyValue kv = keyValues.get(0);
    
    Strings.INCRBYFLOAT(map, kv.keyPtr, kv.keySize, 10d);
    
    int size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    double value = Utils.strToDouble(buffer, size);
    assertEquals(10d, value);
    
    Strings.INCRBYFLOAT(map, kv.keyPtr, kv.keySize, 100d);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToDouble(buffer, size);
    assertEquals(110d, value);
    
    Strings.INCRBYFLOAT(map, kv.keyPtr, kv.keySize, 1000d);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToDouble(buffer, size);
    assertEquals(1110d, value);
    
    Strings.INCRBYFLOAT(map, kv.keyPtr, kv.keySize, 10000d);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToDouble(buffer, size);
    assertEquals(11110d, value);
    
    Strings.INCRBYFLOAT(map, kv.keyPtr, kv.keySize, -11110d);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToDouble(buffer, size);
    assertEquals(0d, value);
    
    double incr = Double.MAX_VALUE / 2;
    double newValue = value + incr;
    
    Strings.INCRBYFLOAT(map, kv.keyPtr, kv.keySize, incr);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToDouble(buffer, size);
    assertEquals(newValue, value);
    
    incr = - Double.MAX_VALUE / 2;
    newValue = value + incr;
    
    Strings.INCRBYFLOAT(map, kv.keyPtr, kv.keySize, incr);
    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    value = Utils.strToDouble(buffer, size);
    assertEquals(newValue, value);
    
  }
  
  @Ignore
  @Test
  public void testSetGet () {
    System.out.println("Test Strings Set/Get ");
 
    long start = System.currentTimeMillis();
    long totalSize = 0;
    for (int i = 0; i < n; i++) {
      KeyValue kv = keyValues.get(i);
      totalSize += kv.keySize + kv.valueSize;
      boolean result = Strings.SET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, 0, 
        MutationOptions.NONE, true);
      assertEquals(true, result);
      if ((i+1) % 10000 == 0) {
        System.out.println(i+1);
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("Total allocated memory ="+ BigSortedMap.getTotalAllocatedMemory() 
    + " for "+ n + " " + (totalSize) + " byte values. Overhead="+ 
        ((double)BigSortedMap.getTotalAllocatedMemory() - totalSize)/n +
    " bytes per key-value. Time to load: "+(end -start)+"ms");
    start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      KeyValue kv = keyValues.get(i);
      long valueSize = Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
      assertEquals(kv.valueSize, (int)valueSize);
      assertTrue(Utils.compareTo(kv.valuePtr, kv.valueSize, buffer, (int)valueSize) == 0);
    }
    end = System.currentTimeMillis();
    System.out.println("Time GET ="+(end -start)+"ms");
    BigSortedMap.printMemoryAllocationStats();
   
 
  }
  
  @Ignore
  @Test
  public void testSetRemove() {
    System.out.println("Test Strings Set/Remove ");
    
    long start = System.currentTimeMillis();
    long totalSize = 0;
    for (int i = 0; i < n; i++) {
      KeyValue kv = keyValues.get(i);
      totalSize += kv.keySize + kv.valueSize;
      boolean result = Strings.SET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, 0, 
        MutationOptions.NONE, true);
      assertEquals(true, result);
      if ((i+1) % 10000 == 0) {
        System.out.println(i+1);
      }
    }
    
    BigSortedMap.printMemoryAllocationStats();
    
    long end = System.currentTimeMillis();
    System.out.println("Total allocated memory ="+ BigSortedMap.getTotalAllocatedMemory() 
    + " for "+ n + " " + (totalSize) + " byte values. Overhead="+ 
        ((double)BigSortedMap.getTotalAllocatedMemory() - totalSize)/n +
    " bytes per key-value. Time to load: "+(end -start)+"ms");
    start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      KeyValue kv = keyValues.get(i);
      boolean result = Strings.DELETE(map, kv.keyPtr, kv.keySize);
      assertEquals(true, result);
    }
    end = System.currentTimeMillis();
    System.out.println("Time DELETE ="+(end - start)+"ms");
    start = System.currentTimeMillis();
    for (int i=0; i < n; i++) {
      KeyValue kv = keyValues.get(i);
      long result = Strings.GET(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
      assertEquals(-1, (int) result);
    }
    end = System.currentTimeMillis();
    System.out.println("Time to GET " + n + " values="+ (end - start)+"ms");
  }
  
  
  @Ignore
  @Test
  public void testAppend() {
    System.out.println("Test Strings Append ");
    KeyValue kv = keyValues.get(0);
    
    int size = Strings.APPEND(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize);
    assertEquals(kv.valueSize, size);
    
    size = Strings.APPEND(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize);
    assertEquals(2 * kv.valueSize, size);

    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize,  buffer, bufferSize);
    
    assertEquals(2 * kv.valueSize, size);
    assertTrue(Utils.compareTo(kv.valuePtr, kv.valueSize, buffer, kv.valueSize) == 0);    
    assertTrue(Utils.compareTo(kv.valuePtr, kv.valueSize, buffer + kv.valueSize, kv.valueSize) == 0);
  
  }
  
  @Ignore
  @Test
  public void testGetDelete() {
    System.out.println("Test Strings GetDelete operation ");
    KeyValue kv = keyValues.get(0);
    
    int size = Strings.APPEND(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize);
    assertEquals(kv.valueSize, size);
    
    size = Strings.GETDEL(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    assertEquals(kv.valueSize, size);
    assertTrue(Utils.compareTo(kv.valuePtr, kv.valueSize, buffer, kv.valueSize) == 0);    

    size = (int) Strings.GET(map, kv.keyPtr, kv.keySize,  buffer, bufferSize);
    assertEquals(-1, (int) size); // not found
    size = Strings.GETDEL(map, kv.keyPtr, kv.keySize, buffer, bufferSize);
    assertEquals(-1, (int) size); // not found    

  }
  
  @Ignore
  @Test
  public void testGetEx() {
    System.out.println("Test Strings GetEx operation ");
    KeyValue kv = keyValues.get(0);
    
    int size = Strings.APPEND(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize);
    assertEquals(kv.valueSize, size);
    
    size = Strings.GETEX(map, kv.keyPtr, kv.keySize, 100,  buffer, bufferSize);
    assertEquals(kv.valueSize, size);
    assertTrue(Utils.compareTo(kv.valuePtr, kv.valueSize, buffer, kv.valueSize) == 0);    

    long expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(100L, expire);

  }
  
  
  @Ignore
  @Test
  public void testGetSet() {
    System.out.println("Test Strings GetSet operation ");
    KeyValue kv = keyValues.get(0);
    KeyValue kv1 = keyValues.get(1);
    
    long size = Strings.GETSET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, buffer, bufferSize);
    assertEquals(-1L, size);
    
    size = Strings.GETSET(map, kv.keyPtr, kv.keySize, kv1.valuePtr, kv1.valueSize, buffer, bufferSize);
    assertEquals(kv.valueSize, (int) size);
    assertTrue(Utils.compareTo(kv.valuePtr, kv.valueSize, buffer, kv.valueSize) == 0);    

    size = Strings.GETSET(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, buffer, bufferSize);
    assertEquals(kv1.valueSize, (int) size);
    assertTrue(Utils.compareTo(kv1.valuePtr, kv1.valueSize, buffer, kv1.valueSize) == 0);   

  }
  
  @Ignore
  @Test
  public void testStrLength() {
    System.out.println("Test Strings StrLength operation ");
    KeyValue kv = keyValues.get(0);
    long size = Strings.STRLEN(map, kv.keyPtr, kv.keySize);
    assertEquals(0L, size);
    size = Strings.APPEND(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize);
    assertEquals(kv.valueSize, (int) size);
    size = Strings.STRLEN(map, kv.keyPtr, kv.keySize);
    assertEquals(kv.valueSize, (int) size);
  }
  
  @Ignore
  @Test
  public void testSetEx() {
    System.out.println("Test Strings SetEx operation ");
    KeyValue kv = keyValues.get(0);
    
    boolean res = Strings.SETEX(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, 100);
    assertTrue(res);
    
    long expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(100L, expire);
    
    res = Strings.SETEX(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, 200);
    assertTrue(res);
    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(200L, expire);

  }
  
  @Ignore
  @Test
  public void testPSetEx() {
    System.out.println("Test Strings PSetEx operation ");
    KeyValue kv = keyValues.get(0);
    
    boolean res = Strings.PSETEX(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, 100);
    assertTrue(res);
    
    long expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(100L, expire);
    
    res = Strings.PSETEX(map, kv.keyPtr, kv.keySize, kv.valuePtr, kv.valueSize, 200);
    assertTrue(res);
    expire = Strings.GETEXPIRE(map, kv.keyPtr, kv.keySize);
    assertEquals(200L, expire);

  }
  
  @Ignore
  @Test
  public void testMget() {
    System.out.println("Test Strings MGet operation ");
    for (KeyValue kv: keyValues) {
      // we use key as a value b/c its small
      Strings.APPEND(map, kv.keyPtr, kv.keySize, kv.keyPtr, kv.keySize);
    }
    
    // Test existing
    for(int i = 0; i < 100; i++) {
     long[] arr =  Utils.randomDistinctArray(keyValues.size() - 1, 11);
     long[] ptrs = new long[arr.length];
     int[] sizes = new int[arr.length];
     for (int k = 0; k < arr.length; k++) {
       KeyValue kv = keyValues.get((int) arr[k]);
       ptrs[k] = kv.keyPtr;
       sizes[k] = kv.keySize;
     }
     
     long size = Strings.MGET(map, ptrs, sizes, buffer, bufferSize);
     verify(arr);
    }
    
    // Test run non-existent
    
    long[] arr = Utils.randomDistinctArray(keyValues.size() - 1, 11);
    long[] ptrs = new long[arr.length];
    int[] sizes = new int[arr.length];
    for (int k = 0; k < arr.length; k++) {
      KeyValue kv = keyValues.get((int) arr[k]);
      ptrs[k] = kv.valuePtr;
      sizes[k] = kv.valueSize;
    }
    
    long size = Strings.MGET(map, ptrs, sizes, buffer, bufferSize);
    assertEquals(Utils.SIZEOF_INT + arr.length * Utils.SIZEOF_INT, (int) size);
    long ptr = buffer;
    assertEquals(arr.length, UnsafeAccess.toInt(ptr));
    ptr += Utils.SIZEOF_INT;
    for (int i = 0; i < arr.length; i++) {
      assertEquals(-1, UnsafeAccess.toInt(ptr));
      ptr += Utils.SIZEOF_INT;
    }
  }
  
  private void verify(long[] arr) {
    assertEquals(arr.length, UnsafeAccess.toInt(buffer));
    long ptr =  buffer + Utils.SIZEOF_INT;
    for (int i = 0; i < arr.length; i++) {
      KeyValue kv = keyValues.get((int) arr[i]);
      assertEquals(kv.keySize, UnsafeAccess.toInt(ptr));
      ptr += Utils.SIZEOF_INT;
      assertTrue(Utils.compareTo(kv.keyPtr, kv.keySize, ptr, kv.keySize) == 0);
      ptr += kv.keySize;
    }
  }
  
  @Ignore
  @Test
  public void testMSet() {
    System.out.println("Test Strings MSet operation ");
    Strings.MSET(map, keyValues);
    for (KeyValue kv: keyValues) {
      assertTrue(Strings.keyExists(map, kv.keyPtr, kv.keySize));
    }
  }
  
  @Ignore
  @Test
  public void testMSetNX() {
    System.out.println("Test Strings MSetNX operation ");
    boolean res = Strings.MSETNX(map, keyValues);
    assertTrue(res);
    for (KeyValue kv: keyValues) {
      assertTrue(Strings.keyExists(map, kv.keyPtr, kv.keySize));
    }
    res = Strings.MSETNX(map, keyValues);
    assertFalse(res);
  }
  
  @Ignore
  @Test
  public void testSetGetBit() {
    
    testSetGetBitInternal(1000);
    testSetGetBitInternal(100000);
    
  }
  
  private void testSetGetBitInternal(int valueSize) {
    // Small bitset test
    long valuePtr = UnsafeAccess.mallocZeroed(valueSize);
    KeyValue kv = keyValues.get(0);
    // Check non-existing key
    int bit = Strings.GETBIT(map, kv.keyPtr, kv.keySize, -1000);
    assertEquals(0, bit);
    bit = Strings.GETBIT(map, kv.keyPtr, kv.keySize, 0);
    assertEquals(0, bit);
    bit = Strings.GETBIT(map, kv.keyPtr, kv.keySize, 1000);
    assertEquals(0, bit);
    
    // Set K-V
    int size = Strings.APPEND(map, kv.keyPtr, kv.keySize, valuePtr, valueSize);
    assertEquals(valueSize, size);
    
    // Check some bits - MUST be 0 all
    for (int i = 0; i < valueSize * Utils.BITS_PER_BYTE; i++) {
      bit = Strings.GETBIT(map, kv.keyPtr, kv.keySize, i);
      assertEquals(0, bit);
    }
    
    for (int i = 0; i < valueSize * Utils.BITS_PER_BYTE; i++) {
      bit = Strings.SETBIT(map, kv.keyPtr, kv.keySize, i, 1);
      assertEquals(0, bit);
      bit = Strings.GETBIT(map, kv.keyPtr, kv.keySize, i);
      assertEquals(1, bit);
      bit = Strings.SETBIT(map, kv.keyPtr, kv.keySize, i, 0);
      assertEquals(1, bit);
      bit = Strings.GETBIT(map, kv.keyPtr, kv.keySize, i);
      assertEquals(0, bit);
    }
    
    // Some out of range ops
    Strings.SETBIT(map, kv.keyPtr, kv.keySize, (long) 2 * valueSize * Utils.BITS_PER_BYTE, 1);
    long len = Strings.STRLEN(map, kv.keyPtr, kv.keySize);
    assertEquals(2 * valueSize + 1, (int) len);
    
    bit = Strings.GETBIT(map, kv.keyPtr, kv.keySize, (long) 2 * valueSize * Utils.BITS_PER_BYTE);
    assertEquals(1, bit);
    
    bit = Strings.SETBIT(map, kv.keyPtr, kv.keySize, (long) 2 * valueSize * Utils.BITS_PER_BYTE, 0);
    assertEquals( 1, bit);
    
    bit = Strings.GETBIT(map, kv.keyPtr, kv.keySize, (long) 2 * valueSize * Utils.BITS_PER_BYTE);
    assertEquals(0, bit);
    
    // Verify all 0s
    for (int i=0; i < (2 * valueSize + 1) * Utils.BITS_PER_BYTE; i++) {
      bit = Strings.GETBIT(map, kv.keyPtr, kv.keySize, (long) 2 * valueSize * Utils.BITS_PER_BYTE);
      assertEquals(0, bit);
    }
    
    boolean result = Strings.DELETE(map, kv.keyPtr, kv.keySize);
    assertTrue(result);
    
    bit = Strings.SETBIT(map, kv.keyPtr, kv.keySize, (long) 2 * valueSize * Utils.BITS_PER_BYTE, 1);
    assertEquals(0, bit);
    
    bit = Strings.GETBIT(map, kv.keyPtr, kv.keySize, (long) 2 * valueSize * Utils.BITS_PER_BYTE);
    assertEquals(1, bit);
    
    len = Strings.STRLEN(map, kv.keyPtr, kv.keySize);
    assertEquals(2 * valueSize + 1, (int) len);
    
    for(int i=0; i < 2 * valueSize * Utils.BITS_PER_BYTE; i++) {
      bit = Strings.GETBIT(map, kv.keyPtr, kv.keySize, i);
      assertEquals(0, bit);
    }
    Strings.DELETE(map, kv.keyPtr, kv.keySize);
    UnsafeAccess.free(valuePtr);
  }
  
  @Ignore
  @Test
  public void testGetRange() {
    System.out.println("Test Strings GetRange");
    testGetRangeInternal(1000);
    testGetRangeInternal(10000);
  }
  
  private void testGetRangeInternal(int valueSize) {
    // Small bitset test
    long valuePtr = UnsafeAccess.mallocZeroed(valueSize);
    Utils.fillRandom(valuePtr, valueSize);
    
    KeyValue kv = keyValues.get(0);
    Strings.APPEND(map, kv.keyPtr, kv.keySize, valuePtr, valueSize);
    long buf = UnsafeAccess.malloc(valueSize);
    int bufSize = valueSize;
    long buf1 = UnsafeAccess.malloc(valueSize);
    int bufSize1 = valueSize;
    
    // Tets edge cases
    // 1. end = start = +-inf
    int size = Strings.GETRANGE(map, kv.keyPtr, kv.keySize, Commons.NULL_LONG, Commons.NULL_LONG, buf, bufSize);
    assertEquals(valueSize, size);
    assertTrue(Utils.compareTo(valuePtr, valueSize, buf, valueSize) == 0);
    
    // 1. start = -inf, end is in range
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Test seed="+ seed);
    
    for (int i = 0; i < 100; i ++) {
      long start = Commons.NULL_LONG;
      long end = r.nextInt(valueSize);
      int expSize = getRange(valuePtr, valueSize, start, end, buf, bufSize);
      size = Strings.GETRANGE(map, kv.keyPtr, kv.keySize, start, end, buf1, bufSize1);
      assertEquals(expSize, size);
      if (size > 0) {
        assertTrue(Utils.compareTo(buf, size, buf1, size) == 0);
      }
    }
    
    // 2. end = +inf, start is in range
    
    for (int i = 0; i < 100; i ++) {
      long end = Commons.NULL_LONG;
      long start = r.nextInt(valueSize);
      int expSize = getRange(valuePtr, valueSize, start, end, buf, bufSize);
      size = Strings.GETRANGE(map, kv.keyPtr, kv.keySize, start, end, buf1, bufSize1);
      assertEquals(expSize, size);
      if (size > 0) {
        assertTrue(Utils.compareTo(buf, size, buf1, size) == 0);
      }
    }
    
    // 3. end and start are in range
    
    for (int i = 0; i < 200; i ++) {
      long end = r.nextInt(valueSize);
      long start = r.nextInt(valueSize);
      int expSize = getRange(valuePtr, valueSize, start, end, buf, bufSize);
      size = Strings.GETRANGE(map, kv.keyPtr, kv.keySize, start, end, buf1, bufSize1);
      assertEquals(expSize, size);
      if (size > 0) {
        assertTrue(Utils.compareTo(buf, size, buf1, size) == 0);
      }
    }
    Strings.DELETE(map, kv.keyPtr, kv.keySize);
    UnsafeAccess.free(buf);
    UnsafeAccess.free(buf1);
    UnsafeAccess.free(valuePtr);

  }
  
  private int getRange(long ptr, int size, long start, long end, long buffer, int bufferSize) {
    // We assume that bufferSize >= size
    if (start == Commons.NULL_LONG) {
      start = 0;
    }
    if (end == Commons.NULL_LONG) {
      end = size - 1;
    }
    if (start < 0) {
      start = start + size;
    }
    if (start < 0) {
      start = 0;
    }
    if (end < 0) {
      end = end + size; 
    }
    if (end >= size) {
      end = size - 1;
    }
    if (end < 0 || end < start || start >= size) {
      return 0;
    }
    UnsafeAccess.copy(ptr + start, buffer, end - start + 1);
    return (int) (end - start + 1);
  }
  
  @Ignore
  @Test
  public void testSetRange() {
    
    System.out.println("Test Strings SetRange");
    int valueSize = 1000;
    long valuePtr = UnsafeAccess.mallocZeroed(valueSize);
    Utils.fillRandom(valuePtr, valueSize);
    
    KeyValue kv = keyValues.get(0);
    Strings.APPEND(map, kv.keyPtr, kv.keySize, valuePtr, valueSize);
    long buf = UnsafeAccess.malloc(valueSize);// To make sure that test won't fail
    int bufSize = valueSize;

    // Test edge cases
    // 1. offset < 0
    int size = (int)Strings.SETRANGE(map, kv.keyPtr, kv.keySize, Commons.NULL_LONG, kv.valuePtr, kv.valueSize);
    assertEquals(-1, size);
    
    for (int i = 0; i < 10000; i ++) {
      long offset = i;
      size = (int)Strings.SETRANGE(map, kv.keyPtr, kv.keySize, offset, kv.valuePtr, kv.valueSize);
      int expSize = (int) Math.max(valueSize, offset + kv.valueSize);
      assertEquals(expSize, size);
      size = Strings.GETRANGE(map, kv.keyPtr, kv.keySize, offset, offset + kv.valueSize - 1, buf, bufSize);
      assertEquals(kv.valueSize, size);
      if (size > 0) {
        assertTrue(Utils.compareTo(buf, size, kv.valuePtr, kv.valueSize) == 0);
      }
    }
    UnsafeAccess.free(buf);
    UnsafeAccess.free(valuePtr);

  }
  
  @Ignore
  @Test
  public void testBitCount() {
    System.out.println("Test Strings BitCount");
    int valueSize = 1000;
    long valuePtr = UnsafeAccess.mallocZeroed(valueSize);
    Utils.fillRandom(valuePtr, valueSize);
    KeyValue kv = keyValues.get(0);
    int size = Strings.APPEND(map, kv.keyPtr, kv.keySize, valuePtr, valueSize);
    assertEquals(valueSize, size);
    
    int expCount = bitcount(valuePtr, valueSize, Commons.NULL_LONG, Commons.NULL_LONG);
    int count = (int) Strings.BITCOUNT(map, kv.keyPtr, kv.keySize, Commons.NULL_LONG, Commons.NULL_LONG);
   
    assertEquals(expCount, count);
    
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Test seed="+ seed);
    
    // 1. start =-inf, end in range
    for(int i = 0; i < 100; i++) {
      long start = Commons.NULL_LONG;
      long end = r.nextInt(valueSize);
      expCount = bitcount(valuePtr, valueSize, start, end);
      count = (int) Strings.BITCOUNT(map, kv.keyPtr, kv.keySize, start, end);
      assertEquals(expCount, count);
    }
    
    // 2. start in range, end is +inf
    for(int i = 0; i < 100; i++) {
      long end = Commons.NULL_LONG;
      long start = r.nextInt(valueSize);
      expCount = bitcount(valuePtr, valueSize, start, end);
      count = (int) Strings.BITCOUNT(map, kv.keyPtr, kv.keySize, start, end);
      assertEquals(expCount, count);
    }
    
    // 2. start in range, end is too
    for(int i = 0; i < 200; i++) {
      long end = r.nextInt(valueSize);
      long start = r.nextInt(valueSize);
      expCount = bitcount(valuePtr, valueSize, start, end);
      count = (int) Strings.BITCOUNT(map, kv.keyPtr, kv.keySize, start, end);
      assertEquals(expCount, count);
    }
    
    UnsafeAccess.free(valuePtr);
  }
  
  private int bitcount(long ptr, int size, long start, long end) {
    if (start == Commons.NULL_LONG) {
      start = 0;
    }    
    if (start < 0) {
      start = Math.max(start + size, 0);
    }
    if (end == Commons.NULL_LONG) {
      end = size - 1;
    }
    if (end < 0) {
      end += size;
    }
    if (end < 0 || start > end || start >= size) return 0;
    
    if (end >= size) {
      end = size - 1;
    }
    
    return (int) Utils.bitcount(ptr + start, (int) (end - start + 1));
  }
  
  @Ignore
  @Test
  public void testBitPosition() {
    System.out.println("Test Strings BitPos");
    int valueSize = 1000;
    long valuePtr = UnsafeAccess.mallocZeroed(valueSize);
    Utils.fillRandom(valuePtr, valueSize);
    KeyValue kv = keyValues.get(0);
    int size = Strings.APPEND(map, kv.keyPtr, kv.keySize, valuePtr, valueSize);
    assertEquals(valueSize, size);
    
    int expPos = (int) Utils.bitposSet(valuePtr, valueSize);
    int pos = (int) Strings.BITPOS(map, kv.keyPtr, kv.keySize, 1, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(expPos, pos);
    
    expPos = (int) Utils.bitposUnset(valuePtr, valueSize);
    pos = (int) Strings.BITPOS(map, kv.keyPtr, kv.keySize, 0, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(expPos, pos);
    
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Test seed="+ seed);
    
    // 1. start =-inf, end in range
    for(int i = 0; i < 100; i++) {
      long start = Commons.NULL_LONG;
      long end = r.nextInt(valueSize);
      expPos = bitpos(valuePtr, valueSize, 1, start, end);
      pos = (int) Strings.BITPOS(map, kv.keyPtr, kv.keySize, 1, start, end);
      assertEquals(expPos, pos);
      expPos = bitpos(valuePtr, valueSize, 0, start, end);
      pos = (int) Strings.BITPOS(map, kv.keyPtr, kv.keySize, 0, start, end);
      assertEquals(expPos, pos);
    }
    
    // 2. start in range, end is +inf
    for(int i = 0; i < 100; i++) {
      long end = Commons.NULL_LONG;
      long start = r.nextInt(valueSize);
      expPos = bitpos(valuePtr, valueSize, 1, start, end);
      pos = (int) Strings.BITPOS(map, kv.keyPtr, kv.keySize, 1, start, end);
      assertEquals(expPos, pos);
      expPos = bitpos(valuePtr, valueSize, 0, start, end);
      pos = (int) Strings.BITPOS(map, kv.keyPtr, kv.keySize, 0, start, end);
      assertEquals(expPos, pos);
    }
    
    // 2. start in range, end is too
    for(int i = 0; i < 200; i++) {
      long start = r.nextInt(valueSize);
      long end = r.nextInt(valueSize);
      expPos = bitpos(valuePtr, valueSize, 1, start, end);
      pos = (int) Strings.BITPOS(map, kv.keyPtr, kv.keySize, 1, start, end);
      assertEquals(expPos, pos);
      expPos = bitpos(valuePtr, valueSize, 0, start, end);
      pos = (int) Strings.BITPOS(map, kv.keyPtr, kv.keySize, 0, start, end);
      assertEquals(expPos, pos);
    }
    
    UnsafeAccess.free(valuePtr);
  }
  
  private int bitpos(long ptr, int size, int bit, long start, long end) {
    
    boolean startEndSet = 
        start != Commons.NULL_LONG || end != Commons.NULL_LONG;
    
    if (start == Commons.NULL_LONG) {
      start = 0;
    }    
    if (start < 0) {
      start = Math.max(start + size, 0);
    }
    if (end == Commons.NULL_LONG) {
      end = size - 1;
    }
    if (end < 0) {
      end += size;
    }
    if (end < 0 || start > end || start >= size) return -1;
    
    if (end >= size) {
      end = size - 1;
    }  
    int pos = -1;
    if (bit == 1) {
      pos = (int) Utils.bitposSet(ptr + start, (int) (end - start + 1));
    } else if (bit == 0) {
      pos = (int) Utils.bitposUnset(ptr + start, (int) (end - start + 1));
    }
    if (pos == -1 && bit == 0 && !startEndSet) {
      pos = size * Utils.BITS_PER_BYTE;
    }
    return pos;
  }
  
  private void tearDown() {
    // Dispose
    map.dispose();
    for (KeyValue k: keyValues) {
      UnsafeAccess.free(k.keyPtr);
      UnsafeAccess.free(k.valuePtr);
    }
    UnsafeAccess.free(buffer);
    BigSortedMap.printMemoryAllocationStats();
    UnsafeAccess.mallocStats.printStats();
  }
}


class FakeUserSession {
  
  static final String[] ATTRIBUTES = new String[] {
      "attr1", "attr2", "attr3", "attr4", "attr5", 
      "attr6", "attr7", "attr8", "attr9", "attr10"
  };
  
  Properties props = new Properties();
  
  FakeUserSession(Properties p){
    this.props = p;
  }
  
  static FakeUserSession newSession(int i) {
    Properties p = new Properties();
    for (String attr: ATTRIBUTES) {
      p.put(attr, attr + ":value:"+ i);
    }
    return new FakeUserSession(p);
  }
  
  public String toString() {
    return props.toString();
  }
  
}