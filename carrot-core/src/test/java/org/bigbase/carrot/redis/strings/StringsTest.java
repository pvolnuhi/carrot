package org.bigbase.carrot.redis.strings;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.Key;
import org.bigbase.carrot.KeyValue;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.redis.MutationOptions;
import org.bigbase.carrot.redis.OperationFailedException;
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
    UnsafeAccess.debug = true;
  }
  
  private List<KeyValue> getKeyValues(long n) {
    List<KeyValue> keyValues = new ArrayList<KeyValue>();
    for (int i=0; i < n; i++) {
      // key
      byte[] key = ("user:"+i).getBytes();
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
  
  //@Ignore
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
  
  //@Ignore
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
  }
  
  
  private void setUp() {
    map = new BigSortedMap(1000000000);
    buffer = UnsafeAccess.mallocZeroed(bufferSize); 
    keyValues = getKeyValues(n);
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
    System.out.println("Time DELETE ="+(end -start)+"ms");
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