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
package org.bigbase.carrot.redis.hashes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.Key;
import org.bigbase.carrot.util.KeyValue;
//import org.bigbase.carrot.redis.Commons;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.bigbase.carrot.util.Value;
import org.junit.Ignore;
import org.junit.Test;

public class HashesTest {
  BigSortedMap map;
  Key key;
  long buffer;
  int bufferSize = 64;
  int keySize = 8;
  int valSize = 8;
  long n = 2000000;
  List<Value> values;
  
  static {
    //UnsafeAccess.debug = true;
  }
  
  private List<Value> getValues(long n) {
    List<Value> values = new ArrayList<Value>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("VALUES SEED=" + seed);
    byte[] buf = new byte[valSize];
    for (int i = 0; i < n; i++) {
      r.nextBytes(buf);
      long ptr = UnsafeAccess.malloc(valSize);
      UnsafeAccess.copy(buf, 0, ptr, valSize);
      values.add(new Value(ptr, valSize));
    }
    return values;
  }
  
  private List<KeyValue> getKeyValues(int n){
    List<KeyValue> list = new ArrayList<KeyValue>(n);
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("VALUES SEED=" + seed);
    byte[] buf = new byte[valSize];
    for (int i = 0; i < n; i++) {
      r.nextBytes(buf);
      long fptr = UnsafeAccess.malloc(valSize);
      long vptr = UnsafeAccess.malloc(valSize);
      UnsafeAccess.copy(buf, 0, fptr, valSize);
      UnsafeAccess.copy(buf, 0, vptr, valSize);
      list.add(new KeyValue(fptr, valSize, vptr, valSize));
    }
    return list;
  }
  
  private Key getKey() {
    long ptr = UnsafeAccess.malloc(keySize);
    byte[] buf = new byte[keySize];
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("KEY SEED=" + seed);
    r.nextBytes(buf);
    UnsafeAccess.copy(buf, 0, ptr, keySize);
    return key = new Key(ptr, keySize);
  }
  
  private void setUp() {
    map = new BigSortedMap(1000000000);
    buffer = UnsafeAccess.mallocZeroed(bufferSize); 
    values = getValues(n);
  }
  

  //@Ignore
  @Test
  public void runAllNoCompression() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    System.out.println();
    for (int i = 0; i < 1; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=NULL");
      allTests();
      BigSortedMap.printGlobalMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
 // @Ignore
  @Test
  public void runAllCompressionLZ4() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    System.out.println();
    for (int i = 0; i < 1; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=LZ4");
      allTests();
      BigSortedMap.printGlobalMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  @Ignore
  @Test
  public void runAllCompressionLZ4HC() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    System.out.println();
    for (int i = 0; i < 1; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=LZ4HC");
      allTests();
      BigSortedMap.printGlobalMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  private void allTests() throws IOException { 
    setUp();
    testNullValues();
    tearDown();
    setUp();
    testSetExists();
    tearDown();
    setUp();
    testAddRemove();
    tearDown();
    setUp();
    testSetGet();
    tearDown();
    setUp();
    testAddRemoveMulti();
    tearDown();
  }
  
  long countRecords(BigSortedMap map) throws IOException {
    return map.countRecords();
  }
  
  @Ignore
  @Test
  public void testMultiSet() {
    System.out.println("\nTest Multi Set");
    map = new BigSortedMap(1000000000);
    List<KeyValue> list = getKeyValues(1000);
    List<KeyValue> copy = new ArrayList<KeyValue>(list.size());
    list.stream().forEach(x -> copy.add(x));
    
    Key key = getKey();
    int nn = Hashes.HSET(map, key.address, key.length, copy);
    assertEquals(list.size(), nn);
    assertEquals(list.size(), (int)Hashes.HLEN(map, key.address, key.length));
    // Verify inserted
    long buffer = UnsafeAccess.malloc(valSize * 2);
    for (KeyValue kv: list) {
      int result = Hashes.HEXISTS(map, key.address, key.length, kv.keyPtr, kv.keySize);
      assertEquals(1, result);
      int size = Hashes.HGET(map, key.address, key.length, kv.keyPtr, kv.keySize, buffer, valSize * 2);
      assertEquals(valSize, size);
      assertTrue(Utils.compareTo(kv.valuePtr, kv.valueSize, buffer, valSize) == 0);
    }
    map.dispose();
    UnsafeAccess.free(buffer);
    UnsafeAccess.free(key.address);
    list.stream().forEach(x -> {UnsafeAccess.free(x.keyPtr); UnsafeAccess.free(x.valuePtr);});
  }
  
  @Ignore
  @Test
  public void testSetExists () throws IOException {
    System.out.println("\nTest Set - Exists");
    Key key = getKey();
    long elemPtr;
    int elemSize;
    long start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      elemPtr = values.get(i).address;
      elemSize = values.get(i).length;
      int num = Hashes.HSET(map, key.address, key.length, elemPtr, elemSize,  elemPtr, elemSize);
      assertEquals(1, num);
    }
    long end = System.currentTimeMillis();
    System.out.println("Total allocated memory ="+ BigSortedMap.getGlobalAllocatedMemory() 
    + " for "+ n + " " + (keySize + valSize)+ " byte values. Overhead="+ 
        ((double)BigSortedMap.getGlobalAllocatedMemory()/n - keySize - valSize)+
    " bytes per value. Time to load: "+(end -start)+"ms");

    assertEquals(n, Hashes.HLEN(map, key.address, key.length));
    start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      int res = Hashes.HEXISTS(map, key.address, key.length, values.get(i).address, values.get(i).length);
      assertEquals(1, res);
    }
    end = System.currentTimeMillis();
    System.out.println("Time exist="+(end -start)+"ms");
    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int) countRecords(map)); 
    assertEquals(0, (int)Hashes.HLEN(map, key.address, key.length));
  }
  
  @Ignore
  @Test
  public void testNullValues() {
    System.out.println("\nTest Set - Null values");
    Key key = getKey();
    long NULL = UnsafeAccess.malloc(1);
    String[] fields = new String[] {"f1", "f2", "f3", "f4"};
    String[] values = new String[] {"v1", null, "v3", null};
    
    for(int i=0; i < fields.length; i++) {
      String f = fields[i];
      String v = values[i];
      long fPtr = UnsafeAccess.allocAndCopy(f, 0, f.length());
      int fSize = f.length();
      long vPtr = v == null? NULL: UnsafeAccess.allocAndCopy(v, 0, v.length());
      int vSize = v == null? 0: v.length();
      Hashes.HSET(map, key.address, key.length, fPtr, fSize, vPtr, vSize);
    }
    
    long buffer = UnsafeAccess.malloc(8);
    
    for(int i=0; i < fields.length; i++) {
      String f = fields[i];
      String v = values[i];
      long fPtr = UnsafeAccess.allocAndCopy(f, 0, f.length());
      int fSize = f.length();
      long vPtr = v == null? NULL: UnsafeAccess.allocAndCopy(v, 0, v.length());
      int vSize = v == null? 0: v.length();
      int size = Hashes.HGET(map, key.address, key.length, fPtr, fSize, buffer, 8);
      
      if (size < 0) {// does not exists
        System.err.println("field not found "+ f);
        System.exit(-1);
      }
      
      if (vPtr == NULL && size != 0) {
        System.err.println("Expected NULL for "+ f);
        System.exit(-1);
      }
      
      if (vPtr == NULL) {
        System.out.println("Found NULL for " + f + " size="+ size);
      }
      
      if (Utils.compareTo(vPtr, vSize, buffer, size) != 0) {
        System.err.println("Failed for "+ f);
        System.exit(-1);
      }
    }
    
    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int)Hashes.HLEN(map, key.address, key.length));
  }
  
  @Ignore
  @Test
  public void testSetGet () throws IOException {
    System.out.println("\nTest Set - Get");
    Key key = getKey();
    long elemPtr;
    int elemSize;
    long start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      elemPtr = values.get(i).address;
      elemSize = values.get(i).length;
      int num = Hashes.HSET(map, key.address, key.length, elemPtr, elemSize,  elemPtr, elemSize);
      assertEquals(1, num);
    }
    long end = System.currentTimeMillis();
    System.out.println("Total allocated memory ="+ BigSortedMap.getGlobalAllocatedMemory() 
    + " for "+ n + " " + (keySize + valSize)+ " byte values. Overhead="+ 
        ((double)BigSortedMap.getGlobalAllocatedMemory()/n - (keySize + valSize))+
    " bytes per value. Time to load: "+(end - start)+"ms");
    
    assertEquals(n, Hashes.HLEN(map, key.address, key.length));
    start = System.currentTimeMillis();
    long buffer = UnsafeAccess.malloc(2 * valSize);
    int bufferSize = 2 * valSize;
    
    for (int i =0; i < n; i++) {
      int size = Hashes.HGET(map, key.address, key.length, values.get(i).address, values.get(i).length, 
        buffer, bufferSize);
      assertEquals(values.get(i).length, size);
      assertTrue(Utils.compareTo(values.get(i).address, values.get(i).length, buffer, size) == 0);
    }
    
    end = System.currentTimeMillis();
    System.out.println("Time get="+(end -start)+"ms");

    BigSortedMap.printGlobalMemoryAllocationStats();

    Hashes.DELETE(map, key.address, key.length);
    assertEquals(0, (int)countRecords(map));
    assertEquals(0, (int)Hashes.HLEN(map, key.address, key.length));
    UnsafeAccess.free(buffer);
  }
  
  @Ignore
  @Test
  public void testAddRemove() throws IOException {
    System.out.println("Test Add - Remove");
    Key key = getKey();
    long elemPtr;
    int elemSize;
    long start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      elemPtr = values.get(i).address;
      elemSize = values.get(i).length;
      int num = Hashes.HSET(map, key.address, key.length, elemPtr, elemSize,  elemPtr, elemSize);
      assertEquals(1, num);
    }
    long end = System.currentTimeMillis();
    System.out.println("Total allocated memory ="+ BigSortedMap.getGlobalAllocatedMemory() 
    + " for "+ n + " " + (keySize + valSize) + " byte values. Overhead="+ 
        ((double)BigSortedMap.getGlobalAllocatedMemory()/n - (keySize + valSize))+
    " bytes per value. Time to load: "+(end -start)+"ms");
    assertEquals(n, Hashes.HLEN(map, key.address, key.length));

    BigSortedMap.printGlobalMemoryAllocationStats();
        
    start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      int res = Hashes.HDEL(map, key.address, key.length, values.get(i).address, values.get(i).length);
      assertEquals(1, res);
    }
    end = System.currentTimeMillis();
    System.out.println("Time to delete="+(end -start)+"ms");
    assertEquals(0, (int)Hashes.HLEN(map, key.address, key.length));
    BigSortedMap.printGlobalMemoryAllocationStats();
    long recc = countRecords(map);
    assertEquals(0, (int)recc);

  }
  
  @Ignore
  @Test
  public void testAddRemoveMulti() throws IOException {
    System.out.println("Test Add - Remove Multi keys");
   
    long elemPtr;
    int elemSize;
    long start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      elemPtr = values.get(i).address;
      elemSize = values.get(i).length;
      int num = Hashes.HSET(map, elemPtr, elemSize, elemPtr, elemSize,  elemPtr, elemSize);
      assertEquals(1, num);
    }
    long end = System.currentTimeMillis();
    System.out.println("Total allocated memory ="+ BigSortedMap.getGlobalAllocatedMemory() 
    + " for "+ n + " " + (keySize + valSize) + " byte values. Overhead="+ 
        ((double)BigSortedMap.getGlobalAllocatedMemory()/n - (keySize + valSize))+
    " bytes per value. Time to load: "+(end -start)+"ms");

    BigSortedMap.printGlobalMemoryAllocationStats();
    
    start = System.currentTimeMillis();
    
    for (int i =0; i < n; i++) {
      boolean res = Hashes.DELETE(map, values.get(i).address, values.get(i).length);
      assertEquals(true, res);
    }
    
    end = System.currentTimeMillis();
    System.out.println("Time to delete="+(end -start)+"ms");
    long recc = countRecords(map);
    System.out.println("Map.size =" + recc);
    assertEquals(0, (int) recc);
 
  }
  
  private void tearDown() {
    // Dispose
    map.dispose();
    if (key != null) {
      UnsafeAccess.free(key.address);
      key = null;
    }
    for (Value v: values) {
      UnsafeAccess.free(v.address);
    }
    UnsafeAccess.free(buffer);
  }
}
