package org.bigbase.carrot.examples.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.KeyValue;
import org.bigbase.carrot.redis.hashes.Hashes;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

import redis.clients.jedis.Jedis;
/**
 * Simple utility class, which can convert 
 * properties to a list of KeyValue objects for testing
 * @author vrodionov
 *
 */
public abstract class KeyValues {
  protected Properties props = new Properties();

  protected KeyValues(Properties p){
    this.props = p;
  }
  
  public abstract String getKey(); 
  
  public static KeyValue fromKeyValue(String key, String value) {
    long keyPtr = UnsafeAccess.allocAndCopy(key.getBytes(), 0, key.length());
    int keySize = key.length();
    long valuePtr = UnsafeAccess.allocAndCopy(value.getBytes(), 0, value.length());
    int valueSize = value.length();
    return new KeyValue(keyPtr, keySize, valuePtr, valueSize);
  }
  
  public static KeyValue fromKeyAndNumericValue(String key, String value) {
    long keyPtr = UnsafeAccess.allocAndCopy(key.getBytes(), 0, key.length());
    int keySize = key.length();
    long valuePtr = 0;
    int valueSize = 0;
    // value is numeric 
    long v = Long.parseLong(value);
    if (v < Byte.MAX_VALUE && v > Byte.MIN_VALUE) {
      valueSize = 1;
      valuePtr = UnsafeAccess.malloc(valueSize);
      UnsafeAccess.putByte(valuePtr, (byte) v);
    } else if ( v < Short.MAX_VALUE && v > Short.MIN_VALUE) {
      valueSize = 2;
      valuePtr = UnsafeAccess.malloc(valueSize);
      UnsafeAccess.putShort(valuePtr, (short) v);
    } else if ( v < Integer.MAX_VALUE && v > Integer.MIN_VALUE) {
      valueSize = 4;
      valuePtr = UnsafeAccess.malloc(valueSize);
      UnsafeAccess.putInt(valuePtr, (int) v);
    } else {
      valueSize = 8;
      valuePtr = UnsafeAccess.malloc(valueSize);
      UnsafeAccess.putLong(valuePtr,  v);
    }
    
    return new KeyValue(keyPtr, keySize, valuePtr, valueSize);
  }
  
  public Map<String, String> getPropsMap() {
    HashMap<String, String> map = new HashMap<String, String>();
    for (Map.Entry<Object, Object> e: props.entrySet()){
      map.put((String)e.getKey(), (String)e.getValue());
    }
    return map;
  }
  
  /**
   * Helper method for Carrot API
   * @return list of key-values
   */
  public List<KeyValue> asList() {
    
    List<KeyValue> list = new ArrayList<KeyValue>();
    
    for (Map.Entry<Object, Object> e: props.entrySet()) {
      String key = (String) e.getKey();
      String value = (String) e.getValue();
      long keyPtr = UnsafeAccess.malloc(key.length());
      int keySize = key.length();
      UnsafeAccess.copy(key.getBytes(), 0, keyPtr, keySize);
      
      long valuePtr = UnsafeAccess.malloc(value.length());

      int valueSize = value.length();
      UnsafeAccess.copy(value.getBytes(), 0, valuePtr, valueSize);
      list.add( new KeyValue(keyPtr, keySize, valuePtr, valueSize));
    }
    
    return list;
  }
  
  /**
   * Helper method for Redis
   * @return map of key, values
   */
  public Map<byte[], byte[]> asMap() {
    Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();
    for (Map.Entry<Object, Object> e: props.entrySet()) {
      String key = (String) e.getKey();
      String value = (String) e.getValue();
      map.put(key.getBytes(), value.getBytes());
    }
    return map;
  }
  
  public void saveToCarrot(BigSortedMap map) {
    List<KeyValue> list = asList();
    Hashes.HSET(map, getKey(), asList());
    Utils.freeKeyValues(list);
  }
  
  public boolean verify(BigSortedMap map) {
    long buffer = UnsafeAccess.malloc(4096);
    String key = getKey();
    long keyPtr = UnsafeAccess.allocAndCopy(key.getBytes(), 0, key.length());
    int keySize = key.length();
    try {
      long size = Hashes.HGETALL(map, keyPtr, keySize, buffer, 4096);
      if (size < 0 || size > 4096) {
        System.err.println("Verification failed, size=" + size);
        return false;
      }

      int num = UnsafeAccess.toInt(buffer);
      if (num != props.size()) {
        System.err.println("Verification failed, num=" + num+" expected="+ 
            props.size());
        return false;
      }
      return true;
    } finally {
      UnsafeAccess.free(keyPtr);
      UnsafeAccess.free(buffer);
    }
  }
  
  public void saveToRedis(Jedis client) {
    client.hset (getKey().getBytes(), asMap());
  }
  
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    for ( Map.Entry<Object, Object> e: props.entrySet()) {
      sb.append(e.getKey()).append("=").append(e.getValue()).append('\n');
    }
    return sb.toString();
  }
}
