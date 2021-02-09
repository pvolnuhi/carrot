package org.bigbase.carrot.examples.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.bigbase.carrot.KeyValue;
import org.bigbase.carrot.util.UnsafeAccess;
/**
 * Simple utility class, which can convert 
 * properties to a list of KeyValue objects for testing
 * @author vrodionov
 *
 */
public class KeyValues {
  Properties props = new Properties();

  KeyValues(Properties p){
    this.props = p;
  }
  
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
}
