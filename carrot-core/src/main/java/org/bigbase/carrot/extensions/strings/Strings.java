package org.bigbase.carrot.extensions.strings;

import static org.bigbase.carrot.extensions.Commons.KEY_SIZE;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.util.UnsafeAccess;

/**
 * Supports various string operation, found in Redis
 * @author Vladimir Rodionov
 *
 */
public class Strings {

  private static ThreadLocal<Long> keyArena = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(512);
    }
  };
  
  private static ThreadLocal<Integer> keyArenaSize = new ThreadLocal<Integer>() {
    @Override
    protected Integer initialValue() {
      return 512;
    }
  };
  
  static ThreadLocal<Long> valueArena = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(512);
    }
  };
  
  static ThreadLocal<Integer> valueArenaSize = new ThreadLocal<Integer>() {
    @Override
    protected Integer initialValue() {
      return 512;
    }
  };
  
  /**
   * Thread local updates String Append
   */
  private static ThreadLocal<StringAppend> stringAppend = new ThreadLocal<StringAppend>() {
    @Override
    protected StringAppend initialValue() {
      return new StringAppend();
    } 
  };
  /**
   * Checks key arena size
   * @param required size
   */
  
  static void checkKeyArena (int required) {
    int size = keyArenaSize.get();
    if (size >= required ) {
      return;
    }
    long ptr = UnsafeAccess.realloc(keyArena.get(), required);
    keyArena.set(ptr);
    keyArenaSize.set(required);
  }
  
  /**
   * Checks value arena size
   * @param required size
   */
  static void checkValueArena (int required) {
    int size = valueArenaSize.get();
    if (size >= required) {
      return;
    }
    long ptr = UnsafeAccess.realloc(valueArena.get(), required);
    valueArena.set(ptr);
    valueArenaSize.set(required);
  }
  /**
   * Build key for Hash. It uses thread local key arena 
   * TODO: data type prefix
   * @param keyPtr original key address
   * @param keySize original key size
   * @param fieldPtr field address
   * @param fieldSize field size
   * @return new key size 
   */
    
   
  private static int buildKey( long keyPtr, int keySize) {
    checkKeyArena(keySize + KEY_SIZE );
    long arena = keyArena.get();
    int kSize = KEY_SIZE + keySize;
    UnsafeAccess.putInt(arena, keySize);
    UnsafeAccess.copy(keyPtr, arena + KEY_SIZE, keySize);
    return kSize;
  }
  /**
   * If key already exists and is a string, this command appends the value at the end of the string. 
   * If key does not exist it is created and set as an empty string, 
   * so APPEND will be similar to SET in this special case.
   * @param map sorted map storage
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @return the length of the string after the append operation (Integer).
   */
  public static int append(BigSortedMap map, long keyPtr, int keySize, long valuePtr, int valueSize) {
    int kSize = buildKey(keyPtr, keySize);
    StringAppend append = stringAppend.get();
    append.reset();
    append.setKeyAddress(keyArena.get());
    append.setKeySize(kSize);
    append.setAppendValue(valuePtr, valueSize);
    // version?    
    boolean result = map.update(append);
    if(result) {
      return append.getSizeAfterAppend();
    } else {
      return -1;
    }
  }
  
  
  
}
