package org.bigbase.carrot.util;

import java.lang.reflect.Field;

/**
 * Mutable string for Long/Double number - string format conversions
 * @author Vladimir Rodionov
 *
 */

public class MutableString {
  /*
   * 30 chars should be OK for longest Long or Double
   */
  final static int MAX_SIZE = 30;
  
  static ThreadLocal<char[][]> buffers = new ThreadLocal<char[][]>() {

    @Override
    protected char[][] initialValue() {
      
      char[][] buf = new char[MAX_SIZE+1][];
      for(int i= 0; i < buf.length; i++) {
        buf[i] = new char[i]; // what happen if i == 0?
      }
      return buf;
    }
  };
  
  static ThreadLocal<MutableString> mStr = new ThreadLocal<MutableString>() {
    @Override
    protected MutableString initialValue() { 
      return new MutableString();
    }
  };
  
  /**
   * Get thread local instance
   * @return instance
   */
  public static MutableString get() {
    return mStr.get();
  }
  
  private String str;
  private Field field;
  
  /**
   * Gets buufer to work on
   * @param size reqired size (must be <= MAX_SIZE)
   * @return buffer or null
   */
  public char[] getBuffer(int size) {
    if (size < 0 || size > MAX_SIZE) {
      return null;
    }
    return buffers.get()[size];
  }
  
  /**
   * Sets char buffer to this mutable string
   * @param buf
   */
  public void setBuffer(char[] buf) {
    try {
      field.set(str, buf);
    } catch (IllegalArgumentException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  MutableString () {
    this.str = new String(getBuffer(0));
    try {
      field = String.class.getDeclaredField("value"); 
      field.setAccessible(true); 
    }
    catch (NoSuchFieldException e) {
        e.printStackTrace();
    }
  }
  
  @Override
  public String toString() {
    return str;
  }
}
