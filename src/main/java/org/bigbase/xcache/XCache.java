package org.bigbase.xcache;

public class XCache {


  // Byte arrays
  /**
   * Put operation
   * @param key
   * @param keyOff
   * @param keyLen
   * @param value
   * @param valOff
   * @param valLen
   * @return true if success, false - otherwise
   */
  public boolean put(byte[] key, int keyOff, int keyLen,
                     byte[] value, int valOff, int valLen )
  {
    return false;
  }

  /**
   * Get operation
   * @param key
   * @param keyOff
   * @param keyLen
   * @param value
   * @param valOff
   * @return true, if success, false otherwise
   */
  public boolean get(byte[] key, int keyOff, int keyLen, byte[] value, int valOff) {
    return false;
  }

  /**
   * Get operation
   * @param key
   * @param keyOff
   * @param keyLen
   * @return value or null
   */
  public byte[] get(byte[] key, int keyOff, int keyLen) {
    return null;
  }

  /**
   * Delete operation
   * @param key
   * @param off
   * @param len
   * @return true, if success
   */
  public boolean delete(byte[] key, int off, int len) {
    return false;
  }

  // Native memory
  /**
   * Put operation (native)
   * @param keyPtr
   * @param keyLen
   * @param valuePtr
   * @param valLen
   * @return true, if success
   */
  public boolean put(long keyPtr, int keyLen, long valuePtr, int valLen) {
    return false;
  }

  /**
   * Get operation (native)
   * @param keyPtr
   * @param keyLen
   * @param valuePtr
   * @return true, if success
   */
  public boolean get(long keyPtr, int keyLen, long valuePtr) {
    return false;
  }

  /**
   * Delete operation (native)
   * @param keyPtr
   * @param keyLen
   * @return true, if success
   */
  public boolean delete(long keyPtr, int keyLen) {
    return false;
  }

  // Direct Byte Buffers

  public long size() {
    return 0;
  }


  public long getMaxSize() {
    return 0;
  }



}
