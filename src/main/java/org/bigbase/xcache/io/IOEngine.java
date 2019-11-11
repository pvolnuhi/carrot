package org.bigbase.xcache.io;

import java.io.IOException;
import java.nio.ByteBuffer;



/**
 * A class implementing IOEngine interface supports data services for
 * {@link XCache}.
 */
public interface IOEngine {
  /**
   * @return true if persistent storage is supported for the cache when shutdown
   */
  boolean isPersistent();

  /**
   * Transfers data from IOEngine to a Cacheable object.
   * @param length How many bytes to be read from the offset
   * @param offset The offset in the IO engine where the first byte to be read
   * @return value
   * @throws IOException
   */
  byte[] read(long offset, int length)
      throws IOException;

  /**
   * Transfers data from IOEngine to a Cacheable object.
   * @param length How many bytes to be read from the offset
   * @param offset The offset in the IO engine where the first byte to be read
   * @return value
   * @throws IOException
   */
  boolean read(long offset, int length, byte[] value, int off)
      throws IOException;
  
  /**
   * Transfers data from the given byte buffer to IOEngine
   * @param srcBuffer the given byte buffer from which bytes are to be read
   * @param offset The offset in the IO engine where the first byte to be
   *          written
   * @throws IOException
   */
  void write(byte[] key, int keyOff, int keyLen, byte[] value, int valOff, int valLen, 
      long offset) throws IOException;


  /**
   * Sync the data to IOEngine after writing
   * @throws IOException
   */
  void sync() throws IOException;

  /**
   * Shutdown the IOEngine
   */
  void shutdown();
}
