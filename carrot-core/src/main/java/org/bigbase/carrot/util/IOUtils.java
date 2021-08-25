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
package org.bigbase.carrot.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
/**
 * 
 * Utility class for network and file I/O related code
 */
public class IOUtils {
  
  /**
   * Drain byte uffer to a file channel
   * @param buf byte buffer
   * @param fc file channel
   * @throws IOException 
   */
  public static void drainBuffer(ByteBuffer buf, FileChannel fc) throws IOException {
    buf.flip();
    while(buf.hasRemaining()) {
      fc.write(buf);
    }
    buf.clear();
  }
  
  /**
   * Load no less than required number of bytes to a byte buffer
   * @param fc file channel
   * @param buf byte buffer 
   * @param required required number of bytes
   * @return available number of bytes
   * @throws IOException
   */
  public static long ensureAvailable(FileChannel fc, ByteBuffer buf, int required) throws IOException {
    int avail = buf.remaining();
    if (avail < required) {
      boolean compact = false;
      if (buf.capacity() - buf.position() < required) {
        buf.compact();
        compact = true;
      } else {
        buf.mark();
      }
      int n = 0;
      while (true) {
        n = fc.read(buf);
        if (n == -1) {
          if (avail == 0) {
            return -1;
          } // End-Of-Stream
          else {
            throw new IOException("Unexpected End-Of-Stream");
          }
        }
        avail += n;
        if (avail >= required) {
          if (compact) {
            buf.flip();
          } else {
            buf.reset();
          }
          break;
        }
      }
    }
    return avail;
  }
  
}
