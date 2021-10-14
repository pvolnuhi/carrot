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
package org.bigbase.carrot.redis.util;

import java.nio.ByteBuffer;

import org.bigbase.carrot.redis.commands.RedisCommand.ReplyType;

import org.bigbase.carrot.util.UnsafeAccess;

import static org.bigbase.carrot.util.Utils.lexToDouble;
import static org.bigbase.carrot.util.Utils.longToStr;

import static org.bigbase.carrot.util.Utils.readUVInt;
import static org.bigbase.carrot.util.Utils.sizeUVInt;
import static org.bigbase.carrot.util.Utils.strToByteBuffer;
import static org.bigbase.carrot.util.Utils.strToLong;
import static org.bigbase.carrot.util.Utils.stringSize;

import static org.bigbase.carrot.util.Utils.SIZEOF_INT;
import static org.bigbase.carrot.util.Utils.SIZEOF_BYTE;
import static org.bigbase.carrot.util.Utils.SIZEOF_DOUBLE;
import static org.bigbase.carrot.util.Utils.SIZEOF_LONG;



/**
 * 
 * Utility class for Redis package
 *
 */
public class Utils {
  
  static final byte ARR_TYPE = (byte) '*';
  static final byte INT_TYPE = (byte) ':';
  static final byte BULK_TYPE = (byte) '$';
  static final byte ERR_TYPE = (byte) '-';
  static final byte PLUS = (byte) '+';
  
  static final byte[] OK_RESP = "+OK\r\n".getBytes();
  static final byte[] CRLF = "\r\n".getBytes();
  
  
  /**
   * Converts Redis request (raw) to an internal Carrot 
   * representation. Provided memory buffer MUST be sufficient to keep all 
   * converted data
   * @param buf request data
   * @return true if success, false - otherwise (not a full request)
   */
  public static boolean requestIsComplete(ByteBuffer buf) {
    
    buf.flip(); // do not double flip!!!
    int len = 0;
    
    // Check first byte
    if (ARR_TYPE != buf.get(0)) {
      buf.rewind();
      /*DEBUG*/System.err.println("PANIC!!! - wrong message format: pos = "+ buf.position() + 
        " remaining=" + buf.remaining());
      int limit = buf.limit();
      if (limit < 2) {
        return false;
      }
      // I hope it is correct
      return buf.get(limit - 2) == (byte) '\r' && buf.get(limit - 1) == (byte) '\n';
    }
    
    // Read first line to get array length
    int lsize = readLine(buf);
    if (lsize < 0) {
      // Wrong format
      return false;
    }
    // Read array length
    len = (int) strToLong(buf, 1, lsize - 3 /*less first byte and last two */);
    if (len <= 0) {
      return false;
    }

    int pos = lsize;
    // move to next line
    buf.position(pos);
    
    for (int i = 0; i < len; i++) {
      lsize = readLine(buf);
      if (lsize < 0) {
        // wrong format
        return false;
      }
      int strlen =  (int) strToLong(buf, pos + 1, lsize - 3);
      if (strlen <= 0) {
        return false;
      }
      if (buf.get(pos) != BULK_TYPE) {
        return false;
      }
      pos += lsize;

      // We expect request consists of array of bulk strings
      buf.position(pos);
      pos += strlen + 2; // 2 - \r\n
      if (pos <= buf.limit()) {
        buf.position(pos);
      } else {
        return false;
      }
    }
    return true;
  }
  
  public static boolean arrayResponseIsComplete(ByteBuffer buf) {
    
    if (buf.position() == 0) return false;
    buf.flip(); // do not double flip!!!
    int len = 0;
    
    // Check first byte
    if (ARR_TYPE != buf.get(0)) {
      buf.rewind();
      /*DEBUG*/System.err.println("PANIC!!! - wrong message format: pos = "+ buf.position() + 
        " remaining=" + buf.remaining());
      int limit = buf.limit();
      if (limit < 2) {
        return false;
      }
      // I hope it is correct
      return buf.get(limit - 2) == (byte) '\r' && buf.get(limit - 1) == (byte) '\n';
    }
    
    // Read first line to get array length
    int lsize = readLine(buf);
    if (lsize < 0) {
      // Wrong format
      return false;
    }
    // Read array length
    len = (int) strToLong(buf, 1, lsize - 3 /*less first byte and last two */);
    if (len <= 0) {
      return false;
    }

    int pos = lsize;
    // move to next line
    buf.position(pos);
    
    for (int i = 0; i < len; i++) {
      lsize = readLine(buf);
      if (lsize < 0) {
        // wrong format
        return false;
      }
      int strlen =  (int) strToLong(buf, pos + 1, lsize - 3);
      if (strlen <= 0) {
        // NULL string
        pos += lsize;
        buf.position(pos);
        continue;
      }
      if (buf.get(pos) != BULK_TYPE) {
        return false;
      }
      pos += lsize;

      // We expect request consists of array of bulk strings
      buf.position(pos);
      pos += strlen + 2; // 2 - \r\n
      if (pos <= buf.limit()) {
        buf.position(pos);
      } else {
        return false;
      }
    }
    return true;
  }

  
  /**
   * Converts Redis request (raw) to an internal Carrot 
   * representation. Provided memory buffer MUST be sufficient to keep all 
   * converted data
   * @param buf request data
   * @param ptr memory pointer, for Carrot representation
   * @param size memory size
   * @return true if success, false - otherwise (wrong request format)
   */
  public static boolean requestToCarrot(ByteBuffer buf, long ptr, int size) {
    
    buf.flip(); // do not double flip!!!
    int len = 0;
    // Check first byte
    if (ARR_TYPE != buf.get(0)) {
      buf.rewind();
      // Try in-line request mode
      return inlineRequestToCarrot(buf, ptr, size);
    }
    // Read first line to get array length
    int lsize = readLine(buf);
    if (lsize < 0) {
      // Wrong format
      return false;
    }
    // Read array length
    len = (int) strToLong(buf, 1, lsize - 3 /*less first byte and last two */);
    if (len <= 0) {
      return false;
    }
    // Write array length to a provided memory buffer
    UnsafeAccess.putInt(ptr, len);
    // Advance memory pointer
    ptr += SIZEOF_INT;

    int pos = lsize;
    // move to next line
    buf.position(pos);
    
    for (int i = 0; i < len; i++) {
      lsize = readLine(buf);
      if (lsize < 0) {
        // wrong format
        return false;
      }
      int strlen =  (int) strToLong(buf, pos + 1, lsize - 3);
      if (strlen <= 0) {
        return false;
      }
      if (buf.get(pos) != BULK_TYPE) {
        return false;
      }
      pos += lsize;

      // We expect request consists of array of bulk strings
      buf.position(pos);
      // Write length of a string
      UnsafeAccess.putInt(ptr, strlen);
      ptr += SIZEOF_INT;
      // write string
      UnsafeAccess.copy(buf, ptr, strlen);
      ptr += strlen;
      pos += strlen + 2; // 2 - \r\n
      buf.position(pos);
    }
    return true;
  }
  
  /**
   * Scans byte buffer till next CR/LF combo
   * 
   * @param buf
   * @return
   */
  private static int readLine(ByteBuffer buf) {
    if (buf.hasRemaining() == false) return -1;
    byte prev = buf.get();
    int count = 1;
    while(buf.hasRemaining()) {
      byte ch = buf.get();
      count ++;
      if (prev == '\r' && ch == '\n') {
        return count;
      }
      prev = ch;
    }
    // No more lines
    return -1;
  }

  /**
   * Converts Redis request (inline, telnet mode) to an internal Carrot 
   * representation. Provided memory buffer MUST be sufficient to keep all 
   * converted data
   * @param buf request data
   * @param ptr memory pointer, for Carrot representation
   * @param size memory size
   * @return true if success, false - otherwise (wrong request format)
   */
  public static boolean inlineRequestToCarrot(ByteBuffer buf, long ptr, int size) {
    
    int off = SIZEOF_INT;
    int count = 0, len = 0;
    
    while(buf.hasRemaining()) {
      skipWhiteSpaces(buf);
      len = countNonWhitespaces(buf);
      if (len == 0) {
        break;
      }
      // Write down string length
      UnsafeAccess.putInt(ptr + off,  len);
      off += SIZEOF_INT;
      // Copy string to a memory buffer
      UnsafeAccess.copy(buf, ptr + off, len);
      // Advance memory buffer offset
      off += len;
      // Advance buffer
      //buf.position(buf.position() + len);
      // Increment count
      count++;
    }
    
    UnsafeAccess.putInt(ptr, count);
    return true;
  }
  
  /**
   * Converts inline request to a Redis RESP2 array
   * @param request inline request
   * @return Redis RESP2 array as a String
   */
  public static String inlineToRedisRequest(String request) {
    String[] splits = request.split(" ");
    StringBuffer sb = new StringBuffer("*"); // ARRAY
    sb.append(Long.toString(splits.length));
    sb.append("\r\n");
    for (String s: splits) {
      sb.append("$"); // bulk string
      sb.append(Long.toString(s.length()));
      sb.append("\r\n");
      sb.append(s);
      sb.append("\r\n");
    }
    return sb.toString();
  }
  
  /**
   * Skips white spaces
   * @param buf byte buffer
   */
  private static void skipWhiteSpaces(ByteBuffer buf) {
    int skipped = 0;
    int pos = buf.position();
    int remaining = buf.remaining();
    while (skipped < remaining && buf.get() == (byte)' ') skipped++;
    buf.position(pos + skipped);
  }
  
  /**
   * Counts consecutive non-white space characters 
   * @param buf byte buffer
   * @return length of a continuous non-white space symbols
   */
  private static int countNonWhitespaces(ByteBuffer buf) {
    int skipped = 0;
    int pos = buf.position();
    int remaining = buf.remaining();
    while (skipped < remaining && buf.get() != (byte)' ') skipped++;
    buf.position(pos);
    return skipped;
  }
  
  /**
   * Converts internal Carrot message to a Redis response format
   * @param ptr memory address of a serialized Carrot response
   * @param buf Redis response buffer
   */
  public static void carrotToRedisResponse(long ptr, ByteBuffer buf) {
    buf.rewind();
    int val = UnsafeAccess.toByte(ptr);
    ReplyType type = ReplyType.values()[val];
    
    switch(type) {
      case OK: 
        okResponse(ptr, buf);
        break;
      case SIMPLE:
        simpleResponse(ptr, buf);
        break;
      case INTEGER: 
        intResponse(ptr, buf);
        break;
      case BULK_STRING:
        bulkResponse(ptr, buf);
        break;
      case ARRAY:
        arrayResponse(ptr, buf);
        break;
      case INT_ARRAY:
        intArrayResponse(ptr, buf);
        break;
      case VARRAY:
        varrayResponse(ptr, buf);
        break;
      case ZARRAY:
        zarrayResponse(ptr, buf);
        break;
      case ZARRAY1:
        zarray1Response(ptr, buf);
        break;
      case MULTI_BULK:
        multiBulkResponse(ptr, buf);
        break;
      case ERROR:
        errorResponse(ptr, buf);
        break;
      default:
        throw new IllegalArgumentException(String.format("Illegal number response type %d", type.ordinal()));
        
    }
  }
  
  private static void simpleResponse(long ptr, ByteBuffer buf) {
    buf.put(PLUS);
    int len = UnsafeAccess.toInt(ptr + SIZEOF_BYTE);
    UnsafeAccess.copy(ptr + SIZEOF_BYTE + SIZEOF_INT, buf, len);
    buf.put(CRLF);
  }

  private static void multiBulkResponse(long ptr, ByteBuffer buf) {
    buf.put(ARR_TYPE);
    longToStr(2, buf, buf.position());
    buf.put(CRLF);
    // 1. CURSOR
    buf.put(BULK_TYPE);
    ptr += SIZEOF_BYTE; // skip multi bulk type    
    ptr += SIZEOF_BYTE; // skip type
    long cursor = UnsafeAccess.toLong(ptr);
    int curlen = stringSize(cursor);
    ptr += SIZEOF_LONG;
    longToStr(curlen, buf, buf.position());
    buf.put(CRLF);
    longToStr(cursor, buf, buf.position());
    buf.put(CRLF);
    int itype = UnsafeAccess.toByte(ptr);
    ReplyType type = ReplyType.values()[itype];

    switch (type) {
      case VARRAY:
        varrayResponse(ptr, buf);
        break;
      case ZARRAY:
        zarrayResponse(ptr, buf);
      default:
        //TODO
    }
  }

  /**
   * Converts ZARRAY1 Carrot type to a Redis response
   * @param ptr memory address of a serialized Carrot response 
   * @param buf Redis response buffer
   */
  private static void zarray1Response(long ptr, ByteBuffer buf) {
    buf.put(ARR_TYPE);
    ptr += SIZEOF_BYTE;
    // skip serialized size for now TODO: later
    ptr += SIZEOF_INT;
    int len = UnsafeAccess.toInt(ptr);
    ptr += SIZEOF_INT;
    longToStr(len , buf, buf.position());
    buf.put(CRLF);
    
    for (int i = 0; i < len; i++) {
      int size = readUVInt(ptr);
      ptr += sizeUVInt(size) + SIZEOF_DOUBLE;
      size -= SIZEOF_DOUBLE;
      // Write field
      buf.put(BULK_TYPE);
      longToStr(size, buf, buf.position());
      buf.put(CRLF);
      UnsafeAccess.copy(ptr, buf, size);
      buf.put(CRLF);
      ptr += size;
    }        
  }
  
  /**
   * Converts ZARRAY Carrot type to a Redis response
   * @param ptr memory address of a serialized Carrot response 
   * @param buf Redis response buffer
   */
  private static void zarrayResponse(long ptr, ByteBuffer buf) {
    buf.put(ARR_TYPE);
    ptr += SIZEOF_BYTE;
    // skip serialized size for now TODO: later
    ptr += SIZEOF_INT;
    int len = UnsafeAccess.toInt(ptr);
    ptr += SIZEOF_INT;
    longToStr(2 * len /* score+field*/, buf, buf.position());
    buf.put(CRLF);
    
    for (int i = 0; i < len; i++) {
      // Read total size (score + field)
      int size = readUVInt(ptr);
      ptr += sizeUVInt(size);
      
      // Write field
      buf.put(BULK_TYPE);
      longToStr(size - SIZEOF_DOUBLE, buf, buf.position());
      buf.put(CRLF);
      UnsafeAccess.copy(ptr + SIZEOF_DOUBLE, buf, size - SIZEOF_DOUBLE);
      buf.put(CRLF);
      // Write score (double - 8 bytes)
      double score = lexToDouble(ptr);
      //TODO: optimize conversion w/o object creation
      // Get score
      String s = Double.toString(score);
      int slen = s.length();
      buf.put(BULK_TYPE);     
      longToStr(slen, buf, buf.position());
      buf.put(CRLF);
      strToByteBuffer(s, buf);
      buf.put(CRLF);
      ptr += size;
    }    
  }
  
  /**
   * Converts VARRAY Carrot type to a Redis response
   * @param ptr memory address of a serialized Carrot response 
   * @param buf Redis response buffer
   */
  private static void varrayResponse(long ptr, ByteBuffer buf) {
    buf.put(ARR_TYPE);
    ptr += SIZEOF_BYTE;
    // skip serialized size for now TODO: later
    ptr += SIZEOF_INT;
    int len = UnsafeAccess.toInt(ptr);
    ptr += SIZEOF_INT;
    longToStr(len, buf, buf.position());
    buf.put(CRLF);
    
    for (int i = 0; i < len; i++) {
      int size = readUVInt(ptr);
      int sizeSize = sizeUVInt(size);
      ptr += sizeSize;
      buf.put(BULK_TYPE);
      longToStr(size, buf, buf.position());
      buf.put(CRLF);
      if(size > 0) {
        UnsafeAccess.copy(ptr, buf, size);
        buf.put(CRLF);
        ptr += size;
      }
    }    
  }
  /**
   * Converts ERROR Carrot type to a Redis response
   * @param ptr memory address of a serialized Carrot response 
   * @param buf Redis response buffer
   */
  private static void errorResponse(long ptr, ByteBuffer buf) {
    buf.put(ERR_TYPE);
    ptr += SIZEOF_BYTE;
    int msgLen = UnsafeAccess.toInt(ptr);
    ptr += SIZEOF_INT;
    UnsafeAccess.copy(ptr, buf, msgLen);
    buf.position(SIZEOF_BYTE + msgLen);
    buf.put(CRLF);
    
  }

  /**
   * Converts TYPED_ARRAY Carrot type to a Redis response
   * TODO: currently we support only INT types
   * @param ptr memory address of a serialized Carrot response 
   * @param buf Redis response buffer
   */
  private static void intArrayResponse(long ptr, ByteBuffer buf) {
    buf.put(ARR_TYPE);
    ptr += SIZEOF_BYTE;
    // skip serialized size for now TODO: later
    ptr += SIZEOF_INT;
    int len = UnsafeAccess.toInt(ptr);
    ptr += SIZEOF_INT;
    longToStr(len, buf, buf.position());
    buf.put(CRLF);
    for (int i=0; i < len; i++) {
      // Works only with ints
      long val = UnsafeAccess.toLong(ptr);
      ptr += SIZEOF_LONG;
      buf.put(INT_TYPE);
      longToStr(val, buf, buf.position());
      buf.put(CRLF);
    }    
  }

  /**
   * Converts simple ARRAY Carrot type to a Redis response
   * @param ptr memory address of a serialized Carrot response 
   * @param buf Redis response buffer
   */
  private static void arrayResponse(long ptr, ByteBuffer buf) {
    buf.put(ARR_TYPE);
    ptr += SIZEOF_BYTE;
    // skip serialized size for now TODO: later
    ptr += SIZEOF_INT;
    int len = UnsafeAccess.toInt(ptr);
    ptr += SIZEOF_INT;
    longToStr(len, buf, buf.position());
    buf.put(CRLF);
    
    for (int i = 0; i < len; i++) {
      int size = UnsafeAccess.toInt(ptr);
      ptr += SIZEOF_INT;
      buf.put(BULK_TYPE);
      longToStr(size, buf, buf.position());
      buf.put(CRLF);
      if(size >= 0) {
        UnsafeAccess.copy(ptr, buf, size);
        buf.put(CRLF);
        ptr += size;
      }
    }
  }

  /**
   * This call is used for CLUSTER SLOTS
   * @param data array objects
   * @param buf buffer to serialize to
   */
  public static void serializeTypedArray(Object[] data, ByteBuffer buf) {
    buf.put(ARR_TYPE);
    int len = data == null? -1: data.length;
    longToStr(len, buf, buf.position());
    buf.put(CRLF);
    if (len < 0) return;
    for (int i = 0; i < data.length; i++) {
      serializeObject(data[i], buf);
    }
  }
  
  private static void serializeObject(Object obj, ByteBuffer buf) {
    if (obj instanceof Long) {
      Long value = (Long) obj;
      serializeLong(value, buf);
    } else if (obj instanceof String) {
      String value = (String) obj;
      serializeString(value, buf);
    } else if (obj instanceof Object[]) {
      Object[] value = (Object[]) obj;
      serializeTypedArray(value, buf);
    }
  }

  private static void serializeLong(long value, ByteBuffer buf) {
    buf.put(INT_TYPE);
    longToStr(value, buf, buf.position());
    buf.put(CRLF);
  }
  
  private static void serializeString(String s, ByteBuffer buf) {
    buf.put(BULK_TYPE);  
    int len = s == null? -1: s.length();
    longToStr(len, buf, buf.position());
    buf.put(CRLF);
    if (len > 0) {
      buf.put(s.getBytes());
      buf.put(CRLF);
    }
  }
  
  /**
   * Converts BULK_STRING Carrot type to a Redis response
   * @param ptr memory address of a serialized Carrot response 
   * @param buf Redis response buffer
   */
  private static void bulkResponse(long ptr, ByteBuffer buf) {
    buf.put(BULK_TYPE);  
    ptr += SIZEOF_BYTE;
    int len = UnsafeAccess.toInt(ptr);
    ptr += SIZEOF_INT;
    longToStr(len, buf, buf.position());
    buf.put(CRLF);
    if (len > 0) {
      UnsafeAccess.copy(ptr, buf, len);
      buf.put(CRLF);
    }

  }

  /**
   * Converts INTEGER Carrot type to a Redis response
   * @param ptr memory address of a serialized Carrot response 
   * @param buf Redis response buffer
   */
  private static void intResponse(long ptr, ByteBuffer buf) {
    buf.put(INT_TYPE);
    ptr += SIZEOF_BYTE;
    long value = UnsafeAccess.toLong(ptr);
    longToStr(value, buf, buf.position());
    buf.put(CRLF);
  }

  /**
   * Converts OK Carrot type to a Redis response
   * @param ptr memory address of a serialized Carrot response 
   * @param buf Redis response buffer
   */
  private static void okResponse(long ptr, ByteBuffer buf) {
    buf.put(OK_RESP);
  }
  
}
