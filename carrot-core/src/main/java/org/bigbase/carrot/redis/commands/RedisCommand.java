package org.bigbase.carrot.redis.commands;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.MutationOptions;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Generic interface for Redis command
 * 
 * Format:
 * 
 * REQUEST:
 * 
 * inDataPtr - points to command parameter list
 * 
 * [4] - number of parameters including command name
 * 
 * Parameter+
 * 
 * Parameter:
 * [4] - length
 * [blob - parameter data] 
 *  
 * First parameter in the list is always command name
 * 
 * REPLY: 
 * outBufferPtr - command execution result
 * [1] reply type (NULL, INTEGER, DOUBLE, STRING, ARRAY, MAP, SET etc)
 * 
 * INTEGER:
 * [8]
 * DOUBLE:
 * [8]
 * NULL:
 * [0]
 * STRING:
 * [4] string length
 * [blob] string data
 * 
 * ARRAY: TODO check
 * [4] array serialized size
 * [4] number of elements
 * ELEMENT*
 * ELEMENT:
 * [4] - size
 * [blob] - element data
 * 
 * VARRAY: TODO check
 * [4] array serialized size
 * [4] number of elements
 * ELEMENT*
 * ELEMENT:
 * [VINT] - size
 * [blob] - element data
 * 
 * TYPED_ARRAY: TODO check
 * [4] array serialized size
 * [4] number of elements
 * ELEMENT*
 * ELEMENT:
 * [1] - type
 * [blob] - element data
 * 
 * ERROR:
 * [4] length
 * [blob] error message
 * @author vrodionov
 *
 */
public interface RedisCommand {
  static enum ReplyType {
    OK, 
    INTEGER, 
    DOUBLE, 
    BULK_STRING, 
    ARRAY, /*fixed length field array - strings only*/
    TYPED_ARRAY,
    VARRAY /*variable length field - strings only*/, 
    TYPED_VARRAY,
    MULTI_BULK /* SCAN return*/,
    MAP, /* not implemented yet */
    SET, /* not implemented yet */
    ERROR
  }

  static final long PERSISTS_FLAG = UnsafeAccess.allocAndCopy("PERSISTS", 0, "PERSISTS".length());
  static final int PERSISTS_LENGTH = "PERSISTS".length();
  static final long EXAT_FLAG = UnsafeAccess.allocAndCopy("EXAT", 0, "EXAT".length());
  static final int EXAT_LENGTH = "EXAT".length();

  static final long PXAT_FLAG = UnsafeAccess.allocAndCopy("PXAT", 0, "PXAT".length());
  static final int PXAT_LENGTH = "PXAT".length();

  static final long EX_FLAG = UnsafeAccess.allocAndCopy("EX", 0, "EX".length());
  static final int EX_LENGTH = "EX".length();

  static final long PX_FLAG = UnsafeAccess.allocAndCopy("PX", 0, "PX".length());
  static final int PX_LENGTH = "PX".length();
  
  static final long KEEPTTL_FLAG = UnsafeAccess.allocAndCopy("KEEPTTL", 0, "KEEPTTL".length());
  static final int KEEPTTL_LENGTH = "KEEPTTL".length();
  
  static final long NX_FLAG = UnsafeAccess.allocAndCopy("NX", 0, "NX".length());
  static final int NX_LENGTH = "NX".length();

  static final long XX_FLAG = UnsafeAccess.allocAndCopy("XX", 0, "XX".length());
  static final int XX_LENGTH = "XX".length();
  
  static final long GET_FLAG = UnsafeAccess.allocAndCopy("GET", 0, "GET".length());
  static final int GET_LENGTH = "GET".length();
  
  static final long WITHVALUES_FLAG = UnsafeAccess.allocAndCopy("WITHVALUES", 0, "WITHVALUES".length());
  static final int WITHVALUES_LENGTH = "WITHVALUES".length();

  static final long OK_REPLY = UnsafeAccess.allocAndCopy("+OK\\r\\n", 0, "+OK\\r\\n".length());
  static final int OK_REPLY_LENGTH = "+OK\\r\\n".length();
  
  static final long NULL_STRING_REPLY = UnsafeAccess.allocAndCopy("$-1\\r\\n", 0, "$-1\\r\\n".length());
  static final int NULL_STRING_REPLY_LENGTH = "$-1\\r\\n".length();
  
  static final long NULL_ARRAY_REPLY = UnsafeAccess.allocAndCopy("*-1\\r\\n", 0, "*-1\\r\\n".length());
  static final int NULL_ARRAY_REPLY_LENGTH = "*-1\\r\\n".length();
  
  static final long MATCH_FLAG = UnsafeAccess.allocAndCopy("MATCH", 0, "MATCH".length());
  static final int MATCH_LENGTH = "MATCH".length();

  static final long COUNT_FLAG = UnsafeAccess.allocAndCopy("COUNT", 0, "COUNT".length());
  static final int COUNT_LENGTH = "COUNT".length();
  
  static final long BEFORE_FLAG = UnsafeAccess.allocAndCopy("BEFORE", 0, "BEFORE".length());
  static final int BEFORE_LENGTH = "BEFORE".length();

  static final long AFTER_FLAG = UnsafeAccess.allocAndCopy("AFTER", 0, "AFTER".length());
  static final int AFTER_LENGTH = "AFTER".length();
  
  static final long LEFT_FLAG = UnsafeAccess.allocAndCopy("LEFT", 0, "LEFT".length());
  static final int LEFT_LENGTH = "LEFT".length();
  
  static final long RIGHT_FLAG = UnsafeAccess.allocAndCopy("RIGHT", 0, "RIGHT".length());
  static final int RIGHT_LENGTH = "RIGHT".length();
  
  default void NULL_STRING (long ptr) {
    UnsafeAccess.putByte(ptr,  (byte) ReplyType.BULK_STRING.ordinal());
    UnsafeAccess.putInt(ptr + Utils.SIZEOF_BYTE, -1);
  }
  
  default void NULL_ARRAY (long ptr) {
    UnsafeAccess.putByte(ptr,  (byte) ReplyType.ARRAY.ordinal());
    UnsafeAccess.putInt(ptr + Utils.SIZEOF_BYTE, -1);
  }
  
  default void DOUBLE_REPLY(long ptr, int bufSize, double value) {
    UnsafeAccess.putByte(ptr, (byte) ReplyType.BULK_STRING.ordinal());
    // skip length of a string
    ptr += Utils.SIZEOF_BYTE;
    int len = Utils.doubleToStr(value, ptr + Utils.SIZEOF_INT, 
      bufSize - Utils.SIZEOF_BYTE - Utils.SIZEOF_INT);
    // We do not check len
    UnsafeAccess.putInt(ptr, len);     
  }
  
  default void INT_REPLY(long ptr, long value) {
    UnsafeAccess.putByte(ptr, (byte) ReplyType.INTEGER.ordinal());
    // skip length of a string
    ptr += Utils.SIZEOF_BYTE;
    UnsafeAccess.putLong(ptr, value);     
  }
  
  public default void executeCommand(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    // By default reply is OK
    UnsafeAccess.putByte(inDataPtr,  (byte) ReplyType.OK.ordinal());
    execute(map, inDataPtr, outBufferPtr, outBufferSize);
  }
  
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize);

  default long getExpire(long ptr, boolean withException)
      throws IllegalArgumentException, NumberFormatException {
    int size = UnsafeAccess.toInt(ptr);
    ptr += Utils.SIZEOF_INT;
    if (size == PERSISTS_LENGTH
        && Utils.compareTo(PERSISTS_FLAG, PERSISTS_LENGTH, ptr, size) == 0) {
      return 0;// reset expiration
    } else if (size == EX_LENGTH && Utils.compareTo(EX_FLAG, EX_LENGTH, ptr, size) == 0) {
      // Read seconds
      ptr += size;
      int vSize = UnsafeAccess.toInt(ptr);
      ptr += Utils.SIZEOF_INT;
      long secs = Utils.strToLong(ptr, vSize);
      return System.currentTimeMillis() + secs * 1000;
    } else if (size == PX_LENGTH && Utils.compareTo(PX_FLAG, PX_LENGTH, ptr, size) == 0) {
      // Read milliseconds
      ptr += size;
      int vSize = UnsafeAccess.toInt(ptr);
      ptr += Utils.SIZEOF_INT;
      long ms = Utils.strToLong(ptr, vSize);
      return System.currentTimeMillis() + ms;
    } else if (size == EXAT_LENGTH && Utils.compareTo(EXAT_FLAG, EXAT_LENGTH, ptr, size) == 0) {
        // Read seconds
        ptr += size;
        int vSize = UnsafeAccess.toInt(ptr);
        ptr += Utils.SIZEOF_INT;
        long secs = Utils.strToLong(ptr, vSize);
        return secs * 1000;
    } else if (size == PXAT_LENGTH && Utils.compareTo(PXAT_FLAG, PXAT_LENGTH, ptr, size) == 0) {
          // Read seconds
          ptr += size;
          int vSize = UnsafeAccess.toInt(ptr);
          ptr += Utils.SIZEOF_INT;
          long ms = Utils.strToLong(ptr, vSize);
          return ms;
    } else if (size == KEEPTTL_LENGTH && Utils.compareTo(KEEPTTL_FLAG, KEEPTTL_LENGTH, ptr, size) == 0) {
      // expire = -1 - means, do not overwrite existing expire - keepTTL
      return -1;
    }
    if (withException) {
      throw new IllegalArgumentException(Utils.toString(ptr, size));
    }
    return 0;
  }

  default long getExpire(long ptr) {
    return getExpire(ptr, false);
  }
  
  default long skip(long ptr, int num) {
    for(int i = 0; i < num; i++) {
      int size = UnsafeAccess.toInt(ptr);
      ptr += Utils.SIZEOF_INT + size;
    }
    return ptr;
  }
  
  default int ttlSectionSize(long ptr) {
    int size = UnsafeAccess.toInt(ptr);
    ptr += Utils.SIZEOF_INT;
    if (Utils.compareTo(KEEPTTL_FLAG, KEEPTTL_LENGTH, ptr, size) == 0) {
      return 1;
    } else if (Utils.compareTo(EX_FLAG, EX_LENGTH, ptr, size) == 0) {
      return 2;
    } else if (Utils.compareTo(PX_FLAG, PX_LENGTH, ptr, size) == 0) {
      return 2;
    } else if (Utils.compareTo(EXAT_FLAG, EXAT_LENGTH, ptr, size) == 0) {
      return 2;
    } else if (Utils.compareTo(PXAT_FLAG, PXAT_LENGTH, ptr, size) == 0) {
      return 2;
    } 
    return 0;
  }
  
  default int mutationSectionSize(long ptr) {
    int size = UnsafeAccess.toInt(ptr);
    ptr += Utils.SIZEOF_INT;
    if (Utils.compareTo(NX_FLAG, NX_LENGTH, ptr, size) == 0) {
      return 1;
    } else if (Utils.compareTo(XX_FLAG, XX_LENGTH, ptr, size) == 0) {
      return 1;
    } 
    return 0;
  }
  
  default MutationOptions getMutationOptions(long ptr) throws IllegalArgumentException{
    int size = UnsafeAccess.toInt(ptr);
    ptr += Utils.SIZEOF_INT;
    if (size == NX_LENGTH && Utils.compareTo(NX_FLAG, NX_LENGTH, ptr, size) == 0) {
      return MutationOptions.NX;
    } else if (size == XX_LENGTH && Utils.compareTo(XX_FLAG, XX_LENGTH, ptr, size) == 0) {
      return MutationOptions.NX;
    }
    
    return MutationOptions.NONE;
  }
  /**
   * Converts single element varray to a bulk string
   * @param ptr address of an varray
   */
  default void varrayToBulkString(long ptr) {
    int off = Utils.SIZEOF_BYTE + 2 * Utils.SIZEOF_INT;
    int size = Utils.readUVInt(ptr + off);
    int sizeSize = Utils.sizeUVInt(size);
    
    // move data from off + sizeSize back by Utils.SIZE_INT + sizeSize
    UnsafeAccess.copy(ptr + off + sizeSize, ptr + Utils.SIZEOF_BYTE + Utils.SIZEOF_INT, size);
    // Update header
    UnsafeAccess.putByte(ptr, (byte)ReplyType.BULK_STRING.ordinal());
    UnsafeAccess.putInt(ptr + Utils.SIZEOF_BYTE, size);
    
  }
}
