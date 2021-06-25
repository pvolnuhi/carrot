package org.bigbase.carrot.redis.commands;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.lists.Lists;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class RPOPLPUSH implements RedisCommand {

  /**
   * RPOPLPUSH source destination
   */
  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    int numArgs = UnsafeAccess.toInt(inDataPtr);
    if (numArgs != 3) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
      return;
    }
    inDataPtr += Utils.SIZEOF_INT;
    // skip command name
    inDataPtr = skip(inDataPtr, 1);
    // read src key
    int srcKeySize = UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT;
    long srcKeyPtr = inDataPtr;
    inDataPtr += srcKeySize;
    // read dst key
    int dstKeySize = UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT;
    long dstKeyPtr = inDataPtr;
    inDataPtr += dstKeySize;
    int off = Utils.SIZEOF_BYTE + Utils.SIZEOF_INT;
    int size = Lists.RPOPLPUSH(map, srcKeyPtr, srcKeySize, dstKeyPtr, dstKeySize, 
      outBufferPtr + off, outBufferSize - off); 
    UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.BULK_STRING.ordinal());
    UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE, size);
  }

}
