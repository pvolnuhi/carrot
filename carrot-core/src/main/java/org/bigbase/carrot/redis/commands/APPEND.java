package org.bigbase.carrot.redis.commands;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.strings.Strings;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class APPEND implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    int numArgs = UnsafeAccess.toInt(inDataPtr);
    if (numArgs != 3) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
      return;
    }
    inDataPtr += Utils.SIZEOF_INT;
    // skip command name
    int clen = UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT + clen;
    
    // FIXME: convert ALL Redis API from long[] / int[] to memory buffer interface
    int keySize = UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT;

    long keyPtr = inDataPtr;
    inDataPtr += keySize;
    int valSize = UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT;
    long valPtr = inDataPtr;
    
    int num = Strings.APPEND(map, keyPtr, keySize, valPtr, valSize);
    
    // INTEGER reply - we do not check buffer size here - should n=be larger than 5
    UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.INTEGER.ordinal());
    UnsafeAccess.putLong(outBufferPtr + Utils.SIZEOF_BYTE, num);
  }

}
