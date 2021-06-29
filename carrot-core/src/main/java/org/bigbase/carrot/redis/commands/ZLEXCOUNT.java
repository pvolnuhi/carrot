package org.bigbase.carrot.redis.commands;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.zsets.ZSets;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class ZLEXCOUNT implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    try {
      int numArgs = UnsafeAccess.toInt(inDataPtr);
      if (numArgs != 4) {
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
        return;
      }
      inDataPtr += Utils.SIZEOF_INT;
      // skip command name
      int clen = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT + clen;
      int keySize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long keyPtr = inDataPtr;
      inDataPtr += keySize;

      long startPtr = 0, endPtr = 0;
      int startSize = 0, endSize = 0;
      boolean startInclusive = true, endInclusive = true;

      startSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      startPtr = inDataPtr;
      if (UnsafeAccess.toByte(startPtr) == (byte) '(') {
        // start is not inclusive
        startInclusive = false;
      } else if (UnsafeAccess.toByte(startPtr) == (byte) '['){
        // start is inclusive
        startInclusive = true;
      } else if (UnsafeAccess.toByte(startPtr) == (byte) '-') {
        startPtr = 0;
        startSize = 0;
      } else {
        throw new IllegalArgumentException("Either '(' or '[' or '-' must be specified for a min argument");
      }
      startPtr = startPtr == 0? 0: startPtr + Utils.SIZEOF_BYTE;
      startSize = startSize == 0? 0: startSize - Utils.SIZEOF_BYTE;
      inDataPtr += Utils.SIZEOF_BYTE;
      inDataPtr += startSize;
      
      endSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      endPtr = inDataPtr;
      
      if (UnsafeAccess.toByte(endPtr) == (byte) '(') {
        // start is not inclusive
        endInclusive = false;
      } else if (UnsafeAccess.toByte(endPtr) == (byte) '['){
        // start is inclusive
        endInclusive = true;
      } else if (UnsafeAccess.toByte(endPtr) == (byte) '+') {
        endPtr = 0;
        endSize = 0;
      } else {
        throw new IllegalArgumentException("Either '(' or '[' or '+' must be specified for a max argument");
      }
      endPtr = endPtr == 0? 0: endPtr + Utils.SIZEOF_BYTE;
      endSize = endSize == 0? 0: endSize - Utils.SIZEOF_BYTE;
      
      long num = ZSets.ZLEXCOUNT(map, keyPtr, keySize, startPtr, startSize, startInclusive, endPtr, endSize, endInclusive);

      // INTEGER reply - we do not check buffer size here - should be larger than 9
      UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.INTEGER.ordinal());
      UnsafeAccess.putLong(outBufferPtr + Utils.SIZEOF_BYTE, num);
    } catch (NumberFormatException e) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT);
    } catch (IllegalArgumentException ee) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_ILLEGAL_ARGS, ee.getMessage());
    }
  }

}
