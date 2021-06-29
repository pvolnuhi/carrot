package org.bigbase.carrot.redis.commands;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.zsets.ZSets;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class ZRANGEBYSCORE implements RedisCommand {

  /**
   * TODO: v. 6.2 support
   * ZRANGEBYSCORE key min max [WITHSCORES]
   */
  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    try {
      boolean withScores = false;
      double start = 0, end = 0;
      int numArgs = UnsafeAccess.toInt(inDataPtr);
      if (numArgs < 4 || numArgs > 5) {
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

      boolean startInclusive = true, endInclusive = true;

      int valSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long valPtr = inDataPtr;
      if (UnsafeAccess.toByte(valPtr) == (byte) '(') {
        // start is not inclusive
        startInclusive = false;
        valSize -= Utils.SIZEOF_BYTE;
        valPtr += Utils.SIZEOF_BYTE; 
        inDataPtr += Utils.SIZEOF_BYTE;
      } else if (UnsafeAccess.toByte(valPtr) == (byte) '['){
        // start is inclusive
        startInclusive = true;
        valSize -= Utils.SIZEOF_BYTE;
        valPtr += Utils.SIZEOF_BYTE; 
        inDataPtr += Utils.SIZEOF_BYTE;
      } else if (Utils.compareTo(NEG_INFINITY_FLAG, NEG_INFINITY_LENGTH, valPtr, valSize) != 0) {
        throw new IllegalArgumentException(Utils.toString(valPtr, valSize));
      }
      
      start = Utils.strToDouble(valPtr, valSize);
      inDataPtr += valSize;
      
      valSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      valPtr = inDataPtr;
      if (UnsafeAccess.toByte(valPtr) == (byte) '(') {
        // start is not inclusive
        endInclusive = false;
        valSize -= Utils.SIZEOF_BYTE;
        valPtr += Utils.SIZEOF_BYTE; 
        inDataPtr += Utils.SIZEOF_BYTE;
      } else if (UnsafeAccess.toByte(valPtr) == (byte) '['){
        // start is inclusive
        endInclusive = true;
        valSize -= Utils.SIZEOF_BYTE;
        valPtr += Utils.SIZEOF_BYTE; 
        inDataPtr += Utils.SIZEOF_BYTE;
      } else if (Utils.compareTo(POS_INFINITY_FLAG, POS_INFINITY_LENGTH, valPtr, valSize) != 0) {
        throw new IllegalArgumentException(Utils.toString(valPtr, valSize));
      }
      
      end = Utils.strToDouble(valPtr, valSize);
      inDataPtr += valSize;
      
      if (numArgs == 5) {
        valSize = UnsafeAccess.toInt(inDataPtr);
        inDataPtr += Utils.SIZEOF_INT;
        valPtr = inDataPtr;
        if (Utils.compareTo(WITHSCORES_FLAG, WITHSCORES_LENGTH, valPtr, valSize) == 0) {
          withScores = true;
        } else {
          throw new IllegalArgumentException(Utils.toString(valPtr, valSize));
        }
      }
      int off = Utils.SIZEOF_BYTE + Utils.SIZEOF_INT;
      int size =
          (int) ZSets.ZRANGEBYSCORE(map, keyPtr, keySize, start, startInclusive, end, endInclusive,
            withScores, outBufferPtr + off, outBufferSize - off);
      
      if (withScores) {
        UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.ZARRAY.ordinal());
      } else {
        UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.ZARRAY1.ordinal());
      }
      UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE, size + off);
      
    } catch (NumberFormatException e) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT);
    } catch (IllegalArgumentException ee) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_ILLEGAL_ARGS, ee.getMessage());
    }
  }

}
