package org.bigbase.carrot.redis.commands;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.zsets.ZSets;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class ZREVRANGE implements RedisCommand {

  /**
   * TODO: v 6.2 support
   * ZREVRANGE key min max [WITHSCORES]
   */
  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    boolean withScores = false;
    long min = 0, max = 0;
    try {
      int numArgs = UnsafeAccess.toInt(inDataPtr);
      if (numArgs < 4 || numArgs > 5) {
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
        return;
      }
      inDataPtr += Utils.SIZEOF_INT;
      // skip command name
      inDataPtr = skip(inDataPtr, 1);
      // read key
      int keySize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long keyPtr = inDataPtr;
      inDataPtr += keySize;
      
      int valSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long valPtr = inDataPtr;
      min =  Utils.strToLong(valPtr, valSize);
      inDataPtr += valSize;
      
      valSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      valPtr = inDataPtr;
      max =  Utils.strToLong(valPtr, valSize);
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
          (int) ZSets.ZREVRANGE(map, keyPtr, keySize, min, max, withScores, outBufferPtr + off, outBufferSize - off);
      
      if (withScores) {
        UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.ZARRAY.ordinal());
      } else {
        UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.ZARRAY1.ordinal());
      }
      UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE, size + off);

    } catch (NumberFormatException e) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT,
        "count is not a valid number");
    } 
  }

}
