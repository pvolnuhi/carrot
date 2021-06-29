package org.bigbase.carrot.redis.commands;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.zsets.ZSets;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class ZSCORE implements RedisCommand {

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
    // read key
    int keySize = UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT;
    long keyPtr = inDataPtr;
    inDataPtr += keySize;
    int memberSize =  UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT;
    long memberPtr = inDataPtr;
    
    Double score = ZSets.ZSCORE(map, keyPtr, keySize, memberPtr, memberSize);
    if (score != null) {
      DOUBLE_REPLY(outBufferPtr, outBufferSize, score);
    } else {
      NULL_STRING_REPLY(outBufferPtr);
    }
  }

}
