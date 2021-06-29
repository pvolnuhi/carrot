package org.bigbase.carrot.redis.commands;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.sparse.SparseBitmaps;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class SSTRLEN implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    int numArgs = UnsafeAccess.toInt(inDataPtr);
    
    if (numArgs != 2) {
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

    long len = SparseBitmaps.SSTRLEN(map, keyPtr, keySize);
    // INTEGER reply - we do not check buffer size here - should be larger than 9
    INT_REPLY(outBufferPtr, len);
    
  }

}
