package org.bigbase.carrot.redis.commands;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.lists.Lists;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class LSET implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    try {
      int index = 0;
      int numArgs = UnsafeAccess.toInt(inDataPtr);
      if (numArgs != 3) {
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
        return;
      }
      inDataPtr += Utils.SIZEOF_INT;
      // skip command name
      inDataPtr = skip(inDataPtr, 1);
      // read list key
      int keySize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long keyPtr = inDataPtr;
      inDataPtr += keySize;
      // read count
      int indexSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long indexPtr = inDataPtr;
      index = (int) Utils.strToLong(indexPtr, indexSize);
      // read element
      int elemSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long elemPtr = inDataPtr;
      inDataPtr += elemSize;
      int num = (int) Lists.LSET(map, keyPtr, keySize, index, elemPtr, elemSize);
      if (num < 0) {
        Errors.write(outBufferSize, Errors.TYPE_GENERIC, Errors.ERR_OUT_OF_RANGE_OR);
      }
    } catch (NumberFormatException e) {
      Errors.write(outBufferSize, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT);
    }
  }

}
