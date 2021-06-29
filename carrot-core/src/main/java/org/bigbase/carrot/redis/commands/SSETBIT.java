package org.bigbase.carrot.redis.commands;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.sparse.SparseBitmaps;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class SSETBIT implements RedisCommand {

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

      int offSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += offSize;
      long offset = Utils.strToLong(inDataPtr, offSize);
      inDataPtr += offSize;
      int bitSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      int bit = (int) Utils.strToLong(inDataPtr, bitSize);
      if (bit != 0 && bit != 1) {
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_ILLEGAL_ARGS);
        return;
      }
      int oldBit = SparseBitmaps.SSETBIT(map, keyPtr, keySize, offset, bit);

      // INTEGER reply
      INT_REPLY(outBufferPtr, oldBit);
      
    } catch (NumberFormatException e) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT);
    }
  }

}
