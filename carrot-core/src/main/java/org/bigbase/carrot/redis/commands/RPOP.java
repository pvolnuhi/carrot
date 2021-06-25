package org.bigbase.carrot.redis.commands;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.lists.Lists;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class RPOP implements RedisCommand {

  /**
   * TODO: Add count support (6.2)
   */
  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    try {
      int count = 1;
      boolean countSet = false;

      int numArgs = UnsafeAccess.toInt(inDataPtr);
      if (numArgs != 2 && numArgs != 3) {
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
      if (numArgs == 3) {
        int countSize = UnsafeAccess.toInt(inDataPtr);
        inDataPtr += Utils.SIZEOF_INT;
        long countPtr = inDataPtr;
        count = (int) Utils.strToLong(countPtr, countSize);
        countSet = true;
      }

      int off = Utils.SIZEOF_BYTE + Utils.SIZEOF_INT;;
     
      // FIXME: We always return ARRAY - this is not original spec
      int size = (int) Lists.RPOP(map, keyPtr, keySize, outBufferPtr + off, outBufferSize - off);
      UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.BULK_STRING.ordinal());
      UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE, size);
    } catch (NumberFormatException e) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT);
    }
  }

}
