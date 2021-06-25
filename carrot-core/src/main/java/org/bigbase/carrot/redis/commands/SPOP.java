package org.bigbase.carrot.redis.commands;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.sets.Sets;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class SPOP implements RedisCommand {

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
      int size = (int) Sets.SPOP(map, keyPtr, keySize, outBufferPtr + off, outBufferSize - off, count);
      if (!countSet) {
        // Return as a Bulk String
        varrayToBulkString(outBufferPtr);
      } else {
        // Return as VARRAY
        UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.VARRAY.ordinal());
        UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE, size);
      }
    } catch (NumberFormatException e) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT);
    }
  }

}
