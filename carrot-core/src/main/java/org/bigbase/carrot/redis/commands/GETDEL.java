package org.bigbase.carrot.redis.commands;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.strings.Strings;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class GETDEL implements RedisCommand {

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
    long size =
        Strings.GETDEL(map, keyPtr, keySize, outBufferPtr + Utils.SIZEOF_BYTE + Utils.SIZEOF_INT,
          outBufferSize - Utils.SIZEOF_BYTE - Utils.SIZEOF_INT);

    // Bulk String reply
    UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.BULK_STRING.ordinal());
    if (size < outBufferSize - Utils.SIZEOF_BYTE - Utils.SIZEOF_INT) {
      UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE, (int) size);
    } else {
      // Buffer is small
      UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE,
        (int) size + Utils.SIZEOF_BYTE + Utils.SIZEOF_INT);

    }
  }

}
