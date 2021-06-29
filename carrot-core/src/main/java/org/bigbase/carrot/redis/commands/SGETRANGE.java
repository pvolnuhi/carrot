package org.bigbase.carrot.redis.commands;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.Commons;
import org.bigbase.carrot.redis.sparse.SparseBitmaps;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class SGETRANGE implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    try {
      int numArgs = UnsafeAccess.toInt(inDataPtr);
      if (numArgs < 2 || numArgs > 4) {
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
      // FIXME - double conversion
      long start = Commons.NULL_LONG;
      long end = Commons.NULL_LONG;
      if (numArgs > 2) {
        // Can only start be defined?
        int valSize = UnsafeAccess.toInt(inDataPtr);
        inDataPtr += Utils.SIZEOF_INT;
        long valPtr = inDataPtr;
        start = Utils.strToLong(valPtr, valSize);
        if (numArgs > 3) {
          inDataPtr += valSize;
          valSize = UnsafeAccess.toInt(inDataPtr);
          inDataPtr += Utils.SIZEOF_INT;
          valPtr = inDataPtr;
          end = Utils.strToLong(valPtr, valSize);
        }
      }
      long size = SparseBitmaps.SGETRANGE(map, keyPtr, keySize, start, end, outBufferPtr + 
        Utils.SIZEOF_BYTE + Utils.SIZEOF_INT, outBufferSize - Utils.SIZEOF_BYTE - Utils.SIZEOF_INT);

      // Bulk string reply 
      UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.BULK_STRING.ordinal());
      if (size >  outBufferSize - Utils.SIZEOF_BYTE - Utils.SIZEOF_INT) {
        size += Utils.SIZEOF_BYTE + Utils.SIZEOF_INT;
      }
      UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE, (int) size);
    } catch (NumberFormatException e) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT);
    }    
  }

}
