package org.bigbase.carrot.redis.commands;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.sets.Sets;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class SMISMEMBER implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    int numArgs = UnsafeAccess.toInt(inDataPtr);
    if (numArgs <= 2) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
      return;
    }
    int requiredSize = Utils.SIZEOF_BYTE /*TYPE*/ + Utils.SIZEOF_INT /*serialized size*/ +
        Utils.SIZEOF_INT /*number of elements*/ + (numArgs - 2) * (Utils.SIZEOF_BYTE + Utils.SIZEOF_LONG);
    if (requiredSize > outBufferSize) {
      UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.TYPED_ARRAY.ordinal());
      UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE, requiredSize);
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
    long[] ptrs = Utils.loadPointers(inDataPtr, numArgs - 2);
    int[] sizes = Utils.loadSizes(inDataPtr, numArgs - 2);
    
    long buffer = Sets.SMISMEMBER(map, keyPtr, keySize, ptrs, sizes);
    
    UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.TYPED_ARRAY.ordinal());
    UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE, requiredSize);
    UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE + Utils.SIZEOF_INT, numArgs - 2);
    outBufferPtr += Utils.SIZEOF_BYTE + 2 * Utils.SIZEOF_INT;
    
    for (int i = 0 ; i < numArgs - 2; i++) {
      INT_REPLY(outBufferPtr, UnsafeAccess.toByte(buffer + i));
      outBufferPtr += Utils.SIZEOF_BYTE + Utils.SIZEOF_LONG;
    }
    
  }

}
