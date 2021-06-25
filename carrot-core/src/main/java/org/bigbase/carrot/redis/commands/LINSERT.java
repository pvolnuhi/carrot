package org.bigbase.carrot.redis.commands;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.lists.Lists;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class LINSERT implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    int numArgs = UnsafeAccess.toInt(inDataPtr);
    if (numArgs != 5) {
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
    // BEFORE | AFTER
    boolean after = false;
    int flagSize = UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT;
    long flagPtr = inDataPtr;
    if (Utils.compareTo(BEFORE_FLAG, BEFORE_LENGTH, flagPtr, flagSize) == 0) {
      after = false;
    } else if (Utils.compareTo(AFTER_FLAG, AFTER_LENGTH, flagPtr, flagSize) == 0) { 
      after = true;
    } else {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_ILLEGAL_ARGS, Utils.toString(flagPtr, flagSize));
      return;
    }
    // read pivot
    int pivotSize = UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT;
    long pivotPtr = inDataPtr;
    inDataPtr += pivotSize;
    
    // read element
    int elemSize = UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT;
    long elemPtr = inDataPtr;
    inDataPtr += elemSize;
    
    long size = Lists.LINSERT(map, keyPtr, keySize, after, pivotPtr, pivotSize, elemPtr, elemSize);
    
    UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.INTEGER.ordinal());
    UnsafeAccess.putLong(outBufferPtr + Utils.SIZEOF_BYTE, size);
  }

}
