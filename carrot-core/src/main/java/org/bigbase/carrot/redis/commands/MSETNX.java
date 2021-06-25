package org.bigbase.carrot.redis.commands;

import java.util.List;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.KeyValue;
import org.bigbase.carrot.redis.strings.Strings;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class MSETNX implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    int numArgs = UnsafeAccess.toInt(inDataPtr);

    if (numArgs < 3 || (numArgs - 1) % 2 != 0) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
      return;
    }
    inDataPtr += Utils.SIZEOF_INT;
    
    List<KeyValue> kvs = Utils.loadKeyValues(inDataPtr, (numArgs - 1) / 2);
    
    boolean result = Strings.MSETNX(map, kvs);

    // Always succeed if not OOM error
    if (!result) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_OPERATION_FAILED);
    }
  }

}
