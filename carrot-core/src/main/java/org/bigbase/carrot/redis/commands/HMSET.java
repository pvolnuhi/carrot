package org.bigbase.carrot.redis.commands;

import java.util.List;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.KeyValue;
import org.bigbase.carrot.redis.hashes.Hashes;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class HMSET implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    int numArgs = UnsafeAccess.toInt(inDataPtr);

    if (numArgs < 3 || (numArgs - 2) % 2 != 0) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
      return;
    }
    inDataPtr += Utils.SIZEOF_INT;
    int keySize = UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT;
    long keyPtr = inDataPtr;
    inDataPtr += keySize;
    List<KeyValue> kvs = Utils.loadKeyValues(inDataPtr, (numArgs - 2) / 2);
    // HMSET is deprecated as of 4.0 - we use HSET instead
    int num = Hashes.HSET(map, keyPtr, keySize, kvs);
    // Send reply
    INT_REPLY(outBufferPtr, num);
  }

}
