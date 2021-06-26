/**
 *    Copyright (C) 2021-present Carrot, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 */
package org.bigbase.carrot.redis.commands;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.hashes.Hashes;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class HMGET implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    int numArgs = UnsafeAccess.toInt(inDataPtr);

    if (numArgs < 3) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
      return;
    }
    inDataPtr += Utils.SIZEOF_INT;
    
    int keySize = UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT;
    long keyPtr = inDataPtr;
    inDataPtr += keySize;
    
    long[] fieldPtrs = Utils.loadPointers(inDataPtr, numArgs - 2);
    int[] fieldSizes = Utils.loadSizes(inDataPtr, numArgs - 2);
    
    int size = (int) Hashes.HMGET(map, keyPtr, keySize, fieldPtrs, fieldSizes, outBufferPtr + Utils.SIZEOF_BYTE + Utils.SIZEOF_INT,
          outBufferSize - Utils.SIZEOF_BYTE - Utils.SIZEOF_INT);

    // Array reply
    UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.ARRAY.ordinal());
    // Array serialized size
    UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE, size + Utils.SIZEOF_BYTE + Utils.SIZEOF_INT);
    // All data is in the out buffer, including number of elements
  }

}
