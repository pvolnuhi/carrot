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
        Utils.SIZEOF_INT /*number of elements*/ + (numArgs - 2) * (Utils.SIZEOF_LONG);
    if (requiredSize > outBufferSize) {
      UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.INT_ARRAY.ordinal());
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
    
    UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.INT_ARRAY.ordinal());
    UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE, requiredSize);
    UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE + Utils.SIZEOF_INT, numArgs - 2);
    outBufferPtr += Utils.SIZEOF_BYTE + 2 * Utils.SIZEOF_INT;
    //TODO: buffer overflow
    for (int i = 0 ; i < numArgs - 2; i++) {
      byte v = UnsafeAccess.toByte(buffer + i);
      UnsafeAccess.putLong(outBufferPtr, v);
      outBufferPtr += Utils.SIZEOF_LONG;
    }
  }
}
