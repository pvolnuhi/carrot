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
import org.bigbase.carrot.redis.server.Server;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * 
 * TODO: time in microseconds
 *
 */
public class TIME implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
   
    int numArgs = UnsafeAccess.toInt(inDataPtr);
    if (numArgs != 1) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
      return;
    }
    // Get time in milliseconds
    long time = Server.TIME();
    long secs = (time / 1000);
    long microsecs= (time - secs * 1000) * 1000;
    
    //  Array reply
    UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.ARRAY.ordinal());
    // Skip serialized size
    outBufferPtr += Utils.SIZEOF_BYTE + Utils.SIZEOF_INT;
    // Write array size 2
    UnsafeAccess.putInt(outBufferPtr, 2);
    outBufferPtr += Utils.SIZEOF_INT;
    // Write first string
    int size = Utils.longToStr(secs, outBufferPtr + Utils.SIZEOF_INT, outBufferSize);
    UnsafeAccess.putInt(outBufferPtr, size);
    outBufferPtr += Utils.SIZEOF_INT + size;
    
    size = Utils.longToStr(microsecs, outBufferPtr + Utils.SIZEOF_INT, outBufferSize);
    UnsafeAccess.putInt(outBufferPtr, size);
  }
}
