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
import org.bigbase.carrot.util.UnsafeAccess;

public class PING implements RedisCommand {
  
  static long PONG = UnsafeAccess.allocAndCopy("PONG", 0, 4); 
  
  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    int numArgs = UnsafeAccess.toInt(inDataPtr);
    if (numArgs > 1) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
      return;
    }
    
    BULK_REPLY(outBufferPtr, PONG, 4);
  }
}
