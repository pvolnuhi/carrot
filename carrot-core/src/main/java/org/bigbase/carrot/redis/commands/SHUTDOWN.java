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

public class SHUTDOWN implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    int numArgs = UnsafeAccess.toInt(inDataPtr);
    boolean save = false;
    if (numArgs > 2) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
      return;
    }
    inDataPtr += Utils.SIZEOF_INT;
    inDataPtr = skip(inDataPtr, 1);
    if (numArgs == 2) {
      int size = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      
      if (Utils.compareTo(SAVE_FLAG, SAVE_LENGTH, inDataPtr, size) == 0 ||
          Utils.compareTo(SAVE_FLAG_LOWER, SAVE_LENGTH, inDataPtr, size) == 0) {
        save = true;
       
      } else  if (Utils.compareTo(NOSAVE_FLAG, NOSAVE_LENGTH, inDataPtr, size) == 0 ||
          Utils.compareTo(NOSAVE_FLAG_LOWER, NOSAVE_LENGTH, inDataPtr, size) == 0) {
        save = false;
      } else {
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_UNSUPPORTED_COMMAND, ": SHUTDOWN " + 
            Utils.toString(inDataPtr, size));
        return;
      }
    }
    boolean result = Server.SHUTDOWN(map, save);
    if (!result) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_OPERATION_FAILED);
      return;
    }
  }
}
