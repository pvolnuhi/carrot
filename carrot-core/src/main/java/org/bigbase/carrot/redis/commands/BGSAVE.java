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

public class BGSAVE implements RedisCommand {

  /**
   * Saves just one DB
   */
  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    boolean schedule = false;
    int numArgs = UnsafeAccess.toInt(inDataPtr);
    if (numArgs > 2) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
      return;
    }
    
    inDataPtr += Utils.SIZEOF_INT;
    inDataPtr = skip(inDataPtr, 1);
    
    // We ignore schedule flag for now
    if (numArgs == 2) {
      int size = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      if (Utils.compareTo(SCHEDULE_FLAG, SCHEDULE_LENGTH, inDataPtr, size) != 0 &&
          Utils.compareTo(SCHEDULE_FLAG_LOWER, SCHEDULE_LENGTH, inDataPtr, size) != 0) {
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_UNSUPPORTED_COMMAND, ": BGSAVE " + 
            Utils.toString(inDataPtr, size));
        return;
      }
      schedule = true;
    }    
    boolean result = Server.BGSAVE(map, schedule);
    if (!result) {
      // Error - snapshot is running and schedule == false
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_SNAPSHOT_RUNNING);
      return;
    }
    String msg = schedule? "Background saving scheduled": "Background saving started";
    SIMPLE_STRING_REPLY(outBufferPtr, msg);
  }
}
