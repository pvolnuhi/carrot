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

import java.util.List;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.strings.Strings;
import org.bigbase.carrot.util.KeyValue;
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
    // Skip command name
    inDataPtr = skip(inDataPtr, 1);
    List<KeyValue> kvs = Utils.loadKeyValues(inDataPtr, (numArgs - 1) / 2);
    boolean result = Strings.MSETNX(map, kvs);
    int retValue = 1;
    if (!result) {
      retValue = 0; // operation failed, at kleast one key existed
    }
    INT_REPLY(outBufferPtr, retValue);
  }
}
