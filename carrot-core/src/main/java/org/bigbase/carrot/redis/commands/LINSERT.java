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
    if (Utils.compareTo(BEFORE_FLAG, BEFORE_LENGTH, flagPtr, flagSize) == 0 ||
        Utils.compareTo(BEFORE_FLAG_LOWER, BEFORE_LENGTH, flagPtr, flagSize) == 0) {
      after = false;
    } else if (Utils.compareTo(AFTER_FLAG, AFTER_LENGTH, flagPtr, flagSize) == 0 ||
        Utils.compareTo(AFTER_FLAG_LOWER, AFTER_LENGTH, flagPtr, flagSize) == 0) { 
      after = true;
    } else {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_COMMAND_FORMAT, 
        ": "+ Utils.toString(flagPtr, flagSize));
      return;
    }
    inDataPtr += flagSize;
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
    INT_REPLY(outBufferPtr, size);
  }
}
