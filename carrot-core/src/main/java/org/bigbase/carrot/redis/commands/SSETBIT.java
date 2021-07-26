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
import org.bigbase.carrot.redis.sparse.SparseBitmaps;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class SSETBIT implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    try {
      int numArgs = UnsafeAccess.toInt(inDataPtr);

      if (numArgs != 4) {
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
        return;
      }
      inDataPtr += Utils.SIZEOF_INT;
      // skip command name
      inDataPtr = skip(inDataPtr, 1);
      
      int keySize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long keyPtr = inDataPtr;
      inDataPtr += keySize;

      int offSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long offset = Utils.strToLong(inDataPtr, offSize);
      inDataPtr += offSize;
      if (offset < 0) {
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_POSITIVE_NUMBER_EXPECTED, ": "+ offset);
        return;
      }
      int bitSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      int bit = (int) Utils.strToLong(inDataPtr, bitSize);
      if (bit != 0 && bit != 1) {
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_BIT_VALUE, ": " + bit);
        return;
      }
      int oldBit = SparseBitmaps.SSETBIT(map, keyPtr, keySize, offset, bit);

      // INTEGER reply
      INT_REPLY(outBufferPtr, oldBit);
      
    } catch (NumberFormatException e) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT, ": " + e.getMessage());
    }
  }

}
