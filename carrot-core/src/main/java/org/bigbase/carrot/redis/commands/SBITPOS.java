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
import org.bigbase.carrot.redis.strings.Strings;
import org.bigbase.carrot.redis.util.Commons;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class SBITPOS implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    try {
      int numArgs = UnsafeAccess.toInt(inDataPtr);
      if (numArgs < 3 || numArgs > 5) {
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
        return;
      }
      inDataPtr += Utils.SIZEOF_INT;
      // skip command name
      int clen = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT + clen;
      int keySize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long keyPtr = inDataPtr;
      inDataPtr += keySize;
      int bitSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long bitPtr = inDataPtr;
      int bit = (int) Utils.strToLong(bitPtr, bitSize);
      if (bit != 1 && bit != 0) {
        // ERROR
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_BIT_VALUE, ": " + bit);
        return;
      }
      inDataPtr += bitSize;
      
      long start = Commons.NULL_LONG;
      long end = Commons.NULL_LONG;
      if (numArgs > 3) {
        int valSize = UnsafeAccess.toInt(inDataPtr);
        inDataPtr += Utils.SIZEOF_INT;
        long valPtr = inDataPtr;
        start = Utils.strToLong(valPtr, valSize);
        if (numArgs > 4) {
          inDataPtr += valSize;
          valSize = UnsafeAccess.toInt(inDataPtr);
          inDataPtr += Utils.SIZEOF_INT;
          valPtr = inDataPtr;
          end = Utils.strToLong(valPtr, valSize);
        }
      }
      long num = SparseBitmaps.SBITPOS(map, keyPtr, keySize, bit, start, end);

      // INTEGER reply - we do not check buffer size here - should be larger than 9
      INT_REPLY(outBufferPtr, num);
    } catch (NumberFormatException e) {
      String msg = e.getMessage();
      if (msg == null) {
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT);
      } else {
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT, ": " + msg);
      }
    }
  }

}
