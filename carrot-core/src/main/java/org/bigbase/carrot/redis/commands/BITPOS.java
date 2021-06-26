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
import org.bigbase.carrot.redis.Commons;
import org.bigbase.carrot.redis.strings.Strings;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class BITPOS implements RedisCommand {

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
      // FIXME: convert ALL Redis API from long[] / int[] to memory buffer interface
      int keySize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long keyPtr = inDataPtr;
      inDataPtr += keySize;
      int bit = UnsafeAccess.toInt(inDataPtr);
      if (bit != 1 && bit != 0) {
        // ERROR
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_BIT_VALUE);
        return;
      }
      // FIXME - double conversion

      long start = Commons.NULL_LONG;
      long end = Commons.NULL_LONG;
      if (numArgs > 3) {
        int valSize = UnsafeAccess.toInt(inDataPtr);
        inDataPtr += Utils.SIZEOF_INT;
        long valPtr = inDataPtr;
        //double conversion
        String s = Utils.toString(valPtr, valSize);
        start = Long.parseLong(s);
        if (numArgs > 4) {
          inDataPtr += s.length();
          valSize = UnsafeAccess.toInt(inDataPtr);
          inDataPtr += Utils.SIZEOF_INT;
          valPtr = inDataPtr;
          s = Utils.toString(valPtr, valSize);
          end = Long.parseLong(s);
        }
      }
      long num = Strings.BITPOS(map, keyPtr, keySize, bit, start, end);

      // INTEGER reply - we do not check buffer size here - should be larger than 9
      UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.INTEGER.ordinal());
      UnsafeAccess.putLong(outBufferPtr + Utils.SIZEOF_BYTE, num);
    } catch (NumberFormatException e) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT);
    }
  }

}
