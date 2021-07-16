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
import org.bigbase.carrot.redis.strings.Strings;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class GETEX implements RedisCommand {
  
 
  
  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    try {
      int numArgs = UnsafeAccess.toInt(inDataPtr);

      if (numArgs < 2 || numArgs > 4) {
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
      long size = 0;
      int argsCount = 2;
      if (numArgs > argsCount) {
        long expire = getExpire(inDataPtr, true, true, numArgs - argsCount);
        int num = ttlSectionSize(keyPtr, true, numArgs - argsCount);
        size =
          Strings.GETEX(map, keyPtr, keySize, expire, outBufferPtr + Utils.SIZEOF_BYTE + Utils.SIZEOF_INT,
            outBufferSize - Utils.SIZEOF_BYTE - Utils.SIZEOF_INT);
      } else {
        // Pure GET
        size =
            Strings.GET(map, keyPtr, keySize, outBufferPtr + Utils.SIZEOF_BYTE + Utils.SIZEOF_INT,
              outBufferSize - Utils.SIZEOF_BYTE - Utils.SIZEOF_INT);
      }
      // Bulk String reply
      UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.BULK_STRING.ordinal());
      if (size < outBufferSize - Utils.SIZEOF_BYTE - Utils.SIZEOF_INT) {
        UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE, (int) size);
      } else {
        // Buffer is small
        UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE,
          (int) size + Utils.SIZEOF_BYTE + Utils.SIZEOF_INT);
      }
    } catch(NumberFormatException ee) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT);
    } catch (IllegalArgumentException e) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_ILLEGAL_ARGS, ": " + e.getMessage());
    } 
  }

 
}
