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
import org.bigbase.carrot.redis.hashes.Hashes;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class HRANDFIELD implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {

    try {
      int count = 1;
      boolean withValues = false;
      int numArgs = UnsafeAccess.toInt(inDataPtr);
      if (numArgs < 2 || numArgs > 4) {
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
        return;
      }
      inDataPtr += Utils.SIZEOF_INT;
      // Skip command name
      inDataPtr = skip(inDataPtr, 1);
      int keySize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long keyPtr = inDataPtr;
      inDataPtr += keySize;

      if (numArgs > 2) {
        int size = UnsafeAccess.toInt(inDataPtr);
        inDataPtr += Utils.SIZEOF_INT;
        count = (int) Utils.strToLong(inDataPtr, size);
        inDataPtr += size;
      }
      if (numArgs > 3) {
        int size = UnsafeAccess.toInt(inDataPtr);
        inDataPtr += Utils.SIZEOF_INT;
        if (Utils.compareTo(WITHVALUES_FLAG, WITHVALUES_LENGTH, inDataPtr, size) == 0 ||
            Utils.compareTo(WITHVALUES_FLAG_LOWER, WITHVALUES_LENGTH, inDataPtr, size) == 0) {
          withValues = true;
        } else {
          throw new IllegalArgumentException(Utils.toString(inDataPtr, size));
        }
      }
      int size = (int) Hashes.HRANDFIELD(map, keyPtr, keySize, count, withValues,
        outBufferPtr + Utils.SIZEOF_BYTE + Utils.SIZEOF_INT,
        outBufferSize - Utils.SIZEOF_BYTE - Utils.SIZEOF_INT);
      if (numArgs == 2) {
        // Bulk string reply
        // We will never get null here
        UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.BULK_STRING.ordinal());

        int num = UnsafeAccess.toInt(outBufferPtr + Utils.SIZEOF_BYTE + Utils.SIZEOF_INT);
        int strLen = num == 0 ? -1
            : Utils.readUVInt(outBufferPtr + Utils.SIZEOF_BYTE + 2 * Utils.SIZEOF_INT);
        if (strLen == -1) {
          // NULL
          UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE, strLen);
        } else {
          int sizeStrLen = Utils.sizeUVInt(strLen);
          // Write string length
          UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE, strLen);
          UnsafeAccess.copy(outBufferPtr + Utils.SIZEOF_BYTE + 2 * Utils.SIZEOF_INT + sizeStrLen,
            outBufferPtr + Utils.SIZEOF_BYTE + Utils.SIZEOF_INT, strLen);
        }
      } else {
        // Array reply
        UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.VARRAY.ordinal());
        // Array serialized size
        UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE,
          size + Utils.SIZEOF_BYTE + Utils.SIZEOF_INT);
      }
      // All data is in the out buffer, including number of elements
    } catch (NumberFormatException e) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT,
        ": " + e.getMessage());
    } catch (IllegalArgumentException ee) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_COMMAND_FORMAT,
        ": " + ee.getMessage());
    }

  }
}
