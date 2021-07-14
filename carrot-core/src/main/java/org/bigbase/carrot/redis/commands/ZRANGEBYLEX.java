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
import org.bigbase.carrot.redis.zsets.ZSets;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class ZRANGEBYLEX implements RedisCommand {

  /**
   * TODO: v 6.2 support
   * ZRANGEBYLEX key min max
   */
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
      int clen = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT + clen;
      int keySize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long keyPtr = inDataPtr;
      inDataPtr += keySize;

      long startPtr = 0, endPtr = 0;
      int startSize = 0, endSize = 0;
      boolean startInclusive = true, endInclusive = true;

      startSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      startPtr = inDataPtr;
      if (UnsafeAccess.toByte(startPtr) == (byte) '(') {
        // start is not inclusive
        startInclusive = false;
      } else if (UnsafeAccess.toByte(startPtr) == (byte) '['){
        // start is inclusive
        startInclusive = true;
      } else if (UnsafeAccess.toByte(startPtr) == (byte) '-') {
        startPtr = 0;
        startSize = 0;
      } else {
        throw new IllegalArgumentException("Either '(' or '[' or '-' must be specified for a min argument");
      }
      startPtr = startPtr == 0? 0: startPtr + Utils.SIZEOF_BYTE;
      startSize = startSize == 0? 0: startSize - Utils.SIZEOF_BYTE;
      inDataPtr += Utils.SIZEOF_BYTE;
      inDataPtr += startSize;
      
      endSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      endPtr = inDataPtr;
      
      if (UnsafeAccess.toByte(endPtr) == (byte) '(') {
        // start is not inclusive
        endInclusive = false;
      } else if (UnsafeAccess.toByte(endPtr) == (byte) '['){
        // start is inclusive
        endInclusive = true;
      } else if (UnsafeAccess.toByte(endPtr) == (byte) '+') {
        endPtr = 0;
        endSize = 0;
      } else {
        throw new IllegalArgumentException("Either '(' or '[' or '+' must be specified for a max argument");
      }
      endPtr = endPtr == 0? 0: endPtr + Utils.SIZEOF_BYTE;
      endSize = endSize == 0? 0: endSize - Utils.SIZEOF_BYTE;
      inDataPtr += Utils.SIZEOF_BYTE;
      inDataPtr += endSize;
      
      int off = Utils.SIZEOF_BYTE + Utils.SIZEOF_INT;
     
      int size = (int) ZSets.ZRANGEBYLEX(map, keyPtr, keySize, startPtr, startSize, startInclusive, endPtr,
        endSize, endInclusive, outBufferPtr + off, outBufferSize - off);

      // VARRAY reply 
      UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.VARRAY.ordinal());
      UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE, size);
      
    } catch (NumberFormatException e) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT);
    } catch (IllegalArgumentException ee) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_ILLEGAL_ARGS, ee.getMessage());
    }
  }

}
