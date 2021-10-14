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

public class ZRANGEBYSCORE implements RedisCommand {

  /**
   * TODO: v. 6.2 support LIMIT offset count
   * ZRANGEBYSCORE key min max [WITHSCORES]
   */
  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    try {
      boolean withScores = false;
      double start = 0, end = 0;
      int numArgs = UnsafeAccess.toInt(inDataPtr);
      if (numArgs < 4 || numArgs > 5) {
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

      boolean startInclusive = true, endInclusive = true;

      int valSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long valPtr = inDataPtr;
      if (UnsafeAccess.toByte(valPtr) == (byte) '(') {
        // start is not inclusive
        startInclusive = false;
        valSize -= Utils.SIZEOF_BYTE;
        valPtr += Utils.SIZEOF_BYTE; 
        inDataPtr += Utils.SIZEOF_BYTE;
      } else if (Utils.compareTo(NEG_INFINITY_FLAG, NEG_INFINITY_LENGTH, valPtr, valSize) == 0) {
        startInclusive = false;
        start = - Double.MAX_VALUE;
        inDataPtr += NEG_INFINITY_LENGTH;
      }
      
      if (start == 0) {
        start = Utils.strToDouble(valPtr, valSize);
        inDataPtr += valSize;
      }
      
      valSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      valPtr = inDataPtr;
      if (UnsafeAccess.toByte(valPtr) == (byte) '(') {
        // start is not inclusive
        endInclusive = false;
        valSize -= Utils.SIZEOF_BYTE;
        valPtr += Utils.SIZEOF_BYTE; 
        inDataPtr += Utils.SIZEOF_BYTE;
      }  else if (Utils.compareTo(POS_INFINITY_FLAG, POS_INFINITY_LENGTH, valPtr, valSize) == 0) {
        endInclusive = false;
        end = Double.MAX_VALUE;
        inDataPtr += POS_INFINITY_LENGTH;
      }
      
      if (end == 0) {
        end = Utils.strToDouble(valPtr, valSize);
        inDataPtr += valSize;
      }
      
      if (numArgs == 5) {
        valSize = UnsafeAccess.toInt(inDataPtr);
        inDataPtr += Utils.SIZEOF_INT;
        valPtr = inDataPtr;
        if (Utils.compareTo(WITHSCORES_FLAG, WITHSCORES_LENGTH, valPtr, valSize) == 0 ||
            Utils.compareTo(WITHSCORES_FLAG_LOWER, WITHSCORES_LENGTH, valPtr, valSize) == 0) {
          withScores = true;
        } else {
          throw new IllegalArgumentException(Utils.toString(valPtr, valSize));
        }
      }
      int off = Utils.SIZEOF_BYTE + Utils.SIZEOF_INT;
      int size =
          (int) ZSets.ZRANGEBYSCORE(map, keyPtr, keySize, start, startInclusive, end, endInclusive,
            withScores, outBufferPtr + off, outBufferSize - off);
      
      if (withScores) {
        UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.ZARRAY.ordinal());
      } else {
        UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.VARRAY.ordinal());
      }
      UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE, size + off);
      
    } catch (NumberFormatException e) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT, ": "+ e.getMessage());
    } catch (IllegalArgumentException ee) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_COMMAND_FORMAT,": " + ee.getMessage());
    }
  }
}
