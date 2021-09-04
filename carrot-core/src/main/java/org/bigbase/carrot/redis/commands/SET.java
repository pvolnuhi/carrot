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
import org.bigbase.carrot.redis.util.MutationOptions;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * SET key value [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|KEEPTTL] [NX|XX] [GET]
 * 
 *
 */

public class SET implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    try {
      
      MutationOptions opts = MutationOptions.NONE;
      // means - no expire
      long expire = 0;
      int numArgs = UnsafeAccess.toInt(inDataPtr);
      int argsCount;
      boolean keepTTL = false;
      if (numArgs < 3 || numArgs > 7) {
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
      int valSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long valPtr = inDataPtr;
      inDataPtr += valSize;
      
      argsCount = 3;
      int num = 0;
      if (numArgs > argsCount) {
        expire = getExpire(inDataPtr, numArgs - argsCount);
        num = ttlSectionSize(inDataPtr, false, numArgs - argsCount);
        inDataPtr = skip(inDataPtr, num);
        argsCount += num;
        if (expire  == -1) keepTTL = true;
      }
      
      if (numArgs > argsCount) {
        opts = getMutationOptions(inDataPtr);
        num = mutationSectionSize(inDataPtr);
        inDataPtr = skip (inDataPtr, num); // Both NX and XX are the same size of 2
        argsCount += num;
      }
      
      boolean withGet = false;
      if (numArgs > argsCount) {
        // Check GET
        int size = UnsafeAccess.toInt(inDataPtr);
        inDataPtr += Utils.SIZEOF_INT;
        if (Utils.compareTo(GET_FLAG, GET_LENGTH, inDataPtr, size) == 0) {
          withGet = true;
          argsCount += 1;
          if (argsCount < numArgs) {
            inDataPtr += size;
            size = UnsafeAccess.toInt(inDataPtr);
            inDataPtr += Utils.SIZEOF_INT;
            throw new IllegalArgumentException(Utils.toString(inDataPtr, size));
          }
        } else {
          throw new IllegalArgumentException(Utils.toString(inDataPtr, size));
        }
      }
      
      long size = 0;    
      if (withGet) {
        size = Strings.SETGET(map, keyPtr, keySize, valPtr, valSize, expire, opts, keepTTL, 
          outBufferPtr + Utils.SIZEOF_BYTE + Utils.SIZEOF_INT, outBufferSize - Utils.SIZEOF_BYTE - Utils.SIZEOF_INT);
        
        // Bulk String reply
        UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.BULK_STRING.ordinal());
        if (size < outBufferSize - Utils.SIZEOF_BYTE - Utils.SIZEOF_INT) {
          UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE, (int) size);
        } else {
          // Buffer is small
          UnsafeAccess.putInt(outBufferPtr + Utils.SIZEOF_BYTE,
            (int) size + Utils.SIZEOF_BYTE + Utils.SIZEOF_INT);
        }
      } else {
        boolean result = Strings.SET(map, keyPtr, keySize, valPtr, valSize, expire, opts, keepTTL);
        if (!result) {
          // null
          NULL_STRING_REPLY(outBufferPtr);
        } else {
          // OK - do nothing
        }
      }
    } catch(NumberFormatException ee) {
      String msg = ee.getMessage();
      if (msg == null) {
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT);
      } else {
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT, ": " + msg);
      }
    } catch (IllegalArgumentException e) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_COMMAND_FORMAT, ": " + e.getMessage());
    } 
  }

}
