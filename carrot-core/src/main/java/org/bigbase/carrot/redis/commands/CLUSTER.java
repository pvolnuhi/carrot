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

import java.nio.ByteBuffer;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.cluster.Cluster;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Supported cluster commands:
 * 
 * 1. CLUSTER SLOTS
 *
 */
public class CLUSTER implements RedisCommand {
  
  private Object[] result;
  boolean autoConvert = false;
  
  private void reset() {
    // Reset state
    autoConvert = false;
    result = null;
  }
  
  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    
    reset();
    
    int numArgs = UnsafeAccess.toInt(inDataPtr);
    if (numArgs != 2) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
      autoConvert = true;
      return;
    }
    inDataPtr += Utils.SIZEOF_INT;
    inDataPtr = skip(inDataPtr, 1);
    int size = UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT;
      
    if (Utils.compareTo(SLOTS_FLAG, SLOTS_LENGTH, inDataPtr, size) != 0 && 
        Utils.compareTo(SLOTS_LOWER_CASE_FLAG, SLOTS_LENGTH, inDataPtr, size) != 0) {
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_UNSUPPORTED_COMMAND, ": CLUSTER " + 
            Utils.toString(inDataPtr, size));
        autoConvert = true;
        return;
    }
    result = Cluster.SLOTS();
  }
  
  /**
   * Do automatic conversion?
   */
  public boolean autoconvertToRedis() {
    return autoConvert;
  }
  
  @Override
  public void convertToRedis(ByteBuffer buf) {
    org.bigbase.carrot.redis.util.Utils.serializeTypedArray(result, buf);
  }
}
