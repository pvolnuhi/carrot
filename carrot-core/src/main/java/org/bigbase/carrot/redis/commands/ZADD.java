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

import java.util.ArrayList;
import java.util.List;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.sets.Sets;
import org.bigbase.carrot.redis.util.MutationOptions;
import org.bigbase.carrot.redis.zsets.ZSets;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.bigbase.carrot.util.ValueScore;

public class ZADD implements RedisCommand {
  
  /**
   * ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
   * TODO: INCR support
   * TODO: [GT|LT] (6.2) support
   */
  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    try {
      boolean changed = false;
      MutationOptions opt = MutationOptions.NONE;
      int count = 2;
      int numArgs = UnsafeAccess.toInt(inDataPtr);
      if (numArgs < 4) {
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
      // Options
      int valSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long valPtr = inDataPtr;

      if (Utils.compareTo(NX_FLAG, NX_LENGTH, valPtr, valSize) == 0) {
        opt = MutationOptions.NX;
        inDataPtr += valSize;
        count++;
      } else if (Utils.compareTo(XX_FLAG, XX_LENGTH, valPtr, valSize) == 0) {
        opt = MutationOptions.XX;
        inDataPtr += valSize;
        count++;
      } else if (Utils.compareTo(CH_FLAG, CH_LENGTH, valPtr, valSize) == 0) {
        changed = true;
        inDataPtr += valSize;
        count++;
      } else {
        // Revert ptr back
        inDataPtr -= Utils.SIZEOF_INT;
      }

      if (count == 3 && !changed) {
        // Check CH flag
        valSize = UnsafeAccess.toInt(inDataPtr);
        inDataPtr += Utils.SIZEOF_INT;
        valPtr = inDataPtr;
        if (Utils.compareTo(CH_FLAG, CH_LENGTH, valPtr, valSize) == 0) {
          changed = true;
          inDataPtr += valSize;
          count++;
        } else {
          // Revert ptr back
          inDataPtr -= Utils.SIZEOF_INT;
        }
      }

      if ((numArgs - count) % 2 != 0) {
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
        return;
      }
      
      int number = (numArgs - count) / 2;

      // Now check if zset exists and mutation option is not MutationOption.XX
      // and this is a bulk operation. In this case we call *fast* version
      if (opt != MutationOptions.XX && number > 1) {
        if (!Sets.keyExists(map, keyPtr, keySize)) {
          List<ValueScore> members = populateAndGetValueScores(inDataPtr, number);
          long num = ZSets.ZADD_NEW(map, keyPtr, keySize, members);
          INT_REPLY(outBufferPtr, num);
          return;
        };
      }
      // For all other cases still old version (TODO: optimize general case)
      long[] ptrs = new long[number];
      int[] ptrSizes = new int[number];
      double[] scores = new double[number];
      populate(inDataPtr, ptrs, ptrSizes, scores);
      
      long num = ZSets.ZADD_GENERIC(map, keyPtr, keySize, scores, ptrs, ptrSizes, changed, opt);
      INT_REPLY(outBufferPtr, num);
      
    } catch (NumberFormatException e) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, 
        Errors.ERR_WRONG_NUMBER_FORMAT, ": " + e.getMessage());
    }
  }

  private List<ValueScore> populateAndGetValueScores(long inDataPtr, int max) {
    List<ValueScore> cached = ZSets.getValueScoreList();
    // Make sure that thread local list is at least 'max' size
    while(cached.size() < max) {
      cached.add(new ValueScore(0,0,0));
    }
    List<ValueScore> list = new ArrayList<ValueScore>(max);
    
    for (int i = 0; i < max; i++) {
      // Read score
      int valSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      double score = Utils.strToDouble(inDataPtr, valSize);
      // Read member
      inDataPtr += valSize;
      int size = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long ptr = inDataPtr;
      ValueScore vs = cached.get(i);
      vs.address = ptr;
      vs.length = size;
      vs.score = score;
      list.add(vs);
      inDataPtr += size;
    }
    return list;
  }
  
  private void populate(long inDataPtr, long[] ptrs, int[] ptrSizes, double[] scores) {
    int max = scores.length;
    int valSize;
    for (int i = 0; i < max; i++) {
      // Read score
      valSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      scores[i] = Utils.strToDouble(inDataPtr, valSize);
      // Read member
      inDataPtr += valSize;
      ptrSizes[i] = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      ptrs[i] = inDataPtr;
      inDataPtr += ptrSizes[i];
    }
  }
}
