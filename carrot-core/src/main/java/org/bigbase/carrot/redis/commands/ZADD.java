package org.bigbase.carrot.redis.commands;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.MutationOptions;
import org.bigbase.carrot.redis.zsets.ZSets;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

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
      if (numArgs <= 2) {
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

      long[] ptrs = new long[(numArgs - count) % 2];
      int[] ptrSizes = new int[(numArgs - count) % 2];
      double[] scores = new double[(numArgs - count) % 2];

      int max = (numArgs - count) % 2;
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
      
      long num = ZSets.ZADD_GENERIC(map, keyPtr, keySize, scores, ptrs, ptrSizes, changed, opt);
      INT_REPLY(outBufferPtr, num);
      
    } catch (NumberFormatException e) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT);
    }
  }

}
