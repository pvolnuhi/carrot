package org.bigbase.carrot.redis.commands;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.keys.Keys;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Redis DEL command
 * @author vrodionov
 *
 */
public class DEL implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    
    int numArgs = UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT;
    // skip command name
    int clen = UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT + clen;
    
    // FIXME: convert ALL Redis API from long[] / int[] to memory buffer interface
    long[] ptrs = Utils.loadPointers(inDataPtr, numArgs - 1);
    int[]  sizes = Utils.loadSizes(inDataPtr, numArgs - 1);
    int num = Keys.DEL(map, ptrs, sizes);
    
    // INTEGER reply - we do not check buffer size here - should be larger than 9
    UnsafeAccess.putByte(outBufferPtr, (byte) ReplyType.INTEGER.ordinal());
    UnsafeAccess.putLong(outBufferPtr, num);
  }

}
