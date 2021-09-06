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
import org.bigbase.carrot.redis.RedisConf;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

// Currently we support only INFO MEMORY
public class INFO implements RedisCommand {

  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    int numArgs = UnsafeAccess.toInt(inDataPtr);

    if (numArgs != 2) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
      return;
    }
    inDataPtr += Utils.SIZEOF_INT;
    inDataPtr = skip(inDataPtr, 1);
    int size = UnsafeAccess.toInt(inDataPtr);
    inDataPtr += Utils.SIZEOF_INT;
    if (Utils.compareTo(MEMORY_FLAG, MEMORY_LENGTH, inDataPtr, size) != 0 && 
          Utils.compareTo(MEMORY_LOWER_CASE_FLAG, MEMORY_LENGTH, inDataPtr, size) != 0) {
      Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_UNSUPPORTED_COMMAND, ": INFO " + 
      Utils.toString(inDataPtr, size));
      return;
    }
    
    String[] info = new String[13];
    info[0] = "# Memory (Carrot)";
    long maxmemory = RedisConf.getInstance().getMaxMemoryLimit();
    info[1] = "maxmemory:"+ maxmemory;

    info[2] = "used_memory:"+ BigSortedMap.getGlobalAllocatedMemory();
    info[3] = "used_memory_dataset:" + BigSortedMap.getGlobalDataSize();
    info[4] = "used_memory_dataset_perc:" + 
        Utils.toString(((double)BigSortedMap.getGlobalDataSize() * 100)/BigSortedMap.getGlobalAllocatedMemory() , 2) +"%";
    info[5] = "used_memory_index:" + BigSortedMap.getGlobalIndexSize();
    info[6] = "used_memory_index_perc:" + 
        Utils.toString(((double)BigSortedMap.getGlobalIndexSize() * 100)/BigSortedMap.getGlobalAllocatedMemory() , 2) +"%";
    info[7] = "used_memory_ext:" + BigSortedMap.getGlobalExternalDataSize();
    info[8] = "used_memory_ext_perc:" + 
        Utils.toString(((double)BigSortedMap.getGlobalExternalDataSize() * 100)/BigSortedMap.getGlobalAllocatedMemory() , 2) +"%";

    info[9] = "used_memory_compressed:" + BigSortedMap.getGlobalCompressedDataSize();
    info[10] = "mem_fragmentation_ratio:" + 
        Utils.toString(((double)BigSortedMap.getGlobalAllocatedMemory())/BigSortedMap.getGlobalDataSize() , 2);
    info[11] = "compression_ratio:" + 
        (BigSortedMap.getGlobalCompressedDataSize() > 0 ?
            Utils.toString(((double)BigSortedMap.getGlobalDataSize())/BigSortedMap.getGlobalAllocatedMemory() , 2):
              "0.0");
    info[12] = "";
    ARRAY_REPLY(outBufferPtr, info);   
  }
}
