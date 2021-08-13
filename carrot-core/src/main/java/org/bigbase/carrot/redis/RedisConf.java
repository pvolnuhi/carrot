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
package org.bigbase.carrot.redis;

import org.bigbase.carrot.compression.Codec;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;

/**
 * Class which keeps all the configuration parameters
 * for Redis server
 * @author Vladimir Rodionov
 *
 */
public class RedisConf {

  public final static int DEFAULT_REDIS_PORT = 6379;
  
  public static RedisConf getInstance() {
    //TODO - read from file
    return new RedisConf();
  }
  /**
   * Maximum size of ZSet in a compact representation
   * @return maximum size
   */
  public int getMaxZSetCompactSize() {
    //TODO
    return 512;
  }
  
  /**
   * Returns number of supported commands
   * @return number of supported 
   */
  public int getCommandsCount() {
    //
    return 103;
  }
  
  /**
   * Returns server port
   * @return server port number
   */
  public int getServerPort() {
    return DEFAULT_REDIS_PORT;
  }
  
  /**
   * Returns working thread pool size
   * 
   */
  public int getWorkingThreadPoolSize() {
    //TODO
    int num = Runtime.getRuntime().availableProcessors();
    return num / 4;
  }
  
  /**
   * Get maximum data store size
   * @return maximum data store size
   */
  public long getDataStoreMaxSize() {
    //TODO
    return (long) 10 * 1024 * 1024 * 1024;
  }
  
  /**
   * Get compression codec
   * @return codec
   */
  public Codec getCompressionCodec() {
    //TODO
    return CodecFactory.getCodec(CodecType.LZ4.ordinal());
  }
}
