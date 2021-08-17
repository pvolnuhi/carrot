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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.bigbase.carrot.compression.Codec;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;

/**
 * Class which keeps all the configuration parameters
 * for Redis server
 *
 */
public class RedisConf {

  public final static String CONF_COMMAND_COUNT = "command.count";
  public final static String CONF_COMPRESSION_CODEC = "compression.codec";
  public final static String CONF_DATASTORE_MAX_SIZE = "datastore.maxsize";
  public final static String CONF_ZSET_MAX_COMPACT_SIZE = "zset.compact.maxsize";
  public final static String CONF_SERVER_PORT = "server.port";
  public final static String CONF_THREAD_POOL_SIZE = "thread.pool.size";

  public final static int DEFAULT_SERVER_PORT = 6379; 
  // As of v. 0.1
  public final static int DEFAULT_COMMAND_COUNT = 104;
  public final static long DEFAULT_DATASTORE_MAX_SIZE = 1024 * 1024 * 1024; // 1GB
  public final static String DEFAULT_COMPRESSION_CODEC = "none";
  public final static int DEFAULT_THREAD_POOL_SIZE = 
      Math.max(1, Runtime.getRuntime().availableProcessors() / 4);
  public final static int DEFAULT_ZSET_MAX_COMPACT_SIZE = 512;

  
  private static RedisConf conf;
  private Properties props;
  
  public static RedisConf getInstance() {
    //TODO - read from file
    return getInstance(null);
  }
  
  public static RedisConf getInstance(String file) {
    if (conf != null) {
      return conf;
    }
    synchronized (RedisConf.class) {
      try {
        conf = new RedisConf(file);
      } catch (IOException e) {
        e.printStackTrace();
      }
      return conf;
    }
  }
  
  
  private RedisConf() {
    props = new Properties();
  }
  
  private RedisConf(String file) throws IOException {
    this();
    if (file != null) {
      FileInputStream fis = new FileInputStream(file);
      props.load(fis);
    }
  }
  
  private int getIntProperty(String name, int defValue) {
    String value = props.getProperty(name);
    if (value == null) return defValue;
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      //TODO log error
      e.printStackTrace();
    }
    return defValue;
  }
  
  private long getLongProperty(String name, long defValue) {
    String value = props.getProperty(name);
    if (value == null) return defValue;
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      //TODO log error
      e.printStackTrace();
    }
    return defValue;
  }
  
  /**
   * Maximum size of ZSet in a compact representation
   * @return maximum size
   */
  public int getMaxZSetCompactSize() {
    return getIntProperty(CONF_ZSET_MAX_COMPACT_SIZE, DEFAULT_ZSET_MAX_COMPACT_SIZE);
  }
  
  /**
   * Returns number of supported commands
   * @return number of supported 
   */
  public int getCommandsCount() {
    return getIntProperty(CONF_COMMAND_COUNT, DEFAULT_COMMAND_COUNT);
  }
  
  /**
   * Returns server port
   * @return server port number
   */
  public int getServerPort() {
    return getIntProperty(CONF_SERVER_PORT, DEFAULT_SERVER_PORT);
  }
  
  /**
   * Returns working thread pool size
   * 
   */
  public int getWorkingThreadPoolSize() {
    return getIntProperty(CONF_THREAD_POOL_SIZE, DEFAULT_THREAD_POOL_SIZE);
  }
  
  /**
   * Get maximum data store size
   * @return maximum data store size
   */
  public long getDataStoreMaxSize() {
    return getLongProperty(CONF_DATASTORE_MAX_SIZE, DEFAULT_DATASTORE_MAX_SIZE);
  }
  
  /**
   * Get compression codec
   * @return codec
   */
  public Codec getCompressionCodec() {
    String value = props.getProperty(CONF_COMPRESSION_CODEC);
    if (value != null) {
      try {
        CodecType codecType = CodecType.valueOf(value.toUpperCase());
        return CodecFactory.getInstance().getCodec(codecType);
      } catch (IllegalArgumentException e) {
        e.printStackTrace();
      }
    }
    return CodecFactory.getCodec(CodecType.NONE.ordinal()); // no compression
  }
}
