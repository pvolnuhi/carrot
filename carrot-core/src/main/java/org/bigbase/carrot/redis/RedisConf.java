package org.bigbase.carrot.redis;

/**
 * Class which keeps all the configuration parameters
 * for Redis server
 * @author Vladimir Rodionov
 *
 */
public class RedisConf {

  
  public static RedisConf getInstance() {
    //TODO
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
  
}
