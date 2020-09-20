package org.bigbase.carrot.redis;

public interface Filter {
  /**
   * Simple scanner filter
   * @param ptr address of a record to 
   * @return true if pass, false - otherwise
   */
  public boolean accept(long ptr);
  
}
