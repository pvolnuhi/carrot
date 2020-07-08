package org.bigbase.carrot;

import org.bigbase.carrot.util.Utils;

public class Key {
  public long address;
  public int size;
  
  public Key(long address, int size){
    this.address = address;
    this.size = size;
  }
  
  @Override
  public int hashCode() {
    return Utils.murmurHash(address, size, 0);
  }
}
