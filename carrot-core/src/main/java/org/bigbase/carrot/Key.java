package org.bigbase.carrot;

import org.bigbase.carrot.util.Utils;

public class Key implements Comparable<Key>{
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

  @Override
  public int compareTo(Key o) {
    return Utils.compareTo(address, size, o.address, o.size);
  }
}
