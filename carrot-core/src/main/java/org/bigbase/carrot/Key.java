package org.bigbase.carrot;

import org.bigbase.carrot.util.Utils;

public class Key implements Comparable<Key>{
  public long address;
  public int length;
  
  public Key(long address, int size){
    this.address = address;
    this.length = size;
  }
  
  @Override
  public int hashCode() {
    return Utils.murmurHash(address, length, 0);
  }

  @Override
  public int compareTo(Key o) {
    return Utils.compareTo(address, length, o.address, o.length);
  }
}
