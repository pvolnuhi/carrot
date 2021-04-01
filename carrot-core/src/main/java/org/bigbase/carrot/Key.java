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
    return Math.abs(Utils.murmurHash(address, length, 0));
  }

  @Override
  public int compareTo(Key o) {
    return Utils.compareTo(address, length, o.address, o.length);
  }
  
  @Override 
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Key)) {
      return false;
    }
    return compareTo((Key)o) == 0;
  }
}
