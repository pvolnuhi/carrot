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
