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
package org.bigbase.carrot.util;

public class Pair<T> implements Comparable<Pair<T>>{
  
  T first;
  T second;
  
  public Pair(T first, T second) {
    this.first = first;
    this.second = second;
  }
  
  public T getFirst() {
    return first;
  }
  
  public T getSecond() {
    return second;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof Pair)) {
      return false;
    }
    Pair<T> p = (Pair<T>) o;
    return first.equals(p.first) && second.equals(p.second);
  }

  @SuppressWarnings("unchecked")
  @Override
  public int compareTo(Pair<T> o) {
    // TODO Auto-generated method stub
    Comparable<T> t = (Comparable<T>) first;
    Comparable<T> s = (Comparable<T>) o.first;
    return t.compareTo((T) s);
  }
}
