package org.bigbase.carrot.util;

public class Pair<T> {
  
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
}
