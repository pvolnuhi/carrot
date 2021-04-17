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
