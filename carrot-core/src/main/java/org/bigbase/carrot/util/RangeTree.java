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

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class RangeTree {
  public static class Range implements Comparable<Range> {
    long start;
    int size;
    
    Range() {
      
    }
    Range(long start, int size) {
      this.start = start;
      this.size = size;
    }
    
    @Override
    public int compareTo(Range o) {
      if(start > o.start) return 1;
      if(start < o.start) return -1;    
      return 0;
    }
  }
  
  private TreeMap<Range, Range> map = new TreeMap<Range, Range>();
  
  public RangeTree() {
    
  }
   
  public synchronized Range add(Range r) {
    return map.put(r, r);
  }
  
  public synchronized Range delete(long address ) {
    search.start = address;
    search.size = 0;
    return map.remove(search);
  }
  
  private Range search = new Range();
  
  public synchronized boolean inside(long start, int size) {
    search.start = start;
    search.size = size;
    Range r = map.floorKey(search);
    boolean result = r != null && start >= r.start && (start + size) <= r.start + r.size;
    if (!result && r != null) {
      System.out.println("Check FAILED for range [" + start+","+size+ "] Found allocation ["+ 
    r.start+","+ r.size+"]");
    } else if (!result) {
      System.out.println("Check FAILED for range [" + start+","+size+ "] No allocation found.");
    }
    return result;
  }
  
  public synchronized int size() {
    return map.size();
  }
  
  public synchronized Set<Map.Entry<Range, Range>> entrySet() {
    return map.entrySet();
  }
}
