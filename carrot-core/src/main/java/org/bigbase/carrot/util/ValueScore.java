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

public class ValueScore extends Key{
  public double score;
  
  public ValueScore(long ptr, int size, double score) {
    super(ptr, size);
    this.score = score;
  }
  
  @Override
  public String toString() {
    return super.toString() + " score="+ score;
  }
  
  @Override
  public int compareTo(Key o) {
    ValueScore vs = (ValueScore) o;
    if (score == vs.score) {
      return Utils.compareTo(address, length, o.address, o.length);
    } else {
      return score > vs.score? 1: -1;
    }
  }
}
