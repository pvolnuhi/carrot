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

import java.io.Closeable;
import java.io.IOException;

import org.bigbase.carrot.redis.Filter;

public abstract class Scanner implements Closeable{

  Filter filter;
  
  public void setFilter(Filter f) {
    this.filter = f;
  }
  
  public Filter getFilter() {
    return filter;
  }
  
  /**
   * Set scanner to a first element
   * @return true on success, false if scanner is empty
   */
  public abstract boolean first() throws IOException;
  /**
   * Set scanner to a last element();
   * @return true on success
   */
  public abstract boolean last() throws IOException;
  /**
   * Next element
   * @return true if exists,  false - otherwise
   */
  public abstract boolean next() throws IOException;
  /**
   * Previous element
   * @return true, if exists, false - otherwise
   */
  public abstract boolean previous() throws IOException;
  /**
   * Has next element?
   * @return true, if yes, false - otherwise
   * @throws IOException 
   */
  public abstract boolean hasNext() throws IOException;
  /**
   * Has previous element?
   * @return true, if - yes, false - otherwise
   */
  public abstract boolean hasPrevious() throws IOException;
  
}
