package org.bigbase.carrot.util;

import java.io.Closeable;
import java.io.IOException;

import org.bigbase.carrot.redis.Filter;

public abstract class BiScanner implements Closeable{

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
