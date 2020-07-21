package org.bigbase.carrot.util;

import java.io.Closeable;
import java.io.IOException;

public interface BidirectionalScanner extends Closeable{

  /**
   * Set scanner to a first element
   * @return true on success, false if scanner is empty
   */
  public boolean first() throws IOException;
  /**
   * Set scanner to a last element();
   * @return true on success
   */
  public boolean last() throws IOException;
  /**
   * Next element
   * @return true if exists,  false - otherwise
   */
  public boolean next() throws IOException;
  /**
   * Previous element
   * @return true, if exists, false - otherwise
   */
  public boolean previous() throws IOException;
  /**
   * Has next element?
   * @return true, if yes, false - otherwise
   * @throws IOException 
   */
  public boolean hasNext() throws IOException;
  /**
   * Has previous element?
   * @return true, if - yes, false - otherwise
   */
  public boolean hasPrevious() throws IOException;
  
}
