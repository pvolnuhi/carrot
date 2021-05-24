package org.bigbase.carrot.ops;

import java.io.IOException;

@SuppressWarnings("serial")
public class OperationFailedException extends IOException {

  public OperationFailedException(String s) {
    super(s);
  }

  public OperationFailedException(Exception e) {
    super(e);
  }

  public OperationFailedException() {
    super();
  }

}
