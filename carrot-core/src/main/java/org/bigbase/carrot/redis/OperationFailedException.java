package org.bigbase.carrot.redis;

@SuppressWarnings("serial")
public class OperationFailedException extends Exception {

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
