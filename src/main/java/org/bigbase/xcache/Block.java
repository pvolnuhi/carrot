package org.bigbase.xcache;


import java.util.concurrent.atomic.AtomicLong;

public abstract class Block {

  protected long maxSize;
  protected AtomicLong size;  
  protected AtomicLong numElements;  
  protected int id;
  
  public Block(long maxSize, int id) 
  {
    this.maxSize = maxSize;
  }
  
  public long size() {
    return size.get();
  }
  
  public long getMaxSize() {
    return maxSize;
  }
  
  public long numElements() {
    return numElements.get();
  }
  
  public int getId() {
    return id;
  }
  
  public abstract void sealBlock();
}

