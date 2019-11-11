package org.bigbase.xcache;

public class VirtualBlock extends Block{

  public VirtualBlock(long maxSize, int id) {
    super(maxSize, id);
  }

  @Override
  public void sealBlock() {
    
  }

}
