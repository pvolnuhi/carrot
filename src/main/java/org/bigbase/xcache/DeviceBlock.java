package org.bigbase.xcache;

import org.bigbase.util.UnsafeAccess;

public class DeviceBlock extends Block {

  static enum Type {
    MEMORY, DEVICE;
  }
  
  private Type type = Type.MEMORY;
  
  // Can be device (SSD) or RAM
  private long address;
  
  public DeviceBlock(long maxSize, int id) {
    super(maxSize, id);
    address = UnsafeAccess.theUnsafe.allocateMemory(maxSize);
    if(address <=0) {
      throw new RuntimeException("memory allocation error");
    }
  }

  @Override
  public void sealBlock() {
    // TODO Auto-generated method stub
    
    
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }
  
  
}
