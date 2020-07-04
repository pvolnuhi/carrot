package org.bigbase.carrot.extensions.sets;

import java.io.Closeable;
import java.io.IOException;

import org.bigbase.carrot.BigSortedMapDirectMemoryScanner;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class SetScanner implements Closeable{
  
  private long keyPtr;
  private BigSortedMapDirectMemoryScanner mapScanner;
  long valueAddress;
  int valueSize;
  int offset;
  int elementSize;
  long elementAddress;
  
  public SetScanner(BigSortedMapDirectMemoryScanner scanner, long keyPtr) {
    this.keyPtr = keyPtr;
    this.mapScanner = scanner;
    init();
  }
  
  private void init() {
    // TODO Auto-generated method stub
    this.valueAddress = mapScanner.valueAddress();
    this.valueSize = mapScanner.valueSize();
    this.offset = Sets.NUM_ELEM_SIZE;
    if (this.valueSize > Sets.NUM_ELEM_SIZE) {
      this.elementSize = Utils.readUVInt(valueAddress + offset);
      this.elementAddress = valueAddress + offset + Utils.sizeUVInt(this.elementSize);
    } else {
      this.elementAddress = this.valueAddress + this.offset;
      this.elementSize = 0;
    }
    
  }

  public boolean hasNext() throws IOException {
    if (offset < valueSize) {
      return elementAddress + elementSize < valueAddress + valueSize;
    }
    // TODO next K-V can not have 0 elements - it must be deleted
    // but first element can, so we has to check what next() call return
    // the best way is to use do
    while(mapScanner.hasNext()) {
      mapScanner.next();
      this.valueAddress = mapScanner.valueAddress();
      this.valueSize = mapScanner.valueSize();
      // check if it it is not empty
      this.offset = Sets.NUM_ELEM_SIZE;
      if (valueSize <= Sets.NUM_ELEM_SIZE) {
        // empty set in K-V? Must be deleted
        continue;
      } else {
        return true;
      }
    }
    return false;
  }
  
  public boolean next() throws IOException {
    if (offset < valueSize) {
      int elSize = Utils.readUVInt(valueAddress + offset);
      int lenSize =Utils.sizeUVInt(elSize);
      offset += elSize + lenSize;
      if (offset < valueSize) {
        this.elementSize = Utils.readUVInt(valueAddress + offset);
        this.elementAddress = valueAddress + offset + Utils.sizeUVInt(this.elementSize);
        return true;
      }
    }
    // TODO next K-V can not have 0 elements - it must be deleted
    // but first element can, so we has to check what next() call return
    // the best way is to use do
    while(mapScanner.hasNext()) {
      mapScanner.next();
      this.valueAddress = mapScanner.valueAddress();
      this.valueSize = mapScanner.valueSize();
      // check if it it is not empty
      this.offset = Sets.NUM_ELEM_SIZE;
      if (valueSize <= Sets.NUM_ELEM_SIZE) {
        // empty set in K-V? Must be deleted
        continue;
      } else {
        return true;
      }
    }
    return false;
  }
  
  public long elementAddress() {
    return this.elementAddress;
  }
  
  public int elementSize() {
    return this.elementSize;
  }
  
  @Override
  public void close() throws IOException {
    UnsafeAccess.free(keyPtr);
    mapScanner.close();
  }
  
}