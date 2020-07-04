package org.bigbase.carrot.extensions.sets;

import java.io.IOException;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.BigSortedMapDirectMemoryScanner;
import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.updates.Update;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * This read-modify-write mutation is executed atomically and isolated
 * It deletes element from a given set, defined by a Key
 * @author Vladimir Rodionov
 *
 */
public class SetDelete extends Update{
  // TODO: use own keyArena

  BigSortedMap map;
    
  public SetDelete() {
    setFloorKey(true);
  }
  
  @Override
  public void reset() {
    super.reset();
    setFloorKey(true);
    map = null;
  }
  
  public void setMap(BigSortedMap map) {
    this.map = map;
  }
  
  @Override
  public boolean execute() {
    if (foundRecordAddress <=0) {
      return false;
    }
    // check prefix
    int setKeySize = Sets.keySize(keyAddress);
    int foundKeySize = DataBlock.keyLength(foundRecordAddress);
    if (foundKeySize <= setKeySize + Sets.KEY_SIZE) {
      return false;
    }
    long foundKeyAddress = DataBlock.keyAddress(foundRecordAddress);
    // Prefix keys must be equals
    if (Utils.compareTo(keyAddress, setKeySize +  Sets.KEY_SIZE, foundKeyAddress, 
      setKeySize +  Sets.KEY_SIZE) != 0) {
      return false;
    }
    
    long elementPtr = Sets.elementAddressFromKey(keyAddress);
    int elementSize = Sets.elementSizeFromKey(keyAddress, keySize);
    // First two bytes are number of elements in a value
    long addr = Sets.exactSearch(foundRecordAddress, elementPtr, elementSize);
    if (addr < 0) {
      return false;
    }
    // found
    int elemSizeSize = Utils.sizeUVInt(elementSize);
    int toCut = elemSizeSize + elementSize;
    long valueAddress = DataBlock.valueAddress(foundRecordAddress);
    // decrement number of elements in this value
    int numElements = Sets.addNumElements(valueAddress, -1);
    int valueSize = DataBlock.valueLength(foundRecordAddress);
    int newValueSize = valueSize - toCut;
    Sets.checkValueArena(newValueSize);
    long ptr = Sets.valueArena.get();
    // TODO: check this
    UnsafeAccess.copy(valueAddress, ptr, addr - valueAddress);
    UnsafeAccess.copy(addr + toCut, ptr + addr - valueAddress, valueSize - toCut - (addr - valueAddress));
    
    // set # of updates to 1
    this.updatesCount = 1;
    this.keys[0] = foundKeyAddress;
    this.keySizes[0] = foundKeySize;
    this.values[0] = ptr;
    this.valueSizes[0] = valueSize - toCut;
    if (numElements == 0 && canDelete(foundKeyAddress, foundKeySize)) {
      // Delete Key, b/c its empty
      this.updateTypes[0] = true;
    }
    return true;
  }
  
  /**
   * We can delete K-V only when it is empty, not a first key (ends with '\0' 
   * or (TODO) first and the only K-V for the set)
   * @param foundKeyAddress
   * @return true if can be deleted, false -otherwise
   * @throws IOException 
   */
  private boolean canDelete(long foundKeyAddress, int foundKeySize) {
    if (!Sets.firstKVinSet(foundKeyAddress, foundKeySize)) {
      return true;
    }
    // this first KV in set, we can delete it if it is the only one in the set
    return !nextKVisInSet(foundKeyAddress);
  }

  
  /**
   * This method checks if next K-V exists in the set
   * @param ptr current key address
   * @return true if exists, false - otherwise
   */
  private boolean nextKVisInSet(long ptr)  {
    int keySize = Sets.keySize(ptr) + Sets.KEY_SIZE;
    BigSortedMapDirectMemoryScanner scanner = map.getPrefixScanner(ptr, keySize);
    try {
      // should not be null
      return scanner.next();
    } finally {
      try {
        scanner.close();
      }catch  (IOException e) {
        // swallow
      }
    }
  }
}
