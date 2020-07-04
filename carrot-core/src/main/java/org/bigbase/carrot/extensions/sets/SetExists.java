package org.bigbase.carrot.extensions.sets;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.updates.Update;
import org.bigbase.carrot.util.Utils;

/**
 * Although not a mutation this operation is executed as a mutation
 * to avoid copy - on -read. It checks if element exists in a set defined by a
 * Key in place w/o copying Value data.
 * @author Vladimir Rodionov
 *
 */
public class SetExists extends Update{
  // TODO: use own keyArena

    
  public SetExists() {
    setFloorKey(true);
  }
  
  @Override
  public void reset() {
    super.reset();
    setFloorKey(true);
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
    // Set no updates
    updatesCount = 0;
    // First two bytes are number of elements in a value
    return Sets.exactSearch(foundRecordAddress, elementPtr, elementSize) > 0;
  }
  

}
