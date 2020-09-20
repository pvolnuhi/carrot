package org.bigbase.carrot.redis.hashes;

import static org.bigbase.carrot.redis.Commons.KEY_SIZE;
import static org.bigbase.carrot.redis.Commons.elementAddressFromKey;
import static org.bigbase.carrot.redis.Commons.elementSizeFromKey;
import static org.bigbase.carrot.redis.Commons.keySize;
import static org.bigbase.carrot.redis.Commons.keySizeWithPrefix;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.redis.sets.Sets;
import org.bigbase.carrot.util.Utils;
/**
 * Although not a mutation this operation is executed as a mutation
 * to avoid copy - on -read. It checks if field exists in a hash defined by a
 * Key in place, w/o copying Value data.
 * @author Vladimir Rodionov
 *
 */
public class HashExists extends Operation {

  public HashExists() {
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
    int setKeySize = keySizeWithPrefix(keyAddress);
    int foundKeySize = DataBlock.keyLength(foundRecordAddress);
    if (foundKeySize <= setKeySize ) {
      return false;
    }
    long foundKeyAddress = DataBlock.keyAddress(foundRecordAddress);
    // Prefix keys must be equals
    if (Utils.compareTo(keyAddress, setKeySize , foundKeyAddress, 
      setKeySize ) != 0) {
      return false;
    }
    
    long fieldPtr = elementAddressFromKey(keyAddress);
    int fieldSize = elementSizeFromKey(keyAddress, keySize);
    // Set no updates
    updatesCount = 0;
    // First two bytes are number of elements in a value
    return Hashes.exactSearch(foundRecordAddress, fieldPtr, fieldSize) > 0;
  }

}
