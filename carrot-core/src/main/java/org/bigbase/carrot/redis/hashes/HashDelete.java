package org.bigbase.carrot.redis.hashes;

import static org.bigbase.carrot.redis.Commons.KEY_SIZE;
import static org.bigbase.carrot.redis.Commons.addNumElements;
import static org.bigbase.carrot.redis.Commons.elementAddressFromKey;
import static org.bigbase.carrot.redis.Commons.elementSizeFromKey;
import static org.bigbase.carrot.redis.Commons.firstKVinType;
import static org.bigbase.carrot.redis.Commons.keySize;
import static org.bigbase.carrot.redis.Commons.keySizeWithPrefix;
import static org.bigbase.carrot.redis.Commons.nextKVisInType;

import java.io.IOException;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;


/**
 * This read-modify-write mutation is executed atomically and isolated
 * It deletes field from a given Hash, defined by a Key
 * @author Vladimir Rodionov
 *
 */
public class HashDelete extends Operation{

  /**
   * Parent big sorted map
   */
  BigSortedMap map;
    
  /**
   * Constructor
   */
  public HashDelete() {
    setFloorKey(true);
  }
  
  @Override
  public void reset() {
    super.reset();
    setFloorKey(true);
    map = null;
  }
  
  /**
   * Sets parent sorted map
   * @param map sorted map
   */
  public void setMap(BigSortedMap map) {
    this.map = map;
  }
  
  @Override
  public boolean execute() {
    if (foundRecordAddress <=0) {
      return false;
    }
    // check prefix
    int setKeySize = keySizeWithPrefix(keyAddress);
    int foundKeySize = DataBlock.keyLength(foundRecordAddress);
    if (foundKeySize <= setKeySize) {
      // Hash does not exists
      return false;
    }
    long foundKeyAddress = DataBlock.keyAddress(foundRecordAddress);
    // Prefix keys must be equals
    if (Utils.compareTo(keyAddress, setKeySize , foundKeyAddress, 
      setKeySize) != 0) {
      // Hash does not exists
      return false;
    }
    
    long fieldPtr = elementAddressFromKey(keyAddress);
    int fieldSize = elementSizeFromKey(keyAddress, keySize);
    // First two bytes are number of elements in a value
    long addr = Hashes.exactSearch(foundRecordAddress, fieldPtr, fieldSize);
    if (addr < 0) {
      // Field not found
      return false;
    }
    // found
    int fieldSizeSize = Utils.sizeUVInt(fieldSize);
    int fieldValueSize = Utils.readUVInt(addr + fieldSizeSize);
    int fieldValueSizeSize = Utils.sizeUVInt(fieldValueSize);
    int toCut = fieldSizeSize + fieldSize + fieldValueSizeSize + fieldValueSize;
    long valueAddress = DataBlock.valueAddress(foundRecordAddress);
    // decrement number of elements in this value
    int numElements = addNumElements(valueAddress, -1);
    int valueSize = DataBlock.valueLength(foundRecordAddress);
    int newValueSize = valueSize - toCut;
    Hashes.checkValueArena(newValueSize);
    long ptr = Hashes.valueArena.get();
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
    if (!firstKVinType(foundKeyAddress, foundKeySize)) {
      return true;
    }
    // this first KV in set, we can delete it if it is the only one in the set
    return !nextKVisInType(map, foundKeyAddress);
  }


}
