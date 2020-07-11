package org.bigbase.carrot.redis.sets;

import static org.bigbase.carrot.redis.Commons.KEY_SIZE;
import static org.bigbase.carrot.redis.Commons.NUM_ELEM_SIZE;
import static org.bigbase.carrot.redis.Commons.ZERO;

import static org.bigbase.carrot.redis.Commons.addNumElements;
import static org.bigbase.carrot.redis.Commons.canSplit;
import static org.bigbase.carrot.redis.Commons.elementAddressFromKey;
import static org.bigbase.carrot.redis.Commons.elementSizeFromKey;
import static org.bigbase.carrot.redis.Commons.keySize;
import static org.bigbase.carrot.redis.Commons.keySizeWithPrefix;
import static org.bigbase.carrot.redis.Commons.numElementsInValue;
import static org.bigbase.carrot.redis.Commons.setNumElements;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.redis.DataType;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;




/**
 * This read-modify-write mutation is executed atomically and isolated
 * It adds new element to a given set, defined by a Key
 * @author Vladimir Rodionov
 *
 */
public class SetAdd extends Operation{
  
 
  /*
   * Thread local key arena storage
   */
  
  private static ThreadLocal<Long> keyArena = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(512);
    }
  };
  /*
   * Size of a key arena
   */
  private static ThreadLocal<Integer> keyArenaSize = new ThreadLocal<Integer>() {
    @Override
    protected Integer initialValue() {
      return 512;
    }
  };
  
  /**
   * Constructor
   */
  public SetAdd() {
    setFloorKey(true);
  }
  
  @Override
  public void reset() {
    super.reset();
    setFloorKey(true);
  }
    
  /**
   * Checks key arena size
   * @param required size
   */
  
  static void checkKeyArena (int required) {
    int size = keyArenaSize.get();
    if (size >= required ) {
      return;
    }
    long ptr = UnsafeAccess.realloc(keyArena.get(), required);
    keyArena.set(ptr);
    keyArenaSize.set(required);
  }
  
  /**
   * Build key for Set. It uses thread local key arena 
   * TODO: data type prefix
   * @param keyPtr original key address
   * @param keySize original key size
   * @param elPtr element address
   * @param elSize element size
   * @return new key size 
   */
    
   
  private static int buildKey( long keyPtr, int keySize, long elPtr, int elSize) {
    checkKeyArena(keySize + KEY_SIZE + elSize + Utils.SIZEOF_BYTE);
    long arena = keyArena.get();
    int kSize = KEY_SIZE + keySize + Utils.SIZEOF_BYTE;
    UnsafeAccess.putByte(arena, (byte)DataType.SET.ordinal());
    UnsafeAccess.putInt(arena + Utils.SIZEOF_BYTE, keySize);
    UnsafeAccess.copy(keyPtr, arena + KEY_SIZE + Utils.SIZEOF_BYTE, keySize);
    if (elPtr > 0) {
      UnsafeAccess.copy(elPtr, arena + kSize, elSize);
      kSize += elSize;
    }
    return kSize;
  }
  
  @Override
  public boolean execute() {
    if (foundRecordAddress <=0) {
      return false;
    }
    // check prefix
    int setKeySize = keySizeWithPrefix(keyAddress);
    int foundKeySize = DataBlock.keyLength(foundRecordAddress);
    long foundKeyAddress = DataBlock.keyAddress(foundRecordAddress);
    // Prefix keys must be equals if set exists, otherwise insert new set KV
    if ((foundKeySize <= setKeySize) || 
        Utils.compareTo(keyAddress, setKeySize , foundKeyAddress, 
      setKeySize) != 0) {
      // Set does not exist yet
      // Insert new set KV
      insertNewKVandElement(ZERO, 1);
      return true;
    }
    // Set exists
    long elementPtr = elementAddressFromKey(keyAddress);
    int elementSize = elementSizeFromKey(keyAddress, keySize);
    // First two bytes are number of elements in a value
    long addr = Sets.insertSearch(foundRecordAddress, elementPtr, elementSize);
    // check if the same element
    if (Sets.compareElements(addr, elementPtr, elementSize) == 0) {
      // Can not insert, because it is already there
      return false;
    }
    // found
    int elemSizeSize = Utils.sizeUVInt(elementSize);
    int toAdd = elemSizeSize + elementSize;
    long valueAddress = DataBlock.valueAddress(foundRecordAddress);

    int valueSize = DataBlock.valueLength(foundRecordAddress);
    int newValueSize = valueSize + toAdd;
    
    boolean needSplit = DataBlock.mustStoreExternally(foundKeySize, newValueSize);

    if (!needSplit) {

      Sets.checkValueArena(newValueSize);
      insertElement(valueAddress, valueSize, addr, elementPtr, elementSize); 
    
      // set # of updates to 1
      this.updatesCount = 1;
      this.keys[0] = foundKeyAddress; // use the key we found
      this.keySizes[0] = foundKeySize; // use the key we found
      this.values[0] = Sets.valueArena.get();
      this.valueSizes[0] = newValueSize;
      return true;
    } else if (!canSplit(valueAddress)){
      // We can't split existing KV , so insert new one
      insertNewKVandElement(elementPtr, elementSize);
      return true;
    } else {
      // Do split
      Sets.checkValueArena(newValueSize + NUM_ELEM_SIZE);
      insertElement(valueAddress, valueSize, addr, elementPtr, elementSize);
      // calculate new key size
      // This is value address for update #1
      long vPtr = Sets.valueArena.get();
      int kSize = keySize(keyAddress);
      // This is value address for update #2
      long splitPos = Sets.splitAddress(vPtr, newValueSize);
      int eSize = Sets.getElementSize(splitPos);
      long ePtr = Sets.getElementAddress(splitPos);
      // This is key size for update #2
      int totalKeySize = buildKey(keyAddress + KEY_SIZE + Utils.SIZEOF_BYTE, kSize, ePtr, eSize);
      // This is key address for update #2
      long kPtr = keyArena.get();
      int totalElNum = numElementsInValue(vPtr);
      int leftSplitElNum = Sets.splitNumber(vPtr, newValueSize);
      int rightSplitElNum = totalElNum - leftSplitElNum;
      // Prepare value for split
      UnsafeAccess.copy(splitPos, splitPos + NUM_ELEM_SIZE, (vPtr + newValueSize - splitPos));
      
      setNumElements(vPtr, leftSplitElNum);
      setNumElements(splitPos, rightSplitElNum);
      // This is value size update #1
      int leftValueSize = (int)(splitPos - vPtr);
      // This is value size update #2
      int rightValueSize = (int)((vPtr + newValueSize - splitPos) + NUM_ELEM_SIZE);
      
      // Prepare updates
      this.updatesCount = 2;
      this.keys[0] = keyAddress;
      this.keySizes[0] = keySize;
      this.values[0] = vPtr;
      this.valueSizes[0] = leftValueSize;
      
      this.keys[1] = kPtr;
      this.keySizes[1] = totalKeySize;
      this.values[1] = splitPos;
      this.valueSizes[1] = rightValueSize;
      return true;
    }
  }
  
  /**
   * Insert new Key-Value with a given element
   * @param elKeyPtr element address
   * @param elKeySize element size
   */
  private void insertNewKVandElement(long ePtr, int eSize) {
    // Get real keySize
    int kSize = keySize(keyAddress);
    checkKeyArena(kSize + KEY_SIZE + Utils.SIZEOF_BYTE+ eSize); 
    // Build first key for the new set
    int totalKeySize = buildKey(keyAddress + KEY_SIZE + Utils.SIZEOF_BYTE, kSize, ePtr, eSize);
    long kPtr = keyArena.get();
    int eSizeSize = Utils.sizeUVInt(eSize);
    Sets.checkValueArena(eSize + eSizeSize + NUM_ELEM_SIZE);
    long vPtr = Sets.valueArena.get();
    setNumElements(vPtr, 1);
    // Write element length
    Utils.writeUVInt(vPtr + NUM_ELEM_SIZE, eSize);
    // Copy element
    UnsafeAccess.copy(ePtr, vPtr + NUM_ELEM_SIZE + eSizeSize, eSize);
    
    // set number of updates to 1
    this.updatesCount = 1;
    keys[0] = kPtr;
    keySizes[0] = totalKeySize;
    values[0] = vPtr;
    valueSizes[0] = eSize + eSizeSize + NUM_ELEM_SIZE;
  }
  /**
   * Insert element(elementPtr, elementSize) into value (valueAddress, valueSize)
   * insertion point is addr. Sets.valueArena is used
   * @param valueAddress value address
   * @param valueSize current value size
   * @param addr insertion point
   * @param elementPtr element pointer
   * @param elementSize element size
   */
  private void insertElement(long valueAddress, int valueSize, long addr, long elementPtr, int elementSize) {
    // increment number of elements in this value
    addNumElements(valueAddress, 1);
    long ptr = Sets.valueArena.get();
    // Copy everything before addr
    UnsafeAccess.copy(valueAddress, ptr, addr - valueAddress);
    // Encode element size
    ptr += addr - valueAddress;
    int elemSizeSize = Utils.writeUVInt(ptr, elementSize);
    ptr += elemSizeSize;
    // copy element
    UnsafeAccess.copy(elementPtr, ptr, elementSize);
    ptr += elementSize;
    // copy rest elements
    int toAdd = elemSizeSize + elementSize;

    UnsafeAccess.copy(addr + toAdd, ptr, valueSize - (addr - valueAddress));
  }

}
