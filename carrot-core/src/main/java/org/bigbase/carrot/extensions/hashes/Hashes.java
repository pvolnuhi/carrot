package org.bigbase.carrot.extensions.hashes;

import static org.bigbase.carrot.extensions.Commons.KEY_SIZE;
import static org.bigbase.carrot.extensions.Commons.NUM_ELEM_SIZE;
import static org.bigbase.carrot.extensions.Commons.numElementsInValue;

import java.io.IOException;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.BigSortedMapDirectMemoryScanner;
import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.extensions.IncrementType;
import org.bigbase.carrot.extensions.OperationFailedException;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Support for packing multiple field-values into one K-V value
 * under the same key. This is for compact representation of naturally ordered
 * HASHEs. key -> field -> value under common key
 * @author Vladimir Rodionov
 *
 */
public class Hashes {
  

  private static ThreadLocal<Long> keyArena = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(512);
    }
  };
  
  private static ThreadLocal<Integer> keyArenaSize = new ThreadLocal<Integer>() {
    @Override
    protected Integer initialValue() {
      return 512;
    }
  };
  
  static ThreadLocal<Long> valueArena = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(512);
    }
  };
  
  static ThreadLocal<Integer> valueArenaSize = new ThreadLocal<Integer>() {
    @Override
    protected Integer initialValue() {
      return 512;
    }
  };
  
  /**
   * Thread local updates Hash Exists
   */
  private static ThreadLocal<HashExists> hashExists = new ThreadLocal<HashExists>() {
    @Override
    protected HashExists initialValue() {
      return new HashExists();
    } 
  };
  
  /**
   * Thread local updates Hash Set
   */
  private static ThreadLocal<HashSet> hashSet = new ThreadLocal<HashSet>() {
    @Override
    protected HashSet initialValue() {
      return new HashSet();
    } 
  };
  
  /**
   * Thread local updates Hash Delete
   */
  private static ThreadLocal<HashDelete> hashDelete = new ThreadLocal<HashDelete>() {
    @Override
    protected HashDelete initialValue() {
      return new HashDelete();
    } 
  };
  
  /**
   * Thread local updates Hash Get
   */
  private static ThreadLocal<HashGet> hashGet = new ThreadLocal<HashGet>() {
    @Override
    protected HashGet initialValue() {
      return new HashGet();
    } 
  };
  
  /**
   * Thread local updates Hash Increment
   */
  private static ThreadLocal<HashIncrement> hashIncrement = new ThreadLocal<HashIncrement>() {
    @Override
    protected HashIncrement initialValue() {
      return new HashIncrement();
    } 
  };
  
  /**
   * Thread local updates Hash Value Length
   */
  private static ThreadLocal<HashValueLength> hashValueLength = new ThreadLocal<HashValueLength>() {
    @Override
    protected HashValueLength initialValue() {
      return new HashValueLength();
    } 
  };
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
   * Checks value arena size
   * @param required size
   */
  static void checkValueArena (int required) {
    int size = valueArenaSize.get();
    if (size >= required) {
      return;
    }
    long ptr = UnsafeAccess.realloc(valueArena.get(), required);
    valueArena.set(ptr);
    valueArenaSize.set(required);
  }
  
  /**
   * Build key for Hash. It uses thread local key arena 
   * TODO: data type prefix
   * @param keyPtr original key address
   * @param keySize original key size
   * @param fieldPtr field address
   * @param fieldSize field size
   * @return new key size 
   */
    
   
  private static int buildKey( long keyPtr, int keySize, long fieldPtr, int fieldSize) {
    checkKeyArena(keySize + KEY_SIZE + fieldSize);
    long arena = keyArena.get();
    int kSize = KEY_SIZE + keySize;
    UnsafeAccess.putInt(arena, keySize);
    UnsafeAccess.copy(keyPtr, arena + KEY_SIZE, keySize);
    if (fieldPtr > 0) {
      UnsafeAccess.copy(fieldPtr, arena + kSize, fieldSize);
      kSize += fieldSize;
    }
    return kSize;
  }
  /**
   * Add field-value to a set defined by key
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @return true if success, false is map is full
   */
  public static boolean setFieldValue(BigSortedMap map, long keyPtr, int keySize, 
      long fieldPtr, int fieldSize, long valuePtr, int valueSize) {
    
    int kSize = buildKey(keyPtr, keySize, fieldPtr, fieldSize);
    HashSet set = hashSet.get();
    set.reset();
    set.setKeyAddress(keyArena.get());
    set.setKeySize(kSize);
    set.setFieldValue(valuePtr, valueSize);
    set.setIfNotExists(false);
    // version?    
    return map.update(set);
  }
  
  /**
   * Add field-value to a set defined by key if not exists
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @return true if success, false is map is full
   */
  public static boolean setFieldValueIfNotExists(BigSortedMap map, long keyPtr, int keySize, 
      long fieldPtr, int fieldSize, long valuePtr, int valueSize) {
    
    int kSize = buildKey(keyPtr, keySize, fieldPtr, fieldSize);
    HashSet set = hashSet.get();
    set.reset();
    set.setKeyAddress(keyArena.get());
    set.setKeySize(kSize);
    set.setFieldValue(valuePtr, valueSize);
    set.setIfNotExists(true);
    // version?    
    return map.update(set);
  }
  
  /**
   * Returns total number of elements in this set
   * @param map
   * @param keyPtr
   * @param keySize
   * @return number of elements(fields)
   */
  public static long getNumberOfFieldsInHash(BigSortedMap map, long keyPtr, int keySize) {
    int kSize = buildKey(keyPtr, keySize, 0, 0);
    long ptr = keyArena.get();
    BigSortedMapDirectMemoryScanner scanner = map.getPrefixScanner(ptr, kSize);
    if (scanner == null) {
      return 0; // empty or does not exists
    }
    long total = 0;
    try {
      while (scanner.hasNext()) {
        long valuePtr = scanner.valueAddress();
        total += numElementsInValue(valuePtr);
      }
      scanner.close();
    } catch (IOException e) {
      // should never be thrown
    }
    return total;
  }
  
  /**
   * Return serialized hash size
   * @param map ordered map
   * @param keyPtr key address
   * @param keySize key size
   * @return hash size in bytes
   */
  public static long getHashSizeInBytes(BigSortedMap map, long keyPtr, int keySize) {
    int kSize = buildKey(keyPtr, keySize, 0, 0);
    long ptr = keyArena.get();
    BigSortedMapDirectMemoryScanner scanner = map.getPrefixScanner(ptr, kSize);
    if (scanner == null) {
      return 0; // empty or does not exists
    }
    long total = 0;
    try {
      while (scanner.hasNext()) {
        long valueSize = scanner.valueSize();
        total += valueSize - NUM_ELEM_SIZE;
      }
      scanner.close();
    } catch (IOException e) {
      // should never be thrown
    }
    return total;
  }
  
  /**
   * Deletes element of a set
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @return true or false
   */
  
  public static boolean deleteField(BigSortedMap map, long keyPtr, int keySize, long fieldPtr, int fieldSize) {
    int kSize = buildKey(keyPtr, keySize, fieldPtr, fieldSize);
    HashDelete update = hashDelete.get();
    update.reset();
    update.setMap(map);
    update.setKeyAddress(keyArena.get());
    update.setKeySize(kSize);
    // version?    
    return map.update(update);
  }
  /**
   * Determine if hash filed exists
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @param fieldPtr field address
   * @param fieldSize field size
   * @return
   */
  public static boolean exists(BigSortedMap map, long keyPtr, int keySize, long fieldPtr, int fieldSize) {
    int kSize = buildKey(keyPtr, keySize, fieldPtr, fieldSize);
    HashExists update = hashExists.get();
    update.reset();
    update.setKeyAddress(keyArena.get());
    update.setKeySize(kSize);
    // version?    
    return map.update(update);  
  }
  
  /**
   * Get field's value into provided buffer
   * @param map ordered map
   * @param keyPtr hash key address
   * @param keySize hash key size
   * @param fieldPtr field to lookup address
   * @param fieldSize field size
   * @param valueBuf value buffer
   * @param valueBufSize value buffer size
   * @return size of value, one should check this and if it is greater than valueBufSize
   *         means that call should be repeated with an appropriately sized value buffer
   */
  public static int getFieldValue (BigSortedMap map, long keyPtr, int keySize, long fieldPtr, int fieldSize,
      long valueBuf, int valueBufSize) {
    int kSize = buildKey(keyPtr, keySize, fieldPtr, fieldSize);
    HashGet get = hashGet.get();
    get.reset();
    get.setKeyAddress(keyArena.get());
    get.setKeySize(kSize);
    get.setBufferPtr(valueBuf);
    get.setBufferSize(valueBufSize);
    // version?    
    map.update(get);
    return get.getFoundValueSize();
  }
  
  /**
   * Integer counter increment, the size of value is expected exactly 4 bytes
   * @param map ordered map
   * @param keyPtr hash key address
   * @param keySize hash key size
   * @param fieldPtr field to lookup address
   * @param fieldSize field size
   * @param value increment
   * @return new counter value
   */
  public static int increment(BigSortedMap map, long keyPtr, int keySize, long fieldPtr, int fieldSize, int value)
    throws OperationFailedException
  {
    int kSize = buildKey(keyPtr, keySize, fieldPtr, fieldSize);
    HashIncrement incr = hashIncrement.get();
    incr.reset();
    incr.setKeyAddress(keyArena.get());
    incr.setKeySize(kSize);
    incr.setIncrementType(IncrementType.INTEGER);
    incr.setIntValue(value);
    // version?    
    boolean result = map.update(incr);
    if (result == false) {
      throw new OperationFailedException();
    }
    return incr.getIntPostIncrement();
  }
  
  /**
   * Long counter increment, the size of value is expected exactly 8 bytes
   * @param map ordered map
   * @param keyPtr hash key address
   * @param keySize hash key size
   * @param fieldPtr field to lookup address
   * @param fieldSize field size
   * @param value increment
   * @return new counter value
   * @throws OperationFailedException 
   */
  public static long increment(BigSortedMap map, long keyPtr, int keySize, long fieldPtr, int fieldSize, long value)
      throws OperationFailedException
  {
    int kSize = buildKey(keyPtr, keySize, fieldPtr, fieldSize);
    HashIncrement incr = hashIncrement.get();
    incr.reset();
    incr.setKeyAddress(keyArena.get());
    incr.setKeySize(kSize);
    incr.setIncrementType(IncrementType.LONG);
    incr.setLongValue(value);
    // version?    
    boolean result = map.update(incr);
    if (result == false) {
      throw new OperationFailedException();
    }
    return incr.getLongPostIncrement(); 
   }
  
  /**
   * Float counter increment, the size of value is expected exactly 4 bytes
   * @param map ordered map
   * @param keyPtr hash key address
   * @param keySize hash key size
   * @param fieldPtr field to lookup address
   * @param fieldSize field size
   * @param value increment
   * @return new counter value
   * @throws OperationFailedException 
   */
  public static float increment(BigSortedMap map, long keyPtr, int keySize, long fieldPtr, int fieldSize, float value) throws OperationFailedException
  {
    int kSize = buildKey(keyPtr, keySize, fieldPtr, fieldSize);
    HashIncrement incr = hashIncrement.get();
    incr.reset();
    incr.setKeyAddress(keyArena.get());
    incr.setKeySize(kSize);
    incr.setIncrementType(IncrementType.FLOAT);
    incr.setFloatValue(value);
    // version?    
    boolean result = map.update(incr);
    if (result == false) {
      throw new OperationFailedException();
    }
    return incr.getFloatPostIncrement(); 
  }
  
  /**
   * Double counter increment, the size of value is expected exactly 8 bytes
   * @param map ordered map
   * @param keyPtr hash key address
   * @param keySize hash key size
   * @param fieldPtr field to lookup address
   * @param fieldSize field size
   * @param value increment
   * @return new counter value
   * @throws OperationFailedException 
   */
  public static double increment(BigSortedMap map, long keyPtr, int keySize, long fieldPtr, int fieldSize, double value) throws OperationFailedException
  {
    int kSize = buildKey(keyPtr, keySize, fieldPtr, fieldSize);
    HashIncrement incr = hashIncrement.get();
    incr.reset();
    incr.setKeyAddress(keyArena.get());
    incr.setKeySize(kSize);
    incr.setIncrementType(IncrementType.DOUBLE);
    incr.setDoubleValue(value);
    // version?    
    boolean result = map.update(incr);
    if (result == false) {
      throw new OperationFailedException();
    }
    return incr.getDoublePostIncrement();
  }
  /**
   * Get the length of the value of a hash field
   * @param map ordered map
   * @param keyPtr key address
   * @param keySize key size
   * @param fieldPtr field address
   * @param fieldSize field size
   * @return length of value
   */
  public static int getValueLength(BigSortedMap map, long keyPtr, int keySize, long fieldPtr, int fieldSize) {
    int kSize = buildKey(keyPtr, keySize, fieldPtr, fieldSize);
    HashValueLength get = hashValueLength.get();
    get.reset();
    get.setKeyAddress(keyArena.get());
    get.setKeySize(kSize);
    // version?    
    map.update(get);
    return get.getFoundValueSize();
  }
  /**
   * TODO: pattern matching
   * Read next 'count' field-value pairs
   * @param map ordered map
   * @param keyAddress hash key address
   * @param keySize hash key size
   * @param lastFieldSeenPtr last seen field address, if 0 - start from beginning
   * @param lastFieldSeenSize last seen field size
   * @param count total pair to read
   * @return total size of read data, if it is larger than bufferSize, the call
   *          must be repeated with appropriately sized buffer
   */
  public static long hashScan(BigSortedMap map, long keyAddress, int keySize, long lastFieldSeenPtr, 
      int lastFieldSeenSize, int count, long buffer, long bufferSize) {
    int startKeySize = buildKey(keyAddress, keySize, lastFieldSeenPtr, lastFieldSeenSize);
    long startKeyPtr = keyArena.get();
    long stopKeyPtr = Utils.prefixKeyEnd(startKeyPtr, keySize + KEY_SIZE);
    if (stopKeyPtr == -1) {
      return -1;
    }
    BigSortedMapDirectMemoryScanner scanner = map.getScanner(startKeyPtr, startKeySize, stopKeyPtr, keySize + KEY_SIZE);
    // TODO - test this call
    scanner.previous(startKeyPtr,  startKeySize);
    HashScanner hs = new HashScanner(scanner, 0);
    //TODO - test this call
    hs.seek(lastFieldSeenPtr, lastFieldSeenSize, true);
    int cc = 0;
    
    try {
      while(cc++ < count && hs.hasNext()) {
        //TODO
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return 0;
  }
  
  /**
   * Get hash scanner for hash operations, as since we can create multiple
   * hash scanners in the same thread we can not use thread local variables
   * WARNING: we can not create multiple scanners in a single thread
   * @param map sorted map to run on
   * @param keyPtr key address
   * @param keySize key size
   * @param safe get safe instance
   * @return hash scanner
   */
  public static HashScanner getHashScanner(BigSortedMap map, long keyPtr, int keySize, boolean safe) {
    long kPtr = UnsafeAccess.malloc(keySize + KEY_SIZE);
    UnsafeAccess.putInt(kPtr, keySize);
    UnsafeAccess.copy(keyPtr, kPtr + KEY_SIZE, keySize);
    BigSortedMapDirectMemoryScanner scanner = safe? 
        map.getSafePrefixScanner(kPtr, keySize + KEY_SIZE): map.getPrefixScanner(kPtr, keySize + KEY_SIZE);
    if (scanner == null) {
      return null;
    }
    HashScanner hs = new HashScanner(scanner, kPtr);
    return hs;
  }
  
  /**
   * Finds location of a given field in a Value object
   * @param foundRecordAddress address of K-V record
   * @param fieldPtr field's address
   * @param fieldSize field's size
   * @return address of field-value in a Value or -1, if not found
   */
  public static long exactSearch(long foundRecordAddress, long fieldPtr, int fieldSize) {
    long valuePtr = DataBlock.valueAddress(foundRecordAddress);
    int valueSize  = DataBlock.valueLength(foundRecordAddress);
    int off = NUM_ELEM_SIZE; // skip number of elements in value
    while(off < valueSize) {
      int fSize = Utils.readUVInt(valuePtr + off);
      int skip = Utils.sizeUVInt(fSize);
      int vSize = Utils.readUVInt(valuePtr + off + skip);
      skip+= Utils.sizeUVInt(vSize);
      if (Utils.compareTo(fieldPtr, fieldSize, valuePtr + off + skip, fSize) == 0) {
        return valuePtr + off;
      }
      off+= skip + fSize + vSize;
    }
    return -1; // NOT_FOUND
  }
  
  /**
   * Finds first field which is greater or equals to a given one
   * in a Value object
   * @param foundRecordAddress address of a K-V record
   * @param fieldPtr field address
   * @param field field size
   * @return address to insert to insert to
   */
  public static long insertSearch(long foundRecordAddress, long fieldPtr, int fieldSize) {
    long valuePtr = DataBlock.valueAddress(foundRecordAddress);
    int valueSize  = DataBlock.valueLength(foundRecordAddress);
    int off = NUM_ELEM_SIZE; // skip number of elements
    while(off < valueSize) {
      int fSize = Utils.readUVInt(valuePtr + off);
      int skip = Utils.sizeUVInt(fSize);
      int vSize = Utils.readUVInt(valuePtr + off + skip);
      skip+= Utils.sizeUVInt(vSize);
      if (Utils.compareTo(fieldPtr, fieldSize, valuePtr + off + skip, fSize) <= 0) {
        return valuePtr + off;
      }
      off+= skip + fSize + vSize;
    }
    return valuePtr + valueSize; // put in the end largest one
  }
  
  /**
   * Gets value size from the address of field-value pair
   * @param fieldValuePtr address
   * @return size of a value
   */
  public static int getValueSize(long fieldValuePtr) {
    int fSize = Utils.readUVInt(fieldValuePtr );
    int skip = Utils.sizeUVInt(fSize);
    int vSize = Utils.readUVInt(fieldValuePtr  + skip);
    return vSize;
  }
  
  /**
   * Gets field size from the address of field-value pair
   * @param fieldValuePtr address
   * @return size of a value
   */
  public static int getFieldSize(long fieldValuePtr) {
    int fSize = Utils.readUVInt(fieldValuePtr );
    return fSize;
  }
  /**
   * Gets value address from the address of field-value pair
   * @param fieldValuePtr address
   * @return address of a value
   */
  public static long getValueAddress(long fieldValuePtr) {
    int fSize = Utils.readUVInt(fieldValuePtr );
    int skip = Utils.sizeUVInt(fSize);
    int vSize = Utils.readUVInt(fieldValuePtr  + skip);
    skip+= Utils.sizeUVInt(vSize);
    
    return fieldValuePtr + fSize + skip;
  }
  
  /**
   * Gets field address from the address of field-value pair
   * @param fieldValuePtr address
   * @return address of a value
   */
  public static long getFieldAddress(long fieldValuePtr) {
    int fSize = Utils.readUVInt(fieldValuePtr );
    int skip = Utils.sizeUVInt(fSize);
    int vSize = Utils.readUVInt(fieldValuePtr  + skip);
    skip+= Utils.sizeUVInt(vSize);
    
    return fieldValuePtr + skip;
  }
  /**
   * Returns suggested split address
   * @param valuePtr value address
   * @return address of a split point
   */
  public static long splitAddress(final long valuePtr, final int valueSize) {
    // First try equaling sizes of splits
    long off = NUM_ELEM_SIZE;
    long prevOff = NUM_ELEM_SIZE;
    while(off < valueSize/2) {
      int fSize = Utils.readUVInt(valuePtr + off);
      int fSizeSize = Utils.sizeUVInt(fSize);
      int vSize = Utils.readUVInt(valuePtr + off + fSizeSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      prevOff = off;
      off+= fSizeSize + fSize + vSize + vSizeSize;
    }
    if (prevOff - NUM_ELEM_SIZE > valueSize - off) {
      return prevOff;
    } else {
      return off;
    }
  }
  
  /**
   * Returns suggested number of elements in a left split
   * @param valuePtr value address
   * @return address of a split point
   */
  public static int splitNumber(final long valuePtr, final int valueSize) {
    // First try equaling sizes of splits
    long off = NUM_ELEM_SIZE;
    long prevOff = NUM_ELEM_SIZE;
    int n = 0;
    while(off < valueSize/2) {
      n++;
      int fSize = Utils.readUVInt(valuePtr + off);
      int fSizeSize = Utils.sizeUVInt(fSize);
      int vSize = Utils.readUVInt(valuePtr + off + fSizeSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      prevOff = off;
      off+= fSizeSize + fSize + vSize + vSizeSize;
    }
    if (prevOff - NUM_ELEM_SIZE > valueSize - off) {
      return n-1;
    } else {
      return n;
    }
  }
  
  /**
   * Compare field, which starts in a given address 
   * with a given field
   * @param ptr address of an first field-value record
   * @param fieldPtr address of a second element
   * @param fieldSize second element size
   * @return o - if equals, -1, +1
   */
  public static int compareFields (long ptr, long fieldPtr, int fieldSize) {
    int fSize = Utils.readUVInt(ptr);
    int fSizeSize = Utils.sizeUVInt(fSize);
    return Utils.compareTo(ptr + fSizeSize, fSize, fieldPtr, fieldSize); 
  }
}