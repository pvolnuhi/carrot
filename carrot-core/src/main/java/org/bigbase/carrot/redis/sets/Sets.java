package org.bigbase.carrot.redis.sets;

import static org.bigbase.carrot.redis.Commons.KEY_SIZE;
import static org.bigbase.carrot.redis.Commons.NUM_ELEM_SIZE;
import static org.bigbase.carrot.redis.Commons.keySize;
import static org.bigbase.carrot.redis.Commons.numElementsInValue;

import java.io.IOException;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.BigSortedMapDirectMemoryScanner;
import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.redis.DataType;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;





/**
 * Support for packing multiple values into one K-V value
 * under the same key. This is for compact representation of naturally ordered SETs
 * for ordered collection of elements:
 * 
 * KEY -> [e1,e2,e3,e4, ..., eN] we store them in a sequence of Key-Value pairs as following:
 * 
 * [KEY_SIZE]KEY0-> Value([e1,e2,e3,...,eX]) [KEY_SIZE]KEYeX+1->Value([eX+1,eX+2,...])
 * [KEY_SIZE] prepends Key to avoid possible key collisions
 * @author Vladimir Rodionov
 *
 */
public class Sets {
  
  
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
   * Thread local updates Set Exists
   */
  private static ThreadLocal<SetExists> setExists = new ThreadLocal<SetExists>() {
    @Override
    protected SetExists initialValue() {
      return new SetExists();
    } 
  };
  
  /**
   * Thread local updates Set Add
   */
  private static ThreadLocal<SetAdd> setAdd = new ThreadLocal<SetAdd>() {
    @Override
    protected SetAdd initialValue() {
      return new SetAdd();
    } 
  };
  
  /**
   * Thread local updates Set Delete
   */
  private static ThreadLocal<SetDelete> setDelete = new ThreadLocal<SetDelete>() {
    @Override
    protected SetDelete initialValue() {
      return new SetDelete();
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
   * Add value to a set defined by key
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @return true if success, false is map is full
   */
  public static boolean add(BigSortedMap map, long keyPtr, int keySize, long elemPtr, int elemSize) {
    int kSize = buildKey(keyPtr, keySize, elemPtr, elemSize);
    SetAdd add = setAdd.get();
    add.reset();
    add.setKeyAddress(keyArena.get());
    add.setKeySize(kSize);
    // version?    
    return map.execute(add);
  }
  
  /**
   * Returns total number of elements in this set, defined by key
   * @param map ordered map
   * @param keyPtr key address
   * @param keySize key size 
   * @return number of elements
   * @throws IOException 
   */
  public static long getNumberOfElementsInSet(BigSortedMap map, long keyPtr, int keySize){
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
   * Returns total size (in bytes) of elements in this set, defined by key
   * This method is good for reading all set elements 
   * @param map ordered map
   * @param keyPtr key address
   * @param keySize key size 
   * @return set size in bytes in serialized format (including variable size prefixes)
   * @throws IOException 
   */
  public static long getSetSizeInBytes(BigSortedMap map, long keyPtr, int keySize){
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

  /**
   * Deletes element of a set
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @return true or false
   */
  
  public static boolean delete(BigSortedMap map, long keyPtr, int keySize, long elemPtr, int elemSize) {
    int kSize = buildKey(keyPtr, keySize, elemPtr, elemSize);
    SetDelete update = setDelete.get();
    update.reset();
    update.setMap(map);
    update.setKeyAddress(keyArena.get());
    update.setKeySize(kSize);
    // version?    
    return map.execute(update);
  }
  
  /**
   * Check if element exists in the set
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @param elemPtr value address
   * @param elemSize value size
   * @return true or false
   */
  public static boolean exists(BigSortedMap map, long keyPtr, int keySize, long elemPtr, int elemSize) {
    int kSize = buildKey(keyPtr, keySize, elemPtr, elemSize);
    SetExists update = setExists.get();
    update.reset();
    update.setKeyAddress(keyArena.get());
    update.setKeySize(kSize);
    // version?    
    return map.execute(update);
  }
  
  /**
   * Get set scanner for set operations, as since we can create multiple
   * set scanners in the same thread we can not use thread local variables
   * WARNING: we can not create multiple scanners in a single thread
   * @param map sorted map to run on
   * @param keyPtr key address
   * @param keySize key size
   * @param safe get safe instance
   * @return set scanner
   */
  public static SetScanner getSetScanner(BigSortedMap map, long keyPtr, int keySize, boolean safe) {
    long kPtr = UnsafeAccess.malloc(keySize + KEY_SIZE + Utils.SIZEOF_BYTE);
    UnsafeAccess.putByte(kPtr, (byte)DataType.SET.ordinal());
    UnsafeAccess.putInt(kPtr + Utils.SIZEOF_BYTE, keySize);
    UnsafeAccess.copy(keyPtr, kPtr + KEY_SIZE + Utils.SIZEOF_BYTE, keySize);
    //TODO do not use thread local in scanners - check it
    BigSortedMapDirectMemoryScanner scanner = safe? 
        map.getSafePrefixScanner(kPtr, keySize + KEY_SIZE + Utils.SIZEOF_BYTE):
          map.getPrefixScanner(kPtr, keySize + KEY_SIZE + Utils.SIZEOF_BYTE);
    if (scanner == null) {
      return null;
    }
    SetScanner sc = new SetScanner(scanner, kPtr);
    return sc;
  }

  
  /**
   * Finds location of a given element in a Value object
   * @param foundRecordAddress address of K-V record
   * @param elementPtr element address
   * @param elementSize element size
   * @return address of element in a Value or -1, if not found
   */
  public static long exactSearch(long foundRecordAddress, long elementPtr, int elementSize) {
    long valuePtr = DataBlock.valueAddress(foundRecordAddress);
    int valueSize  = DataBlock.valueLength(foundRecordAddress);
    int off = NUM_ELEM_SIZE; // skip number of elements in value
    while(off < valueSize) {
      int eSize = Utils.readUVInt(valuePtr + off);
      int skip = Utils.sizeUVInt(eSize);
      if (Utils.compareTo(elementPtr, elementSize, valuePtr + off, eSize) == 0) {
        return valuePtr + off;
      }
      off+= skip + eSize;
    }
    return -1; // NOT_FOUND
  }
  
  /**
   * Finds first element which is greater or equals to a given one
   * in a Value object
   * @param foundRecordAddress address of a K-V record
   * @param elementPtr element address
   * @param elementSize element size
   * @return address to insert to insert to
   */
  public static long insertSearch(long foundRecordAddress, long elementPtr, int elementSize) {
    long valuePtr = DataBlock.valueAddress(foundRecordAddress);
    int valueSize  = DataBlock.valueLength(foundRecordAddress);
    int off = NUM_ELEM_SIZE; // skip number of elements
    while(off < valueSize) {
      int eSize = Utils.readUVInt(valuePtr + off);
      int eSizeSize = Utils.sizeUVInt(eSize);
      if (Utils.compareTo(elementPtr, elementSize, valuePtr + off + eSizeSize, eSize) <= 0) {
        return valuePtr + off;
      }
      off+= eSizeSize + eSize;
    }
    return valuePtr + valueSize; // put in the end largest one
  }
  
  /**
   * Compare elements which starts in a given address 
   * with a given element
   * @param ptr address of an first element
   * @param elemPtr address of a second element
   * @param elemSize second element size
   * @return o - if equals, -1, +1
   */
  public static int compareElements (long ptr, long elemPtr, int elemSize) {
    int eSize = Utils.readUVInt(ptr);
    int eSizeSize = Utils.sizeUVInt(eSize);
    return Utils.compareTo(ptr + eSizeSize, eSize, elemPtr, elemSize); 
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
      int eSize = Utils.readUVInt(valuePtr + off);
      int eSizeSize = Utils.sizeUVInt(eSize);
      prevOff = off;
      off+= eSizeSize + eSize;
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
      int eSize = Utils.readUVInt(valuePtr + off);
      int eSizeSize = Utils.sizeUVInt(eSize);
      prevOff = off;
      off+= eSizeSize + eSize;
    }
    if (prevOff - NUM_ELEM_SIZE > valueSize - off) {
      return n-1;
    } else {
      return n;
    }
  }
  
  /**
   * Gets element size by address
   * @param addr address
   * @return size of element
   */
  public static int getElementSize(long addr) {
    return Utils.readUVInt(addr);
  }
  
  /**
   * Get element address
   * @param addr of size:element tuple
   * @return address of element (skips variable size)
   */
  
  public static long getElementAddress(long addr) {
    int size = Utils.readUVInt(addr);
    return addr + Utils.sizeUVInt(size);
  }
  /**
   * Gets total element size including variable part
   * @param addr address of size:element tuple
   * @return size total
   */
  public static int getTotalElementSize(long addr) {
    int size = Utils.readUVInt(addr);
    return size + Utils.sizeUVInt(size);
  }
  
}
