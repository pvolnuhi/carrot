package org.bigbase.carrot.extensions.hashes;

import org.bigbase.carrot.BigSortedMap;

/**
 * Support for packing multiple ke-values into one K-V value
 * under the same key. This is for compact representation of naturally ordered
 * HASHEs. key -> field -> value under common key
 * @author Vladimir Rodionov
 *
 */
public class Hashes {
  

  /**
   * Add field-value to a set defined by key
   * @param map sorted map
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @return true if success, false is map is full
   */
  public static boolean addFieldValue(BigSortedMap map, long keyPtr, int keySize, 
      long fieldPtr, int filedSize, long valuePtr, int valueSize) {
    return false;
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
  public static boolean addFieldValueIfNotExists(BigSortedMap map, long keyPtr, int keySize, 
      long valuePtr, int valueSize) {
    return false;
  }
  
  /**
   * Returns total number of elements in this set
   * @param map
   * @param keyPtr
   * @param keySize
   * @return number of elements
   */
  public static long getHashSize(BigSortedMap map, long keyPtr, int keySize) {
    return 0;
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
    return false;
  }
  
  
}
