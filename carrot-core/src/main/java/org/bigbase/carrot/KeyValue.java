package org.bigbase.carrot;

import org.bigbase.carrot.util.Utils;

/**
 * Key - Value class
 * @author Vladimir Rodionov
 *
 */
public class KeyValue implements Comparable<KeyValue>{

  /*
   * Key address
   */
  public long keyPtr;
  /*
   * Key size
   */
  public int keySize;
  /*
   * Value address
   */
  public long valuePtr;
  /*
   * Value size 
   */
  public int valueSize;
  
 /**
  * Constructor
  * @param keyPtr key address
  * @param keySize key size
  * @param valuePtr value address
  * @param valueSize value size
  */
  public KeyValue(long keyPtr, int keySize, long valuePtr, int valueSize) {
    this.keyPtr = keyPtr;
    this.keySize = keySize;
    this.valuePtr = valuePtr;
    this.valueSize = valueSize;
  }
  
  @Override
  public int compareTo(KeyValue o) {
    return Utils.compareTo(keyPtr, keySize, o.keyPtr, o.keySize);
  }

  @Override
  public int hashCode() {
    return Utils.murmurHash(keyPtr, keySize, 0);
  }
}
