package org.bigbase.carrot.updates;

/**
 * This class encapsulate read-modify-write 
 * transactional access pattern. Subclasses must provide
 * key (address, size), version and implement execute()
 * operation. The result can be one or two PUTs, which will be 
 * executed in the context of this atomic operation
 * @author vrodionov
 *
 */
public abstract class Update {

  /*
   * operation sequence number
   */
  long version;
  /*
   * Key address associated with update
   */
  long keyAddress;
  
  /*
   * Expiration
   */
  long expire;
  
  /*
   * Key size
   */
  int keySize;
  /*
   * Found K-V record address
   */
  long foundRecordAddress;

  /* 
   * These are result of update operation
   * There can be 1 or 2 K_V pairs to insert
   * Result key addresses
   */
  long[] keys = new long[2];
  /*
   * Result key sizes 
   */
  int[]  keySizes = new int[2];
  /*
   * Result value addresses
   */
  long[] values = new long[2];
  /*
   * Result value sizes
   */
  int[] valueSizes = new int[2];
  
  /*
   * Number of results (1 or 2 updates/puts) 
   */
  int updatesCount; // 1 or 2
  
  public Update() {
  }
  
  public final void setExpire(long expire) {
    this.expire = expire;
  }
  
  public final long getExpire() {
    return expire;
  }
  
  public final void setKeyAddress(long address) {
    this.keyAddress = address;
  }
  
  public final long getKeyAddress() {
    return keyAddress;
  }
  
  public final void setKeySize (int size) {
    this.keySize = size;
  }
  
  public final int getKeySize() {
    return keySize;
  }
  
  public final void setVersion(long version) {
    this.version = version;
  }
  
  public final long getVersion() {
    return version;
  }
  
  public void reset() {
    this.keyAddress = 0;
    this.keySize = 0;
    this.version = 0;
    this.keys[0] = 0;
    this.keys[1] = 0;
    this.keySizes[0] = 0;
    this.keySizes[1] = 0;
    this.values[0] = 0;
    this.values[1] = 0;
    this.valueSizes[0] = 0;
    this.valueSizes[1] = 0;
    this.foundRecordAddress = 0;
  }
  
  /**
   * Set found record address before execution update
   * Must handle NOT_FOUND
   * @param address
   */
  public final void setFoundRecordAddress(long address) {
    this.foundRecordAddress = address;
  }

  /**
   * Execute update operation on a found K-V record
   * @return true, if success, false - to abort
   */
  public abstract boolean execute();
  
 
  /**
   * Gets total update result count (zero, one or two puts)
   * 0 - means update in place was done
   * 1 - update for current key
   * 2 - split of a current Key Value into two consecutive key values
   * @return number of updates required
   */
  public final int getUpdatesCount() {
    return updatesCount;
  }
  
  /**
   * Gets result key addresses
   * @return key addresses
   */
  public final long[] keys() {
    return this.keys;
  }
  
  /**
   * Gets result key sizes
   * @return key sizes
   */
  public final int[] keySizes() {
    return keySizes;
  }
  
  /**
   * Get result value addresses
   * @return value addresses
   */
  public final long[] values() {
    return values;
  }
  
  /**
   * Get result value sizes
   * @return value sizes
   */
  public final int[] valueSizes() {
    return valueSizes;
  }
  
}
