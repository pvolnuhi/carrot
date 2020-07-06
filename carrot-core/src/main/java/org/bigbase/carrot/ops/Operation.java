package org.bigbase.carrot.ops;


/**
 * This class encapsulate read-modify-write 
 * transactional access pattern. Subclasses must provide
 * key (address, size), version and implement execute()
 * operation. The result can be one or two PUTs, which will be 
 * executed in the context of this atomic operation or one Delete and PUT
 * or no updates at all
 * @author Vladimir Rodionov
 *
 */
public abstract class Operation {

  /*
   * operation sequence number
   */
  protected long version;
  /*
   * Key address associated with update
   */
  protected long keyAddress;
  
  /*
   * Expiration
   */
  protected long expire;
  
  /*
   * Key size
   */
  protected int keySize;
  /*
   * Found K-V record address
   */
  protected long foundRecordAddress;

  /* 
   * These are result of update operation
   * There can be 1 or 2 K_V pairs to insert
   * Result key addresses
   */
  protected long[] keys = new long[2];
  /*
   * Result key sizes 
   */
  protected int[]  keySizes = new int[2];
  /*
   * Result value addresses
   */
  protected long[] values = new long[2];
  /*
   * Result value sizes
   */
  protected int[] valueSizes = new int[2];
  /*
   * Update types for updates: false - PUT, true - Delete
   */
  protected boolean[] updateTypes = new boolean[] {false, false};
  /*
   * Number of results (0, 1,  2 updates/puts/ deletes) 
   */
  protected int updatesCount; // 1 or 2
  
  protected boolean floorKey = false; // if true, look for largest key which less or equals
  
  public Operation() {
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
    this.updateTypes[0] = false;
    this.updateTypes[1] = false;
    this.floorKey = false;
  }
  
  public final void setFloorKey(boolean b) {
    this.floorKey = b;
  }
  
  public final boolean isFloorKey() {
    return floorKey;
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
  public abstract boolean execute() ;
  
 
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
  /**
   * Get update types for update operations
   * @return
   */
  public final boolean[] updateTypes() {
    return updateTypes;
  }
}
