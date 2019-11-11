package org.bigbase.util;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Allows multiple concurrent clients to lock on a numeric id with ReentrantReadWriteLock.
 * The intended usage for read lock is as follows:
 *
 * <pre>
 * ReentrantReadWriteLock lock = idReadWriteLock.getLock(id);
 * try {
 *   lock.readLock().lock();
 *   // User code.
 * } finally {
 *   lock.readLock().unlock();
 * }
 * </pre>
 *
 * For write lock, use lock.writeLock()
 */
public class IdStrippedReadWriteLock {

  private final static int STRIPES_COUNT_DEFAULT = 8*1024 -1;
  private ReentrantReadWriteLock[] locks; 
  
  public IdStrippedReadWriteLock() {
    this(STRIPES_COUNT_DEFAULT);
  }
  
  public IdStrippedReadWriteLock(int numStripes) {
    locks = new ReentrantReadWriteLock[numStripes];
    for(int i=0; i < numStripes; i++) {
      locks[i] = new ReentrantReadWriteLock();
    }
  }
  
  public ReentrantReadWriteLock getLock(long id) {
    long hash = hash64(id);
    return locks[(int) (hash % locks.length)];
  }
  
  public int getLockIndex(long id) {
    long hash = hash64(id);
    return (int) (hash % locks.length);
  }
  
  public int getLockIndex(long slotId, long totalSlots) {
    int numLock = locks.length;
    return (int)(slotId * numLock / totalSlots) ;
  }
  
  public ReentrantReadWriteLock getLockByIndex(int index) {
    return locks[index];
  }

  /**
   * 64 bit FNV hash. Produces more "random" hashes than (say) String.hashCode().
   * @param val The value to hash.
   * @return The hash value
   */
  private long hash64(long val) {
    // from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
    long FNV_prime_64 = 1099511628211L;
    long hashval = 0xCBF29CE484222325L;
    for (int i = 0; i < 8; i++) {
      long octet = val & 0x00ff;
      val = val >> 8;
      hashval = hashval ^ octet;
      hashval = hashval * FNV_prime_64;
    }
    return Math.abs(hashval);
  }
}
