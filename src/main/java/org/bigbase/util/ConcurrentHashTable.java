package org.bigbase.util;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.openhft.hashing.LongHashFunction;

/**
 * Concurrent version of an open addressing hash table
 * with linear probing
 * We keep max load factor = 0.75, that gives 99% insert success rate
 * with max attempts = 60 (its good for cache). At this load factor (0.75)
 * 1 out of 100 insert attempts will fail. 
 * 90% with max attempts = 20
 * 50% - less than 4
 * Cache miss will require 60 comparisons ( 2K memory scan )
 * We should not exceed 0.75
 *
 */

public class ConcurrentHashTable {
  static long seed;
  
  static {
    Random r = new Random(0);
    seed = r.nextLong();
  }
  
  final IdStrippedReadWriteLock lock ;
  final int keyLength;
  final int valueLength;
  final int elemSize;
  final long allocatedMemory;
  final long maxCapacity;
  final AtomicLong currentCapacity = new AtomicLong(0);
  
  final int maxInsertAttempts = 60;
  
  final int slotSize;
  // We use numSlots to lock hash slots
  //
  // kv(1), kv(2), kv(3) .. kv(maxInsertAttempts) - this is size of a slot
  // 
  final long numSlots;  
  final long address;
  boolean disposed;
  final byte[] zeros;
  boolean debug = false;
  /**
   * Format:
   * 
   * byte + key + value
   * 
   * byte = 0 - slot is vacant
   * byte = 1 - slot is occupied
   * 
   * @param numElements
   * @param keyLength
   * @param valueLength
   */
  public ConcurrentHashTable(long numElements, int keyLength, int valueLength)
  {
    int elemSize = 1 + keyLength + valueLength;
    
    this.allocatedMemory = elemSize * numElements;    
    this.address = UnsafeAccess.theUnsafe.allocateMemory(allocatedMemory);    
    this.keyLength = keyLength;
    this.valueLength = valueLength;
    this.elemSize = keyLength + valueLength + 1;
    this.lock = new IdStrippedReadWriteLock();
    this.maxCapacity = numElements ;
    this.slotSize = elemSize * maxInsertAttempts;
    this.numSlots = allocatedMemory/ slotSize  +1;
    this.zeros = new byte[valueLength];
  }
  
  public void setDebug(boolean debug)
  {
    this.debug = debug;
  }
  
  public boolean isDebug() {
    return this.debug;
  }
  
  private final long hash(byte[] arr, int off) {
    long hash = LongHashFunction.xx(seed).hashBytes(arr, off, keyLength);
    if(hash == Long.MIN_VALUE) {
      hash += 1;
    }
    return Math.abs(hash);
  }
  
  private final long hash(long address) {
    long hash = LongHashFunction.xx(seed).hashMemory(address, keyLength);
    if(hash == Long.MIN_VALUE) {
      hash += 1;
    }
    return Math.abs(hash);
  }
  
  
  /**
   * Put API
   * @param keyArr - key array
   * @param keyOff - key offset
   * @param valueArr - value array
   * @param valueOff - value offset
   * @return true if put was successful, false otherwise
   */
  public boolean put (byte[] keyArr, int keyOff, byte[] valueArr, int valueOff) 
    throws IllegalArgumentException
  {
    sanityCheck(keyArr, keyOff, valueArr, valueOff);
    final long hash = hash(keyArr, keyOff);
    long index = hash % maxCapacity;
    long offset = index * elemSize;
    long slotIndex = offset /slotSize;
    
    ReentrantReadWriteLock lock1 = null, lock2 = null;
    boolean success = false;
    // Get lock on slot
    try {
      
      int lockIndex1 = lock.getLockIndex(slotIndex, numSlots);
      int lockIndex2 = lock.getLockIndex(slotIndex + 1 == numSlots? 0: slotIndex + 1, numSlots);
      if(lockIndex1 < lockIndex2) {
        lock1 = lock.getLockByIndex(lockIndex1);
        lock2 = lock.getLockByIndex(lockIndex2);
      } else {
        lock1 = lock.getLockByIndex(lockIndex2);
        lock2 = lock.getLockByIndex(lockIndex1);
      }
      
      lock1.writeLock().lock();
      lock2.writeLock().lock();
      
      int attempt = 0;
      // Ready to scan
      for(; offset < allocatedMemory; offset+= elemSize, attempt++) {
        if(attempt == maxInsertAttempts) {
          // not found
          return false;
        }        
        if(UnsafeAccess.theUnsafe.getByte(address + offset) == 1) {
          continue;
        }       
        // Copy key to destination
        UnsafeAccess.copy(keyArr, keyOff, address + offset + 1, keyLength);
        // Copy value to destination
        UnsafeAccess.copy(valueArr, valueOff, address + offset + 1 + keyLength, valueLength);
        UnsafeAccess.theUnsafe.putByte(address + offset, (byte)1);
        success = true;
        return true;
      }       
      return false;
    } finally {
      lock1.writeLock().unlock();
      lock2.writeLock().unlock();
      if(success) {
        currentCapacity.incrementAndGet();
      }
    }    
  }
  
  private void sanityCheck(byte[] keyArr, int keyOff, byte[] valueArr, int valueOff) {
    if(keyArr == null || valueArr == null || keyOff < 0 ||
        valueOff < 0 || keyOff >= keyArr.length  || valueOff >= valueArr.length ) {
      throw new IllegalArgumentException();
    }
  }


  /**
   * Put
   * @param keyAddress
   * @param valueAddress
   * @return
   */
  public boolean put(long keyAddress, long valueAddress) {
    final long hash = hash(keyAddress);
    long index = hash % maxCapacity;
    long offset = index * elemSize;
    long slotIndex = offset / slotSize;

    ReentrantReadWriteLock lock1 = null, lock2 = null;
    boolean success = false;
    // Get lock on slot
    try {

      int lockIndex1 = lock.getLockIndex(slotIndex, numSlots);
      int lockIndex2 = lock.getLockIndex(slotIndex + 1 == numSlots ? 0 : slotIndex + 1, numSlots);
      if (lockIndex1 < lockIndex2) {
        lock1 = lock.getLockByIndex(lockIndex1);
        lock2 = lock.getLockByIndex(lockIndex2);
      } else {
        lock1 = lock.getLockByIndex(lockIndex2);
        lock2 = lock.getLockByIndex(lockIndex1);
      }
      // Ordered locking prevents deadlocks
      lock1.writeLock().lock();
      lock2.writeLock().lock();

      int attempt = 0;
      // Ready to scan
      for (; offset < allocatedMemory; offset += elemSize, attempt++) {
        if (attempt == maxInsertAttempts) {
          // not found
          return false;
        }
        if (UnsafeAccess.theUnsafe.getByte(address + offset) == 1) {
          continue;
        }
        // Copy key to destination
        UnsafeAccess.copy(keyAddress, address + offset + 1, keyLength);
        // Copy value to destination
        UnsafeAccess.copy(valueAddress, address + offset + 1 + keyLength, valueLength);
        UnsafeAccess.theUnsafe.putByte(address + offset, (byte)1);
        success = true;
        return true;
      }

      return success;
    } finally {
      lock1.writeLock().unlock();
      lock2.writeLock().unlock();
      if (success) {
        currentCapacity.incrementAndGet();
      }
    }
  }

  /**
   * Get value by key
   * @param keyArr
   * @param off
   * @return value array
   */
  public byte[] get(byte[] arr, int off) 
    throws IllegalArgumentException
  {
    sanityCheck(arr, off);
    final long hash = hash(arr, off);
    long index = hash % maxCapacity;
    long offset = index * elemSize;
    long slotIndex = offset /slotSize;
    
    ReentrantReadWriteLock lock1 = null, lock2 = null;
    // Get lock on slot
    try {
      
      int lockIndex1 = lock.getLockIndex(slotIndex, numSlots);
      int lockIndex2 = lock.getLockIndex(slotIndex + 1 == numSlots? 0: slotIndex + 1, numSlots);
      if(lockIndex1 < lockIndex2) {
        lock1 = lock.getLockByIndex(lockIndex1);
        lock2 = lock.getLockByIndex(lockIndex2);
      } else {
        lock1 = lock.getLockByIndex(lockIndex2);
        lock2 = lock.getLockByIndex(lockIndex1);
      }
      // Ordered locking prevents deadlocks
      lock1.readLock().lock();
      lock2.readLock().lock();
      
      int attempt = 0;
      // Ready to scan
      for(; offset < allocatedMemory; offset+= elemSize, attempt++) {
        if(attempt == maxInsertAttempts) {
          // not found
          return null;
        }        
        if(UnsafeAccess.theUnsafe.getByte(address + offset) == 0) {
          continue;
        }        
        if( Utils.compareTo(arr, off, keyLength, address + offset + 1, keyLength) == 0) {
          
          byte[] value = new byte[valueLength];
          UnsafeAccess.copy(address + offset + 1 + keyLength, value, 0, valueLength);
          return value;
        }
      }     
      return null;
      
    } finally {
      lock1.readLock().unlock();
      lock2.readLock().unlock();
    }
  }
  
  /**
   * Get value by key
   * @param keyArr
   * @param off
   * @return value array
   */
  public boolean get(byte[] arr, int off, byte[] value, int voff) 
    throws IllegalArgumentException
  {
    sanityCheck(arr, off);
    final long hash = hash(arr, off);
    long index = hash % maxCapacity;
    long offset = index * elemSize;
    long slotIndex = offset /slotSize;
    
    ReentrantReadWriteLock lock1 = null, lock2 = null;
    // Get lock on slot
    try {
      
      int lockIndex1 = lock.getLockIndex(slotIndex, numSlots);
      int lockIndex2 = lock.getLockIndex(slotIndex + 1 == numSlots? 0: slotIndex + 1, numSlots);
      if(lockIndex1 < lockIndex2) {
        lock1 = lock.getLockByIndex(lockIndex1);
        lock2 = lock.getLockByIndex(lockIndex2);
      } else {
        lock1 = lock.getLockByIndex(lockIndex2);
        lock2 = lock.getLockByIndex(lockIndex1);
      }
      // Ordered locking prevents deadlocks
      lock1.readLock().lock();
      lock2.readLock().lock();
      
      int attempt = 0;
      // Ready to scan
      for(; offset < allocatedMemory; offset+= elemSize, attempt++) {
        if(attempt == maxInsertAttempts) {
          // not found
          return false;
        }        
        if(UnsafeAccess.theUnsafe.getByte(address + offset) == 0) {
          continue;
        }        
        if( Utils.compareTo(arr, off, keyLength, address + offset + 1, keyLength) == 0) {
          UnsafeAccess.copy(address + offset + 1 + keyLength, value, voff, valueLength);
          return true;
        }
      }     
      return false;
      
    } finally {
      lock1.readLock().unlock();
      lock2.readLock().unlock();
    }
  }
  
  
  /**
   * Get
   * @param keyAddress
   * @return address of value, or -1;
   */
  public byte[] get(long keyAddress) {
    final long hash = hash(keyAddress);
    long index = hash % maxCapacity;
    long offset = index * elemSize;
    long slotIndex = offset /slotSize;
    
    ReentrantReadWriteLock lock1 = null, lock2 = null;
    // Get lock on slot
    try {
      
      int lockIndex1 = lock.getLockIndex(slotIndex, numSlots);
      int lockIndex2 = lock.getLockIndex(slotIndex + 1 == numSlots? 0: slotIndex + 1, numSlots);
      if(lockIndex1 < lockIndex2) {
        lock1 = lock.getLockByIndex(lockIndex1);
        lock2 = lock.getLockByIndex(lockIndex2);
      } else {
        lock1 = lock.getLockByIndex(lockIndex2);
        lock2 = lock.getLockByIndex(lockIndex1);
      }
      
      lock1.readLock().lock();
      lock2.readLock().lock();
      
      int attempt = 0;
      // Ready to scan
      for(; offset < allocatedMemory; offset+= elemSize, attempt++) {
        if(attempt == maxInsertAttempts) {
          // not found
          return null;
        }        
        if(UnsafeAccess.theUnsafe.getByte(address + offset) == 0) {
          continue;
        }        
        if( Utils.compareTo(keyAddress, keyLength, address + offset + 1, keyLength) == 0) {
          byte[] value = new byte[valueLength];
          UnsafeAccess.copy(address + offset + 1 + keyLength, value, 0, valueLength);
          return value;
        }
      }     
      return null;      
    } finally {
      lock1.readLock().unlock();
      lock2.readLock().unlock();
    }    
  }
  
  /**
   * Delete value by key
   * @param keyArr
   * @param off
   * @return true, if deleted
   */
  public boolean delete(byte[] keyArr, int off) 
    throws IllegalArgumentException
  {
    sanityCheck(keyArr, off);
    final long hash = hash(keyArr, off);
    long index = hash % maxCapacity;
    long offset = index * elemSize;
    long slotIndex = offset / slotSize;

    ReentrantReadWriteLock lock1 = null, lock2 = null;
    boolean deleted = false;
    // Get lock on slot
    try {

      int lockIndex1 = lock.getLockIndex(slotIndex, numSlots);
      int lockIndex2 = lock.getLockIndex(slotIndex + 1 == numSlots ? 0 : slotIndex + 1, numSlots);
      // Ordered (by index in lock array) 
      // locking/unlocking prevents deadlocks
      if (lockIndex1 < lockIndex2) {
        lock1 = lock.getLockByIndex(lockIndex1);
        lock2 = lock.getLockByIndex(lockIndex2);
      } else {
        lock1 = lock.getLockByIndex(lockIndex2);
        lock2 = lock.getLockByIndex(lockIndex1);
      }
      
      lock1.writeLock().lock();
      lock2.writeLock().lock();

      int attempt = 0;
      // Ready to scan
      for (; offset < allocatedMemory; offset += elemSize, attempt++) {
        if (attempt == maxInsertAttempts) {
          // not found
          break;
        }
        if (UnsafeAccess.theUnsafe.getByte(address + offset) == 0) {
          continue;
        }
        if (Utils.compareTo(keyArr, off, keyLength, address + offset + 1, keyLength) == 0) {
          //Free slot
          UnsafeAccess.theUnsafe.putByte(address + offset, (byte) 0);
          if(debug) {
            // nullify value
            UnsafeAccess.copy(zeros, 0, address + offset + 1 + keyLength, valueLength);
          }
          deleted = true;
          break;
        }
      }
      return deleted;
    } finally {
      lock1.writeLock().unlock();
      lock2.writeLock().unlock();
      if(deleted) {
        currentCapacity.decrementAndGet();
      }
    }    
  }
  
  private void sanityCheck(byte[] keyArr, int keyOff) {
    if(keyArr == null || keyOff < 0 ||
        keyOff >= keyArr.length   ) {
      throw new IllegalArgumentException();
    }
  }
  
  
  public void debugKey(byte[] key) {
    
  }
  /**
   * Delete value by key
   * @param keyAddress
   * @return true if deleted
   */
  public boolean delete(long keyAddress) {
    final long hash = hash(keyAddress);
    long index = hash % maxCapacity;
    long offset = index * elemSize;
    long slotIndex = offset / slotSize;

    ReentrantReadWriteLock lock1 = null, lock2 = null;
    boolean deleted = false;
    // Get lock on slot
    try {

      int lockIndex1 = lock.getLockIndex(slotIndex, numSlots);
      int lockIndex2 = lock.getLockIndex(slotIndex + 1 == numSlots ? 0 : slotIndex + 1, numSlots);
      // Ordered (by index in lock array) 
      // locking/unlocking prevents deadlocks
      if (lockIndex1 < lockIndex2) {
        lock1 = lock.getLockByIndex(lockIndex1);
        lock2 = lock.getLockByIndex(lockIndex2);
      } else {
        lock1 = lock.getLockByIndex(lockIndex2);
        lock2 = lock.getLockByIndex(lockIndex1);
      }
      
      lock1.writeLock().lock();
      lock2.writeLock().lock();

      int attempt = 0;
      // Ready to scan
      for (; offset < allocatedMemory; offset += elemSize, attempt++) {
        if (attempt == maxInsertAttempts) {
          // not found
          break;
        }
        if (UnsafeAccess.theUnsafe.getByte(address + offset) == 0) {
          continue;
        }
        if (Utils.compareTo(keyAddress, keyLength, address + offset + 1, keyLength) == 0) {
          //Free slot
          UnsafeAccess.theUnsafe.putByte(address + offset, (byte) 0);
          if(debug) {
            // nullify value
            UnsafeAccess.copy(zeros, 0, address + offset + 1 + keyLength, valueLength);
          }
          deleted = true;
          break;
        }
      }
      return deleted;
    } finally {
      lock1.writeLock().unlock();
      lock2.writeLock().unlock();
      if(deleted) {
        currentCapacity.decrementAndGet();
      }
    }
  }
  
  
  public long allocatedMemory() {
    return allocatedMemory;
  }
    
  public int getKeyLength() {
    return keyLength;
  }
  
  public int getValueLength() {
    return valueLength;
  }
  
  public long maxCapacity () {
    return maxCapacity;
  }
  
  public long currentCapacity() {
    return currentCapacity.get();
  }
  
  public long getBufferAddress() {
    return address;
  }
  
  public long size() {
    return currentCapacity.get();
  }
  
  public synchronized void dispose() {
    if(disposed) {
      return;
    }
    UnsafeAccess.theUnsafe.freeMemory(address);
    disposed = true;
  }
  
  public static void main(String[] args) {
    int size = 10000000;

    float[] loadFactors = new float[] {  0.75f };
    Random r = new Random();
    for (int i = 0; i < loadFactors.length; i++) {
      System.out.println("Load factor=" + loadFactors[i]);
      int total = (int) (size * loadFactors[i]);
      byte[] key = new byte[16];
      byte[] array = new byte[size];
      int[] attempts = new int[20];
      for (int k = 0; k < total; k++) {
        r.nextBytes(key);
        long hash = LongHashFunction.xx(seed).hashBytes(key, 0, key.length);
        if (hash == Long.MIN_VALUE) {
          hash += 1;
        }
        hash = Math.abs(hash);
        int count = 1;
        int index = (int)(hash % size);
        boolean success = false;
        for( ; count <= attempts.length/2; count++, index++) {
          if(index == array.length) break;
          if(array[index] == 0) {
            success = true;
            array[index] = 1;
            attempts[count -1]++;
            break;
          }
        }
        
        
        if(!success) {
          hash = LongHashFunction.xx(seed ^ 0xf3d8672c).hashBytes(key, 0, key.length);
          if (hash == Long.MIN_VALUE) {
            hash += 1;
          }
          hash = Math.abs(hash);
          index = (int)(hash % size);
          success = false;
          for( ; count <= attempts.length; count++, index++) {
            if(index == array.length) break;
            if(array[index] == 0) {
              success = true;
              array[index] = 1;
              attempts[count -1]++;
              break;
            }
          }
        }
        
        if(!success) {
          attempts[attempts.length -1]++;
        }
      }
      int runTotal = 0;
      for(int j=0; j < attempts.length; j++) {
        runTotal += attempts[j];
        System.out.println((j+1)+":"+attempts[j] + " " + (runTotal * (double)100/ total)+"%");
      }
      
      
        attempts = new int[40];
        
        for (int k = 0; k < total/100; k++) {
          r.nextBytes(key);
          long hash = LongHashFunction.xx(seed).hashBytes(key, 0, key.length);
          if (hash == Long.MIN_VALUE) {
            hash += 1;
          }
          hash = Math.abs(hash);
          int count = 1;
          int index = (int)(hash % size);
          boolean success = false;
          for( ; count <= attempts.length/2; count++, index++) {
            if(index == array.length) break;
            if(array[index] == 0) {
              success = true;
              //array[index] = 1;
              attempts[count -1]++;
              break;
            }
          }
          if(!success) {
            hash = LongHashFunction.xx(seed ^ 0xfab4539f).hashBytes(key, 0, key.length);
            if (hash == Long.MIN_VALUE) {
              hash += 1;
            }
            hash = Math.abs(hash);
            index = (int)(hash % size);
            success = false;
            for( ; count <= attempts.length; count++, index++) {
              if(index == array.length) break;
              if(array[index] == 0) {
                success = true;
                //array[index] = 1;
                attempts[count -1]++;
                break;
              }
            }          }
          
          if(!success) {
            attempts[attempts.length -1]++;
          }
        }
        System.out.println("%% at " + loadFactors[i] +" capacity:");
        runTotal = 0;
        for(int j=0; j < attempts.length; j++) {
          runTotal += attempts[j];
          System.out.println((j+1)+":"+attempts[j] + " " + (runTotal * (double)100*100/ total)+"%");
        }
    }
  }
}
