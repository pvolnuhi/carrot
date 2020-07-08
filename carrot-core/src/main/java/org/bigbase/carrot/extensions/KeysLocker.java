package org.bigbase.carrot.extensions;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.bigbase.carrot.Key;
import org.bigbase.carrot.KeyValue;

/**
 * THis helper class performs safe group Key/KeyValue locking/unlocking
 * @author Vladimir Rodionov
 *
 */
public class KeysLocker {

  /*
   * Read-Write Lock TODO: StampedLock (Java 8)
   */
  static ReentrantReadWriteLock[] locks = new ReentrantReadWriteLock[11113];
  static {
    for (int i = 0; i < locks.length; i++) {
      locks[i] = new ReentrantReadWriteLock();
    }
  }

  /**
   * Read Lock Key
   * @param key
   */
  public static void readLock(Key key) {
    int hash = key.hashCode();
    int index = hash % locks.length;
    locks[index].readLock().lock();
  }

  /**
   * Read unlock Key
   * @param key
   */
  public static void readUnlock(Key key) {
    int hash = key.hashCode();
    int index = hash % locks.length;
    locks[index].readLock().unlock();
  }

  /**
   * Read Lock KeyValue
   * @param key
   */
  public static void readLock(KeyValue key) {
    int hash = key.hashCode();
    int index = hash % locks.length;
    locks[index].readLock().lock();
  }

  /**
   * Read unlock KeyValue
   * @param key
   */
  public static void readUnlock(KeyValue key) {
    int hash = key.hashCode();
    int index = hash % locks.length;
    locks[index].readLock().unlock();
  }

  /**
   * Write Lock Key
   * @param key
   */
  public static void writeLock(Key key) {
    int hash = key.hashCode();
    int index = hash % locks.length;
    locks[index].writeLock().lock();
  }

  /**
   * Write unlock Key
   * @param key
   */
  public static void writeUnlock(Key key) {
    int hash = key.hashCode();
    int index = hash % locks.length;
    locks[index].writeLock().unlock();
  }

  /**
   * Write Lock KeyValue
   * @param key
   */
  public static void writeLock(KeyValue key) {
    int hash = key.hashCode();
    int index = hash % locks.length;
    locks[index].writeLock().lock();
  }

  /**
   * Write unlock KeyValue
   * @param key
   */
  public static void writeUnlock(KeyValue key) {
    int hash = key.hashCode();
    int index = hash % locks.length;
    locks[index].writeLock().unlock();
  }

  /**
   * Read lock list of KeyValues
   * @param kvs list of KeyValues
   */
  public static void readLockAll(List<KeyValue> kvs) {
    // Sort them first to avoid deadlocks in a future
    Collections.sort(kvs);
    for(int i=0; i < kvs.size(); i++) {
      readLock(kvs.get(i));
    }
  }
  /**
   * Read unlock list of KeyValues
   * @param kvs list of KeyValues
   */
  public static void readUnlockAll(List<KeyValue> kvs) {
    for(int i=0; i < kvs.size(); i++) {
      readUnlock(kvs.get(i));
    }
  }
  
  /**
   * Write lock list of KeyValues
   * @param kvs list of KeyValues
   */
  public static void writeLockAll(List<KeyValue> kvs) {
    // Sort them first to avoid deadlocks in a future
    Collections.sort(kvs);
    for(int i=0; i < kvs.size(); i++) {
      writeLock(kvs.get(i));
    }
  }
  /**
   * Write unlock list of KeyValues
   * @param kvs list of KeyValues
   */
  public static void writeUnlockAll(List<KeyValue> kvs) {
    for(int i=0; i < kvs.size(); i++) {
      readUnlock(kvs.get(i));
    }
  }
}
