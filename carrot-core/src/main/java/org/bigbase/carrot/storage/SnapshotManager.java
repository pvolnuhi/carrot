/**
 *    Copyright (C) 2021-present Carrot, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 */
package org.bigbase.carrot.storage;

import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

import org.bigbase.carrot.BigSortedMap;

/**
 * Snapshot Manager
 */

public class SnapshotManager {
  
  private static SnapshotManager manager;
  SnapshotThread worker;
  
  private SnapshotManager() {
    worker = new SnapshotThread();
    worker.setPriority(Thread.MIN_PRIORITY);
    worker.start();
  }
  
  public static SnapshotManager getInstance() {
    if (manager != null) return manager;
    synchronized(SnapshotManager.class){
      manager = new SnapshotManager();
    }
    return manager;
  }
  
  public boolean takeSnapshot(BigSortedMap store, boolean sync) {
    boolean result = worker.take(store, sync);
    if (!result) {
      // WARN
      System.out.println("WARNING! Active snapshot started at "+ worker.getLastSnapshotTime() + " is still in progress.");
    }
    return result;
  }
}

class SnapshotThread extends Thread {
  
  AtomicReference<BigSortedMap> storeRef = new AtomicReference<BigSortedMap>();
  Date lastSnapshotTime;
  
  SnapshotThread() {
    super("snapshot-thread");
  }
  
  Date getLastSnapshotTime() {
    return lastSnapshotTime;
  }
  
  boolean take(BigSortedMap store, boolean sync) {
    if(storeRef.compareAndSet(null, store) == false) {
      return false;
    }
    notify();
    if (sync) {
      while(storeRef.get() != null) {
        try {
          Thread.sleep(10);
        } catch(InterruptedException e) {}
      }
    }
    return true;
  }
  
  public void run() {
    System.out.println("Thread "+ getName() + " started at " + new Date());
    for(;;) {
      try {
        wait();
      } catch (InterruptedException e) {
        
      }
      BigSortedMap map = storeRef.get();
      if (map == null) {
        continue;
      }
      System.out.println("Snapshot started at " + lastSnapshotTime);
      lastSnapshotTime = new Date();
      map.snapshot();
      storeRef.set(null);
      System.out.println("Snapshot finished at " + new Date());
    }
  }
}
