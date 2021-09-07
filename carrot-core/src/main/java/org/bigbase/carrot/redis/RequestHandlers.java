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
package org.bigbase.carrot.redis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.util.Utils;

public class RequestHandlers {
  
  static long epochStartNanos = System.nanoTime();
  
  static class Attachment {
    private long accessTime;
    private boolean inUse = false;
    
    Attachment(){
      accessTime = System.nanoTime() - epochStartNanos;
      setInUse(true);
    }
    
    boolean  inUse() {
      return inUse;
    }
    
    void setInUse(boolean b) {
      this.inUse = b;
    }
    
    long lastAccess() {
      return accessTime;
    }
    
    void access() {
      accessTime = System.nanoTime() - epochStartNanos;
    }
        
  }  
  /*
   * Request handlers
   */
  WorkThread[] workers;
  
  private RequestHandlers(BigSortedMap store, int numThreads) {
    workers = new WorkThread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      workers[i] = new WorkThread(store);
    }
  }
  
  public static RequestHandlers create(BigSortedMap store, int numThreads) {
    return new RequestHandlers(store, numThreads); 
  }
  
  public void start() {
    Arrays.stream(workers).forEach(x -> x.start());
    System.out.println("Started request handlers: count=" + workers.length);
  }
  
  /**
   * Submit next socket channel for processing
   * @param key selection key for socket channel
   */
  public void submit(SelectionKey key) {
    while(true) {
      for (int i = 0; i < workers.length; i++) {
        if (workers[i].isBusy()) continue;
        workers[i].nextKey(key);
        return;
      }
    }
  }
  
  /**
   * Shutdown service
   */
  public void shutdown() {
    //TODO
  }
}

class WorkThread extends Thread {
  
  /*
   * Busy loop max iteration
   */
  private final static long BUSY_LOOP_MAX = 10000;

  static int bufferSize = 256 * 1024;
  
  /*
   * Input buffer
   */
  static ThreadLocal<ByteBuffer> inBuf = new ThreadLocal<ByteBuffer>() {
    @Override
    protected ByteBuffer initialValue() {
      return ByteBuffer.allocateDirect(bufferSize);
    }
  };
  
  /*
   * Output buffer
   */
  static ThreadLocal<ByteBuffer> outBuf = new ThreadLocal<ByteBuffer>() {
    @Override
    protected ByteBuffer initialValue() {
      return ByteBuffer.allocateDirect(bufferSize);
    }
  };
  
  /*
   * Data store
   */
  private final BigSortedMap store;
  
  /**
   * Next selection key atomic reference
   */
  private final AtomicReference<SelectionKey> nextKey = new AtomicReference<>();
  
  /*
   * Busy flag
   */
  private volatile boolean busy = false;
  
  /**
   * Default constructor
   * @param store data store
   */
  WorkThread(BigSortedMap store){
    this.store = store;
  }
  
  /**
   * Is thread busy working?
   * @return busy 
   */
  boolean isBusy() {
    return busy;
  }
  
  /**
   * Submits next selection key for processing 
   * @param key selection key
   */
  void nextKey(SelectionKey key) {
    key.attach(new RequestHandlers.Attachment());
    while (!nextKey.compareAndSet(null, key)) {
      Thread.onSpinWait();
    }
  }
  
  /**
   * Release key - mark it not in use
   * @param key
   */
  void release(SelectionKey key) {
    RequestHandlers.Attachment att = (RequestHandlers.Attachment) key.attachment();
    att.setInUse(false);
  }
  
  
 /**
  *  Busy loop with expo-linear back off
  */
  
  private SelectionKey waitForKey() {
    long counter = 0;
    long timeout = 0;
    SelectionKey key = null;
    // wait for next task
    while((key = nextKey.getAndSet(null)) == null) {    
      // Exponential (actually, linear :)) back off
      if (counter < BUSY_LOOP_MAX) {
        counter ++;
        Thread.onSpinWait();
      } else {
        timeout += 1;
        if (timeout > 10) timeout = 10;
        try {
          Thread.sleep(timeout);
        } catch (InterruptedException e) {
        }
      }  
    }
    return key;
  }
  /*
   * Main loop
   */
  public void run() {
    
    // infinite loop
    while (true) {
      SelectionKey key = null;
      busy = false;
      key = waitForKey();
      // We are busy now
      busy = true;
      SocketChannel channel = (SocketChannel) key.channel();
      // Read request first
      ByteBuffer in = inBuf.get();
      ByteBuffer out = outBuf.get();
      in.clear(); out.clear();
      
      try {
        long startCounter = System.nanoTime();
        long max_wait_ns = 100000000; // 100ms
        
        while (true) {
          int num = channel.read(in);
          if (num < 0) {
            // End-Of-Stream - socket was closed, cancel the key
            key.cancel();
            break;
          } else if (num == 0) {
            if (System.nanoTime() - startCounter > max_wait_ns) {
              break;
            }
            continue;
          }
          // Try to parse
          int oldPos = in.position();
          if (!requestIsComplete(in)) {
            // restore position
            in.position(oldPos);
            continue;
          }
          in.position(oldPos);
          // Process request
          CommandProcessor.process(store, in, out);
          // send response back
          out.flip();
          int limit = out.limit();
          byte[] bb = new byte[limit];
          out.get(bb);
          System.out.println("SERVER:\n"+ new String(bb));
          out.position(0);
          out.limit(limit);
          while (out.hasRemaining()) {
            channel.write(out);
          }
          break;
        }
      } catch (IOException e) {
        String msg = e.getMessage();
        if (!msg.equals("Connection reset by peer")) {
          // TODO 
          e.printStackTrace();
        }
      } finally {
        // Release selection key - ready for the next request
        release(key);
        nextKey.set(null);
        // set busy flag to false
        busy = false;
      }
    }
  }
  
  private boolean requestIsComplete(ByteBuffer in) {
    return Utils.requestIsComplete(in);
  }
  
}
