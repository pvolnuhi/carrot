package org.bigbase.carrot.redis.lists;



import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.Key;
import org.bigbase.carrot.redis.DataType;
import org.bigbase.carrot.redis.KeysLocker;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import static org.bigbase.carrot.redis.Commons.KEY_SIZE;

import java.util.Arrays;
import java.util.List;

/**
 * Lists: collections of string elements sorted according to the order of insertion. 
 * They are basically linked lists.
 * @author Vladimir Rodionov
 *
 */
public class Lists {
  
  public static enum Side {
    LEFT, RIGHT;
  }

  private static ThreadLocal<Long> keyArena = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return UnsafeAccess.mallocZeroed(512);
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
      return UnsafeAccess.mallocZeroed(512);
    }
  };
  
  static ThreadLocal<Integer> valueArenaSize = new ThreadLocal<Integer>() {
    @Override
    protected Integer initialValue() {
      return 512;
    }
  };
  
  private static ThreadLocal<Key> key = new ThreadLocal<Key>() {
    @Override
    protected Key initialValue() {
      return new Key(0,0);
    }   
  };
  
  private static ThreadLocal<Segment> segment = new ThreadLocal<Segment>() {
    @Override
    protected Segment initialValue() {
      return new Segment();
    } 
  };
  
  private static ThreadLocal<ListsLindex> listsLindex = new ThreadLocal<ListsLindex>() {
    @Override
    protected ListsLindex initialValue() {
      return new ListsLindex();
    }
  };
  
  static long allocMemory(long size) {
    return BigSortedMap.totalAllocatedMemory.addAndGet(size);
  }
  
  static long freeMemory(long size) {
    return BigSortedMap.totalAllocatedMemory.addAndGet(-size);
  }
  
  static long getTotalAllocatedMemory() {
    return BigSortedMap.totalAllocatedMemory.get();
  }
  
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
   * Build key for List. It uses thread local key arena 
   * TODO: data type prefix
   * @param keyPtr original key address
   * @param keySize original key size
   * @param fieldPtr field address
   * @param fieldSize field size
   * @return new key size 
   */
    
   
  public static int buildKey( long keyPtr, int keySize) {
    checkKeyArena(keySize  + Utils.SIZEOF_BYTE + KEY_SIZE);
    long arena = keyArena.get();
    int kSize = + keySize + Utils.SIZEOF_BYTE;
    UnsafeAccess.putByte(arena, (byte) DataType.LIST.ordinal());
    UnsafeAccess.putInt(arena + Utils.SIZEOF_BYTE , keySize);
    UnsafeAccess.copy(keyPtr, arena + Utils.SIZEOF_BYTE + KEY_SIZE, keySize);
    return kSize;
  }
  
  /**
   * Gets and initializes Key
   * @param ptr key address
   * @param size key size
   * @return key instance
   */
  private static Key getKey(long ptr, int size) {
    Key k = key.get();
    k.address = ptr;
    k.length = size;
    return k;
  }
  
  /**
   * TODO: verify thread-safe (looks OK)
   * Deletes and frees memory resources
   * @param map sorted map storage
   * @param keyPtr list key pointer
   * @param keySize list key size
   */
  public static void DELETE(BigSortedMap map, long keyPtr, int keySize) {
    //TODO: implement as Operation
    Key key = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(key);
      int kSize = buildKey(keyPtr, keySize);
      long kPtr = keyArena.get();
      long valueBuf = valueArena.get();
      int valueBufSize = valueArenaSize.get();
      Segment s = firstSegment(map, kPtr, kSize, valueBuf, valueBufSize);
      if (s!= null) {
        long nextPtr = 0;
        do {
          nextPtr = s.getNextAddress();
          s.free();
          s.setDataPointer(nextPtr);
        } while(nextPtr > 0);
      }
      // Delete key
      map.delete(kPtr, kSize);
      
    } finally {
      KeysLocker.writeUnlock(key);
    }
  }
  
  /**
   * LMOVE source destination LEFT|RIGHT LEFT|RIGHT
   *
   * Available since 6.2.0.
   * Time complexity: O(1)
   * Atomically returns and removes the first/last element (head/tail depending on the where
   * from argument) of the list stored at source, and pushes the element at the first/last element 
   * (head/tail depending on the whereto argument) of the list stored at destination.
   * For example: consider source holding the list a,b,c, and destination holding the list x,y,z. 
   * Executing LMOVE source destination RIGHT LEFT results in source holding a,b and destination 
   * holding c,x,y,z.
   * If source does not exist, the value nil is returned and no operation is performed. If source and 
   * destination are the same, the operation is equivalent to removing the first/last element from 
   * the list and pushing it as first/last element of the list, so it can be considered as a list 
   * rotation command (or a no-op if wherefrom is the same as whereto).
   * This command comes in place of the now deprecated RPOPLPUSH. Doing LMOVE RIGHT LEFT is equivalent.
   * 
   * Return value
   * Bulk string reply: the element being popped and pushed.
   * 
   *   
   * @param map sorted map storage
   * @param srcKeyPtr source list key address
   * @param srcKeySize source list key size
   * @param dstKeyPtr dst key list address
   * @param dstKeySize dst key list size
   * @param src source side (LEFT|RIGHT)
   * @param dst dst side (LEFT|RIGHT)
   * @param buffer buffer for return value
   * @param bufferSize buffer size
   * @return size of a value being moved or -1 
   */
    
   public static long LMOVE (BigSortedMap map, long srcKeyPtr, int srcKeySize, 
       long dstKeyPtr, int dstKeySize, Side src, Side dst, long buffer, int bufferSize) {
     
     //TODO we produce temporary garbage in this method
     // Looks thread-safe
     Key srcKey = new Key(srcKeyPtr, srcKeySize);
     Key dstKey = new Key(srcKeyPtr, srcKeySize);
     List<Key> keyList = Arrays.asList(srcKey, dstKey);
     long size = 0;
     try {
       KeysLocker.writeLockAllKeys(keyList);
       if(src == Side.LEFT) {
         size = LPOP(map, srcKeyPtr, srcKeySize, buffer, bufferSize);
       } else {
         size = RPOP(map, srcKeyPtr, srcKeySize, buffer, bufferSize);
       }
       if (size == -1) {
         return -1; // Empty or src does  not exists
       }
       if (size > bufferSize) {
         return size; // repeat call with a large buffer
       }
       // now we have element in a buffer of size = 'size'
       if (dst == Side.LEFT) {
         LPUSH(map, dstKeyPtr, dstKeySize, new long[] {buffer}, new int[] {(int)size});
       } else {
         RPUSH(map, dstKeyPtr, dstKeySize, new long[] {buffer}, new int[] {(int)size});
       }
     } finally {
       KeysLocker.writeUnlockAllKeys(keyList);
     }
     return size;
   }
   
   
  /**
   * BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
   *
   * Available since 6.2.0.
   * Time complexity: O(1)
   * BLMOVE is the blocking variant of LMOVE. When source contains elements, this command behaves exactly 
   * like LMOVE. When used inside a MULTI/EXEC block, this command behaves exactly like LMOVE. When source 
   * is empty, Redis will block the connection until another client pushes to it or until timeout is reached. 
   * A timeout of zero can be used to block indefinitely.
   * This command comes in place of the now deprecated BRPOPLPUSH. Doing BLMOVE RIGHT LEFT is equivalent.
   * See LMOVE for more information.
   * 
   * Return value
   * Bulk string reply: the element being popped from source and pushed to destination. If timeout is reached,
   *  a Null reply is returned.
   * Pattern: Reliable queue
   * Please see the pattern description in the LMOVE documentation.
   * Pattern: Circular list
   * Please see the pattern description in the LMOVE documentation.
   * @param map sorted map storage
   * @param srcKeyPtr source list key address
   * @param srcKeySize source list key size
   * @param dstKeyPtr dst key list address
   * @param dstKeySize dst key list size
   * @param src source side (LEFT|RIGHT)
   * @param dst dst side (LEFT|RIGHT)
   * @param timeout time to wait (sec? ms?)
   * @param buffer buffer for return value
   * @param bufferSize buffer size
   * @return size of a value being moved or -1 
   */
    
   public static long BLMOVE (BigSortedMap map, long srcKeyPtr, int srcKeySize, 
       long dstKeyPtr, int dstKeySize, Side src, Side dst, long timeout, long buffer, int bufferSize) {
     return LMOVE(map, srcKeyPtr, srcKeySize, dstKeyPtr, dstKeySize, src, dst, buffer, bufferSize);
   }
  /**
   * Available since 2.0.0.
   * Time complexity: O(1)
   * BLPOP is a blocking list pop primitive. It is the blocking version of LPOP because it blocks 
   * the connection when there are no elements to pop from any of the given lists. An element is 
   * popped from the head of the first list that is non-empty, with the given keys being checked 
   * in the order that they are given.
   * 
   * Non-blocking behavior
   * 
   * When BLPOP is called, if at least one of the specified keys contains a non-empty list, an element 
   * is popped from the head of the list and returned to the caller together with the key it was popped from.
   * Keys are checked in the order that they are given. Let's say that the key list1 doesn't exist and 
   * list2 and list3 hold non-empty lists. Consider the following command:
   * 
   * BLPOP list1 list2 list3 0
   * BLPOP guarantees to return an element from the list stored at list2 (since it is the first non empty 
   * list when checking list1, list2 and list3 in that order).
   * 
   * Blocking behavior
   * 
   * If none of the specified keys exist, BLPOP blocks the connection until another client performs 
   * an LPUSH or RPUSH operation against one of the keys.
   * Once new data is present on one of the lists, the client returns with the name of the key unblocking it 
   * and the popped value.
   * When BLPOP causes a client to block and a non-zero timeout is specified, the client will unblock returning 
   * a nil multi-bulk value when the specified timeout has expired without a push operation against at least 
   * one of the specified keys.
   * The timeout argument is interpreted as an integer value specifying the maximum number of seconds 
   * to block. A timeout of zero can be used to block indefinitely.
   * What key is served first? What client? What element? Priority ordering details.
   * If the client tries to blocks for multiple keys, but at least one key contains elements, 
   * the returned key / element pair is the first key from left to right that has one or more elements. 
   * In this case the client is not blocked. So for instance BLPOP key1 key2 key3 key4 0, assuming that
   *  both key2 and key4 are non-empty, will always return an element from key2.
   * If multiple clients are blocked for the same key, the first client to be served is the one that was 
   * waiting for more time (the first that blocked for the key). Once a client is unblocked it does not retain 
   * any priority, when it blocks again with the next call to BLPOP it will be served accordingly to the number 
   * of clients already blocked for the same key, that will all be served before it (from the first to 
   * the last that blocked).
   * When a client is blocking for multiple keys at the same time, and elements are available at the same 
   * time in multiple keys (because of a transaction or a Lua script added elements to multiple lists), 
   * the client will be unblocked using the first key that received a push operation (assuming it has 
   * enough elements to serve our client, as there may be other clients as well waiting for this key). 
   * Basically after the execution of every command Redis will run a list of all the keys that received 
   * data AND that have at least a client blocked. The list is ordered by new element arrival time, from 
   * the first key that received data to the last. For every key processed, Redis will serve all the clients
   *  waiting for that key in a FIFO fashion, as long as there are elements in this key. When the key 
   *  is empty or there are no longer clients waiting for this key, the next key that received new data 
   *  in the previous command / transaction / script is processed, and so forth.
   *  
   * Behavior of BLPOP when multiple elements are pushed inside a list.
   * 
   * There are times when a list can receive multiple elements in the context of the same conceptual command:
   * Variadic push operations such as LPUSH mylist a b c.
   * After an EXEC of a MULTI block with multiple push operations against the same list.
   * 
   * Executing a Lua Script with Redis 2.6 or newer.
   * 
   * When multiple elements are pushed inside a list where there are clients blocking, the behavior is 
   * different for Redis 2.4 and Redis 2.6 or newer.
   * For Redis 2.6 what happens is that the command performing multiple pushes is executed, and only after 
   * the execution of the command the blocked clients are served. Consider this sequence of commands.
   * 
   * Client A:   BLPOP foo 0
   * Client B:   LPUSH foo a b c
   * 
   * If the above condition happens using a Redis 2.6 server or greater, Client A will be served with the c element, 
   * because after the LPUSH command the list contains c,b,a, so taking an element from the left means to return c.
   * Instead Redis 2.4 works in a different way: clients are served in the context of the push operation, so as 
   * long as LPUSH foo a b c starts pushing the first element to the list, it will be delivered to the Client A, 
   * that will receive a (the first element pushed).
   * The behavior of Redis 2.4 creates a lot of problems when replicating or persisting data into the AOF file, 
   * so the much more generic and semantically simpler behavior was introduced into Redis 2.6 to prevent problems.
   * Note that for the same reason a Lua script or a MULTI/EXEC block may push elements into a list and afterward 
   * delete the list. In this case the blocked clients will not be served at all and will continue to be blocked 
   * as long as no data is present on the list after the execution of a single command, transaction, or script.
   * 
   * BLPOP inside a MULTI / EXEC transaction
   * 
   * BLPOP can be used with pipelining (sending multiple commands and reading the replies in batch), however this 
   * setup makes sense almost solely when it is the last command of the pipeline.
   * Using BLPOP inside a MULTI / EXEC block does not make a lot of sense as it would require blocking the entire 
   * server in order to execute the block atomically, which in turn does not allow other clients to perform a push 
   * operation. For this reason the behavior of BLPOP inside MULTI / EXEC when the list is empty is to return 
   * a nil multi-bulk reply, which is the same thing that happens when the timeout is reached.
   * If you like science fiction, think of time flowing at infinite speed inside a MULTI / EXEC block...
   * 
   * Return value
   * Array reply: specifically:
   * A nil multi-bulk when no element could be popped and the timeout expired.
   * A two-element multi-bulk with the first element being the name of the key where an element was popped and 
   * the second element being the value of the popped element.

   * @param map sorted map storage
   * @param keyPtrs array of key pointers
   * @param keySizes array o key sizes
   * @param buffer buffer for the response
   * @param bufferSize buffer size
   * @return total response size (index of a key (4 bytes) + value) (-1 - nil)
   * 
   * Buffer format:
   * 
   * Key index = 4 bytes
   * Value
   */
  public static long BLPOP(BigSortedMap map, long[] keyPtrs, int[] keySizes, long buffer, int bufferSize)
  {
    // Looks thread-safe
    int size; 
    for (int i = 0; i < keyPtrs.length; i++) {
      size = (int) LPOP(map, keyPtrs[i], keySizes[i], buffer + Utils.SIZEOF_INT, bufferSize - Utils.SIZEOF_INT);
      if (size + Utils.SIZEOF_INT > bufferSize) {
        return size + Utils.SIZEOF_INT;
      } else if (size > 0) {
        // Set index
        UnsafeAccess.putInt(buffer,  i);
        return size + Utils.SIZEOF_INT;
      }
    }
    return -1;// all empty or non-existent
  }
  
  /**
   * Available since 2.0.0.
   * Time complexity: O(1)
   * BRPOP is a blocking list pop primitive. It is the blocking version of RPOP because it blocks 
   * the connection when there are no elements to pop from any of the given lists. An element is popped 
   * from the tail of the first list that is non-empty, with the given keys being checked in 
   * the order that they are given.
   * See the BLPOP documentation for the exact semantics, since BRPOP is identical to BLPOP with the only 
   * difference being that it pops elements from the tail of a list instead of popping from the head.
   * 
   * Return value
   * 
   * Array reply: specifically:
   * 
   * A nil multi-bulk when no element could be popped and the timeout expired.
   * A two-element multi-bulk with the first element being the name of the key where an element was popped 
   * and the second element being the value of the popped element.
   * 
   * @param map sorted map storage
   * @param keyPtrs array of key pointers
   * @param keySizes array o key sizes
   * @param buffer buffer for the response
   * @param bufferSize buffer size
   * @return total response size (index of a key (4 bytes) + value) (-1 - nil)
   * 
   * Buffer format:
   * 
   * Key index = 4 bytes
   * Value
   */
  
  public static long BRPOP(BigSortedMap map, long[] keyPtrs, int[] keySizes, long buffer, int bufferSize)
  {
    // Looks thread - safe
    int size; 
    for (int i = 0; i < keyPtrs.length; i++) {
      size = (int) RPOP(map, keyPtrs[i], keySizes[i], buffer + Utils.SIZEOF_INT, bufferSize - Utils.SIZEOF_INT);
      if (size + Utils.SIZEOF_INT > bufferSize) {
        return size + Utils.SIZEOF_INT;
      } else if (size > 0) {
        // Set index
        UnsafeAccess.putInt(buffer,  i);
        return size + Utils.SIZEOF_INT;
      }
    }
    return -1;// all empty or non-existent
  }
  
  /**
   * Available since 2.2.0.
   * Time complexity: O(1)
   * BRPOPLPUSH is the blocking variant of RPOPLPUSH. When source contains elements, this command behaves 
   * exactly like RPOPLPUSH. When used inside a MULTI/EXEC block, this command behaves exactly like RPOPLPUSH.
   * When source is empty, Redis will block the connection until another client pushes to it or until 
   * timeout is reached. A timeout of zero can be used to block indefinitely.
   * See RPOPLPUSH for more information.
   * 
   * Return value
   * Bulk string reply: the element being popped from source and pushed to destination. If timeout is reached,
   *  a Null reply is returned.
   * 
   * Pattern: Reliable queue
   * Please see the pattern description in the RPOPLPUSH documentation.
   * 
   * Pattern: Circular list
   * Please see the pattern description in the RPOPLPUSH documentation.
   * 
   * @param map sorted map storage
   * @param strKeyPtr source key pointer
   * @param srcKeySize source key size
   * @param dstKeyPtr destination key pointer
   * @param dstKeySize destination key size
   * @param timeout timeout
   * @param buffer buffer for response
   * @param bufferSize buffer size
   * @return size of element or -1 (NULL)
   */
  public static long BRPOPLPUSH(BigSortedMap map, long srcKeyPtr, int srcKeySize, long dstKeyPtr, 
      int dstKeySize, long timeout, long buffer, int bufferSize)
  {
    return BLMOVE(map, srcKeyPtr, srcKeySize, dstKeyPtr, dstKeySize, Side.LEFT, Side.RIGHT, timeout, buffer, bufferSize);
  }
  
  /**
   * 
   * Available since 1.2.0.
   * Time complexity: O(1)
   * Atomically returns and removes the last element (tail) of the list stored at source, and pushes 
   * the element at the first element (head) of the list stored at destination.
   * For example: consider source holding the list a,b,c, and destination holding the list x,y,z. 
   * Executing RPOPLPUSH results in source holding a,b and destination holding c,x,y,z.
   * If source does not exist, the value nil is returned and no operation is performed. If source 
   * and destination are the same, the operation is equivalent to removing the last element from 
   * the list and pushing it as first element of the list, so it can be considered as a list rotation command.
   * 
   * Return value
   * Bulk string reply: the element being popped and pushed.
   * 
   * Examples
   * redis> RPUSH mylist "one"
   * (integer) 1
   * redis> RPUSH mylist "two"
   * (integer) 2
   * redis> RPUSH mylist "three"
   * (integer) 3
   * redis> RPOPLPUSH mylist myotherlist
   * "three"
   * redis> LRANGE mylist 0 -1
   * 1) "one"
   * 2) "two"
   * redis> LRANGE myotherlist 0 -1
   * 1) "three"
   * redis> 
   * 
   * Pattern: Reliable queue
   * 
   * Redis is often used as a messaging server to implement processing of background jobs or other kinds 
   * of messaging tasks. A simple form of queue is often obtained pushing values into a list in the producer side, 
   * and waiting for this values in the consumer side using RPOP (using polling), or BRPOP if the client 
   * is better served by a blocking operation.
   * However in this context the obtained queue is not reliable as messages can be lost, for example in the case 
   * there is a network problem or if the consumer crashes just after the message is received but it is still to process.
   * RPOPLPUSH (or BRPOPLPUSH for the blocking variant) offers a way to avoid this problem: the consumer fetches 
   * the message and at the same time pushes it into a processing list. It will use the LREM command in order 
   * to remove the message from the processing list once the message has been processed.
   * An additional client may monitor the processing list for items that remain there for too much time, and will 
   * push those timed out items into the queue again if needed.
   * 
   * Pattern: Circular list
   * 
   * Using RPOPLPUSH with the same source and destination key, a client can visit all the elements of an N-elements 
   * list, one after the other, in O(N) without transferring the full list from the server to the client using a single 
   * LRANGE operation.
   * The above pattern works even if the following two conditions:
   * There are multiple clients rotating the list: they'll fetch different elements, until all the elements of 
   * the list are visited, and the process restarts.
   * Even if other clients are actively pushing new items at the end of the list.
   * The above makes it very simple to implement a system where a set of items must be processed by N workers 
   * continuously as fast as possible. An example is a monitoring system that must check that a set of web sites 
   * are reachable, with the smallest delay possible, using a number of parallel workers.
   * Note that this implementation of workers is trivially scalable and reliable, because even if a message is 
   * lost the item is still in the queue and will be processed at the next iteration.
   * 
   * @param map sorted map storage
   * @param strKeyPtr source key pointer
   * @param srcKeySize source key size
   * @param dstKeyPtr destination key pointer
   * @param dstKeySize destination key size
   * @param timeout timeout
   * @return size of element or -1 (NULL)
   */
  public static long RPOPLPUSH(BigSortedMap map, long srcKeyPtr, int srcKeySize, long dstKeyPtr, 
      int dstKeySize, long timeout, long buffer, int bufferSize)
  {
    return LMOVE(map, srcKeyPtr, srcKeySize, dstKeyPtr, dstKeySize, Side.LEFT, Side.RIGHT,  buffer, bufferSize);
  }
  
  /**
   * Returns the element at index in the list stored at key. The index is zero-based, so 0 means 
   * the first element, 1 the second element and so on. Negative indices can be used to designate elements 
   * starting at the tail of the list. Here, -1 means the last element, -2 means the penultimate and so forth.
   * When the value at key is not a list, an error is returned.
   * 
   * Return value
   * Bulk string reply: the requested element, or nil when index is out of range.
   * @param map sorted map storage
   * @param keyPtr list key pointer
   * @param keySize list key size
   * @param index index
   * @param buffer buffer for the response
   * @param bufferSize buffer size
   * @return size of an element, if greater then bufferSize - repeat the call, -1 - NULL
   */
  
  public static long LINDEX(BigSortedMap map, long keyPtr, int keySize, long index, 
      long buffer, int bufferSize) {
    
    Key key = getKey(keyPtr, keySize);
    try {
      KeysLocker.readLock(key);
      int kSize = buildKey(keyPtr, keySize);
      long kPtr = keyArena.get(); 
      ListsLindex lindex  =  listsLindex.get();
      lindex.reset();
      lindex.setKeyAddress(kPtr);
      lindex.setKeySize(kSize);
      lindex.setBuffer(buffer, bufferSize);
      lindex.setIndex(index);
      map.execute(lindex);
      return lindex.getLength();
    } finally {
      KeysLocker.readUnlock(key);
    }
  }
  
  public static int findSegmentForIndex(Segment s, long index) {
    long count = 0;
    long prev = 0;
    do {
      prev = count;
      count += s.getNumberOfElements();
      if (count > index) {
        return (int)(index - prev);
      }
    } while (s.next(s) != null);
    return -1;
  }

  /**
   * LINSERT key BEFORE|AFTER pivot element
   * 
   * Available since 2.2.0.
   * Time complexity: O(N) where N is the number of elements to traverse before seeing the value pivot. 
   * This means that inserting somewhere on the left end on the list (head) can be considered O(1) and 
   * inserting somewhere on the right end (tail) is O(N).
   * Inserts element in the list stored at key either before or after the reference value pivot.
   * When key does not exist, it is considered an empty list and no operation is performed.
   * An error is returned when key exists but does not hold a list value.
   * 
   * Return value
   * Integer reply: the length of the list after the insert operation, or -1 when the value pivot was not found.
   * 
   * @param map sorted map storage
   * @param keyPtr list key pointer
   * @param keySize list key size
   * @param after before or after (true- after, false - before)
   * @param pivotPtr pivot element name pointer
   * @param pivotSize pivot element name size
   * @param elemPtr element pointer
   * @param elemSize element size
   * @return size of a list or -1
   */
  public static long LINSERT(BigSortedMap map, long keyPtr, int keySize, boolean after, 
      long pivotPtr, int pivotSize, long elemPtr, int elemSize )
  {
    // THREAD-SAFE and atomic if use only Lists API (FIXME)
    // By bypassing Lists API we can access this key in parallel
    // using generic BSM API
    // We perform GET/PUT on a same key 
    // TODO: implement as the Operation (better performance)
    
    Key key = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(key);
      int kSize = buildKey(keyPtr, keySize);
      long kPtr = keyArena.get();
      long valueBuf = valueArena.get();
      int valueBufSize = valueArenaSize.get();
      long size = map.get(kPtr, kSize, valueBuf, valueBufSize, 0);
      if (size < 0) {
        return -1; // Key does not exist
      }
      // Address of a first segment after total number of elements in this list (INT value)
      long ptr = UnsafeAccess.toLong(valueBuf + Utils.SIZEOF_INT);
      if (ptr <= 0) {
        return -1;
      }
      Segment s = segment.get();
      s.setDataPointer(ptr);
      do {
        long dataPtr = s.insert(pivotPtr, pivotSize, elemPtr, elemSize, after);
        if (dataPtr > 0) {
          if (s.isFirst()) {
            // update address of the first segment
            UnsafeAccess.putLong(valueBuf + Utils.SIZEOF_INT, dataPtr);
          } else if (s.isLast()) {
            // update last segment address
            UnsafeAccess.putLong(valueBuf + Utils.SIZEOF_INT + Utils.SIZEOF_LONG, dataPtr);
          } else {
            // check if the next one is the last
            if (Segment.isLast(s.getNextAddress())) {
              // Update last segment address
              UnsafeAccess.putLong(valueBuf + Utils.SIZEOF_INT + Utils.SIZEOF_LONG, 
                s.getNextAddress());
            }
          }
          // Update number of elements in the list
          int n = UnsafeAccess.toInt(valueBuf);
          UnsafeAccess.putInt(valueBuf, n + 1);
          // Update list header in a map
          map.put(kPtr, kSize, valueBuf, Utils.SIZEOF_INT + 2 * Utils.SIZEOF_LONG, 0);
          return n + 1;
        }
      } while (s.next(s) != null);
      
      return -1;
      
    } finally {
      KeysLocker.writeUnlock(key);
    }  
  }
  
  /**
   * THREAD-SAFE
   * Returns the length of the list stored at key. If key does not exist, it is interpreted as 
   * an empty list and 0 is returned. An error is returned when the value stored at key is not a list.
   
   * @param map sorted map storage
   * @param keyPtr list key pointer
   * @param keySize list key size
   */
  
  public static long LLEN(BigSortedMap map, long keyPtr, int keySize) {
    Key key = getKey(keyPtr, keySize);
    try {
      KeysLocker.readLock(key);
      int kSize = buildKey(keyPtr, keySize);
      long kPtr = keyArena.get();
      long valueBuf = valueArena.get();
      int valueBufSize = valueArenaSize.get();
      long size = map.get(kPtr, kSize, valueBuf, valueBufSize, 0);
      if (size < 0) {
        return 0; // Key does not exist
      }
      // We keep length of the list in a first 4 bytes of a Value
      // Value: SIZE FIRST-SEGMENT-ADDRESS LAST-SEGMENT-ADDRESS (20 bytes total)
      return UnsafeAccess.toInt(valueBuf);
    } finally {
      KeysLocker.readUnlock(key);
    }
  }
  
  /**
   * 
   * LPOP key
   * Available since 1.0.0.
   * Time complexity: O(1)
   * Removes and returns the first element of the list stored at key.
   * Return value
   * Bulk string reply: the value of the first element, or nil when key does not exist.
   * 
   * @param map sorted map storage
   * @param keyPtr list key pointer
   * @param keySize list key size
   * @param buffer buffer for response
   * @param bufferSize buffer size
   * @return serialized size of an element or -1
   * 
   */
  public static long LPOP(BigSortedMap map, long keyPtr, int keySize, long buffer, int bufferSize)
  {
    // THREAD-SAFE and atomic only in Lists API (FIXME in a future)
    // TODO: implement as the Operation
    Key key = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(key);
      int kSize = buildKey(keyPtr, keySize);
      long kPtr = keyArena.get();
      long valueBuf = valueArena.get();
      int valueBufSize = valueArenaSize.get();
      long size = map.get(kPtr, kSize, valueBuf, valueBufSize, 0);
      if (size < 0) {
        return -1; // Key does not exist
      }
      // First segment pointer
      long ptr = UnsafeAccess.toLong(valueBuf + Utils.SIZEOF_INT);
      if (ptr == 0) {
        return -1;
      }
      Segment s = segment.get();
      s.setDataPointer(ptr);
      int elSize = (int) s.popLeft(buffer, bufferSize);
      if (elSize > bufferSize) {
        return elSize;
      }
      int numElements = UnsafeAccess.toInt(valueBuf);
      numElements--;
      // Update numElements
      UnsafeAccess.putInt(valueBuf, numElements);
      if (s.isEmpty()) {
        long nextSegmentPtr = s.getNextAddress();
        if (nextSegmentPtr > 0) {
          Segment.setPreviousSegmentAddress(nextSegmentPtr, 0);
        }
        s.free();
        UnsafeAccess.putLong(valueBuf + Utils.SIZEOF_INT, nextSegmentPtr);
      }
      // Should we delete list if it is empty now?
      if (numElements == 0) {
        DELETE(map, keyPtr, keySize);
      } else {
        // Update list element number and first segment
        map.put(kPtr, kSize, valueBuf, Utils.SIZEOF_INT + Utils.SIZEOF_LONG, 0);
      }
      return elSize;
    } finally {
      KeysLocker.writeUnlock(key);
    }
  }
  
  /**
   * 
   * LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
   * Available since 6.0.6.
   * Time complexity: O(N) where N is the number of elements in the list, for the average case. When 
   * searching for elements near the head or the tail of the list, or when the MAXLEN option is provided, 
   * the command may run in constant time.
   * The command returns the index of matching elements inside a Redis list. By default, when no 
   * options are given, it will scan the list from head to tail, looking for the first match 
   * of "element". If the element is found, its index (the zero-based position in the list) 
   * is returned. Otherwise, if no match is found, NULL is returned.
   * 
   * > RPUSH mylist a b c 1 2 3 c c
   * > LPOS mylist c
   * 2
   * 
   * The optional arguments and options can modify the command's behavior. The RANK option specifies 
   * the "rank" of the first element to return, in case there are multiple matches. A rank of 1 means 
   * to return the first match, 2 to return the second match, and so forth.
   * For instance, in the above example the element "c" is present multiple times, if I want the index
   *  of the second match, I'll write:
   * 
   * > LPOS mylist c RANK 2
   * 6
   * 
   * That is, the second occurrence of "c" is at position 6. A negative "rank" as the RANK argument 
   * tells LPOS to invert the search direction, starting from the tail to the head.
   * So, we want to say, give me the first element starting from the tail of the list:
   * 
   * > LPOS mylist c RANK -1
   * 7
   * 
   * Note that the indexes are still reported in the "natural" way, that is, considering the first 
   * element starting from the head of the list at index 0, the next element at index 1, and so forth. 
   * This basically means that the returned indexes are stable whatever the rank is positive or negative.
   * Sometimes we want to return not just the Nth matching element, but the position of all the first 
   * N matching elements. This can be achieved using the COUNT option.
   * 
   * > LPOS mylist c COUNT 2
   * [2,6]
   * 
   * We can combine COUNT and RANK, so that COUNT will try to return up to the specified number of matches, 
   * but starting from the Nth match, as specified by the RANK option.
   * 
   * > LPOS mylist c RANK -1 COUNT 2
   * [7,6]
   * 
   * When COUNT is used, it is possible to specify 0 as the number of matches, as a way to tell the command 
   * we want all the matches found returned as an array of indexes. This is better than giving a very large 
   * COUNT option because it is more general.
   * 
   * > LPOS mylist c COUNT 0
   * [2,6,7]
   *
   * When COUNT is used and no match is found, an empty array is returned. However when COUNT is not used
   *  and there are no matches, the command returns NULL.
   * Finally, the MAXLEN option tells the command to compare the provided element only with a given maximum 
   * number of list items. So for instance specifying MAXLEN 1000 will make sure that the command performs 
   * only 1000 comparisons, effectively running the algorithm on a subset of the list (the first part or 
   * the last part depending on the fact we use a positive or negative rank). This is useful to limit 
   * the maximum complexity of the command. It is also useful when we expect the match to be found very 
   * early, but want to be sure that in case this is not true, the command does not take too much time to run.
   * 
   * Return value
   * 
   * The command returns the integer representing the matching element, or null if there is no match. However, 
   * if the COUNT option is given the command returns an array (empty if there are no matches).
   * 
   * @param map sorted map storage
   * @param keyPtr list key pointer
   * @param keySize list key size
   * @param elemPtr element pointer
   * @param elemSize element size
   * @param rank rank of a found
   * @param numMatches number of matches
   * @param maxlen maximum number of comparisons
   * @param buffer buffer for array response
   * @param bufferSize size of a buffer
   * @return serialized response size
   */
  public static int LPOS(BigSortedMap map, long keyPtr, int keySize, long elemPtr, 
      int elemSize, int rank, int numMatches, int maxlen, long buffer, int bufferSize) {
    //TODO
    return 0;
  }
  
  /**
   * 
   * LPUSH key element [element ...]
   * 
   * Available since 1.0.0.
   * Time complexity: O(1) for each element added, so O(N) to add N elements when the command is 
   * called with multiple arguments.
   * Insert all the specified values at the head of the list stored at key. If key does not exist, 
   * it is created as empty list before performing the push operations. When key holds a value that 
   * is not a list, an error is returned.
   * It is possible to push multiple elements using a single command call just specifying multiple 
   * arguments at the end of the command. Elements are inserted one after the other to the head of 
   * the list, from the leftmost element to the rightmost element. So for instance the command 
   * LPUSH mylist a b c will result into a list containing c as first element, b as second element 
   * and a as third element.
   * 
   * Return value
   * Integer reply: the length of the list after the push operations.
   * History
   * >= 2.4: Accepts multiple element arguments. In Redis versions older than 2.4 it was possible 
   * to push a single value per command.
   * 
   * @param map sorted map storage
   * @param keyPtr list key pointer
   * @param keySize list key size
   * @param elemPtrs array of element pointers
   * @param elemSizes array of element sizes
   * @return length of a list after the operation
   */
  public static long LPUSH(BigSortedMap map, long keyPtr, int keySize, long[] elemPtrs, int[] elemSizes)
  {
    Key key = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(key);
      int kSize = buildKey(keyPtr, keySize);
      long kPtr = keyArena.get();
      long valueBuf = valueArena.get();
      int valueBufSize = valueArenaSize.get();
      Segment s = segment.get();
      long size = map.get(kPtr, kSize, valueBuf, valueBufSize, 0);
      boolean exists = true;
      if (size < 0) {
        exists = false;
        s = Segment.allocateNew(s, 0);
      } else {
        long dataPtr = UnsafeAccess.toLong(valueBuf + Utils.SIZEOF_INT);
        s.setDataPointer(dataPtr);
      }
      boolean singleSegment = s.getNextAddress() == 0;
      // Add to the first segment
      for(int i = 0; i < elemPtrs.length; i++) {
        long ptr = s.prepend(elemPtrs[i], elemSizes[i]);
        // Update first segment address
        UnsafeAccess.putLong(valueBuf + Utils.SIZEOF_INT, ptr);
      }
      // Last segment address can change only if there were only one segment
      int numberToPush = elemPtrs.length;
      if (!exists || singleSegment) {
        s = s.last(s);
        long lastSegmentPtr = s.getDataPtr();
        // Update last segment pointer
        UnsafeAccess.putLong(valueBuf + Utils.SIZEOF_INT + Utils.SIZEOF_LONG, lastSegmentPtr);
      } 
      // Update length of the list
      int n = exists? UnsafeAccess.toInt(valueBuf): 0;
      n += numberToPush;
      UnsafeAccess.putInt(valueBuf, n);
      // Update list 
      map.put(kPtr, kSize, valueBuf, Utils.SIZEOF_INT + 2 * Utils.SIZEOF_LONG, 0);
      // Now we have first segment
      return n;
    } finally {
      KeysLocker.writeUnlock(key);
    }
  }
  
  private static void dumpListKey(long valueBuf) {
    System.out.println("N="+ UnsafeAccess.toInt(valueBuf));
    System.out.println("FIRST="+ UnsafeAccess.toLong(valueBuf + Utils.SIZEOF_INT));
    System.out.println("LAST="+ UnsafeAccess.toLong(valueBuf + Utils.SIZEOF_INT + Utils.SIZEOF_LONG));

  }

  /**
   * LPUSHX key element [element ...]
   * 
   * Available since 2.2.0.
   * Time complexity: O(1) for each element added, so O(N) to add N elements when the command is called 
   * with multiple arguments.
   * Inserts specified values at the head of the list stored at key, only if key already exists and holds 
   * a list. In contrary to LPUSH, no operation will be performed when key does not yet exist.
   * Return value
   * Integer reply: the length of the list after the push operation.
   * History
   * >= 4.0: Accepts multiple element arguments. In Redis versions older than 4.0 it was possible 
   * to push a single value per command.
   *
   * @param map sorted map storage
   * @param keyPtr list key pointer
   * @param keySize list key size
   * @param elemPtrs array of element pointers
   * @param elemSizes array of element sizes
   * @return length of a list after the operation or -1 if does not exists
   */
  
  public static long LPUSHX(BigSortedMap map, long keyPtr, int keySize, long[] elemPtrs, int[] elemSizes)
  {
    Key key = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(key);
      int kSize = buildKey(keyPtr, keySize);
      long kPtr = keyArena.get();
      long valueBuf = valueArena.get();
      int valueBufSize = valueArenaSize.get();
      Segment s = firstSegment(map, kPtr, kSize, valueBuf, valueBufSize);
      if (s == null) {
        return -1;
      }
      boolean singleSegment = s.getNextAddress() == 0;
      // Add to the first segment
      for(int i=0; i < elemPtrs.length; i++) {
        long ptr = s.prepend(elemPtrs[i], elemSizes[i]);
        // Update first segment address
        UnsafeAccess.putLong(valueBuf + Utils.SIZEOF_INT, ptr);
      }
      // Last segment address can change only if there were only one segment
      int numberToPush = elemPtrs.length;
      if (singleSegment) {
        // Update last segment pointer
        s = s.last(s);
        UnsafeAccess.putLong(valueBuf + Utils.SIZEOF_INT + Utils.SIZEOF_LONG, s.getDataPtr());
      }
      // Update length of the list
      int n =  UnsafeAccess.toInt(valueBuf);
      n += numberToPush;
      UnsafeAccess.putInt(valueBuf, n);
      // Update list 
      map.put(kPtr, kSize, valueBuf, Utils.SIZEOF_INT + 2 * Utils.SIZEOF_LONG, 0);
      // Now we have first segment
      return n;
    } finally {
      KeysLocker.writeUnlock(key);
    } 
  }
  
  private static Segment firstSegment(BigSortedMap map, long kPtr, int kSize, 
      long valueBuf, int valueBufSize) {
    Segment s = segment.get();
    long size = map.get(kPtr, kSize, valueBuf, valueBufSize, 0);
    if (size < 0) {
      return null;
    } else {
      long dataPtr = UnsafeAccess.toLong(valueBuf + Utils.SIZEOF_INT);
      if (dataPtr == 0) return null;
      s.setDataPointer(dataPtr);
    }
    return s;
  }
  
  private static Segment lastSegment(BigSortedMap map, long kPtr, int kSize, 
      long valueBuf, int valueBufSize) {
    Segment s = segment.get();
    long size = map.get(kPtr, kSize, valueBuf, valueBufSize, 0);
    if (size < 0) {
      return null;
    } else {
      long dataPtr = UnsafeAccess.toLong(valueBuf + Utils.SIZEOF_INT + Utils.SIZEOF_LONG);
      s.setDataPointer(dataPtr);
    }
    return s;
  }
  
  /**
   * 
   * LRANGE key start stop
   * Available since 1.0.0.
   * Time complexity: O(S+N) where S is the distance of start offset from HEAD for small lists, from 
   * nearest end (HEAD or TAIL) for large lists; and N is the number of elements in the specified range.
   * Returns the specified elements of the list stored at key. The offsets start and stop are zero-based 
   * indexes, with 0 being the first element of the list (the head of the list), 1 being the next 
   * element and so on.
   * These offsets can also be negative numbers indicating offsets starting at the end of the list. 
   * For example, -1 is the last element of the list, -2 the penultimate, and so on.
   * 
   * Consistency with range functions in various programming languages
   * 
   * Note that if you have a list of numbers from 0 to 100, LRANGE list 0 10 will return 11 elements, 
   * that is, the rightmost item is included. This may or may not be consistent with behavior of range-related 
   * functions in your programming language of choice (think Ruby's Range.new, Array#slice or Python's range() function).
   * 
   * Out-of-range indexes
   * 
   * Out of range indexes will not produce an error. If start is larger than the end of the list, 
   * an empty list is returned. If stop is larger than the actual end of the list, Redis will treat 
   * it like the last element of the list.
   * 
   * Return value
   * Array reply: list of elements in the specified range.
   * 
   * @param map sorted map storage
   * @param keyPtr list key pointer
   * @param keySize list key size
   * @param start range start
   * @param end range end
   * @param buffer buffer for response
   * @param bufferSize buffer size
   * @return serialized size of the response
   */
  public static long LRANGE (BigSortedMap map, long keyPtr, int keySize, long start, long end, 
      long buffer, int bufferSize) {
    Key key = getKey(keyPtr, keySize);
    try {
      KeysLocker.readLock(key);
      int kSize = buildKey(keyPtr, keySize);
      long kPtr = keyArena.get();
      long valueBuf = valueArena.get();
      int valueBufSize = valueArenaSize.get();
      int num = UnsafeAccess.toInt(valueBuf);
      if (start < 0) {
        start += num;
        if (start < 0) {
          start = 0;
        }
      }
      if (end < 0) {
        end += num;
        if (end < 0) return 0;
      }
      if (end >= num) {
        end = num -1;
      }
      if (start > end) return 0;
      // TODO optimize
      Segment s = firstSegment(map, kPtr, kSize, valueBuf, valueBufSize);
      if (s == null) {
        return 0;
      }
      int off = findSegmentForIndex(s, start);
      if (off < 0) return -1;
      long ptr = buffer + Utils.SIZEOF_INT;
      long limit = buffer + bufferSize;
      long counter = 0;
      long totalSize = Utils.SIZEOF_INT;
      do {
        int n = s.getNumberOfElements();
        for (int k = off; k < n; k++) {
          int sz = s.getByIndex(k, ptr, (int)(limit - ptr));
          totalSize += sz;
          ptr += sz;
          counter++;
          if (counter > (end - start)) {
            break;
          }
        }
        off = 0;
      } while( counter <= (end - start) && s.next(s) != null);
      return totalSize;
    } finally {
      KeysLocker.readUnlock(key);
    }
  }
  
  /**
   * 
   * LREM key count element
   * 
   * Available since 1.0.0.
   * Time complexity: O(N+M) where N is the length of the list and M is the number of elements removed.
   * Removes the first count occurrences of elements equal to element from the list stored at key. 
   * The count argument influences the operation in the following ways:
   * count > 0: Remove elements equal to element moving from head to tail.
   * count < 0: Remove elements equal to element moving from tail to head.
   * count = 0: Remove all elements equal to element.
   * For example, 
   * LREM list -2 "hello" will remove the last two occurrences of "hello" in the list stored at list.
   * Note that non-existing keys are treated like empty lists, so when key does not exist, the command will always return 0.
   * 
   * Return value
   * Integer reply: the number of removed elements.
   
   * @param map sorted map storage
   * @param keyPtr list key pointer
   * @param keySize list key size
   * @param count count (see above)
   * @param elemPtr element pointer
   * @param elemSize element size
   * @return number of elements removed
   */
  
  public static long LREM(BigSortedMap map, long keyPtr, int keySize, int count, long elemPtr, int elemSize) {
    Key key = getKey(keyPtr, keySize);
    boolean reverse = false;
    if (count < 0) {
      reverse = true;
      count = -count;
    } else if (count == 0) {
      count = Integer.MAX_VALUE;
    }
    try {
      KeysLocker.writeLock(key);
      int kSize = buildKey(keyPtr, keySize);
      long kPtr = keyArena.get();
      long valueBuf = valueArena.get();
      int valueBufSize = valueArenaSize.get();
      Segment s = reverse? lastSegment(map, kPtr, kSize, valueBuf, valueBufSize):
        firstSegment(map, kPtr, kSize, valueBuf, valueBufSize);
      if (s == null) {
        return 0;
      }
      int deleted = 0;
      do {
        if (deleted == count) {
          break;
        }
        while((reverse? s.removeReverse(elemPtr, elemSize): 
          s.remove(elemPtr, elemSize)) > 0) {
          deleted ++;
          if (deleted == count) {
            break;
          }
        }
        updateSegmentInChain(s, valueBuf);
      } while( (reverse? s.previous(s): s.next(s)) != null);
      incrementNumberOfElements(valueBuf, -deleted);
      return deleted;
    } finally {
      KeysLocker.writeUnlock(key);
    }
  }
  
  private static long incrementNumberOfElements(long valueBuf, int incr) {
    if (incr == 0) return UnsafeAccess.toInt(valueBuf);
    int v = UnsafeAccess.toInt(valueBuf);
    UnsafeAccess.putLong(valueBuf, v + incr);
    return v + incr;
  }

  /**
   * Update segments first and last
   * @param s segment
   * @param buffer buffer
   */
  private static void updateSegmentInChain(Segment s, long buffer) {
    if (s.isFirst()) {
      UnsafeAccess.putLong(buffer + Utils.SIZEOF_INT, s.getDataPtr());
    }
    if (s.isLast()) {
      UnsafeAccess.putLong(buffer + Utils.SIZEOF_INT + Utils.SIZEOF_LONG, s.getDataPtr());
    } 
  }

  /**
   * LSET key index element
   * Available since 1.0.0.
   * Time complexity: O(N) where N is the length of the list. Setting either the first or the last 
   * element of the list is O(1).
   * Sets the list element at index to element. For more information on the index argument, see LINDEX.
   * An error is returned for out of range indexes.
   * Return value
   * Simple string reply
   * 
   * @param map sorted map storage
   * @param keyPtr list key pointer
   * @param keySize list key size
   * @param index to insert
   * @param elemPtr element pointer
   * @param elemSize element size
   * @return number of elements after the operation or -1;
   */
  public static long LSET (BigSortedMap map, long keyPtr, int keySize, long index, long elemPtr, int elemSize) {
    Key key = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(key);
      int kSize = buildKey(keyPtr, keySize);
      long kPtr = keyArena.get();
      long valueBuf = valueArena.get();
      int valueBufSize = valueArenaSize.get();
      Segment s = firstSegment(map, kPtr, kSize, valueBuf, valueBufSize);
      if (s == null) {
        return -1;
      }
      int listSize = UnsafeAccess.toInt(valueBuf);
      if (index < 0) {
        index += listSize;
        if (index < 0) return -1; 
      }
      if (index >= listSize) {
        return -1;
      }
      int off = findSegmentForIndex(s, index);
      if (off < 0) {
        return -1; // Index is too big
      }
      long oldAddr = s.getDataPtr();
      // remove old one
      int n = s.removeByIndex(off);
      if (n == 0) {
        return -1;
      }
      // Insert new one
      long newAddr = s.insert(off, elemPtr, elemSize);
      boolean needUpdate = false;
      // Update firstLast
      if (s.isFirst() && oldAddr != newAddr) {
        needUpdate = true;
        UnsafeAccess.putLong(valueBuf + Utils.SIZEOF_INT, newAddr);
      } else if (s.isLast() && oldAddr != newAddr) {
        needUpdate = true;
        UnsafeAccess.putLong(valueBuf + Utils.SIZEOF_INT + Utils.SIZEOF_LONG, newAddr); 
      } else {
        s = s.next(s);
        if (s != null && s.isLast()) {
          needUpdate = true;
          UnsafeAccess.putLong(valueBuf + Utils.SIZEOF_INT + Utils.SIZEOF_LONG, s.getDataPtr()); 
        }
      }
      if (needUpdate) {
        map.put(kPtr, kSize, valueBuf, Utils.SIZEOF_INT + Utils.SIZEOF_LONG + Utils.SIZEOF_LONG, 0);
      }
      // Number of elements in this list
      return UnsafeAccess.toInt(valueBuf);
      
    } finally {
      KeysLocker.writeUnlock(key);
    }
  }
  
  /**
   * LTRIM key start stop
   * 
   * Available since 1.0.0.
   * Time complexity: O(N) where N is the number of elements to be removed by the operation.
   * Trim an existing list so that it will contain only the specified range of elements specified. 
   * Both start and stop are zero-based indexes, where 0 is the first element of the list (the head), 
   * 1 the next element and so on.
   * For example: LTRIM foobar 0 2 will modify the list stored at foobar so that only the first three 
   * elements of the list will remain.
   * start and end can also be negative numbers indicating offsets from the end of the list, where -1 is 
   * the last element of the list, -2 the penultimate element and so on.
   * Out of range indexes will not produce an error: if start is larger than the end of the list, 
   * or start > end, the result will be an empty list (which causes key to be removed). If end is larger 
   * than the end of the list, Redis will treat it like the last element of the list.
   * A common use of LTRIM is together with LPUSH / RPUSH. For example:
   * LPUSH mylist someelement
   * LTRIM mylist 0 99
   * This pair of commands will push a new element on the list, while making sure that the list will 
   * not grow larger than 100 elements. This is very useful when using Redis to store logs for example.
   * It is important to note that when used in this way LTRIM is an O(1) operation because in the average
   * case just one element is removed from the tail of the list.
   * 
   * Return value
   * Simple string reply
   * 
   * @param map sorted map storage
   * @param keyPtr list key pointer
   * @param keySize list key size
   * @param start interval start
   * @param end interval end
   * @return TODO
   */
  public static long LTRIM(BigSortedMap map, long keyPtr, int keySize, long start, long end) {
    return 0;
  }
  
  /**
   * RPOP key
   * 
   * Available since 1.0.0.
   * Time complexity: O(1)
   * Removes and returns the last element of the list stored at key.
   * Return value
   * Bulk string reply: the value of the last element, or nil when key does not exist.
   * 
   * @param map sorted map storage
   * @param keyPtr list key pointer
   * @param keySize list key size
   * @param buffer buffer for the response
   * @param bufferSize buffer size
   * @return serialized size of the response or -1 (nil)
   */
  public static long RPOP(BigSortedMap map, long keyPtr, int keySize, long buffer, int bufferSize) {
    Key key = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(key);
      int kSize = buildKey(keyPtr, keySize);
      long kPtr = keyArena.get();
      long valueBuf = valueArena.get();
      int valueBufSize = valueArenaSize.get();
      long size = map.get(kPtr, kSize, valueBuf, valueBufSize, 0);
      if (size < 0) return -1; // Key does not exist
      // Last segment pointer
      long ptr = UnsafeAccess.toLong(valueBuf + Utils.SIZEOF_INT + Utils.SIZEOF_LONG);
      if (ptr == 0) {
        return -1;
      }
      Segment s = segment.get();
      s.setDataPointer(ptr);
      int elSize = (int) s.popRight(buffer, bufferSize);
      if (elSize > bufferSize) {
        return elSize;
      }
      int numElements = UnsafeAccess.toInt(valueBuf);
      numElements--;
      // Update numElements
      UnsafeAccess.putInt(valueBuf, numElements);
      if (s.isEmpty()) {
        if (s.isFirst()) {
          UnsafeAccess.putLong(valueBuf + Utils.SIZEOF_INT + Utils.SIZEOF_LONG, 0);
          UnsafeAccess.putLong(valueBuf + Utils.SIZEOF_INT , 0);
        } else {
          long prevSegmentPtr = s.getPreviousAddress();
          UnsafeAccess.putLong(valueBuf + Utils.SIZEOF_INT + Utils.SIZEOF_LONG, prevSegmentPtr);
          // last segment does not have the next segment address
          Segment.setNextSegmentAddress(prevSegmentPtr, 0);
        }
        s.free();
      }
      // Should we delete list if it is empty now?
      // Update list element number and first segment
      map.put(kPtr, kSize, valueBuf, Utils.SIZEOF_INT + Utils.SIZEOF_LONG + Utils.SIZEOF_LONG, 0);
      return elSize;
    } finally {
      KeysLocker.writeUnlock(key);
    }  
  }
  
  /**
   * 
   * RPUSH key element [element ...]
   * 
   * Available since 1.0.0.
   * Time complexity: O(1) for each element added, so O(N) to add N elements when the command is called 
   * with multiple arguments.
   * Insert all the specified values at the tail of the list stored at key. If key does not exist, it is 
   * created as empty list before performing the push operation. When key holds a value that is not a list, 
   * an error is returned.
   * It is possible to push multiple elements using a single command call just specifying multiple arguments 
   * at the end of the command. Elements are inserted one after the other to the tail of the list, from 
   * the leftmost element to the rightmost element. So for instance the command RPUSH mylist a b c will 
   * result into a list containing a as first element, b as second element and c as third element.
   * 
   * Return value
   * Integer reply: the length of the list after the push operation.
   * History
   * >= 2.4: Accepts multiple element arguments. In Redis versions older than 2.4 it was possible to push 
   * a single value per command.
   * 
   * @param map sorted map storage
   * @param keyPtr list key pointer
   * @param keySize list key size
   * @param elemPtrs array of element pointers
   * @param elemSizes array of element sizes
   * @return the length of the list after the push operation
   */
  public static long RPUSH(BigSortedMap map, long keyPtr, int keySize, long[] elemPtrs, int[] elemSizes) {
    Key key = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(key);
      int kSize = buildKey(keyPtr, keySize);
      long kPtr = keyArena.get();
      long valueBuf = valueArena.get();
      int valueBufSize = valueArenaSize.get();
      Segment s = segment.get();
      long size = map.get(kPtr, kSize, valueBuf, valueBufSize, 0);
      boolean exists = true;
      if (size < 0) {
        exists = false;
        s = Segment.allocateNew(s, 0);
        // Set first segment now
        UnsafeAccess.putLong(valueBuf + Utils.SIZEOF_INT, s.getDataPtr());
      } else {
        long dataPtr = UnsafeAccess.toLong(valueBuf + Utils.SIZEOF_INT + Utils.SIZEOF_LONG);
        // Set the last segment
        s.setDataPointer(dataPtr);
      }
      // Add to the last segment
      for(int i=0; i< elemPtrs.length; i++) {
        long ptr = s.append(elemPtrs[i], elemSizes[i]);
        if (s.isFirst()) {
          UnsafeAccess.putLong(valueBuf + Utils.SIZEOF_INT, ptr);
        }
        if (!s.isLast()) {
          s = s.next(s);
        }
      }
      // Last segment address can change only if there were only one segment
      int numberToPush = elemPtrs.length;
      
      s = s.last(s);
      // Update last segment pointer
      UnsafeAccess.putLong(valueBuf + Utils.SIZEOF_INT + Utils.SIZEOF_LONG, s.getDataPtr());
      // Update length of the list
      int n = exists? UnsafeAccess.toInt(valueBuf): 0;
      n += numberToPush;
      UnsafeAccess.putInt(valueBuf, n);
      // Update list 
      map.put(kPtr, kSize, valueBuf, Utils.SIZEOF_INT + 2 * Utils.SIZEOF_LONG, 0);
      // Now we have first segment
      return n;
    } finally {
      KeysLocker.writeUnlock(key);
    }
  }
  
  /**
   *  RPUSHX key element [element ...]
   *
   * Available since 2.2.0.
   * Time complexity: O(1) for each element added, so O(N) to add N elements when the command is called 
   * with multiple arguments.
   * Inserts specified values at the tail of the list stored at key, only if key already exists and 
   * holds a list. In contrary to RPUSH, no operation will be performed when key does not yet exist.
   * 
   * Return value
   * Integer reply: the length of the list after the push operation.
   * 
   * History
   *  >= 4.0: Accepts multiple element arguments. In Redis versions older than 4.0 it was possible 
   *  to push a single value per command.
   *  
   * @param map sorted map storage
   * @param keyPtr list key pointer
   * @param keySize list key size
   * @param elemPtrs array of element pointers
   * @param elemSizes array of element sizes
   * @return the length of the list after the push operation
   */
  public static long RPUSHX(BigSortedMap map, long keyPtr, int keySize, long[] elemPtrs, int[] elemSizes) {
    Key key = getKey(keyPtr, keySize);
    try {
      KeysLocker.writeLock(key);
      int kSize = buildKey(keyPtr, keySize);
      long kPtr = keyArena.get();
      long valueBuf = valueArena.get();
      int valueBufSize = valueArenaSize.get();
      Segment s = segment.get();
      long size = map.get(kPtr, kSize, valueBuf, valueBufSize, 0);
      if (size < 0) {
        // Key does not exists
        return -1;
      } else {
        long dataPtr = UnsafeAccess.toLong(valueBuf + Utils.SIZEOF_INT + Utils.SIZEOF_LONG);
        // Set the last segment
        s.setDataPointer(dataPtr);
      }
      // Add to the last segment
      for(int i=0; i< elemPtrs.length; i++) {
        long ptr = s.append(elemPtrs[i], elemSizes[i]);
        if (!s.isLast()) {
          if (s.isFirst()) {
            UnsafeAccess.putLong(valueBuf + Utils.SIZEOF_INT, ptr);
          }
          s = s.next(s);
        }
      }
      // Last segment address can change only if there were only one segment
      int numberToPush = elemPtrs.length;
      s = s.last(s);
      // Update last segment pointer
      UnsafeAccess.putLong(valueBuf + Utils.SIZEOF_INT + Utils.SIZEOF_LONG, s.getDataPtr());
      // Update length of the list
      int n = UnsafeAccess.toInt(valueBuf);
      n += numberToPush;
      UnsafeAccess.putInt(valueBuf, n);
      // Update list 
      map.put(kPtr, kSize, valueBuf, Utils.SIZEOF_INT + 2 * Utils.SIZEOF_LONG, 0);
      return n;
    } finally {
      KeysLocker.writeUnlock(key);
    }  
  }
}
