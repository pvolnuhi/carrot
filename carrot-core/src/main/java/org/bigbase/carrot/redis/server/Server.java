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
package org.bigbase.carrot.redis.server;

import static org.bigbase.carrot.redis.util.Commons.KEY_SIZE;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.RedisConf;
import org.bigbase.carrot.redis.util.DataType;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class Server {

  static final long LASTSAVE_KEY = UnsafeAccess.allocAndCopy("persistence.lastsave.time", 0, 
    "persistence.lastsave.time".length());
  static final int LASTSAVE_LENGTH =  "persistence.lastsave.time".length();
  

  
  private static ThreadLocal<Long> keyArena = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(512);
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
      return UnsafeAccess.malloc(512);
    }
  };
  
  static ThreadLocal<Integer> valueArenaSize = new ThreadLocal<Integer>() {
    @Override
    protected Integer initialValue() {
      return 512;
    }
  };
  
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
   * Build key for String. It uses thread local key arena 
   * @param keyPtr original key address
   * @param keySize original key size
   * @param fieldPtr field address
   * @param fieldSize field size
   * @return new key size 
   */
    
   
  private static int buildKey( long keyPtr, int keySize) {
    checkKeyArena(keySize + KEY_SIZE + Utils.SIZEOF_BYTE);
    long arena = keyArena.get();
    int kSize = KEY_SIZE + keySize + Utils.SIZEOF_BYTE;
    UnsafeAccess.putByte(arena, (byte)DataType.SYSTEM.ordinal());
    UnsafeAccess.putInt(arena + Utils.SIZEOF_BYTE, keySize);
    UnsafeAccess.copy(keyPtr, arena + KEY_SIZE + Utils.SIZEOF_BYTE, keySize);
    return kSize;
  }
  
  /**
   * The TIME command returns the current server time as a two items lists: a Unix timestamp 
   * and the amount of microseconds already elapsed in the current second. 
   * Basically the interface is very similar to the one of the gettimeofday system call.
   * Return value:
   * Array reply, specifically:
   * A multi bulk reply containing two elements:
   * unix time in seconds.
   * microseconds.
   * @return current time in milliseconds
   */
  public static long TIME() {
    return System.currentTimeMillis();
  }
  
  /**
   * SHUTDOWN [NOSAVE|SAVE]
   *
   *  Available since 1.0.0.
   * The command behavior is the following:
   * 
   * Stop all the clients.
   * Perform a blocking SAVE if at least one save point is configured.
   * Flush the Append Only File if AOF is enabled.
   * Quit the server.
   * If persistence is enabled this commands makes sure that Redis is switched off without the lost of any data. 
   * This is not guaranteed if the client uses simply SAVE and then QUIT because other clients may alter the DB data
   *  between the two commands.
   * Note: A Redis instance that is configured for not persisting on disk (no AOF configured, nor "save" 
   * directive) will not dump the RDB file on SHUTDOWN, as usually you don't want Redis instances used only 
   * for caching to block on when shutting down.
   * 
   * SAVE and NOSAVE modifiers
   * 
   * It is possible to specify an optional modifier to alter the behavior of the command. Specifically:
   * SHUTDOWN SAVE will force a DB saving operation even if no save points are configured.
   * SHUTDOWN NOSAVE will prevent a DB saving operation even if one or more save points are configured. 
   * (You can think of this variant as an hypothetical ABORT command that just stops the server).
   * Conditions where a SHUTDOWN fails
   * When the Append Only File is enabled the shutdown may fail because the system is in a state that does 
   * not allow to safely immediately persist on disk.
   * Normally if there is an AOF child process performing an AOF rewrite, Redis will simply kill it and exit. 
   * However there are two conditions where it is unsafe to do so, and the SHUTDOWN command will be refused 
   * with an error instead. This happens when:
   * The user just turned on AOF, and the server triggered the first AOF rewrite in order to create the initial 
   * AOF file. In this context, stopping will result in losing the dataset at all: once restarted, the server 
   * will potentially have AOF enabled without having any AOF file at all.
   * A replica with AOF enabled, reconnected with its master, performed a full resynchronization, and restarted 
   * the AOF file, triggering the initial AOF creation process. In this case not completing the AOF rewrite 
   * is dangerous because the latest dataset received from the master would be lost. The new master can actually
   *  be even a different instance (if the REPLICAOF or SLAVEOF command was used in order to reconfigure the replica), 
   *  so it is important to finish the AOF rewrite and start with the correct data set representing the data set 
   *  in memory when the server was terminated.
   * There are conditions when we want just to terminate a Redis instance ASAP, regardless of what its content is. 
   * In such a case, the right combination of commands is to send a CONFIG appendonly no followed by a SHUTDOWN NOSAVE. T
   * he first command will turn off the AOF if needed, and will terminate the AOF rewriting child if there is 
   * one active. The second command will not have any problem to execute since the AOF is no longer enabled.
   * 
   * Return value:
   * 
   * Simple string reply on error. On success nothing is returned since the server quits and the connection is closed.
   * @param map sorted map storage
   * @param save save data on exit
   * @return true on success, false - otherwise
   */
  public static boolean SHUTDOWN(BigSortedMap map, boolean save) {
    //TODO
    if (save) {
      SAVE(map);
    }
    return true;
  }
  
  /**
   * The SAVE commands performs a synchronous save of the dataset producing a point in time snapshot of 
   * all the data inside the Redis instance, in the form of an RDB file.
   * You almost never want to call SAVE in production environments where it will block 
   * all the other clients. Instead usually BGSAVE is used. However in case of issues 
   * preventing Redis to create the background saving child (for instance errors in the fork(2) 
   * system call), the SAVE command can be a good last resort to perform the dump of the latest dataset.
   * Please refer to the persistence documentation for detailed information.
   * Return value
   * Simple string reply: The commands returns OK on success.
   * 
   * @param map sorted map storage
   * @return true on success, false -otherwise
   */
  public static boolean SAVE(BigSortedMap map) {
    //TODO
    
    // save last save
    long saveTime = System.currentTimeMillis();
    long ptr = LASTSAVE_KEY;
    int size = LASTSAVE_LENGTH;
    size = buildKey(ptr, size);
    ptr = keyArena.get();
    
    long valPtr = valueArena.get();
    int valueSize = Utils.SIZEOF_LONG;
    // Set time
    UnsafeAccess.putLong(valPtr, saveTime);
    
    boolean result = map.put(ptr, size, valPtr, valueSize, 0);
    //TODO assert result
    return true;
  }
  
  /**
   * Save the DB in background.
   * Normally the OK code is immediately returned. Redis forks, the parent continues 
   * to serve the clients, the child saves the DB on disk then exits.
   * An error is returned if there is already a background save running or if there is 
   * another non-background-save process running, specifically an in-progress AOF rewrite.
   * If BGSAVE SCHEDULE is used, the command will immediately return OK when an AOF 
   * rewrite is in progress and schedule the background save to run at the next opportunity.
   * A client may be able to check if the operation succeeded using the LASTSAVE command.
   * Please refer to the persistence documentation for detailed information.
   * Return value
   * Simple string reply: Background saving started if BGSAVE started correctly or Background 
   * saving scheduled when used with the SCHEDULE subcommand.
   * History
   * >= 3.2.2: Added the SCHEDULE option.
   * @param map sorted map storage
   */
  public static void BGSAVE(BigSortedMap map) {
    //TODO - start new thread
    
    
    // save last save
    long saveTime = System.currentTimeMillis();
    long ptr = LASTSAVE_KEY;
    int size = LASTSAVE_LENGTH;
    size = buildKey(ptr, size);
    ptr = keyArena.get();
    
    long valPtr = valueArena.get();
    int valueSize = Utils.SIZEOF_LONG;
    // Set time
    UnsafeAccess.putLong(valPtr, saveTime);
    
    boolean result = map.put(ptr, size, valPtr, valueSize, 0);
    //TODO assert result
    
  }
  
  /**
   * Available since 1.0.0.
   * Return the UNIX TIME of the last DB save executed with success. A client may check if a BGSAVE command 
   * succeeded reading the LASTSAVE value, then issuing a BGSAVE command and checking at regular intervals
   *  every N seconds if LASTSAVE changed.
   * Return value
   * Integer reply: an UNIX time stamp.
   * @param map sorted map storage
   * @return UNIX time stamp
   */
  public static long LASTSAVE(BigSortedMap map) {
    long ptr = LASTSAVE_KEY;
    int size = LASTSAVE_LENGTH;
    size = buildKey(ptr, size);
    ptr = keyArena.get();
    
    long result = map.get(ptr, size, valueArena.get(), valueArenaSize.get(), 0);
    if (result < 0) {
      return -1;// not found
    }
    
    return UnsafeAccess.toLong(valueArena.get());
  }
  
  /**
   * COMMAND COUNT 
   * 
   * Available since 2.8.13.
   * Time complexity: O(1)
   * Returns Integer reply of number of total commands in this Redis server.
   * Return value
   * Integer reply: number of commands returned by COMMAND
   * @return total number of supported commands
   */
  public static int COMMAND_COUNT() {
    return RedisConf.getInstance().getCommandsCount();
  }
}