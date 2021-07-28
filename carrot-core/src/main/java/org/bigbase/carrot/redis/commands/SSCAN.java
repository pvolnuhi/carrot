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
package org.bigbase.carrot.redis.commands;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.db.DBSystem;
import org.bigbase.carrot.redis.sets.Sets;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class SSCAN implements RedisCommand{
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
   * TODO: update Sets.SCAN implementation 
   * SSCAN key cursor [MATCH pattern] [COUNT count] 
   */
  @Override
  public void execute(BigSortedMap map, long inDataPtr, long outBufferPtr, int outBufferSize) {
    
    boolean cursorOK = false;

    try {
      long lastSeenPtr = 0;
      int lastSeenSize = 0;
      int count = 10;// default
      String regex = null;
      
      int numArgs = UnsafeAccess.toInt(inDataPtr);
      if (numArgs != 3 && numArgs != 5 && numArgs != 7) {
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_ARGS_NUMBER);
        return;
      }
      inDataPtr += Utils.SIZEOF_INT;
      // skip command name
      inDataPtr = skip(inDataPtr, 1);
      
      // Read key
      int keySize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long keyPtr = inDataPtr;
      inDataPtr += keySize;
      
      // Read cursor
      int curSize = UnsafeAccess.toInt(inDataPtr);
      inDataPtr += Utils.SIZEOF_INT;
      long cursorPtr = inDataPtr;
      inDataPtr += curSize;
      
      long cursor = Utils.strToLong(cursorPtr, curSize);
      cursorOK = true;
      
      if (cursor != 0) {
        // continuation of a scan operation
        int size = DBSystem.getCursor(map, cursor, keyArena.get(), keyArenaSize.get());
        if (size < 0) {
          // Invalid cursor
          Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_INVALID_CURSOR, ": "+Utils.toString(cursorPtr, curSize));
          return;
        } else if (size > keyArenaSize.get()) {
          checkKeyArena(size);
          size = DBSystem.getCursor(map, cursor, keyArena.get(), keyArenaSize.get());
        }
        lastSeenPtr = keyArena.get();
        lastSeenSize = size;
        // Delete current cursor
        boolean result = DBSystem.deleteCursor(map, cursor);
        assert result;
      }
      if (numArgs == 5) {
        // Either MATCH or COUNT
        int size = UnsafeAccess.toInt(inDataPtr);
        inDataPtr += Utils.SIZEOF_INT;
        
        if (Utils.compareTo(MATCH_FLAG, MATCH_LENGTH, inDataPtr, size) == 0) {
          inDataPtr += size;
          size = UnsafeAccess.toInt(inDataPtr);
          inDataPtr += Utils.SIZEOF_INT;
          regex = Utils.toString(inDataPtr, size);
        } else if (Utils.compareTo(COUNT_FLAG, COUNT_LENGTH, inDataPtr, size) == 0) {
          inDataPtr += size;
          size = UnsafeAccess.toInt(inDataPtr);
          inDataPtr += Utils.SIZEOF_INT;
          count = (int) Utils.strToLong(inDataPtr, size);
        } else {
          Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_COMMAND_FORMAT, ": " + Utils.toString(inDataPtr, size));
          return;
        }
      } else if (numArgs == 7) {
        int size = UnsafeAccess.toInt(inDataPtr);
        inDataPtr += Utils.SIZEOF_INT;
        if (Utils.compareTo(MATCH_FLAG, MATCH_LENGTH, inDataPtr, size) == 0) {
          inDataPtr += size;
          size = UnsafeAccess.toInt(inDataPtr);
          inDataPtr += Utils.SIZEOF_INT;
          regex = Utils.toString(inDataPtr, size);
          inDataPtr += size;
        } else {
          Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_COMMAND_FORMAT, ": " + Utils.toString(inDataPtr, size));
          return;
        }
        size = UnsafeAccess.toInt(inDataPtr);
        inDataPtr += Utils.SIZEOF_INT;
        if (Utils.compareTo(COUNT_FLAG, COUNT_LENGTH, inDataPtr, size) == 0) {
          inDataPtr += size;
          size = UnsafeAccess.toInt(inDataPtr);
          inDataPtr += Utils.SIZEOF_INT;
          count = (int) Utils.strToLong(inDataPtr, size);
          inDataPtr += size;
        } else {
          Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_COMMAND_FORMAT,": " + Utils.toString(inDataPtr, size));
          return;
        }
      }
      
      // Prepare reply
      int off = 0;
      UnsafeAccess.putByte(outBufferPtr + off, (byte) ReplyType.MULTI_BULK.ordinal());
      off += Utils.SIZEOF_BYTE;
      // First is INTEGER type - next cursor
      UnsafeAccess.putByte(outBufferPtr + off, (byte) ReplyType.INTEGER.ordinal());
      off += Utils.SIZEOF_BYTE;
      
      cursor = DBSystem.nextId();
      UnsafeAccess.putLong(outBufferPtr + off, cursor);
      off += Utils.SIZEOF_LONG;
      // Second is VARRAY type 
      UnsafeAccess.putByte(outBufferPtr + off, (byte) ReplyType.VARRAY.ordinal());
      off += Utils.SIZEOF_BYTE;
      
      // Actual call
      long serLen = Sets.SSCAN(map, keyPtr, keySize, lastSeenPtr, lastSeenSize, count, 
        outBufferPtr + off + Utils.SIZEOF_INT /*first 4 bytes keeps serialized size*/, 
        outBufferSize - off - Utils.SIZEOF_INT, regex);
      
      //TODO: how to handle non-existing keys?
      if (serLen == 0) {
        // We are done
        cursor = 0;
        UnsafeAccess.putLong(outBufferPtr + 2 * Utils.SIZEOF_BYTE, cursor);
      } else {
        // Update VARRAY serialized size
        UnsafeAccess.putInt(outBufferPtr + off, (int) serLen);
        off += Utils.SIZEOF_INT; // skip serialized size of VARRAY
        int numElements = UnsafeAccess.toInt(outBufferPtr + off);
        if (numElements > 0) {
          // get last seen
          long ptr = outBufferPtr + off + Utils.SIZEOF_INT;
          int size = 0;
          for (int i = 0; i < numElements - 1; i++) {
            size = Utils.readUVInt(ptr);
            int sizeSize = Utils.sizeUVInt(size);
            ptr += size + sizeSize;
          }
          size = Utils.readUVInt(ptr);
          int sizeSize = Utils.sizeUVInt(size);
          ptr += sizeSize;
          // Save new cursor with last seen field
          // Cursor has expiration time
          DBSystem.saveCursor(map, cursor, ptr, size);
          // Decrement by 1 number of elements
          UnsafeAccess.putInt(outBufferPtr + off, numElements - 1);
        }
      }
    } catch (NumberFormatException e) {
      if (cursorOK) {
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_WRONG_NUMBER_FORMAT, 
          ": " + e.getMessage());
      } else {
        Errors.write(outBufferPtr, Errors.TYPE_GENERIC, Errors.ERR_INVALID_CURSOR, 
          ": " + e.getMessage());
      }
    }
  }
}
