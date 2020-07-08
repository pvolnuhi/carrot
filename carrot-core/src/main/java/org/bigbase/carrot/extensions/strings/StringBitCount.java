package org.bigbase.carrot.extensions.strings;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.extensions.Commons;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Bit count operation.
 * Count the number of set bits (population counting) in a string.
 * By default all the bytes contained in the string are examined. 
 * It is possible to specify the counting operation only in an interval 
 * passing the additional arguments start and end.
 * Like for the GETRANGE command start and end can contain negative values 
 * in order to index bytes starting from the end of the string, where -1 is 
 * the last byte, -2 is the penultimate, and so forth.
 * Non-existent keys are treated as empty strings, so the command will return zero.
 * @author Vladimir Rodionov
 *
 */
public class StringBitCount extends Operation {

  long start;
  long end;
  long count;
  boolean startEndSet = false;
  
  @Override
  public boolean execute() {
    this.updatesCount = 0;
    if (foundRecordAddress < 0) {
      // Yes we return true
      return true;
    }
    long foundKeyPtr = DataBlock.keyAddress(foundRecordAddress);
    int foundKeySize = DataBlock.keyLength(foundRecordAddress);
    if (Utils.compareTo(foundKeyPtr, foundKeySize, keyAddress, keySize) != 0) {
      // Key not found
      return true;
    }
   
    long valuePtr = DataBlock.valueAddress(foundRecordAddress);
    int valueSize =DataBlock.valueLength(foundRecordAddress);
    
    if (startEndSet) {
      // sanity checks
      if (start < 0) {
        start = valueSize + start;
      }
      if (end == Commons.NULL_LONG) {
        end = valueSize -1;
      }
      if (end < 0) {
        end = valueSize + end;
      }
      if (start < 0 || start > valueSize -1) {
        return true;
      }
      if (end < 0 || end > valueSize -1) {
        return true;
      }
      if (start > end) {
        // 0
        return true;
      }
    } else {
      start = 0;
      end = valueSize -1;
    }
    this.count = bitcount(valuePtr, valueSize);
    return true;
  }
  
  private long bitcount(long valuePtr, int valueSize) {
    valuePtr += start;
    valueSize = (int)(end -start + 1);
    int num8 = valueSize / Utils.SIZEOF_LONG;
    int rem8 = valueSize - Utils.SIZEOF_LONG * num8;
    long c = 0;
    long ptr = valuePtr;
    for(int i=0; i < num8; i++) {
      long v = UnsafeAccess.toLong(ptr);
      c += Long.bitCount(v);
      ptr += Utils.SIZEOF_LONG;
    }
    int num4 = rem8 / Utils.SIZEOF_INT;
    int rem4 = rem8 - Utils.SIZEOF_INT * num4;
    for(int i=0; i < num4; i++) {
      int v = UnsafeAccess.toInt(ptr);
      c += Integer.bitCount(v);
      ptr += Utils.SIZEOF_INT;
    }
    
    int num2 = rem4 / Utils.SIZEOF_SHORT;
    int rem2 = rem4 - Utils.SIZEOF_SHORT * num2;
    
    for(int i=0; i < num2; i++) {
      short v = UnsafeAccess.toShort(ptr);
      c += Utils.bitCount(v);
      ptr += Utils.SIZEOF_SHORT;
    }
    if (rem2 == 1) {
      byte v = UnsafeAccess.toByte(ptr);
      c += Utils.bitCount(v);
    }
    return c;
  }

  @Override
  public void reset() {
    super.reset();
    this.start = 0;
    this.end = 0;
    this.count = 0;
    this.startEndSet = false;
  }
  
  
  public void setStartEnd (long from, long to) {
    this.start = from;
    this.end = to;
    this.startEndSet = true;
  }
  
  public long getBitCount() {
    return count;
  }

}
