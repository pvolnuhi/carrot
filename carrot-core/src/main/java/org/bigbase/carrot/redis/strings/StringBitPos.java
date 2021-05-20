package org.bigbase.carrot.redis.strings;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.redis.Commons;
import org.bigbase.carrot.util.Utils;
import static org.bigbase.carrot.util.UnsafeAccess.firstBitSetByte;
import static org.bigbase.carrot.util.UnsafeAccess.firstBitUnSetByte;
import static org.bigbase.carrot.util.UnsafeAccess.firstBitSetShort;
import static org.bigbase.carrot.util.UnsafeAccess.firstBitUnSetShort;
import static org.bigbase.carrot.util.UnsafeAccess.firstBitSetInt;
import static org.bigbase.carrot.util.UnsafeAccess.firstBitUnSetInt;
import static org.bigbase.carrot.util.UnsafeAccess.firstBitSetLong;
import static org.bigbase.carrot.util.UnsafeAccess.firstBitUnSetLong;



/**
 * Returns the position of the first bit set to 1 or 0 in a string.
 * The position is returned, thinking of the string as an array of bits from left to right, 
 * where the first byte's most significant bit is at position 0, the second byte's most 
 * significant bit is at position 8, and so forth.
 * The same bit position convention is followed by GETBIT and SETBIT.
 * By default, all the bytes contained in the string are examined. It is possible to look for 
 * bits only in a specified interval passing the additional arguments start and end 
 * (it is possible to just pass start, the operation will assume that the end is the last byte 
 * of the string. However there are semantic differences as explained later). The range is interpreted 
 * as a range of bytes and not a range of bits, so start=0 and end=2 means to look at the first three bytes.
 * Note that bit positions are returned always as absolute values starting from bit zero even when start 
 * and end are used to specify a range.
 * Like for the GETRANGE command start and end can contain negative values in order to index bytes starting 
 * from the end of the string, where -1 is the last byte, -2 is the penultimate, and so forth.
 * Non-existent keys are treated as empty strings.
 * 
 * The command returns the position of the first bit set to 1 or 0 according to the request.
 * If we look for set bits (the bit argument is 1) and the string is empty or composed of just zero bytes,
 *  -1 is returned.
 * If we look for clear bits (the bit argument is 0) and the string only contains bit set to 1, 
 * the function returns the first bit not part of the string on the right. So if the string is three bytes 
 * set to the value 0xff the command BITPOS key 0 will return 24, since up to bit 23 all the bits are 1.
 * Basically, the function considers the right of the string as padded with zeros if you look for clear 
 * bits and specify no range or the start argument only.
 * However, this behavior changes if you are looking for clear bits and specify a range with both start 
 * and end. If no clear bit is found in the specified range, the function returns -1 as the user specified 
 * a clear range and there are no 0 bits in that range.
 * @author Vladimir Rodionov
 *
 */
public class StringBitPos extends Operation {

  long start;
  long end;
  long position = -1;
  boolean startEndSet = false;
  int bit;
  
  @Override
  public boolean execute() {
    this.updatesCount = 0;
    if (foundRecordAddress < 0) {
      // Yes we return true
      return true;
    }
//TODO: remove after testing    
//    long foundKeyPtr = DataBlock.keyAddress(foundRecordAddress);
//    int foundKeySize = DataBlock.keyLength(foundRecordAddress);
//    if (Utils.compareTo(foundKeyPtr, foundKeySize, keyAddress, keySize) != 0) {
//      // Key not found
//      return true;
//    }
   
    long valuePtr = DataBlock.valueAddress(foundRecordAddress);
    int valueSize = DataBlock.valueLength(foundRecordAddress);
    
    if (start == Commons.NULL_LONG) {
      start = 0;
    }
    if (end == Commons.NULL_LONG) {
      end = valueSize -1;
    }
    
    if (startEndSet) {
 
      // sanity checks
      if (start < 0) {
        start = valueSize + start;
      }

      //TODO: is it correct?
      if (start < 0) {
        start = 0;
      }
      
      if (end < 0) {
        end = valueSize + end;
      }
      
      if (end < 0) {
        return true;
      }
      
      if (start > valueSize - 1) {
        return true;
      }
      
      if (end > valueSize - 1) {
        end = valueSize - 1;
      }
      
      if (start > end) {
        // 0
        return true;
      }
    } else {
      start = 0;
      end = valueSize -1;
    }
    this.position = bit == 1? Utils.bitposSet(valuePtr + start, (int)(end - start) + 1): 
      Utils.bitposUnset(valuePtr + start, (int)(end - start) +1);
    if (this.position == -1 && bit == 0 && !startEndSet) {
      this.position = valueSize * Utils.BITS_PER_BYTE;
    }
    return true;
  }
  
  @Override
  public void reset() {
    super.reset();
    this.start = 0;
    this.end = 0;
    this.position = -1;
    this.startEndSet = false;
    this.bit = 0;
  }
  
  /**
   * Set bit value - 0 or 1 to search
   * @param bit value
   */
  public void setBit(int bit) {
    this.bit = bit;
  }
  
  
  public void setStartEnd (long start, long end) {
    this.start = start;
    this.end = end;
    if (start != Commons.NULL_LONG || end != Commons.NULL_LONG) {
      this.startEndSet = true;
    }
  }
  
  public long getPosition() {
    return position;
  }

}
