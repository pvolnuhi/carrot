package org.bigbase.carrot.redis.strings;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.util.Utils;

/**
 * 
 * String GETEXPIRE operation. (Utility helper)
 * 
 * @author Vladimir Rodionov
 *
 */
public class StringGetExpire extends Operation {

  
  @Override
  public boolean execute() {

    if (foundRecordAddress > 0) {
      long kPtr = DataBlock.keyAddress(foundRecordAddress);
      int kSize = DataBlock.keyLength(foundRecordAddress);
      int result = Utils.compareTo(kPtr, kSize, keyAddress, keySize);
      
      this.expire = DataBlock.getRecordExpire(foundRecordAddress);
    } else {
      // Does not exist
      this.updatesCount = 0;
      return false;
    }
    return true;
  }
  
  @Override
  public void reset() {
    super.reset();
  }
}
