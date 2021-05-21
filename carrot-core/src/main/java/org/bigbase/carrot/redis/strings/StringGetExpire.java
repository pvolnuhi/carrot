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

  public StringGetExpire() {
    setReadOnly(true);
  }
  
  @Override
  public boolean execute() {

    if (foundRecordAddress > 0) {
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
    setReadOnly(true);
  }
}
