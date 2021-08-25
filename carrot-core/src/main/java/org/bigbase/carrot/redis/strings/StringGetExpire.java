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
package org.bigbase.carrot.redis.strings;

import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.ops.Operation;
import org.bigbase.carrot.util.Utils;

/**
 * 
 * String GETEXPIRE operation. (Utility helper)
 * 
 * 
 *
 */
public class StringGetExpire extends Operation {

  public StringGetExpire() {
    setReadOnlyOrUpdateInPlace(true);
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
    setReadOnlyOrUpdateInPlace(true);
  }
}
