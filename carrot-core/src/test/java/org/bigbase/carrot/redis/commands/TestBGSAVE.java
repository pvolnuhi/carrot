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

import org.bigbase.carrot.redis.db.DBSystem;
import org.junit.After;

public class TestBGSAVE extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "BGSAVE"               /* OK */,
      "BGSAVE SCHEDULE"      /* OK */
  };
  
  protected String[] validResponses = new String[] {
      "+OK\r\n",
      "+OK\r\n"
  };
  
  
  protected String[] invalidRequests = new String[] {
      "BGSAVE COUNT X",                  /* wrong number of arguments*/
      "BGSAVE FCUK"                      /* Wrong command format */
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Unsupported command: BGSAVE FCUK\r\n"
  };
  
  /**
   * Subclasses must override
   */
  protected String[] getValidRequests() {
    return validRequests;
  }
  
  protected String[] getValidResponses() {
    return validResponses;
  }
  protected String[] getInvalidRequests() {
    return invalidRequests;
  }
  protected String[] getInvalidResponses() {
    return invalidResponses;
  }
  @After
  public void tearDown() {
    DBSystem.reset();
  }
}
