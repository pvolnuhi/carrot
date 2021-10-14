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

import java.io.IOException;
import java.net.UnknownHostException;

import org.bigbase.carrot.redis.db.DBSystem;
import org.junit.After;

public class TestBGSAVE extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "BGSAVE"               /* OK */,
      "BGSAVE SCHEDULE"      /* OK */,
      "bgsave schedule"      /* OK */  
  };
  
  protected String[] validResponses = new String[] {
      "+Background saving started\r\n",
      "+Background saving scheduled\r\n",
      "+Background saving scheduled\r\n"
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
  
  
  
  @Override
  public void testValidRequests() {
    // TODO Auto-generated method stub
    super.testValidRequests();
  }

  @Override
  public void testValidRequestsNetworkMode()
      throws UnknownHostException, IOException, InterruptedException {
    // TODO Auto-generated method stub
    // Wait 100 msec
    Thread.sleep(100);
    super.testValidRequestsNetworkMode();
  }

  @Override
  public void testValidRequestsInline() {
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
    }
    super.testValidRequestsInline();
  }

  @Override
  public void testValidRequestsDirectBuffer() {
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
    }
    super.testValidRequestsDirectBuffer();
  }

  @Override
  public void testValidRequestsInlineDirectBuffer() {
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
    }
    super.testValidRequestsInlineDirectBuffer();
  }

  @After
  public void tearDown() {
    DBSystem.reset();
  }
}
