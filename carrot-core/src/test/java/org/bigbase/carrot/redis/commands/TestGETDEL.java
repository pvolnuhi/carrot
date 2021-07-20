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

public class TestGETDEL extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "SET key1 100",
      "GETDEL key1",
      "GETDEL key1",
      "GETDEL key2"
  };
  
  protected String[] validResponses = new String[] {
      "+OK\r\n",
      "$3\r\n100\r\n",
      "$-1\r\n",
      "$-1\r\n"
  };
  
  
  protected String[] invalidRequests = new String[] {
      "getdel x",                     /* unsupported command */
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: getdel\r\n"
  };
  
  /**
   * Subclasses must override
   */
  protected String[] getValidRequests() {
    return validRequests;
  }
  
  @Override
  protected String[] getValidResponses() {
    return validResponses;
  }
  @Override  
  protected String[] getInvalidRequests() {
    return invalidRequests;
  }
  @Override
  protected String[] getInvalidResponses() {
    return invalidResponses;
  }
}
