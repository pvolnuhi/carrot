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


public class TestFLUSHALL extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "SET x y",             /* OK */
      "GET x",               /* y */ 
      "FLUSHALL"             /* OK */,
      "GET x"                /* nil */,
      "flushall"             /* OK */ 
  };
  
  protected String[] validResponses = new String[] {
      "+OK\r\n",
      "$1\r\ny\r\n",
      "+OK\r\n",
      "$-1\r\n",
      "+OK\r\n"
  };
  
  
  protected String[] invalidRequests = new String[] {
      "FLUSHALL COUNT X",                  /* wrong number of arguments*/
      "FLUSHALL FCUK"                      /* Wrong command format */
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Unsupported command: FLUSHALL FCUK\r\n"
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
}
