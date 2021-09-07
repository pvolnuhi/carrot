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

public class TestINCR extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "INCR key",
      "INCR key",
      "incr key"
  };
  
  protected String[] validResponses = new String[] {
      ":1\r\n",
      ":2\r\n",
      ":3\r\n",
  };
  
  
  protected String[] invalidRequests = new String[] {
      "INCR",                        /* wrong number of arguments*/
      "INCR x y",                    /* wrong number of arguments*/
      "SET key1 value1",             /* OK */
      "INCR key1"                    /* wrong number format */ 
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",  
    "+OK\r\n",
    "-ERR: Wrong number format: Value at key is not a number\r\n",
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
