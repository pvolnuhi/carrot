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

public class TestSETBIT extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "SETBIT key 100 1",               /* 0 */
      "SETBIT key 100 0"                /* 1 */
  };
  
  protected String[] validResponses = new String[] {
      ":0\r\n",
      ":1\r\n"
  };
  
  
  protected String[] invalidRequests = new String[] {
      "setbit x y",                      /* unsupported command */
      "SETBIT",                          /* wrong number of arguments*/
      "SETBIT key",                      /* wrong number of arguments*/
      "SETBIT key value",                /* wrong number of arguments*/
      "SETBIT key XXX 1",                /* wrong number format*/
      "SETBIT key 100 a",
      "SETBIT key 100 2"
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: setbit\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number format: XXX\r\n",
    "-ERR: Wrong number format: a\r\n",
    "-ERR: Wrong bit value (must be 0 or 1): 2\r\n",
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
