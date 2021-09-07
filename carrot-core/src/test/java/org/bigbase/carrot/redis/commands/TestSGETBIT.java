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

public class TestSGETBIT extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "SSETBIT key 100 1",               /* 0 */
      "SSETBIT key 101 1",               /* 0 */
      "SGETBIT key 100",
      "SGETBIT key 101",
      "SGETBIT key 102",
      "sgetbit key1 100"
  };
  
  protected String[] validResponses = new String[] {
      ":0\r\n",
      ":0\r\n",
      ":1\r\n",
      ":1\r\n",
      ":0\r\n",
      ":0\r\n"
  };
  
  
  protected String[] invalidRequests = new String[] {
      "sgetbi x y",                      /* unsupported command */
      "SGETBIT",                          /* wrong number of arguments*/
      "SGETBIT key",                      /* wrong number of arguments*/
      "SGETBIT key value x",              /* wrong number of arguments*/
      "SGETBIT key XXX",                  /* wrong number format*/
      "SGETBIT key -10"                   /* positive number expected */
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: SGETBI\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number format: XXX\r\n",
    "-ERR: Positive number expected: -10\r\n"

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
