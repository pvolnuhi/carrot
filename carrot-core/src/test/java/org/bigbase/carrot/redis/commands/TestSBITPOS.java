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

public class TestSBITPOS extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "SSETBIT key 100 1",               /* 0 */
      "SSETBIT key 101 1",               /* 0 */
      "SSETBIT key 102 1",               /* 0 */
      "SSETBIT key 103 1",               /* 0 */
      "SSETBIT key 104 1",               /* 0 */
      "SBITPOS key 1",                   /* 100 */
      "SBITPOS key 0",                   /* 0 */
      "SBITPOS key 1 0 11",              /* -1 */
      "SBITPOS key 1 0 -1",              /* 100 */
      "SBITPOS key 1 10 14",             /* 100 */
      "SBITPOS key1 0 1000 10000",       /* -1 */
      "SBITPOS key1 1 0 -1",             /* -1 */
      "SBITPOS key1 0",                  /* 0 */
      "SBITPOS key 1 0",                 /* 100 */
      "SBITPOS key 1 11",                /* 100 */
      "SBITPOS key 1 14"                /* -1 */
  };
  
  protected String[] validResponses = new String[] {
      ":0\r\n",
      ":0\r\n",
      ":0\r\n",
      ":0\r\n",
      ":0\r\n",
      ":100\r\n",
      ":0\r\n",
      ":-1\r\n",
      ":100\r\n",
      ":100\r\n",
      ":-1\r\n",
      ":-1\r\n",
      ":0\r\n",
      ":100\r\n",
      ":100\r\n",
      ":-1\r\n"
  };
  
  
  protected String[] invalidRequests = new String[] {
      "sbitpos x y",                      /* unsupported command */
      "SBITPOS",                          /* wrong number of arguments*/
      "SBITPOS key",                      /* wrong number of arguments*/
      "SBITPOS key 1 2 3 4",              /* wrong number of arguments*/
      "SBITPOS key 2",                    /* wrong bit value */
      "SBITPOS key 1 w 2",              /* wrong number format */
      "SBITPOS key 0 1 z",              /* wrong number format */
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: sbitpos\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong bit value (must be 0 or 1): 2\r\n",
    "-ERR: Wrong number format: w\r\n",
    "-ERR: Wrong number format: z\r\n"
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
