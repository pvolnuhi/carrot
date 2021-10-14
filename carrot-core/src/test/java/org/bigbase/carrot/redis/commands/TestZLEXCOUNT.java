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

public class TestZLEXCOUNT extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "ZADD key 10.0 c1 9.0 c2 8.0 c3 7.0 c4 6.0 c5",                         /* 5 */
      "ZADD key 5.0 c6 4.0 c7 3.0 c8 2.0 c9 1.0 c10",                         /* 5 */
      "ZCARD key",                                                            /* 10 */
      
      "ZLEXCOUNT key - +",                                                    /* 10 */ 
      "ZLEXCOUNT key - (d",                                                   /* 10 */
      "ZLEXCOUNT key - [d",                                                   /* 10 */
      "ZLEXCOUNT key - [c",                                                   /* 0 */
      "zlexcount key - (c",                                                   /* 0 */
      
      "ZLEXCOUNT key (c1 [c99",                                               /* 9 */
      "ZLEXCOUNT key [c1 [c99",                                               /* 10 */
      "ZLEXCOUNT key (c2 [c99",                                               /* 7 */
      "ZLEXCOUNT key [c2 [c99",                                               /* 8 */
      "ZLEXCOUNT key (c3 [c99",                                               /* 6 */
      "ZLEXCOUNT key [c3 [c99",                                               /* 7 */
      "ZLEXCOUNT key (c4 [c99",                                               /* 5 */
      "ZLEXCOUNT key [c4 [c99",                                               /* 6 */
      "ZLEXCOUNT key (c5 [c99",                                               /* 4 */
      "ZLEXCOUNT key [c5 [c99",                                               /* 5 */
      "ZLEXCOUNT key (c6 [c99",                                               /* 3 */
      "ZLEXCOUNT key [c6 [c99",                                               /* 4 */
      "ZLEXCOUNT key (c7 [c99",                                               /* 2 */
      "ZLEXCOUNT key [c7 [c99",                                               /* 3 */
      "ZLEXCOUNT key (c8 [c99",                                               /* 1 */
      "ZLEXCOUNT key [c8 [c99",                                               /* 2 */
      "ZLEXCOUNT key (c9 [c99",                                               /* 0 */
      "zlexcount key [c9 [c99",                                               /* 1 */
  };
  
  protected String[] validResponses = new String[] {
      ":5\r\n",
      ":5\r\n",
      ":10\r\n",
      
      ":10\r\n",
      ":10\r\n",
      ":10\r\n",
      ":0\r\n",
      ":0\r\n",
      
      ":9\r\n",
      ":10\r\n",
      ":7\r\n",
      ":8\r\n",
      ":6\r\n",
      ":7\r\n",
      ":5\r\n",
      ":6\r\n",
      ":4\r\n",
      ":5\r\n",
      ":3\r\n",
      ":4\r\n",
      ":2\r\n",
      ":3\r\n",
      ":1\r\n",
      ":2\r\n",
      ":0\r\n",
      ":1\r\n"
   };
  
  protected String[] invalidRequests = new String[] {
      "zlexcoun x y",                           /* unsupported command */
      "ZLEXCOUNT",                               /* wrong number of arguments*/
      "ZLEXCOUNT key",                           /* wrong number of arguments*/
      "ZLEXCOUNT key a",                         /* wrong number of arguments*/
      "ZLEXCOUNT key a b c",                     /* wrong number of arguments*/
      "ZLEXCOUNT key a b",                       /* wrong command format */
      "ZLEXCOUNT key (a b",                      /* wrong command format */
      "ZLEXCOUNT key + b",                       /* wrong command format */
      "ZLEXCOUNT key [b -",                       /* wrong command format */

  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: ZLEXCOUN\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    
    "-ERR: Wrong command format, unexpected argument: " + Errors.ERR_MIN_SPECIFIED +"\r\n",
    "-ERR: Wrong command format, unexpected argument: " + Errors.ERR_MAX_SPECIFIED +"\r\n",
    "-ERR: Wrong command format, unexpected argument: " + Errors.ERR_MIN_SPECIFIED +"\r\n",
    "-ERR: Wrong command format, unexpected argument: " + Errors.ERR_MAX_SPECIFIED +"\r\n"

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
