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

public class TestZREMRANGEBYLEX extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "ZADD key 0 c1 0 c2 0 c3 0 c4 0 c5 0 c6 0 c7 0 c8 0 c9 0 c10",          /* 10 */
      "ZCARD key",                                                            /* 10 */
      
      "ZREMRANGEBYLEX key - +",                                               /* 10 */ 
      "ZCARD key",                                                            /*  0 */
      "ZADD key 0 c1 0 c2 0 c3 0 c4 0 c5 0 c6 0 c7 0 c8 0 c9 0 c10",          /* 10 */

      "ZREMRANGEBYLEX key - (d",                                              /* 10 */
      "ZCARD key",                                                            /*  0 */
      "ZADD key 0 c1 0 c2 0 c3 0 c4 0 c5 0 c6 0 c7 0 c8 0 c9 0 c10",          /* 10 */
      
      "ZREMRANGEBYLEX key - [d",                                              /* 10 */
      "ZCARD key",                                                            /*  0 */
      "ZADD key 0 c1 0 c2 0 c3 0 c4 0 c5 0 c6 0 c7 0 c8 0 c9 0 c10",          /* 10 */
      
      "ZREMRANGEBYLEX key - [c",                                              /* 0 */
      "ZCARD key",                                                            /* 10 */
      
      "ZREMRANGEBYLEX key - (c",                                              /* 0 */
      "ZCARD key",                                                            /* 10 */
      
      "ZREMRANGEBYLEX key (c1 [c99",                                          /* 9 */
      "ZCARD key",                                                            /* 1 */
      "ZADD key 0 c1 0 c2 0 c3 0 c4 0 c5 0 c6 0 c7 0 c8 0 c9 0 c10",          /* 9 */

      
      "ZREMRANGEBYLEX key [c1 [c99",                                          /* 10 */
      "ZCARD key",                                                            /* 0 */
      "ZADD key 0 c1 0 c2 0 c3 0 c4 0 c5 0 c6 0 c7 0 c8 0 c9 0 c10",          /* 10 */
      
      "ZREMRANGEBYLEX key (c2 [c99",                                          /* 7 */
      "ZCARD key",                                                            /* 3 */
      "ZADD key 0 c1 0 c2 0 c3 0 c4 0 c5 0 c6 0 c7 0 c8 0 c9 0 c10",          /* 7 */
      
      "ZREMRANGEBYLEX key [c2 [c99",                                          /* 8 */
      "ZCARD key",                                                            /* 2 */
      "ZADD key 0 c1 0 c2 0 c3 0 c4 0 c5 0 c6 0 c7 0 c8 0 c9 0 c10",          /* 8 */
      
      "ZREMRANGEBYLEX key (c3 [c99",                                          /* 6 */
      "ZCARD key",                                                            /* 4 */
      "ZADD key 0 c1 0 c2 0 c3 0 c4 0 c5 0 c6 0 c7 0 c8 0 c9 0 c10",          /* 6 */
      
      "ZREMRANGEBYLEX key [c3 [c99",                                          /* 7 */
      "ZCARD key",                                                            /* 3 */
      "ZADD key 0 c1 0 c2 0 c3 0 c4 0 c5 0 c6 0 c7 0 c8 0 c9 0 c10",          /* 7 */
      
      "ZREMRANGEBYLEX key (c4 [c99",                                          /* 5 */
      "ZCARD key",                                                            /* 5 */
      "ZADD key 0 c1 0 c2 0 c3 0 c4 0 c5 0 c6 0 c7 0 c8 0 c9 0 c10",          /* 5 */
      
      "ZREMRANGEBYLEX key [c4 [c99",                                          /* 6 */
      "ZCARD key",                                                            /* 4 */
      "ZADD key 0 c1 0 c2 0 c3 0 c4 0 c5 0 c6 0 c7 0 c8 0 c9 0 c10",          /* 6 */
      
      "ZREMRANGEBYLEX key (c5 [c99",                                          /* 4 */
      "ZCARD key",                                                            /* 6 */
      "ZADD key 0 c1 0 c2 0 c3 0 c4 0 c5 0 c6 0 c7 0 c8 0 c9 0 c10",          /* 4 */
      
      "ZREMRANGEBYLEX key [c5 [c99",                                          /* 5 */
      "ZCARD key",                                                            /* 5 */
      "ZADD key 0 c1 0 c2 0 c3 0 c4 0 c5 0 c6 0 c7 0 c8 0 c9 0 c10",          /* 5 */
      
      "ZREMRANGEBYLEX key (c6 [c99",                                          /* 3 */
      "ZCARD key",                                                            /* 7 */
      "ZADD key 0 c1 0 c2 0 c3 0 c4 0 c5 0 c6 0 c7 0 c8 0 c9 0 c10",          /* 3 */
      
      "ZREMRANGEBYLEX key [c6 [c99",                                          /* 4 */
      "ZCARD key",                                                            /* 6 */
      "ZADD key 0 c1 0 c2 0 c3 0 c4 0 c5 0 c6 0 c7 0 c8 0 c9 0 c10",          /* 4 */
      
      "ZREMRANGEBYLEX key (c7 [c99",                                          /* 2 */
      "ZCARD key",                                                            /* 8 */
      "ZADD key 0 c1 0 c2 0 c3 0 c4 0 c5 0 c6 0 c7 0 c8 0 c9 0 c10",          /* 2 */
      
      "ZREMRANGEBYLEX key [c7 [c99",                                          /* 3 */
      "ZCARD key",                                                            /* 7 */
      "ZADD key 0 c1 0 c2 0 c3 0 c4 0 c5 0 c6 0 c7 0 c8 0 c9 0 c10",          /* 3 */
      
      "ZREMRANGEBYLEX key (c8 [c99",                                          /* 1 */
      "ZCARD key",                                                            /* 9 */
      "ZADD key 0 c1 0 c2 0 c3 0 c4 0 c5 0 c6 0 c7 0 c8 0 c9 0 c10",          /* 1 */
      
      "ZREMRANGEBYLEX key [c8 [c99",                                          /* 2 */
      "ZCARD key",                                                            /* 8 */
      "ZADD key 0 c1 0 c2 0 c3 0 c4 0 c5 0 c6 0 c7 0 c8 0 c9 0 c10",          /* 2 */
      
      "ZREMRANGEBYLEX key (c9 [c99",                                          /* 0 */
      "ZCARD key",                                                            /* 10 */
      
      "ZREMRANGEBYLEX key [c9 [c99",                                          /* 1 */
      "ZCARD key",                                                            /* 9 */
  };
  
  protected String[] validResponses = new String[] {
      ":10\r\n",
      ":10\r\n",
      
      ":10\r\n",
      ":0\r\n",
      ":10\r\n",
      
      ":10\r\n",
      ":0\r\n",
      ":10\r\n",
      
      ":10\r\n",
      ":0\r\n",
      ":10\r\n",
      
      ":0\r\n",
      ":10\r\n",
      
      ":0\r\n",
      ":10\r\n",
      
      ":9\r\n",
      ":1\r\n",
      ":9\r\n",
 
      ":10\r\n",
      ":0\r\n",
      ":10\r\n",
      
      ":7\r\n",
      ":3\r\n",
      ":7\r\n",
      
      ":8\r\n",
      ":2\r\n",
      ":8\r\n",
      
      ":6\r\n",
      ":4\r\n",
      ":6\r\n",
      
      ":7\r\n",
      ":3\r\n",
      ":7\r\n",
      
      ":5\r\n",
      ":5\r\n",
      ":5\r\n",
      
      ":6\r\n",
      ":4\r\n",
      ":6\r\n",
      
      ":4\r\n",
      ":6\r\n",
      ":4\r\n",
      
      ":5\r\n",
      ":5\r\n",
      ":5\r\n",
      
      ":3\r\n",
      ":7\r\n",
      ":3\r\n",
      
      ":4\r\n",
      ":6\r\n",
      ":4\r\n",
      
      ":2\r\n",
      ":8\r\n",
      ":2\r\n",
      
      ":3\r\n",
      ":7\r\n",
      ":3\r\n",
      
      ":1\r\n",
      ":9\r\n",
      ":1\r\n",
      
      ":2\r\n",
      ":8\r\n",
      ":2\r\n",
      
      ":0\r\n",
      ":10\r\n",
      
      ":1\r\n",
      ":9\r\n"
   };
  
  protected String[] invalidRequests = new String[] {
      "zremrangebylex x y",                           /* unsupported command */
      "ZREMRANGEBYLEX",                               /* wrong number of arguments*/
      "ZREMRANGEBYLEX key",                           /* wrong number of arguments*/
      "ZREMRANGEBYLEX key a",                         /* wrong number of arguments*/
      "ZREMRANGEBYLEX key a b c",                     /* wrong number of arguments*/
      "ZREMRANGEBYLEX key a b",                       /* wrong command format */
      "ZREMRANGEBYLEX key (a b",                      /* wrong command format */
      "ZREMRANGEBYLEX key + b",                       /* wrong command format */
      "ZREMRANGEBYLEX key [b -",                       /* wrong command format */

  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: zremrangebylex\r\n",
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
