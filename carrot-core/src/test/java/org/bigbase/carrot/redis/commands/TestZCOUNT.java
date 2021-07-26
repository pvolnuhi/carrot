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

public class TestZCOUNT extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "ZADD key 10.0 v1 9.0 v2 8.0 v3 7.0 v4 6.0 v5",                         /* 5 */
      "ZADD key 5.0 v6 4.0 v7 3.0 v8 2.0 v9 1.0 v10",                         /* 5 */
      "ZCARD key",                                                            /* 10 */
      "ZCOUNT key -inf +inf",                                                 /* 10 */ 
      "ZCOUNT key +inf -inf",                                                 /* 0 */
      "ZCOUNT key -inf 1.0",                                                  /* 1 */ 
      "ZCOUNT key -inf (1.0",                                                 /* 0 */ 
      "ZCOUNT key -1.0 10.0",                                                 /* 10 */ 
      "ZCOUNT key -1.0 (10.0",                                                /* 9 */ 
      "ZCOUNT key 3.0 (10.0",                                                 /* 7 */ 
      "ZCOUNT key 3.0 10.0",                                                 /* 8 */ 
      "ZCOUNT key (3.0 (10.0",                                                /* 6 */ 
      "ZCOUNT key (3.0 10.0"                                                  /* 7 */ 
           
  };
  
  protected String[] validResponses = new String[] {
      ":5\r\n",
      ":5\r\n",
      ":10\r\n",
      ":10\r\n",
      
      ":0\r\n",
      ":1\r\n",
      ":0\r\n",
      ":10\r\n",
      ":9\r\n",
      ":7\r\n",
      ":8\r\n",
      ":6\r\n",
      ":7\r\n"
   };
  
  protected String[] invalidRequests = new String[] {
      "zcount x y",                           /* unsupported command */
      "ZCOUNT",                               /* wrong number of arguments*/
      "ZCOUNT key",                           /* wrong number of arguments*/
      "ZCOUNT key a",                         /* wrong number of arguments*/
      "ZCOUNT key a",                         /* wrong number of arguments*/
      "ZCOUNT key a b c",                     /* wrong number of arguments*/
      "ZCOUNT key a 1",                       /* wrong number format */
      "ZCOUNT key 1 b"                        /* wrong number format */

  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: zcount\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number format: a\r\n",
    "-ERR: Wrong number format: b\r\n"
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
