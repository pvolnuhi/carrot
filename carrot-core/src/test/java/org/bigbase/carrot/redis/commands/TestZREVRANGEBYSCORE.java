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

public class TestZREVRANGEBYSCORE extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "ZADD key 0.0 c1 1.0 c2 2.0 c3 3.0 c4 4.0 c5",                          /* 5 */
      "ZADD key 5.0 c6 6.0 c7 7.0 c8 8.0 c9 0.5 c10",                         /* 5 */
      "ZCARD key",                                                            /* 10 */
      
      "ZREVRANGEBYSCORE key -inf +inf",                                              /* ALL */ 
      "ZREVRANGEBYSCORE key -1.0 10.0",                                              /* ALL */
      "ZREVRANGEBYSCORE key -inf (10.0",                                             /* ALL */
      "ZREVRANGEBYSCORE key -100 (0.",                                               /* [] */
      "ZREVRANGEBYSCORE key (9.0 10.0",                                              /* [] */
      
      "ZREVRANGEBYSCORE key (0.0 9.9",                                               /* 9 */
      "ZREVRANGEBYSCORE key 0.0 9.9",                                                /* ALL */
      "ZREVRANGEBYSCORE key (1.0 9.9",                                               /* 7 */
      "ZREVRANGEBYSCORE key 1.0 9.9",                                                /* 8 */
      "ZREVRANGEBYSCORE key (2.0 9.9",                                               /* 6 */
      "ZREVRANGEBYSCORE key 2.0 9.9",                                                /* 7 */
      "ZREVRANGEBYSCORE key (3.0 9.9",                                               /* 5 */
      "ZREVRANGEBYSCORE key 3.0 9.9",                                                /* 6 */
      "ZREVRANGEBYSCORE key (4.0 9.9",                                               /* 4 */
      "ZREVRANGEBYSCORE key 4.0 9.9",                                                /* 5 */
      "ZREVRANGEBYSCORE key (5.0 9.9",                                               /* 3 */
      "ZREVRANGEBYSCORE key 5.0 9.9",                                                /* 4 */
      
      "ZREVRANGEBYSCORE key (6.0 9.9",                                               /* 2 */
      "ZREVRANGEBYSCORE key 6.0 9.9",                                                /* 3 */
      "ZREVRANGEBYSCORE key (7.0 9.9",                                               /* 1 */
      "ZREVRANGEBYSCORE key 7.0 9.9",                                                /* 2 */
      "ZREVRANGEBYSCORE key (8.0 9.9",                                               /* 0 */
      "ZREVRANGEBYSCORE key 8.0 9.9",                                                 /* 1 */
      
      "ZREVRANGEBYSCORE key (6.0 9.9 WITHSCORES",                                               /* 2 */
      "ZREVRANGEBYSCORE key 6.0 9.9 WITHSCORES",                                                /* 3 */
      "ZREVRANGEBYSCORE key (7.0 9.9 WITHSCORES",                                               /* 1 */
      "ZREVRANGEBYSCORE key 7.0 9.9 WITHSCORES",                                                /* 2 */
      "ZREVRANGEBYSCORE key (8.0 9.9 WITHSCORES",                                               /* 0 */
      "zrevrangebyscore key 8.0 9.9 withscores"                                                 /* 1 */
      
  };
  
  protected String[] validResponses = new String[] {
      ":5\r\n",
      ":5\r\n",
      ":10\r\n",
      
      "*10\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n$2\r\nc6\r\n$2\r\nc5\r\n$2\r\nc4\r\n" +
      "$2\r\nc3\r\n$2\r\nc2\r\n$3\r\nc10\r\n$2\r\nc1\r\n",
      
      "*10\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n$2\r\nc6\r\n$2\r\nc5\r\n$2\r\nc4\r\n" +
      "$2\r\nc3\r\n$2\r\nc2\r\n$3\r\nc10\r\n$2\r\nc1\r\n",
      
      "*10\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n$2\r\nc6\r\n$2\r\nc5\r\n$2\r\nc4\r\n" +
      "$2\r\nc3\r\n$2\r\nc2\r\n$3\r\nc10\r\n$2\r\nc1\r\n",
      
      "*0\r\n",
      "*0\r\n",

      "*9\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n$2\r\nc6\r\n$2\r\nc5\r\n" +
      "$2\r\nc4\r\n$2\r\nc3\r\n$2\r\nc2\r\n$3\r\nc10\r\n",
      
      "*10\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n$2\r\nc6\r\n$2\r\nc5\r\n$2\r\nc4\r\n" +
      "$2\r\nc3\r\n$2\r\nc2\r\n$3\r\nc10\r\n$2\r\nc1\r\n",
      
      
      "*7\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n" +
      "$2\r\nc6\r\n$2\r\nc5\r\n$2\r\nc4\r\n$2\r\nc3\r\n",
      
      "*8\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n$2\r\nc6\r\n" +
      "$2\r\nc5\r\n$2\r\nc4\r\n$2\r\nc3\r\n$2\r\nc2\r\n",
      
      "*6\r\n$2\r\nc9\r\n$2\r\nc8\r\n" +
      "$2\r\nc7\r\n$2\r\nc6\r\n$2\r\nc5\r\n$2\r\nc4\r\n",
      
      "*7\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n" +
      "$2\r\nc6\r\n$2\r\nc5\r\n$2\r\nc4\r\n$2\r\nc3\r\n",
      
      "*5\r\n$2\r\nc9\r\n" +
      "$2\r\nc8\r\n$2\r\nc7\r\n$2\r\nc6\r\n$2\r\nc5\r\n",
      
      "*6\r\n$2\r\nc9\r\n$2\r\nc8\r\n" +
      "$2\r\nc7\r\n$2\r\nc6\r\n$2\r\nc5\r\n$2\r\nc4\r\n",
      
      "*4\r\n" +
      "$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n$2\r\nc6\r\n",
      
      "*5\r\n$2\r\nc9\r\n" +
      "$2\r\nc8\r\n$2\r\nc7\r\n$2\r\nc6\r\n$2\r\nc5\r\n",
          
      "*3\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n",
      
      "*4\r\n" +
      "$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n$2\r\nc6\r\n",
      
      "*2\r\n$2\r\nc9\r\n$2\r\nc8\r\n",
      "*3\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n",

      "*1\r\n$2\r\nc9\r\n",
      "*2\r\n$2\r\nc9\r\n$2\r\nc8\r\n",

      "*0\r\n",
      "*1\r\n$2\r\nc9\r\n",
      
      "*4\r\n$2\r\nc9\r\n$3\r\n8.0\r\n$2\r\nc8\r\n$3\r\n7.0\r\n",
      "*6\r\n$2\r\nc9\r\n$3\r\n8.0\r\n$2\r\nc8\r\n$3\r\n7.0\r\n$2\r\nc7\r\n$3\r\n6.0\r\n",

      "*2\r\n$2\r\nc9\r\n$3\r\n8.0\r\n",
      "*4\r\n$2\r\nc9\r\n$3\r\n8.0\r\n$2\r\nc8\r\n$3\r\n7.0\r\n",

      "*0\r\n",
      "*2\r\n$2\r\nc9\r\n$3\r\n8.0\r\n"
   };
  
  protected String[] invalidRequests = new String[] {
      "zrevrangebyscare x y",                           /* unsupported command */
      "ZREVRANGEBYSCORE",                               /* wrong number of arguments*/
      "ZREVRANGEBYSCORE key",                           /* wrong number of arguments*/
      "ZREVRANGEBYSCORE key a",                         /* wrong number of arguments*/
      "ZREVRANGEBYSCORE key a b c d",                   /* wrong number of arguments*/
      "ZREVRANGEBYSCORE key a 0",                       /* wrong number format */
      "ZREVRANGEBYSCORE key 0 b",                       /* wrong number format */
      "ZREVRANGEBYSCORE key 0 0 WITH",                  /* wrong command format */

  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: ZREVRANGEBYSCARE\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    
    "-ERR: Wrong number format: a\r\n",
    "-ERR: Wrong number format: b\r\n",
    
    "-ERR: Wrong command format, unexpected argument: WITH\r\n"
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
