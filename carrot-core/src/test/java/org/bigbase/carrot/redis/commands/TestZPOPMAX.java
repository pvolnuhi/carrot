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

public class TestZPOPMAX extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "ZADD key 10.0 c1 9.0 c2 8.0 c3 7.0 c4 6.0 c5",                         /* 5 */
      "ZADD key 5.0 c6 4.0 c7 3.0 c8 2.0 c9 1.0 c10",                         /* 5 */
      "ZCARD key",                                                            /* 10 */
      
      "ZPOPMAX key",                                                          /* c1 10.0 */ 
      "ZCARD key",                                                            /* 9 */
      "ZPOPMAX key",                                                          /* c2 9.0 */ 
      "ZCARD key",                                                            /* 8 */
      "ZPOPMAX key",                                                          /* c3 8.0 */ 
      "ZCARD key",                                                            /* 7 */
      "ZPOPMAX key",                                                          /* c4 7.0 */ 
      "ZCARD key",                                                            /* 6 */
      "ZPOPMAX key",                                                          /* c5 6.0 */ 
      "ZCARD key",                                                            /* 5 */
      "ZPOPMAX key",                                                          /* c6 5.0 */ 
      "ZCARD key",                                                            /* 4 */
      "ZPOPMAX key",                                                          /* c7 4.0 */ 
      "ZCARD key",                                                            /* 3 */
      "ZPOPMAX key",                                                          /* c8 3.0 */ 
      "ZCARD key",                                                            /* 2 */
      "ZPOPMAX key",                                                          /* c9 2.0 */ 
      "ZCARD key",                                                            /* 1 */
      "zpopmax key",                                                          /* c10 1.0 */ 
      
      "ZCARD key",                                                            /* 0 */
      
      "ZPOPMAX key",                                                          /* [] */
      "ZPOPMAX key1",                                                         /* [] */
      
      
      "ZADD key 10.0 c1 9.0 c2 8.0 c3 7.0 c4 6.0 c5",                         /* 5 */
      "ZADD key 5.0 c6 4.0 c7 3.0 c8 2.0 c9 1.0 c10",                         /* 5 */
      "ZPOPMAX key 11",                                                       /* ALL */
      "ZCARD key",                                                            /* 0 */
      "ZADD key 10.0 c1 9.0 c2 8.0 c3 7.0 c4 6.0 c5",                         /* 5 */
      "ZADD key 5.0 c6 4.0 c7 3.0 c8 2.0 c9 1.0 c10",                         /* 5 */ 
      
      "ZPOPMAX key 5",                                                        /* c1 - c5 */ 
      "ZCARD key"                                                             /* 5 */
  };
  
  protected String[] validResponses = new String[] {
      ":5\r\n",
      ":5\r\n",
      ":10\r\n",
      
      "*2\r\n$2\r\nc1\r\n$4\r\n10.0\r\n",
      ":9\r\n",
      "*2\r\n$2\r\nc2\r\n$3\r\n9.0\r\n",
      ":8\r\n",
      "*2\r\n$2\r\nc3\r\n$3\r\n8.0\r\n",
      ":7\r\n",
      "*2\r\n$2\r\nc4\r\n$3\r\n7.0\r\n",
      ":6\r\n",
      "*2\r\n$2\r\nc5\r\n$3\r\n6.0\r\n",
      ":5\r\n",
      "*2\r\n$2\r\nc6\r\n$3\r\n5.0\r\n",
      ":4\r\n",
      "*2\r\n$2\r\nc7\r\n$3\r\n4.0\r\n",
      ":3\r\n",
      "*2\r\n$2\r\nc8\r\n$3\r\n3.0\r\n",
      ":2\r\n",
      "*2\r\n$2\r\nc9\r\n$3\r\n2.0\r\n",
      ":1\r\n",
      "*2\r\n$3\r\nc10\r\n$3\r\n1.0\r\n",
      
      ":0\r\n",
      
      "*0\r\n",
      "*0\r\n",
      
      ":5\r\n",
      ":5\r\n",
      
      "*20\r\n$2\r\nc1\r\n$4\r\n10.0\r\n$2\r\nc2\r\n$3\r\n9.0\r\n$2\r\nc3\r\n$3\r\n8.0\r\n$2\r\nc4\r\n$3\r\n7.0\r\n$2\r\nc5\r\n$3\r\n6.0\r\n" +
      "$2\r\nc6\r\n$3\r\n5.0\r\n$2\r\nc7\r\n$3\r\n4.0\r\n$2\r\nc8\r\n$3\r\n3.0\r\n$2\r\nc9\r\n$3\r\n2.0\r\n$3\r\nc10\r\n$3\r\n1.0\r\n",
      
      ":0\r\n",
      ":5\r\n",
      ":5\r\n",
      "*10\r\n$2\r\nc1\r\n$4\r\n10.0\r\n$2\r\nc2\r\n$3\r\n9.0\r\n$2\r\nc3\r\n$3\r\n8.0\r\n$2\r\nc4\r\n$3\r\n7.0\r\n$2\r\nc5\r\n$3\r\n6.0\r\n",
      ":5\r\n"
   };
  
  protected String[] invalidRequests = new String[] {
      "zpopma x y",                           /* unsupported command */
      "ZPOPMAX",                               /* wrong number of arguments*/
      "ZPOPMAX key a b",                       /* wrong number of arguments*/
      "ZPOPMAX key a",                         /* wrong number format */
      "ZPOPMAX key -10"                        /* positive number expected*/
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: ZPOPMA\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number format: a\r\n",
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
