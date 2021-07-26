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

public class TestZRANK extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "ZADD key 10.0 v1 9.0 v2 8.0 v3 7.0 v4 6.0 v5",                         /* 5 */
      "ZADD key 5.0 v6 4.0 v7 3.0 v8 2.0 v9 1.0 v10",                         /* 5 */
      "ZCARD key",                                                            /* 10 */
      "ZRANK key v1",                                                         /* 9 */ 
      "ZRANK key v2",                                                         /* 8 */ 
      "ZRANK key v3",                                                         /* 7 */ 
      "ZRANK key v4",                                                         /* 6 */ 
      "ZRANK key v5",                                                         /* 5 */ 
      "ZRANK key v6",                                                         /* 4 */ 
      "ZRANK key v7",                                                         /* 3 */ 
      "ZRANK key v8",                                                         /* 2 */ 
      "ZRANK key v9",                                                         /* 1 */ 
      "ZRANK key v10",                                                        /* 0 */ 
      "ZRANK key v100",                                                       /* NULL */ 
      "ZRANK key1 v10"                                                       /* NULL */ 
  };
  
  protected String[] validResponses = new String[] {
      ":5\r\n",
      ":5\r\n",
      ":10\r\n",
      ":9\r\n",
      ":8\r\n",
      ":7\r\n",
      ":6\r\n",
      ":5\r\n",
      ":4\r\n",
      ":3\r\n",
      ":2\r\n",
      ":1\r\n",
      ":0\r\n",
      "$-1\r\n",
      "$-1\r\n"
  };
  
  protected String[] invalidRequests = new String[] {
      "zrank x y",                          /* unsupported command */
      "ZRANK",                              /* wrong number of arguments*/
      "ZRANK key",                          /* wrong number of arguments*/
      "ZRANK key a b"                      /* wrong number of arguments*/
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: zrank\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n"
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
