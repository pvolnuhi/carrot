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

public class TestZREM extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "ZADD key 1234 v1 1.9E-6 v2 0.123 v3 12.15 v4",                         /* 4 */
      "ZADD key 1234 v1 1.9E-6 v2 0.123 v5 12.15 v6",                         /* 2 */
      "ZCARD key",                                                            /* 6 */
      "ZREM key v1 v11 v22",                                                  /* 1 */ 
      "ZCARD key",                                                            /* 5 */
      
      "ZREM key v1 v2 v3 v33",                                                /* 2 */
      "ZCARD key",                                                            /* 3 */
      
      "ZREM key v3 v4 v44 v444",                                              /* 1 */
      "ZCARD key",                                                            /* 2 */
      
      "ZREM key v5 v6 v66",                                                   /* 2 */
      "ZCARD key",                                                            /* 0 */
      
      "zrem key1 v1 v2",                                                      /* 0 */

  };
  
  protected String[] validResponses = new String[] {
      ":4\r\n",
      ":2\r\n",
      ":6\r\n",
      ":1\r\n",
      ":5\r\n",

      ":2\r\n",
      ":3\r\n",

      ":1\r\n",
      ":2\r\n",

      ":2\r\n",
      ":0\r\n",
      ":0\r\n"

  };
  
  protected String[] invalidRequests = new String[] {
      "zremm x y",                          /* unsupported command */
      "ZREM key"                          /* wrong number of arguments*/
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: ZREMM\r\n",
    "-ERR: Wrong number of arguments\r\n",
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
