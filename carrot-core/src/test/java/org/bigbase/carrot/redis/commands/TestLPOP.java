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

public class TestLPOP extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "RPUSH key v1 v2 v3 v4 v5 v6 v7 v8 v9 v10",                 /* 10 */
      "LLEN key",                                                 /* 10 */
      
      "LPOP key",                                                 /* v1 */
      "LLEN key",                                                  /* 9 */      
      "LPOP key",                                                 /* v2 */
      "LLEN key",                                                  /* 8 */      
      "LPOP key",                                                 /* v3 */
      "LLEN key",                                                  /* 7 */      
      "LPOP key",                                                 /* v4 */
      "LLEN key",                                                  /* 6 */      
      "LPOP key",                                                 /* v5 */
      "LLEN key",                                                  /* 5 */      
      "LPOP key",                                                 /* v6 */
      "LLEN key",                                                  /* 4 */      
      "LPOP key",                                                 /* v7 */
      "LLEN key",                                                  /* 3 */      
      "LPOP key",                                                 /* v8 */
      "LLEN key",                                                  /* 2 */      
      "LPOP key",                                                 /* v9 */
      "LLEN key",                                                  /* 1 */      
      "LPOP key",                                                 /* v10 */
      "LLEN key",                                                  /* 0 */
      
      "LPOP key"                                                  /* NULL */
  }; 
  
  protected String[] validResponses = new String[] {
      ":10\r\n",
      ":10\r\n",
      
      "$2\r\nv1\r\n",
      ":9\r\n",
      "$2\r\nv2\r\n",
      ":8\r\n",
      "$2\r\nv3\r\n",
      ":7\r\n",
      "$2\r\nv4\r\n",
      ":6\r\n",
      "$2\r\nv5\r\n",
      ":5\r\n",
      "$2\r\nv6\r\n",
      ":4\r\n",
      "$2\r\nv7\r\n",
      ":3\r\n",
      "$2\r\nv8\r\n",
      ":2\r\n",
      "$2\r\nv9\r\n",
      ":1\r\n",
      "$3\r\nv10\r\n",
      ":0\r\n",

      "$-1\r\n",
   
  };
  
  
  protected String[] invalidRequests = new String[] {
      "lpop x y",                      /* unsupported command */
      "LPOP",                          /* wrong number of arguments*/
      "LPOP key value",                /* wrong number of arguments*/      
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: lpop\r\n",
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
