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

public class TestRPOP extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "LPUSH key v1 v2 v3 v4 v5 v6 v7 v8 v9 v10",                 /* 10 */
      "LLEN key",                                                 /* 10 */
      
      "RPOP key",                                                 /* v1 */
      "LLEN key",                                                  /* 9 */      
      "RPOP key",                                                 /* v2 */
      "LLEN key",                                                  /* 8 */      
      "RPOP key",                                                 /* v3 */
      "LLEN key",                                                  /* 7 */      
      "RPOP key",                                                 /* v4 */
      "LLEN key",                                                  /* 6 */      
      "RPOP key",                                                 /* v5 */
      "LLEN key",                                                  /* 5 */      
      "RPOP key",                                                 /* v6 */
      "LLEN key",                                                  /* 4 */      
      "RPOP key",                                                 /* v7 */
      "LLEN key",                                                  /* 3 */      
      "RPOP key",                                                 /* v8 */
      "LLEN key",                                                  /* 2 */      
      "RPOP key",                                                 /* v9 */
      "LLEN key",                                                  /* 1 */      
      "RPOP key",                                                 /* v10 */
      "LLEN key",                                                  /* 0 */
      
      "rpop key"                                                  /* NULL */
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
      "rpo x y",                      /* unsupported command */
      "RPOP",                          /* wrong number of arguments*/
      "RPOP key value",                /* wrong number of arguments*/      
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: RPO\r\n",
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
