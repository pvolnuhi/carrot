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

public class TestLREM extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "RPUSH key v1 v2 v1 v2 v1 v2 v1 v2 v1 v2",               /* 10 */
      // Test negatives
      "LREM key 0 v3",                                         /* 0 */
      "LREM key 2 v3",                                         /* 0 */
      "LREM key -2 v3",                                        /* 0 */
      // Test remove ALL
      "LREM key 0 v1",                                         /* 5 */
      "LREM key 0 v2",                                         /* 5 */
      // Test NULL key
      "LREM key 0 v1",                                         /* 0 */
      "LREM key 0 v2",                                         /* 0 */
      "LREM key1 0 v1",                                        /* 0 */
      
      "RPUSH key v1 v2 v1 v2 v1 v2 v1 v2 v1 v2",               /* 10 */
      
      // Test remove from head
      "LREM key 2 v1",                                         /* 2 */
      "LLEN key",                                              /* 8 */
      
      "LINDEX key 0",                                          /* v2 */
      "LINDEX key 1",                                          /* v2 */
      "LINDEX key 2",                                          /* v1 */
      "LINDEX key 3",                                          /* v2 */
      "LINDEX key 4",                                          /* v1 */
      "LINDEX key 5",                                          /* v2 */
      "LINDEX key 6",                                          /* v1 */
      "LINDEX key 7",                                          /* v2 */
     
      "LREM key -3 v1",                                        /* 3 */
      "LLEN key",                                              /* 5 */
      
      "LINDEX key 0",                                          /* v2 */
      "LINDEX key 1",                                          /* v2 */
      "LINDEX key 2",                                          /* v2 */
      "LINDEX key 3",                                          /* v2 */
      "LINDEX key 4",                                          /* v2 */
      
      "LREM key -5 v2",                                        /* 5 */
      "LLEN key",                                              /* 0 */
      "LINDEX key 0"                                          /* NULL */
  };
  
  protected String[] validResponses = new String[] {
      ":10\r\n",
      
      ":0\r\n",
      ":0\r\n",
      ":0\r\n",
   
      ":5\r\n",
      ":5\r\n",
      
      ":0\r\n",
      ":0\r\n",
      ":0\r\n",

      ":10\r\n",

      ":2\r\n",
      ":8\r\n",
      
      "$2\r\nv2\r\n",
      "$2\r\nv2\r\n",
      "$2\r\nv1\r\n",
      "$2\r\nv2\r\n",
      "$2\r\nv1\r\n",
      "$2\r\nv2\r\n",
      "$2\r\nv1\r\n",
      "$2\r\nv2\r\n",
      
      ":3\r\n",
      ":5\r\n",
      
      "$2\r\nv2\r\n",
      "$2\r\nv2\r\n",
      "$2\r\nv2\r\n",
      "$2\r\nv2\r\n",
      "$2\r\nv2\r\n",
      
      ":5\r\n",
      ":0\r\n",
      
      "$-1\r\n"

  };
  
  
  protected String[] invalidRequests = new String[] {
      "lrem x y",                      /* unsupported command */
      "LREM",                          /* wrong number of arguments*/
      "LREM key",                      /* wrong number of arguments*/
      "LREM key value",                /* wrong number of arguments*/
      "LREM key value a b",            /* wrong number of arguments*/

      "LREM key ZZZ value",            /* wrong number format */

  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: lrem\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number format: ZZZ\r\n",
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
