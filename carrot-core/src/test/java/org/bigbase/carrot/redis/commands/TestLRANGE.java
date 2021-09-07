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

public class TestLRANGE extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "RPUSH key v0 v1 v2 v3 v4 v5 v6 v7 v8 v9",               /* 10 */
      // Test1 same start and end
      "LRANGE key 0 0",                                        /* array=1: v0 */
      "LRANGE key 9 9",                                        /* array=1: v9 */
      "LRANGE key 5 5",                                        /* array=1: v5 */
      "LRANGE key -2 -2",                                      /* array=1: v8 */

      // Test2 end > start
      "LRANGE key 5 4",                                        /* array=0:*/
      "LRANGE key 50 40",                                      /* array=0:*/
      
      // Test3 - ALL
      "LRANGE key 0 -1",                                       /* array=10: all*/

      // Test4  - sublist
      "LRANGE key 2 -2",                                       /* array=7: v2 - v8*/
      "LRANGE key 3 7",                                        /* array=5: v3 - v7*/
      
      // Test5 - out of range
      "LRANGE key -12 -11",                                    /* array=0: */
      "lrange key 11 100"                                      /* array=0: */

  };
  
  protected String[] validResponses = new String[] {
      ":10\r\n",
      
      "*1\r\n$2\r\nv0\r\n",
      "*1\r\n$2\r\nv9\r\n",
      "*1\r\n$2\r\nv5\r\n",
      "*1\r\n$2\r\nv8\r\n",
            
      "*0\r\n",
      "*0\r\n",

      "*10\r\n$2\r\nv0\r\n$2\r\nv1\r\n$2\r\nv2\r\n$2\r\nv3\r\n$2\r\nv4\r\n" +
      "$2\r\nv5\r\n$2\r\nv6\r\n$2\r\nv7\r\n$2\r\nv8\r\n$2\r\nv9\r\n",
      
      "*7\r\n$2\r\nv2\r\n$2\r\nv3\r\n$2\r\nv4\r\n" +
          "$2\r\nv5\r\n$2\r\nv6\r\n$2\r\nv7\r\n$2\r\nv8\r\n",
      "*5\r\n$2\r\nv3\r\n$2\r\nv4\r\n" +
          "$2\r\nv5\r\n$2\r\nv6\r\n$2\r\nv7\r\n",
      "*0\r\n",
      "*0\r\n"
  };
  
  
  protected String[] invalidRequests = new String[] {
      "lrang x y",                      /* unsupported command */
      "LRANGE",                          /* wrong number of arguments*/
      "LRANGE key",                      /* wrong number of arguments*/
      "LRANGE key value",                /* wrong number of arguments*/
      "LRANGE key value a b",            /* wrong number of arguments*/

      "LRANGE key ZZZ 10",               /* wrong number format */
      "LRANGE key 10 YYY"                /* wrong number format */

  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: LRANG\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number format: ZZZ\r\n",
    "-ERR: Wrong number format: YYY\r\n",

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
