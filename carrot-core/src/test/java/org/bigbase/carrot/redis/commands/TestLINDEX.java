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

public class TestLINDEX extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "LPUSH key v1 v2 v3 v4 v5 v6 v7 v8 v9 v10",                 /* 10 */
      "LINDEX key 0",                                             /* v10 */
      "LINDEX key 1",                                             /* v9 */
      "LINDEX key 2",                                             /* v8 */
      "LINDEX key 3",                                             /* v7 */
      "LINDEX key 4",                                             /* v6 */
      "LINDEX key 5",                                             /* v5 */
      "LINDEX key 6",                                             /* v4 */
      "LINDEX key 7",                                             /* v3 */
      "LINDEX key 8",                                             /* v2 */
      "LINDEX key 9",                                             /* v1 */
      "lindex key 10"                                            /* NULL */
  };
  
  protected String[] validResponses = new String[] {
      ":10\r\n",
      "$3\r\nv10\r\n",
      "$2\r\nv9\r\n",
      "$2\r\nv8\r\n",
      "$2\r\nv7\r\n",
      "$2\r\nv6\r\n",
      "$2\r\nv5\r\n",
      "$2\r\nv4\r\n",
      "$2\r\nv3\r\n",
      "$2\r\nv2\r\n",
      "$2\r\nv1\r\n",
      "$-1\r\n"
  };
  
  
  protected String[] invalidRequests = new String[] {
      "linde x y",                      /* unsupported command */
      "LINDEX",                          /* wrong number of arguments*/
      "LINDEX key",                      /* wrong number of arguments*/
      "LINDEX key a",                    /* wrong number format*/
      
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: LINDE\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number format: a\r\n"    
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
