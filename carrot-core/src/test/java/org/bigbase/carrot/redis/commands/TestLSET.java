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

public class TestLSET extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "RPUSH key v1 v2 v3 v4 v5 v6 v7 v8 v9 v10",                 /* 10 */
      "LSET key 0 v0",                                            /* OK */      
      "LSET key 9 v0",                                            /* OK */
      "LSET key 5 v0",                                            /* OK */
      
      "LINDEX key 0",                                             /* v0 */
      "LINDEX key 1",                                             /* v2 */
      "LINDEX key 2",                                             /* v3 */
      "LINDEX key 3",                                             /* v4 */
      "LINDEX key 4",                                             /* v5 */
      "LINDEX key 5",                                             /* v0 */
      "LINDEX key 6",                                             /* v7 */
      "LINDEX key 7",                                             /* v8 */
      "LINDEX key 8",                                             /* v9*/
      "LINDEX key 9",                                             /* v0 */
      
      "LSET key 10 vv",                                            /* ERR */
      "LSET key 11 vv",                                            /* ERR */
      "lset key 12 vv"                                             /* ERR */
  };
  
  protected String[] validResponses = new String[] {
      ":10\r\n",
      "+OK\r\n",
      "+OK\r\n",
      "+OK\r\n",

      "$2\r\nv0\r\n",
      "$2\r\nv2\r\n",
      "$2\r\nv3\r\n",
      "$2\r\nv4\r\n",
      "$2\r\nv5\r\n",
      "$2\r\nv0\r\n",
      "$2\r\nv7\r\n",
      "$2\r\nv8\r\n",
      "$2\r\nv9\r\n",
      "$2\r\nv0\r\n",
      
      "-ERR: Index is out of range or key does not exist\r\n",
      "-ERR: Index is out of range or key does not exist\r\n",
      "-ERR: Index is out of range or key does not exist\r\n",
      
  };
  
  
  protected String[] invalidRequests = new String[] {
      "lse x y",                     /* unsupported command */
      "LSET",                         /* wrong number of arguments*/
      "LSET x",                       /* wrong number of arguments*/
      "LSET x y",                     /* wrong number of arguments*/
      "LSET x y x y",                 /* wrong number of arguments*/
      "LSET x y z"                    /* wrong number format */
      
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: LSE\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",    
    "-ERR: Wrong number of arguments\r\n",    
    "-ERR: Wrong number format: y\r\n"    
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
