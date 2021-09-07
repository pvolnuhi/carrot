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

public class TestSPOP extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "SADD key v0 v1 v2 v3 v4 v5 v6 v7 v8 v9",        /* 10 */
      "SPOP key 10",
      "SCARD key",                                     /* 0 */ 
      "SADD key v0 v1 v2 v3 v4 v5 v6 v7 v8 v9",        /* 10 */
      "SPOP key 100",
      "SCARD key",                                     /* 0 */ 
      "SADD key v0 v1 v2 v3 v4 v5 v6 v7 v8 v9",        /* 10 */
      "SPOP key 0",                                    /* ERR - Positive number expected */
      "SPOP key -1",                                   /* ERR - Positive number expected */
      "SPOP key1 2",                                   /* empty array*/
      "SCARD key",                                     /* 10 */ 
      "SPOP key",
      "SCARD key",                                     /* 9 */ 
      "spop key 5",
      "SCARD key",                                     /* 4 */ 

  };
  
  protected String[] validResponses = new String[] {
      ":10\r\n",
      "*10\r\n$2\r\nv0\r\n$2\r\nv1\r\n$2\r\nv2\r\n$2\r\nv3\r\n$2\r\nv4\r\n$2\r\nv5\r\n" +
      "$2\r\nv6\r\n$2\r\nv7\r\n$2\r\nv8\r\n$2\r\nv9\r\n",
      ":0\r\n",
      ":10\r\n",
      "*10\r\n$2\r\nv0\r\n$2\r\nv1\r\n$2\r\nv2\r\n$2\r\nv3\r\n$2\r\nv4\r\n$2\r\nv5\r\n" +
          "$2\r\nv6\r\n$2\r\nv7\r\n$2\r\nv8\r\n$2\r\nv9\r\n",
      ":0\r\n",
      ":10\r\n",
      "-ERR: Positive number expected: 0\r\n",
      "-ERR: Positive number expected: -1\r\n",
      "*0\r\n",
      ":10\r\n",
      SKIP_VERIFY,
      ":9\r\n",
      SKIP_VERIFY,      
      ":4\r\n"
  };
  
  protected String[] invalidRequests = new String[] {
      "spo x",                     /* unsupported command */
      "SPOP",                       /* Wrong number of arguments*/
      "SPOP x 0 0",                 /* Wrong number of arguments */
      "SPOP x x",                   /* Wrong number format*/
      "SPOP x 0",                   /* Wrong number format */
      "SPOP x -10"                  /* Wrong number format */
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: SPO\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",    
    "-ERR: Wrong number format: x\r\n",
    "-ERR: Positive number expected: 0\r\n",
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
