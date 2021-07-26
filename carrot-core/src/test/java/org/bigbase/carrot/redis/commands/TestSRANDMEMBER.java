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

public class TestSRANDMEMBER extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "SADD key v0 v1 v2 v3 v4 v5 v6 v7 v8 v9",        /* 10 */
      "SRANDMEMBER key 10",                            /* all */
      "SRANDMEMBER key 100",                           /* all */
      "SRANDMEMBER key 0",                             /* ERR - Positive number expected */
      "SRANDMEMBER key -1",                            /*  one random member */
      "SRANDMEMBER key -10",                           /*  ten random members */
      "SRANDMEMBER key1 2",                            /* empty array*/
      "SRANDMEMBER key",                               /* one random member */
      "SRANDMEMBER key 5",                             /* five random members */
      "SCARD key"                                      /* 10 */ 
  };
  
  protected String[] validResponses = new String[] {
      ":10\r\n",
      "*10\r\n$2\r\nv0\r\n$2\r\nv1\r\n$2\r\nv2\r\n$2\r\nv3\r\n$2\r\nv4\r\n$2\r\nv5\r\n" +
      "$2\r\nv6\r\n$2\r\nv7\r\n$2\r\nv8\r\n$2\r\nv9\r\n",
      "*10\r\n$2\r\nv0\r\n$2\r\nv1\r\n$2\r\nv2\r\n$2\r\nv3\r\n$2\r\nv4\r\n$2\r\nv5\r\n" +
          "$2\r\nv6\r\n$2\r\nv7\r\n$2\r\nv8\r\n$2\r\nv9\r\n",
      "-ERR: Positive number expected: 0\r\n",
      SKIP_VERIFY,
      SKIP_VERIFY,
      "*0\r\n",
      SKIP_VERIFY,
      SKIP_VERIFY,
      ":10\r\n"
  };
  
  protected String[] invalidRequests = new String[] {
      "srandmember x",                     /* unsupported command */
      "SRANDMEMBER",                       /* Wrong number of arguments*/
      "SRANDMEMBER x 0 0",                 /* Wrong number of arguments */
      "SRANDMEMBER x x",                   /* Wrong number format*/
      "SRANDMEMBER key 0"                  /* Positive number expected */
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: srandmember\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",    
    "-ERR: Wrong number format: x\r\n",
    "-ERR: Positive number expected: 0\r\n"
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
