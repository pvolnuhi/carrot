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

public class TestGETRANGE extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "SET key 1234567890",                         /* OK */
      "GETRANGE key 0 -1",                          /* 1234567890*/
      "GETRANGE key 1 -2",                          /* 23456789*/

      "GETRANGE key 0 0",                           /* 1 */
      "GETRANGE key 0 100",                         /* 1234567890*/
      "GETRANGE key 5 100",                         /* 67890*/
      "GETRANGE key 20 100",                        /* NULL */
      "GETRANGE key 10 9",                          /* NULL */
      "GETRANGE key -10 -10",                       /* 1 */
      "GETRANGE key -10 -9",                        /* 12 */
      "getrange key -11 -12"                        /* NULL */
  };
  
  protected String[] validResponses = new String[] {
      "+OK\r\n",
      "$10\r\n1234567890\r\n",
      "$8\r\n23456789\r\n",
      "$1\r\n1\r\n",
      "$10\r\n1234567890\r\n",
      "$5\r\n67890\r\n",
      "$-1\r\n",
      "$-1\r\n",
      "$1\r\n1\r\n",
      "$2\r\n12\r\n",
      "$-1\r\n"
  };
  
  
  protected String[] invalidRequests = new String[] {
      "GETRANGE",                             /* wrong number of arguments*/
      "GETRANGE key",                         /* wrong number of arguments*/
      "GETRANGE key 1",                       /* wrong number of arguments*/
      "GETRANGE key 1 2 3",                   /* wrong number of arguments*/
      "GETRANGE key A 2",                     /* wrong number format*/
      "GETRANGE key 1 B"                      /* wrong number format*/
      
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number format: A\r\n",
    "-ERR: Wrong number format: B\r\n"
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
