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

public class TestSETRANGE extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "SETRANGE key 0 1234567890",                  /* 10 */
      "STRLEN key",                                 /* 10 */ 
      "GET key",                                    /*1234567890*/
      "SETRANGE key 10 1234567890",                 /* 20 */
      "STRLEN key",                                 /* 20 */
      "SETRANGE key 0 0",                           /* 02345678901234567890*/
      "SETRANGE key 1 0",                           /* 00345678901234567890*/
      "GET key",                                    /* 00345678901234567890*/ 
      "setrange key 2 00000000",                    /* 00000000001234567890*/
      "GET key",                                    /* 00000000001234567890*/
      "SETRANGE key 100 1234567890",                /* 110 */
      "GETRANGE key 100 -1",                        /* 1234567890 */
      "STRLEN key"                                  /* 110 */
  };
  
  protected String[] validResponses = new String[] {
      ":10\r\n",
      ":10\r\n",
      "$10\r\n1234567890\r\n",
      ":20\r\n",
      ":20\r\n",
      ":20\r\n",
      ":20\r\n",

      "$20\r\n00345678901234567890\r\n",
      
      ":20\r\n",
      "$20\r\n00000000001234567890\r\n",
      ":110\r\n",
      
      "$10\r\n1234567890\r\n",
      ":110\r\n"

  };
  
  
  protected String[] invalidRequests = new String[] {
      "setrang x y",                         /* unsupported command */
      "SETRANGE",                             /* wrong number of arguments*/
      "SETRANGE key",                         /* wrong number of arguments*/
      "SETRANGE key 1",                       /* wrong number of arguments*/
      "SETRANGE key 1 2 3",                   /* wrong number of arguments*/
      "SETRANGE key A 2",                     /* wrong number format*/      
      "SETRANGE key -2 2"                     /* positive number expected */      

  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: SETRANG\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number format: A\r\n",
    "-ERR: Positive number expected: -2\r\n"
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
