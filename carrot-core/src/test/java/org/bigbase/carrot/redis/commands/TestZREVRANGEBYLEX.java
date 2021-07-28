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

public class TestZREVRANGEBYLEX extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "ZADD key 0 c1 0 c2 0 c3 0 c4 0 c5",                          /* 5 */
      "ZADD key 0 c6 0 c7 0 c8 0 c9 0 c10",                         /* 5 */
      "ZCARD key",                                                            /* 10 */
      
      "ZREVRANGEBYLEX key - +",                                                    /* ALL */ 
      "ZREVRANGEBYLEX key - (d",                                                   /* ALL */
      "ZREVRANGEBYLEX key - [d",                                                   /* ALL */
      "ZREVRANGEBYLEX key - [c",                                                   /* [] */
      "ZREVRANGEBYLEX key - (c",                                                   /* [] */
      
      "ZREVRANGEBYLEX key (c1 [c99",                                               /* 9 */
      "ZREVRANGEBYLEX key [c1 [c99",                                               /* ALL */
      "ZREVRANGEBYLEX key (c2 [c99",                                               /* 7 */
      "ZREVRANGEBYLEX key [c2 [c99",                                               /* 8 */
      "ZREVRANGEBYLEX key (c3 [c99",                                               /* 6 */
      "ZREVRANGEBYLEX key [c3 [c99",                                               /* 7 */
      "ZREVRANGEBYLEX key (c4 [c99",                                               /* 5 */
      "ZREVRANGEBYLEX key [c4 [c99",                                               /* 6 */
      "ZREVRANGEBYLEX key (c5 [c99",                                               /* 4 */
      "ZREVRANGEBYLEX key [c5 [c99",                                               /* 5 */
      "ZREVRANGEBYLEX key (c6 [c99",                                               /* 3 */
      "ZREVRANGEBYLEX key [c6 [c99",                                               /* 4 */
      "ZREVRANGEBYLEX key (c7 [c99",                                               /* 2 */
      "ZREVRANGEBYLEX key [c7 [c99",                                               /* 3 */
      "ZREVRANGEBYLEX key (c8 [c99",                                               /* 1 */
      "ZREVRANGEBYLEX key [c8 [c99",                                               /* 2 */
      "ZREVRANGEBYLEX key (c9 [c99",                                               /* 0 */
      "ZREVRANGEBYLEX key [c9 [c99",                                               /* 1 */
  };
  
  protected String[] validResponses = new String[] {
      ":5\r\n",
      ":5\r\n",
      ":10\r\n",
      
      "*10\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n$2\r\nc6\r\n$2\r\nc5\r\n$2\r\nc4\r\n" +
      "$2\r\nc3\r\n$2\r\nc2\r\n$3\r\nc10\r\n$2\r\nc1\r\n",
      
      "*10\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n$2\r\nc6\r\n$2\r\nc5\r\n$2\r\nc4\r\n" +
      "$2\r\nc3\r\n$2\r\nc2\r\n$3\r\nc10\r\n$2\r\nc1\r\n",
      
      "*10\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n$2\r\nc6\r\n$2\r\nc5\r\n$2\r\nc4\r\n" +
      "$2\r\nc3\r\n$2\r\nc2\r\n$3\r\nc10\r\n$2\r\nc1\r\n",
      
      "*0\r\n",
      "*0\r\n",

      "*9\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n$2\r\nc6\r\n$2\r\nc5\r\n" +
      "$2\r\nc4\r\n$2\r\nc3\r\n$2\r\nc2\r\n$3\r\nc10\r\n",
      
      "*10\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n$2\r\nc6\r\n$2\r\nc5\r\n$2\r\nc4\r\n" +
      "$2\r\nc3\r\n$2\r\nc2\r\n$3\r\nc10\r\n$2\r\nc1\r\n",
      
      "*7\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n" +
      "$2\r\nc6\r\n$2\r\nc5\r\n$2\r\nc4\r\n$2\r\nc3\r\n",
      
      "*8\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n" +
      "$2\r\nc6\r\n$2\r\nc5\r\n$2\r\nc4\r\n$2\r\nc3\r\n$2\r\nc2\r\n",
      
      "*6\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n" +
      "$2\r\nc6\r\n$2\r\nc5\r\n$2\r\nc4\r\n",
      
      "*7\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n" +
      "$2\r\nc6\r\n$2\r\nc5\r\n$2\r\nc4\r\n$2\r\nc3\r\n",
      
      "*5\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n" +
      "$2\r\nc6\r\n$2\r\nc5\r\n",      
      
      "*6\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n" +
      "$2\r\nc6\r\n$2\r\nc5\r\n$2\r\nc4\r\n",
      
      "*4\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n" +
      "$2\r\nc6\r\n",        
      
      "*5\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n" +
      "$2\r\nc6\r\n$2\r\nc5\r\n",  
      
      "*3\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n", 
      
      "*4\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n" +
      "$2\r\nc6\r\n", 
      
      "*2\r\n$2\r\nc9\r\n$2\r\nc8\r\n", 
      "*3\r\n$2\r\nc9\r\n$2\r\nc8\r\n$2\r\nc7\r\n", 

      "*1\r\n$2\r\nc9\r\n",
      "*2\r\n$2\r\nc9\r\n$2\r\nc8\r\n", 

      "*0\r\n",
      "*1\r\n$2\r\nc9\r\n"
   };
  
  protected String[] invalidRequests = new String[] {
      "zrevrangebylex x y",                           /* unsupported command */
      "ZREVRANGEBYLEX",                               /* wrong number of arguments*/
      "ZREVRANGEBYLEX key",                           /* wrong number of arguments*/
      "ZREVRANGEBYLEX key a",                         /* wrong number of arguments*/
      "ZREVRANGEBYLEX key a b c",                     /* wrong number of arguments*/
      "ZREVRANGEBYLEX key a b",                       /* wrong command format */
      "ZREVRANGEBYLEX key (a b",                      /* wrong command format */
      "ZREVRANGEBYLEX key + b",                       /* wrong command format */
      "ZREVRANGEBYLEX key [b -",                       /* wrong command format */

  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: zrevrangebylex\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    
    "-ERR: Wrong command format, unexpected argument: " + Errors.ERR_MIN_SPECIFIED +"\r\n",
    "-ERR: Wrong command format, unexpected argument: " + Errors.ERR_MAX_SPECIFIED +"\r\n",
    "-ERR: Wrong command format, unexpected argument: " + Errors.ERR_MIN_SPECIFIED +"\r\n",
    "-ERR: Wrong command format, unexpected argument: " + Errors.ERR_MAX_SPECIFIED +"\r\n"

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
