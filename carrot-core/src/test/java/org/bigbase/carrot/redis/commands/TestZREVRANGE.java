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

public class TestZREVRANGE extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "ZADD key 9.0 c0 8.0 c1 7.0 c2 6.0 c3 5.0 c4",                       /* 5 */
      "ZADD key 4.0 c5 3.0 c6 2.0 c7 1.0 c8 0.0 c9",                       /* 5 */
      "ZCARD key",                                                         /* 10 */
      
      // Test ALL
      "ZREVRANGE key 0 -1",                                                   /* ALL */ 
      "ZREVRANGE key 0 9",                                                    /* ALL */
      "ZREVRANGE key 0 100",                                                  /* ALL */
      "ZREVRANGE key 0 -1 WITHSCORES",                                        /* ALL */
      "ZREVRANGE key 0 9 WITHSCORES",                                         /* ALL */
      "ZREVRANGE key 0 100 WITHSCORES",                                       /* ALL */
      
      "ZREVRANGE key 0 0",                                                    /* 1 */
      "ZREVRANGE key 6 6",                                                    /* 1 */
      "ZREVRANGE key 0 0 WITHSCORES",                                         /* 1 */
      "ZREVRANGE key 6 6 WITHSCORES",                                         /* 1 */ 
      
      "ZREVRANGE key 6 5",                                                    /* [] */
      "ZREVRANGE key 6 -6",                                                   /* [] */
      "ZREVRANGE key 10 100",                                                 /* [] */
      
      "ZREVRANGE key 6 5 WITHSCORES",                                         /* [] */
      "ZREVRANGE key 6 -6 WITHSCORES",                                        /* [] */
      "ZREVRANGE key 10 100 WITHSCORES",                                      /* [] */
      
      
      "ZREVRANGE key 2 5",                                                    /* 4 */
      "ZREVRANGE key 3 9",                                                    /* 7 */
      "ZREVRANGE key 2 5 WITHSCORES",                                         /* 4 */
      "zrevrange key 3 9 withscores"                                         /* 7 */
  };
  
  protected String[] validResponses = new String[] {
      ":5\r\n",
      ":5\r\n",
      ":10\r\n",
      
      "*10\r\n$2\r\nc0\r\n$2\r\nc1\r\n$2\r\nc2\r\n$2\r\nc3\r\n$2\r\nc4\r\n$2\r\nc5\r\n" +
      "$2\r\nc6\r\n$2\r\nc7\r\n$2\r\nc8\r\n$2\r\nc9\r\n",
      
      "*10\r\n$2\r\nc0\r\n$2\r\nc1\r\n$2\r\nc2\r\n$2\r\nc3\r\n$2\r\nc4\r\n$2\r\nc5\r\n" +
      "$2\r\nc6\r\n$2\r\nc7\r\n$2\r\nc8\r\n$2\r\nc9\r\n",
      
      "*10\r\n$2\r\nc0\r\n$2\r\nc1\r\n$2\r\nc2\r\n$2\r\nc3\r\n$2\r\nc4\r\n$2\r\nc5\r\n" +
      "$2\r\nc6\r\n$2\r\nc7\r\n$2\r\nc8\r\n$2\r\nc9\r\n",
      
      "*20\r\n$2\r\nc0\r\n$3\r\n9.0\r\n$2\r\nc1\r\n$3\r\n8.0\r\n$2\r\nc2\r\n$3\r\n7.0\r\n$2\r\nc3\r\n$3\r\n6.0\r\n"+
      "$2\r\nc4\r\n$3\r\n5.0\r\n$2\r\nc5\r\n$3\r\n4.0\r\n" +
      "$2\r\nc6\r\n$3\r\n3.0\r\n$2\r\nc7\r\n$3\r\n2.0\r\n" + 
      "$2\r\nc8\r\n$3\r\n1.0\r\n$2\r\nc9\r\n$3\r\n0.0\r\n",
      
      "*20\r\n$2\r\nc0\r\n$3\r\n9.0\r\n$2\r\nc1\r\n$3\r\n8.0\r\n$2\r\nc2\r\n$3\r\n7.0\r\n$2\r\nc3\r\n$3\r\n6.0\r\n"+
      "$2\r\nc4\r\n$3\r\n5.0\r\n$2\r\nc5\r\n$3\r\n4.0\r\n" +
      "$2\r\nc6\r\n$3\r\n3.0\r\n$2\r\nc7\r\n$3\r\n2.0\r\n" + 
      "$2\r\nc8\r\n$3\r\n1.0\r\n$2\r\nc9\r\n$3\r\n0.0\r\n",
      
      "*20\r\n$2\r\nc0\r\n$3\r\n9.0\r\n$2\r\nc1\r\n$3\r\n8.0\r\n$2\r\nc2\r\n$3\r\n7.0\r\n$2\r\nc3\r\n$3\r\n6.0\r\n"+
      "$2\r\nc4\r\n$3\r\n5.0\r\n$2\r\nc5\r\n$3\r\n4.0\r\n" +
      "$2\r\nc6\r\n$3\r\n3.0\r\n$2\r\nc7\r\n$3\r\n2.0\r\n" + 
      "$2\r\nc8\r\n$3\r\n1.0\r\n$2\r\nc9\r\n$3\r\n0.0\r\n",
          
      "*1\r\n$2\r\nc9\r\n",
      "*1\r\n$2\r\nc3\r\n",
      
      "*2\r\n$2\r\nc9\r\n$3\r\n0.0\r\n",
      "*2\r\n$2\r\nc3\r\n$3\r\n6.0\r\n",
      
      
      "*0\r\n",
      "*0\r\n",
      "*0\r\n",
      "*0\r\n",
      "*0\r\n",
      "*0\r\n",
      
      
      "*4\r\n$2\r\nc4\r\n$2\r\nc5\r\n$2\r\nc6\r\n$2\r\nc7\r\n",
      "*7\r\n$2\r\nc0\r\n$2\r\nc1\r\n$2\r\nc2\r\n$2\r\nc3\r\n$2\r\nc4\r\n$2\r\nc5\r\n$2\r\nc6\r\n",

      "*8\r\n$2\r\nc4\r\n$3\r\n5.0\r\n$2\r\nc5\r\n$3\r\n4.0\r\n$2\r\nc6\r\n$3\r\n3.0\r\n$2\r\nc7\r\n$3\r\n2.0\r\n",
      
      "*14\r\n$2\r\nc0\r\n$3\r\n9.0\r\n$2\r\nc1\r\n$3\r\n8.0\r\n$2\r\nc2\r\n$3\r\n7.0\r\n$2\r\nc3\r\n$3\r\n6.0\r\n"+
      "$2\r\nc4\r\n$3\r\n5.0\r\n$2\r\nc5\r\n$3\r\n4.0\r\n$2\r\nc6\r\n$3\r\n3.0\r\n"
   };
  
  protected String[] invalidRequests = new String[] {
      "zrevrang x y",                           /* unsupported command */
      "ZREVRANGE",                               /* wrong number of arguments*/
      "ZREVRANGE key",                           /* wrong number of arguments*/
      "ZREVRANGE key a",                         /* wrong number of arguments*/
      "ZREVRANGE key a b c d",                   /* wrong number of arguments*/
      "ZREVRANGE key a 10",                      /* wrong number format */
      "ZREVRANGE key 10 b",                      /* wrong number format */
      "ZREVRANGE key 10 20 WITHVALUES"           /* wrong command format */
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: ZREVRANG\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    
    "-ERR: Wrong number format: a\r\n",
    "-ERR: Wrong number format: b\r\n",
    "-ERR: Wrong command format, unexpected argument: WITHVALUES\r\n"

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
