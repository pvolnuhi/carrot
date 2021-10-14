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

public class TestZRANDMEMBER extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "ZADD key 0.0 c0 1.0 c1 2.0 c2 3.0 c3 4.0 c4",                       /* 5 */
      "ZADD key 5.0 c5 6.0 c6 7.0 c7 8.0 c8 9.0 c9",                       /* 5 */
      "ZCARD key",                                                         /* 10 */
      
      "zrandmember key", 
      "ZRANDMEMBER key", 
      "ZRANDMEMBER key", 
      "ZRANDMEMBER key", 
      
      "ZRANDMEMBER key 10", 
      "ZRANDMEMBER key 100", 
      "ZRANDMEMBER key 1000", 

      "zrandmember key 10 withscores", 
      "ZRANDMEMBER key 100 WITHSCORES", 
      "ZRANDMEMBER key 1000 WITHSCORES", 
 
      "ZRANDMEMBER key 2", 
      "ZRANDMEMBER key 3", 
      "ZRANDMEMBER key 4",
      "ZRANDMEMBER key 5",
      "ZRANDMEMBER key 6",
      "ZRANDMEMBER key 7",
      "ZRANDMEMBER key 8",
      "ZRANDMEMBER key 9",

      "ZRANDMEMBER key 2 WITHSCORES", 
      "ZRANDMEMBER key 3 WITHSCORES", 
      "ZRANDMEMBER key 4 WITHSCORES",
      "ZRANDMEMBER key 5 WITHSCORES",
      "ZRANDMEMBER key 6 WITHSCORES",
      "ZRANDMEMBER key 7 WITHSCORES",
      "ZRANDMEMBER key 8 WITHSCORES",
      "ZRANDMEMBER key 9 WITHSCORES",
      
      "ZRANDMEMBER key -2", 
      "ZRANDMEMBER key -3", 
      "ZRANDMEMBER key -4",
      "ZRANDMEMBER key -5",
      "ZRANDMEMBER key -6",
      "ZRANDMEMBER key -7",
      "ZRANDMEMBER key -8",
      "ZRANDMEMBER key -9",

      "ZRANDMEMBER key -2 WITHSCORES", 
      "ZRANDMEMBER key -3 WITHSCORES", 
      "ZRANDMEMBER key -4 WITHSCORES",
      "ZRANDMEMBER key -5 WITHSCORES",
      "ZRANDMEMBER key -6 WITHSCORES",
      "ZRANDMEMBER key -7 WITHSCORES",
      "ZRANDMEMBER key -8 WITHSCORES",
      "zrandmember key -9 withscores"
      
  };
  
  protected String[] validResponses = new String[] {
      ":5\r\n",
      ":5\r\n",
      ":10\r\n",
      
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      
      
      "*10\r\n$2\r\nc0\r\n$2\r\nc1\r\n$2\r\nc2\r\n$2\r\nc3\r\n$2\r\nc4\r\n$2\r\nc5\r\n" +
      "$2\r\nc6\r\n$2\r\nc7\r\n$2\r\nc8\r\n$2\r\nc9\r\n",
      
      "*10\r\n$2\r\nc0\r\n$2\r\nc1\r\n$2\r\nc2\r\n$2\r\nc3\r\n$2\r\nc4\r\n$2\r\nc5\r\n" +
      "$2\r\nc6\r\n$2\r\nc7\r\n$2\r\nc8\r\n$2\r\nc9\r\n",
      
      "*10\r\n$2\r\nc0\r\n$2\r\nc1\r\n$2\r\nc2\r\n$2\r\nc3\r\n$2\r\nc4\r\n$2\r\nc5\r\n" +
      "$2\r\nc6\r\n$2\r\nc7\r\n$2\r\nc8\r\n$2\r\nc9\r\n",
      
      "*20\r\n$2\r\nc0\r\n$3\r\n0.0\r\n$2\r\nc1\r\n$3\r\n1.0\r\n$2\r\nc2\r\n$3\r\n2.0\r\n$2\r\nc3\r\n$3\r\n3.0\r\n"+
      "$2\r\nc4\r\n$3\r\n4.0\r\n$2\r\nc5\r\n$3\r\n5.0\r\n" +
      "$2\r\nc6\r\n$3\r\n6.0\r\n$2\r\nc7\r\n$3\r\n7.0\r\n" + 
      "$2\r\nc8\r\n$3\r\n8.0\r\n$2\r\nc9\r\n$3\r\n9.0\r\n",
      
      "*20\r\n$2\r\nc0\r\n$3\r\n0.0\r\n$2\r\nc1\r\n$3\r\n1.0\r\n$2\r\nc2\r\n$3\r\n2.0\r\n$2\r\nc3\r\n$3\r\n3.0\r\n"+
      "$2\r\nc4\r\n$3\r\n4.0\r\n$2\r\nc5\r\n$3\r\n5.0\r\n" +
      "$2\r\nc6\r\n$3\r\n6.0\r\n$2\r\nc7\r\n$3\r\n7.0\r\n" + 
      "$2\r\nc8\r\n$3\r\n8.0\r\n$2\r\nc9\r\n$3\r\n9.0\r\n",
      
      "*20\r\n$2\r\nc0\r\n$3\r\n0.0\r\n$2\r\nc1\r\n$3\r\n1.0\r\n$2\r\nc2\r\n$3\r\n2.0\r\n$2\r\nc3\r\n$3\r\n3.0\r\n"+
      "$2\r\nc4\r\n$3\r\n4.0\r\n$2\r\nc5\r\n$3\r\n5.0\r\n" +
      "$2\r\nc6\r\n$3\r\n6.0\r\n$2\r\nc7\r\n$3\r\n7.0\r\n" + 
      "$2\r\nc8\r\n$3\r\n8.0\r\n$2\r\nc9\r\n$3\r\n9.0\r\n",          
      
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY
   };
  
  protected String[] invalidRequests = new String[] {
      "zrandmembe x y",                       /* unsupported command */
      "ZRANDMEMBER",                           /* wrong number of arguments*/
      "ZRANDMEMBER key a b c",                   /* wrong number of arguments*/
      "ZRANDMEMBER key a WITHSCORES",            /* wrong number format */
      "ZRANDMEMBER key WITHVALUES",              /* wrong number format */
      "ZRANDMEMBER key 10 WITHVALUES",           /* wrong command format */
      
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: ZRANDMEMBE\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number format: a\r\n",
    "-ERR: Wrong number format: WITHVALUES\r\n",
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
