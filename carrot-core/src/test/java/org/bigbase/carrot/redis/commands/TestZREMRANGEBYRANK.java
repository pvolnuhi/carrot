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

public class TestZREMRANGEBYRANK extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "ZADD key 0.0 c0 1.0 c1 2.0 c2 3.0 c3 4.0 c4 5.0 c5 6.0 c6 7.0 c7 8.0 c8 9.0 c9",   /* 10 */
      "ZCARD key",                                                                        /* 10 */
      
      // Test ALL
      "ZREMRANGEBYRANK key 0 -1",                                                         /* 10 */
      "ZCARD key",                                                                        /*  0 */
      "ZADD key 0.0 c0 1.0 c1 2.0 c2 3.0 c3 4.0 c4 5.0 c5 6.0 c6 7.0 c7 8.0 c8 9.0 c9",   /* 10 */

      
      "ZREMRANGEBYRANK key 0 9",                                                          /* 10 */
      "ZCARD key",                                                                        /*  0 */
      "ZADD key 0.0 c0 1.0 c1 2.0 c2 3.0 c3 4.0 c4 5.0 c5 6.0 c6 7.0 c7 8.0 c8 9.0 c9",   /* 10 */
      
      "ZREMRANGEBYRANK key 0 100",                                                        /* 10 */
      "ZCARD key",                                                                        /*  0 */
      "ZADD key 0.0 c0 1.0 c1 2.0 c2 3.0 c3 4.0 c4 5.0 c5 6.0 c6 7.0 c7 8.0 c8 9.0 c9",   /* 10 */
      
      "ZREMRANGEBYRANK key 0 0",                                                          /* 1 */
      "ZCARD key",                                                                        /* 9 */
      "ZADD key 0.0 c0 1.0 c1 2.0 c2 3.0 c3 4.0 c4 5.0 c5 6.0 c6 7.0 c7 8.0 c8 9.0 c9",   /* 1 */
      
      "ZREMRANGEBYRANK key 6 6",                                                          /* 1 */
      "ZCARD key",                                                                        /* 9 */
      "ZADD key 0.0 c0 1.0 c1 2.0 c2 3.0 c3 4.0 c4 5.0 c5 6.0 c6 7.0 c7 8.0 c8 9.0 c9",   /* 1 */
      
      "ZREMRANGEBYRANK key 6 5",                                                          /* 0 */
      "ZCARD key",                                                                        /* 10 */

      "ZREMRANGEBYRANK key 6 -6",                                                         /* 0 */
      "ZCARD key",                                                                        /* 10 */
      
      "ZREMRANGEBYRANK key 10 100",                                                       /* 0 */           
      "ZCARD key",                                                                        /* 10 */
      
      "ZREMRANGEBYRANK key 2 5",                                                          /* 4 */
      "ZCARD key",                                                                        /* 6 */
      "ZADD key 0.0 c0 1.0 c1 2.0 c2 3.0 c3 4.0 c4 5.0 c5 6.0 c6 7.0 c7 8.0 c8 9.0 c9",   /* 4 */
     
      "zremrangebyrank key 3 9",                                                          /* 7 */
      "ZCARD key",                                                                        /* 3 */
      "ZADD key 0.0 c0 1.0 c1 2.0 c2 3.0 c3 4.0 c4 5.0 c5 6.0 c6 7.0 c7 8.0 c8 9.0 c9",   /* 7 */
  };
  
  protected String[] validResponses = new String[] {
      ":10\r\n",
      ":10\r\n",
      
      ":10\r\n",
      ":0\r\n",
      ":10\r\n",
      
      ":10\r\n",
      ":0\r\n",
      ":10\r\n",
   
      ":10\r\n",
      ":0\r\n",
      ":10\r\n",
      
      ":1\r\n",
      ":9\r\n",
      ":1\r\n",
      
      ":1\r\n",
      ":9\r\n",
      ":1\r\n",
      
      ":0\r\n",
      ":10\r\n",
      
      ":0\r\n",
      ":10\r\n",
      
      ":0\r\n",
      ":10\r\n",
      
      ":4\r\n",
      ":6\r\n",
      ":4\r\n",
      
      ":7\r\n",
      ":3\r\n",
      ":7\r\n",
   };
  
  protected String[] invalidRequests = new String[] {
      "zremrangebyronk x y",                           /* unsupported command */
      "ZREMRANGEBYRANK",                               /* wrong number of arguments*/
      "ZREMRANGEBYRANK key",                           /* wrong number of arguments*/
      "ZREMRANGEBYRANK key a",                         /* wrong number of arguments*/
      "ZREMRANGEBYRANK key a b c d",                   /* wrong number of arguments*/
      "ZREMRANGEBYRANK key a 10",                      /* wrong number format */
      "ZREMRANGEBYRANK key 10 b",                      /* wrong number format */
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: ZREMRANGEBYRONK\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    
    "-ERR: Wrong number format: a\r\n",
    "-ERR: Wrong number format: b\r\n"

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
