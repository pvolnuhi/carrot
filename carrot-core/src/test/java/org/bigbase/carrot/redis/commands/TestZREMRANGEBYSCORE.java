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

public class TestZREMRANGEBYSCORE extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "ZADD key 0.0 c1 1.0 c2 2.0 c3 3.0 c4 4.0 c5 5.0 c6 6.0 c7 7.0 c8 8.0 c9 0.5 c10",  /* 10 */
      "ZCARD key",                                                                        /* 10 */
      
      "ZREMRANGEBYSCORE key -inf +inf",                                                   /* 10 */
      "ZCARD key",                                                                        /*  0 */
      "ZADD key 0.0 c1 1.0 c2 2.0 c3 3.0 c4 4.0 c5 5.0 c6 6.0 c7 7.0 c8 8.0 c9 0.5 c10",  /* 10 */
      
      "ZREMRANGEBYSCORE key -1.0 10.0",                                                   /* 10 */
      "ZCARD key",                                                                        /*  0 */
      "ZADD key 0.0 c1 1.0 c2 2.0 c3 3.0 c4 4.0 c5 5.0 c6 6.0 c7 7.0 c8 8.0 c9 0.5 c10",  /* 10 */

      "ZREMRANGEBYSCORE key -inf (10.0",                                                  /* 10 */
      "ZCARD key",                                                                        /*  0 */
      "ZADD key 0.0 c1 1.0 c2 2.0 c3 3.0 c4 4.0 c5 5.0 c6 6.0 c7 7.0 c8 8.0 c9 0.5 c10",  /* 10 */

      "ZREMRANGEBYSCORE key -100 (0.",                                                    /* 0  */
      "ZCARD key",                                                                        /* 10 */
      
      "ZREMRANGEBYSCORE key (9.0 10.0",                                                   /* 0 */
      "ZCARD key",                                                                        /* 10 */

      "ZREMRANGEBYSCORE key (0.0 9.9",                                                    /* 9 */
      "ZCARD key",                                                                        /* 1 */
      "ZADD key 0.0 c1 1.0 c2 2.0 c3 3.0 c4 4.0 c5 5.0 c6 6.0 c7 7.0 c8 8.0 c9 0.5 c10",  /* 9 */

      "ZREMRANGEBYSCORE key 0.0 9.9",                                                     /* 10 */
      "ZCARD key",                                                                        /*  0 */
      "ZADD key 0.0 c1 1.0 c2 2.0 c3 3.0 c4 4.0 c5 5.0 c6 6.0 c7 7.0 c8 8.0 c9 0.5 c10",  /* 10 */

      
      "ZREMRANGEBYSCORE key (1.0 9.9",                                                    /* 7 */
      "ZCARD key",                                                                        /* 3 */
      "ZADD key 0.0 c1 1.0 c2 2.0 c3 3.0 c4 4.0 c5 5.0 c6 6.0 c7 7.0 c8 8.0 c9 0.5 c10",  /* 7 */

      "ZREMRANGEBYSCORE key 1.0 9.9",                                                     /* 8 */
      "ZCARD key",                                                                        /* 2 */
      "ZADD key 0.0 c1 1.0 c2 2.0 c3 3.0 c4 4.0 c5 5.0 c6 6.0 c7 7.0 c8 8.0 c9 0.5 c10",  /* 8 */

      "ZREMRANGEBYSCORE key (2.0 9.9",                                                    /* 6 */
      "ZCARD key",                                                                        /* 4 */
      "ZADD key 0.0 c1 1.0 c2 2.0 c3 3.0 c4 4.0 c5 5.0 c6 6.0 c7 7.0 c8 8.0 c9 0.5 c10",  /* 6 */

      "ZREMRANGEBYSCORE key 2.0 9.9",                                                     /* 7 */
      "ZCARD key",                                                                        /* 3 */
      "ZADD key 0.0 c1 1.0 c2 2.0 c3 3.0 c4 4.0 c5 5.0 c6 6.0 c7 7.0 c8 8.0 c9 0.5 c10",  /* 7 */
      
      "ZREMRANGEBYSCORE key (3.0 9.9",                                                    /* 5 */
      "ZCARD key",                                                                        /* 5 */
      "ZADD key 0.0 c1 1.0 c2 2.0 c3 3.0 c4 4.0 c5 5.0 c6 6.0 c7 7.0 c8 8.0 c9 0.5 c10",  /* 5 */

      "ZREMRANGEBYSCORE key 3.0 9.9",                                                     /* 6 */
      "ZCARD key",                                                                        /* 4 */
      "ZADD key 0.0 c1 1.0 c2 2.0 c3 3.0 c4 4.0 c5 5.0 c6 6.0 c7 7.0 c8 8.0 c9 0.5 c10",  /* 6 */

      "ZREMRANGEBYSCORE key (4.0 9.9",                                                    /* 4 */
      "ZCARD key",                                                                        /* 6 */
      "ZADD key 0.0 c1 1.0 c2 2.0 c3 3.0 c4 4.0 c5 5.0 c6 6.0 c7 7.0 c8 8.0 c9 0.5 c10",  /* 4 */

      "ZREMRANGEBYSCORE key 4.0 9.9",                                                     /* 5 */
      "ZCARD key",                                                                        /* 5 */
      "ZADD key 0.0 c1 1.0 c2 2.0 c3 3.0 c4 4.0 c5 5.0 c6 6.0 c7 7.0 c8 8.0 c9 0.5 c10",  /* 5 */

      "ZREMRANGEBYSCORE key (5.0 9.9",                                                    /* 3 */
      "ZCARD key",                                                                        /* 7 */
      "ZADD key 0.0 c1 1.0 c2 2.0 c3 3.0 c4 4.0 c5 5.0 c6 6.0 c7 7.0 c8 8.0 c9 0.5 c10",  /* 3 */

      "ZREMRANGEBYSCORE key 5.0 9.9",                                                     /* 4 */
      "ZCARD key",                                                                        /* 6 */
      "ZADD key 0.0 c1 1.0 c2 2.0 c3 3.0 c4 4.0 c5 5.0 c6 6.0 c7 7.0 c8 8.0 c9 0.5 c10",  /* 4 */

      "ZREMRANGEBYSCORE key (6.0 9.9",                                                    /* 2 */
      "ZCARD key",                                                                        /* 8 */
      "ZADD key 0.0 c1 1.0 c2 2.0 c3 3.0 c4 4.0 c5 5.0 c6 6.0 c7 7.0 c8 8.0 c9 0.5 c10",  /* 2 */

      "ZREMRANGEBYSCORE key 6.0 9.9",                                                     /* 3 */
      "ZCARD key",                                                                        /* 7 */
      "ZADD key 0.0 c1 1.0 c2 2.0 c3 3.0 c4 4.0 c5 5.0 c6 6.0 c7 7.0 c8 8.0 c9 0.5 c10",  /* 3 */

      "ZREMRANGEBYSCORE key (7.0 9.9",                                                    /* 1 */
      "ZCARD key",                                                                        /* 9 */
      "ZADD key 0.0 c1 1.0 c2 2.0 c3 3.0 c4 4.0 c5 5.0 c6 6.0 c7 7.0 c8 8.0 c9 0.5 c10",  /* 1 */

      "ZREMRANGEBYSCORE key 7.0 9.9",                                                     /* 2 */
      "ZCARD key",                                                                        /* 8 */
      "ZADD key 0.0 c1 1.0 c2 2.0 c3 3.0 c4 4.0 c5 5.0 c6 6.0 c7 7.0 c8 8.0 c9 0.5 c10",  /* 2 */

      "ZREMRANGEBYSCORE key (8.0 9.9",                                                    /* 0 */
      "ZCARD key",                                                                        /* 10 */

      "zremrangebyscore key 8.0 9.9",                                                     /* 1 */
      "ZCARD key"                                                                         /* 9 */
      
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
      
      ":0\r\n",
      ":10\r\n",
      
      ":0\r\n",
      ":10\r\n",
      
      ":9\r\n",
      ":1\r\n",
      ":9\r\n",
      
      ":10\r\n",
      ":0\r\n",
      ":10\r\n",
      
      ":7\r\n",
      ":3\r\n",
      ":7\r\n",
      
      ":8\r\n",
      ":2\r\n",
      ":8\r\n",
      
      ":6\r\n",
      ":4\r\n",
      ":6\r\n",
      
      ":7\r\n",
      ":3\r\n",
      ":7\r\n",
      
      ":5\r\n",
      ":5\r\n",
      ":5\r\n",
      
      ":6\r\n",
      ":4\r\n",
      ":6\r\n",
      
      ":4\r\n",
      ":6\r\n",
      ":4\r\n",
      
      ":5\r\n",
      ":5\r\n",
      ":5\r\n",
      
      ":3\r\n",
      ":7\r\n",
      ":3\r\n",
      
      ":4\r\n",
      ":6\r\n",
      ":4\r\n",
      
      ":2\r\n",
      ":8\r\n",
      ":2\r\n",
      
      ":3\r\n",
      ":7\r\n",
      ":3\r\n",
      
      ":1\r\n",
      ":9\r\n",
      ":1\r\n",
      
      ":2\r\n",
      ":8\r\n",
      ":2\r\n",
      
      ":0\r\n",
      ":10\r\n",
      
      ":1\r\n",
      ":9\r\n"      
   };
  
  protected String[] invalidRequests = new String[] {
      "zremrangebyscare x y",                           /* unsupported command */
      "ZREMRANGEBYSCORE",                               /* wrong number of arguments*/
      "ZREMRANGEBYSCORE key",                           /* wrong number of arguments*/
      "ZREMRANGEBYSCORE key a",                         /* wrong number of arguments*/
      "ZREMRANGEBYSCORE key a b c d",                   /* wrong number of arguments*/
      "ZREMRANGEBYSCORE key a 0",                       /* wrong number format */
      "ZREMRANGEBYSCORE key 0 b",                       /* wrong number format */

  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: ZREMRANGEBYSCARE\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    
    "-ERR: Wrong number format: a\r\n",
    "-ERR: Wrong number format: b\r\n",    
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
