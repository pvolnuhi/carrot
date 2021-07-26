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

public class TestZADD extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "ZADD key 1234 v1 1.9E-6 v2 0.123 v3 12.15 v4",                         /* 4 */
      "ZADD key 1234 v1 1.9E-6 v2 0.123 v5 12.15 v6",                         /* 2 */
      "ZCARD key",                                                            /* 6 */
      "ZADD key CH 1234 v1 1.9E-6 v2 0.123 v5 12.16 v6 1.0 v7 2.0 v8",        /* 3 */
      "ZCARD key",                                                            /* 8 */
      "ZADD key NX 1234 v1 1.9E-6 v2 0.123 v5 12.15 v6 1.0 v7 2.0 v8",        /* 0 */
      "ZCARD key",                                                            /* 8 */
      "ZADD key XX 1234 v11 1.9E-6 v21 0.123 v51 12.15 v61 1.0 v71 2.0 v81",  /* 0 */
      "ZCARD key",                                                            /* 8 */
      "ZADD key NX 1234 v11 1.9E-6 v21 0.123 v51 12.15 v61 1.0 v71 2.0 v81",  /* 6 */
      "ZCARD key",                                                            /* 14 */
      "ZADD key XX CH 12345 v11 1.9E-6 v21 0.123 v51 12.15 v61 1.0 v71 2.0 v81 22.0 v9 11.0 v10 ",  /* 1 */
      "ZCARD key",                                                            /* 14 */
      "ZADD key NX CH 1234 v11 1.9E-6 v21 0.123 v51 12.15 v61 1.0 v71 2.0 v81 22.0 v9 11.0 v10",  /* 2 */
      "ZCARD key"                                                            /* 16 */
  };
  
  protected String[] validResponses = new String[] {
      ":4\r\n",
      ":2\r\n",
      ":6\r\n",
      ":3\r\n",
      ":8\r\n",
      ":0\r\n",
      ":8\r\n",
      ":0\r\n",
      ":8\r\n",      
      ":6\r\n",
      ":14\r\n",
      ":1\r\n",      
      ":14\r\n",      
      ":2\r\n",      
      ":16\r\n"      
  };
  
  
  protected String[] invalidRequests = new String[] {
      "zadd x y",                         /* unsupported command */
      "ZADD",                             /* wrong number of arguments*/
      "ZADD key",                         /* wrong number of arguments*/
      "ZADD key 1",                       /* wrong number of arguments*/
      "ZADD key 1 2 3",                   /* wrong number of arguments*/
      "ZADD key A 2"                     /* wrong number format*/
      
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: zadd\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number format: A\r\n"
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
