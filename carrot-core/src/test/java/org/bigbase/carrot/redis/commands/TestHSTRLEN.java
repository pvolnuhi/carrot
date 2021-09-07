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

public class TestHSTRLEN extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "HSET key1 f1 value1111 f2 value2222222 f3 value3333333333 f4 value444444444444",        /* 4 */
      "hstrlen key1 f1", /* 9 */
      "HSTRLEN key1 f2", /* 12 */
      "HSTRLEN key1 f3", /* 15 */
      "HSTRLEN key1 f4", /* 17 */
      "HSTRLEN key1 f5",  /* 0 */
      "HSTRLEN key2 f3"   /* 0 */

  };
  
  protected String[] validResponses = new String[] {
      ":4\r\n",
      ":9\r\n",
      ":12\r\n",
      ":15\r\n",
      ":17\r\n",
      ":0\r\n",
      ":0\r\n"
  };
  
  protected String[] invalidRequests = new String[] {
      "HSTRLEN",                       /* wrong number of arguments*/
      "HSTRLEN x",                     /* wrong number of arguments*/
      "HSTRLEN x y z",                 /* wrong number of arguments*/
      "HSTRLEN x y z a b bb",          /* wrong number of arguments*/

  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n"
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
