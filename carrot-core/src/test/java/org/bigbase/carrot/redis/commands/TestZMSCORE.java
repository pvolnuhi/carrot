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

public class TestZMSCORE extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "ZADD key 1234 v1 1.9E-6 v2 0.123 v3 12.15 v4",                         /* 4 */
      "ZADD key 1234 v1 1.9E-6 v2 0.123 v5 12.15 v6",                         /* 2 */
      "ZMSCORE key v1 v2",
      "ZMSCORE key v3 v4",
      "ZMSCORE key v5 v6",
      "ZMSCORE key1 v1 v2",
      "zmscore key v1 v7 v2"
  };
  
  protected String[] validResponses = new String[] {
      ":4\r\n",
      ":2\r\n",
      "*2\r\n$6\r\n1234.0\r\n$6\r\n1.9E-6\r\n",
      "*2\r\n$5\r\n0.123\r\n$5\r\n12.15\r\n",
      "*2\r\n$5\r\n0.123\r\n$5\r\n12.15\r\n",
      "*2\r\n$-1\r\n$-1\r\n",
      "*3\r\n$6\r\n1234.0\r\n$-1\r\n$6\r\n1.9E-6\r\n"
  };
  
  
  protected String[] invalidRequests = new String[] {
      "zmscor x y",                          /* unsupported command */
      "ZMSCORE",                              /* wrong number of arguments*/
      "ZSCORE key"                            /* wrong number of arguments*/
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: ZMSCOR\r\n",
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
