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

public class TestZSCORE extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "ZADD key 1234 v1 1.9E-6 v2 0.123 v3 12.15 v4",                         /* 4 */
      "ZADD key 1234 v1 1.9E-6 v2 0.123 v5 12.15 v6",                         /* 2 */
      "ZSCORE key v1",                                                        /* 1234 */
      "ZSCORE key v2",                                                        /* 1.9E-6 ??? */
      "ZSCORE key v3",                                                        /* 0.123 */
      "ZSCORE key v4",                                                        /* 12.15 */
      "ZSCORE key v5",                                                        /* 0.123 */
      "ZSCORE key v6",                                                        /* 12.15 */
      "ZSCORE key v7",                                                        /* NULL */
      "ZSCORE key1 v7",                                                       /* NULL */
      "ZADD key -inf v7",                                                      
      "ZADD key +inf v8",
      "ZSCORE key v7",
      "zscore key v8"
  };
  
  protected String[] validResponses = new String[] {
      ":4\r\n",
      ":2\r\n",
      "$6\r\n1234.0\r\n",
      "$6\r\n1.9E-6\r\n",
      "$5\r\n0.123\r\n",
      "$5\r\n12.15\r\n",
      "$5\r\n0.123\r\n",
      "$5\r\n12.15\r\n",
      "$-1\r\n",
      "$-1\r\n",
      ":1\r\n",
      ":1\r\n",
      "$23\r\n-1.7976931348623157E308\r\n",
      "$22\r\n1.7976931348623157E308\r\n"
  };
  
  
  protected String[] invalidRequests = new String[] {
      "zscoree x y",                          /* unsupported command */
      "ZSCORE",                              /* wrong number of arguments*/
      "ZSCORE key 1 2 3",                    /* wrong number of arguments*/
      "ZSCORE key A 2"                       /* wrong number of arguments*/
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: ZSCOREE\r\n",
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
