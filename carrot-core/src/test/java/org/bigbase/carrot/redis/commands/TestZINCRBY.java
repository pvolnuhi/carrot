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

public class TestZINCRBY extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "ZADD key 1234 v1 1.9E-6 v2 0.123 v3 12.15 v4",                         /* 4 */
      "ZADD key 1234 v1 1.9E-6 v2 0.123 v5 12.15 v6",                         /* 2 */
      
      "ZINCRBY key 100.1 v1",                                                 /* 1334.1*/
      "ZSCORE key v1",
      
      "ZINCRBY key 1.01E-6 v2",                                               /* 2.91E-6*/ 
      "ZSCORE key v2",
      
      "ZINCRBY key 1.0 v3",                                                   /* 1.123*/
      "ZSCORE key v3",
      
      "ZINCRBY key 12.15 v4",                                                 /* 24.30 ??*/
      "ZSCORE key v4",
      
      "ZINCRBY key 2.0 v5",                                                   /* 2.123*/
      "ZSCORE key v5",
      
      "ZINCRBY key 3.1 v6",                                                   /* 15.25*/
      "ZSCORE key v6",
      
      "ZINCRBY key 3.1 v7",                                                   /* 3.1 */
      "ZSCORE key v7",
      
      "zincrby key1 3.1 v1",                                                  /* 3.1 */
      "ZSCORE key1 v1"
  };
  
  protected String[] validResponses = new String[] {
      ":4\r\n",
      ":2\r\n",
      
      "$6\r\n1334.1\r\n",
      "$6\r\n1334.1\r\n",

      "$7\r\n2.91E-6\r\n",
      "$7\r\n2.91E-6\r\n",
      
      "$5\r\n1.123\r\n",
      "$5\r\n1.123\r\n",
      
      "$4\r\n24.3\r\n",
      "$4\r\n24.3\r\n",
      
      "$5\r\n2.123\r\n",
      "$5\r\n2.123\r\n",
      
      "$5\r\n15.25\r\n",
      "$5\r\n15.25\r\n",
      
      "$3\r\n3.1\r\n",
      "$3\r\n3.1\r\n",
      
      "$3\r\n3.1\r\n",
      "$3\r\n3.1\r\n"
  };
  
  
  protected String[] invalidRequests = new String[] {
      "zincrbyy x y",                          /* unsupported command */
      "ZINCRBY",                              /* wrong number of arguments*/
      "ZINCRBY key",                          /* wrong number of arguments*/
      "ZINCRBY key a",                        /* wrong number of arguments*/
      "ZINCRBY key a b c",                    /* wrong number of arguments*/
      "ZINCRBY key a b",                      /* wrong number format*/

  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: ZINCRBYY\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number format: a\r\n"
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
