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

public class TestHINCRBY extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "HSET key field1 100 field2 200",
      "HINCRBY key field1 100",
      "HINCRBY key field2 100",
      "HINCRBY key field3 100"         
  };
  
  protected String[] validResponses = new String[] {
      ":2\r\n",
      ":200\r\n",
      ":300\r\n",
      ":100\r\n"
  };
  
  
  protected String[] invalidRequests = new String[] {
      "hincrby x",                     /* unsupported command */
      "HINCRBY",                       /* wrong number of arguments*/
      "HINCRBY x",                     /* wrong number of arguments*/
      "HINCRBY x y",                   /* wrong number of arguments */
      "HINCRBY x y z",                 /* wrong number format*/
      "HSET key1 field1 value1",       /* 1 */
      "HINCRBY key1 field1 1"          /* wrong number format */ 
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: hincrby\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",  
    "-ERR: Wrong number of arguments\r\n",  
    "-ERR: Wrong number format: z\r\n",
    ":1\r\n",
    "-ERR: Wrong number format: Value at key is not a number\r\n"
  };
  
  /**
   * Subclasses must override
   */
  protected String[] getValidRequests() {
    return validRequests;
  }
  
  @Override
  protected String[] getValidResponses() {
    return validResponses;
  }
  @Override  
  protected String[] getInvalidRequests() {
    return invalidRequests;
  }
  @Override
  protected String[] getInvalidResponses() {
    return invalidResponses;
  }
}
