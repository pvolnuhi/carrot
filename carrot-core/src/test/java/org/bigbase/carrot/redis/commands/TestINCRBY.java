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

public class TestINCRBY extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "INCRBY key 100",
      "INCRBY key 100",
      "incrby key 100"
  };
  
  protected String[] validResponses = new String[] {
      ":100\r\n",
      ":200\r\n",
      ":300\r\n",
  };
  
  
  protected String[] invalidRequests = new String[] {
      "incrb x",                     /* unsupported command */
      "INCRBY",                       /* wrong number of arguments*/
      "INCRBY x",                     /* wrong number of arguments*/
      "INCRBY x y z",                 /* wrong number of arguments*/
      "INCRBY x y",                   /* wrong number format */
      "SET key1 value1",              /* OK */
      "INCRBY key1 1"                 /* wrong number format */ 
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: INCRB\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",  
    "-ERR: Wrong number of arguments\r\n",  
    "-ERR: Wrong number format: y\r\n",
    "+OK\r\n",
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
