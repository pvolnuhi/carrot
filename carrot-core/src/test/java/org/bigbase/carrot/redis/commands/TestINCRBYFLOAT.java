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

public class TestINCRBYFLOAT extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "INCRBYFLOAT key 100.5",
      "INCRBYFLOAT key 100.1",
      "incrbyfloat key 100.2"
  };
  
  protected String[] validResponses = new String[] {
      "$5\r\n100.5\r\n",
      "$5\r\n200.6\r\n",
      "$5\r\n300.8\r\n"
  };
  
  
  protected String[] invalidRequests = new String[] {
      "incrbyfloa x",                      /* unsupported command */
      "INCRBYFLOAT",                        /* wrong number of arguments*/
      "INCRBYFLOAT x",                      /* wrong number of arguments*/
      "INCRBYFLOAT x y z",                  /* wrong number of arguments*/
      "INCRBYFLOAT x y",                    /* wrong number format */
      "SET key1 value1",                    /* OK */
      "INCRBYFLOAT key1 1"                  /* wrong number format */ 
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: INCRBYFLOA\r\n",
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
