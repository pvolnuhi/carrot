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

public class TestHINCRBYFLOAT extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "HSET key field1 100 field2 200",
      "HINCRBYFLOAT key field1 100.1",
      "HINCRBYFLOAT key field2 100.1",
      "HINCRBYFLOAT key field3 100.1"         
  };
  
  protected String[] validResponses = new String[] {
      ":2\r\n",
      "$5\r\n200.1\r\n",
      "$5\r\n300.1\r\n",
      "$5\r\n100.1\r\n"
  };
  
  
  protected String[] invalidRequests = new String[] {
      "hincrbyfloat x",                     /* unsupported command */
      "HINCRBYFLOAT",                       /* wrong number of arguments*/
      "HINCRBYFLOAT x",                     /* wrong number of arguments*/
      "HINCRBYFLOAT x y",                   /* wrong number of arguments */
      "HINCRBYFLOAT x y z",                 /* wrong number format*/
      "HSET key1 field1 value1",            /* 1 */
      "HINCRBYFLOAT key1 field1 1"          /* wrong number format */ 
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: hincrbyfloat\r\n",
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
