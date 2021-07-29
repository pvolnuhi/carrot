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

public class TestHSET extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "HSET key1 field1 value1 field2 value2 field3 value3 field4 value4",        /* 4 */
      "HGET key1 field1",
      "HGET key1 field2",
      "HGET key1 field3",
      "HGET key1 field4"
  };
  
  protected String[] validResponses = new String[] {
      ":4\r\n",
      "$6\r\nvalue1\r\n",
      "$6\r\nvalue2\r\n",
      "$6\r\nvalue3\r\n",
      "$6\r\nvalue4\r\n"
  };
  
  protected String[] invalidRequests = new String[] {
      "hset x",                     /* unsupported command */
      "HSET",                       /* wrong number of arguments*/
      "HSET x",                     /* wrong number of arguments*/
      "HSET x y",                   /* wrong number of arguments*/
      "HSET x y z zz",              /* wrong number of arguments*/
      "HSET x y z a b bb",          /* wrong number of arguments*/

  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: hset\r\n",
    "-ERR: Wrong number of arguments\r\n",
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