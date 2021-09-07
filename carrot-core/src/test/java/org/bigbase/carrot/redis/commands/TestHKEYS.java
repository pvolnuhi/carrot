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

public class TestHKEYS extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "HSET key1 field1 value1 field2 value2 field3 value3 field4 value4",        /* 4 */
      "HKEYS key1",
      "hkeys key2"
  };
  
  protected String[] validResponses = new String[] {
      ":4\r\n",
      "*4\r\n$6\r\nfield1\r\n$6\r\nfield2\r\n$6\r\nfield3\r\n$6\r\nfield4\r\n",
      "*0\r\n"
  };
  
  protected String[] invalidRequests = new String[] {
      "HKEYS",                       /* wrong number of arguments*/
      "HKEYS x y z",                 /* wrong number of arguments*/
      "HKEYS x y z a b bb",          /* wrong number of arguments*/

  };
  
  protected String[] invalidResponses = new String[] {
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
