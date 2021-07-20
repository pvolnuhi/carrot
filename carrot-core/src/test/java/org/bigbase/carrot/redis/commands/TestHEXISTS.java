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

public class TestHEXISTS extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "HSET key1 field1 value1 field2 value2 field3 value3 field4 value4",        /* 4 */
      "HEXISTS key1 field1",                                                      /* 1 */
      "HEXISTS key1 field2",                                                      /* 1 */
      "HEXISTS key1 field3",                                                      /* 1 */
      "HEXISTS key1 field4",                                                      /* 1 */
      "HEXISTS key1 field5",                                                      /* 0 */
      "HEXISTS key2 field1",                                                      /* 0 */

  };
  
  protected String[] validResponses = new String[] {
      ":4\r\n",
      ":1\r\n",
      ":1\r\n",
      ":1\r\n",
      ":1\r\n",
      ":0\r\n",
      ":0\r\n"      
  };
  
  protected String[] invalidRequests = new String[] {
      "hexists x",                  /* unsupported command */
      "HEXISTS",                    /* wrong number of arguments*/
      "HEXISTS x",                  /* wrong number of arguments*/
      "HEXISTS x y z",              /* wrong number of arguments*/
      "HEXISTS x y z zz"            /* wrong number of arguments*/      
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: hexists\r\n",
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
