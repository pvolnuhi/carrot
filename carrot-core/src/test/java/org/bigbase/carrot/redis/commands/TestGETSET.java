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

public class TestGETSET extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "GETSET key value1",                        /* NULL*/
      "GETSET key value2",                        /* value1 */
      "GETSET key value3",                        /* value2 */
      "GETSET key value4",                        /* value3 */
      "GETSET key value5"                         /* value4 */
  };
  
  protected String[] validResponses = new String[] {
      "$-1\r\n",
      "$6\r\nvalue1\r\n",
      "$6\r\nvalue2\r\n",
      "$6\r\nvalue3\r\n",
      "$6\r\nvalue4\r\n"
 
  };
  
  protected String[] invalidRequests = new String[] {
      "getset x y",                        /* unsupported command */
      "GETSET",                            /* wrong number of arguments*/
      "GETSET key",                        /* wrong number of arguments*/
      "GETSET key 1 2 3"                   /* wrong number of arguments*/
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: getset\r\n",
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
