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

public class TestMGET extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "MSET key1 value1 key2 value2 key3 value3 key4 value4",        /* OK */
      "MGET key1 key11 key2 key22 key3 key33 key4 key44"
  };
  
  protected String[] validResponses = new String[] {
      "+OK\r\n",
      "*8\r\n$6\r\nvalue1\r\n$-1\r\n$6\r\nvalue2\r\n$-1\r\n$6\r\nvalue3\r\n$-1\r\n$6\r\nvalue4\r\n$-1\r\n",
  };
  
  protected String[] invalidRequests = new String[] {
      "mge x",                     /* unsupported command */
      "MGET"                        /* Wrong number of arguments*/
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: MGE\r\n",
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
