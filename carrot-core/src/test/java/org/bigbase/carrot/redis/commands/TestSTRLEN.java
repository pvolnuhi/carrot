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

public class TestSTRLEN extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "SET key1 1000",                      /* OK */
      "SET key2 10000",                     /* OK */
      "SET key3 100000",                    /* OK */ 
      "STRLEN key1",                        /* 4 */
      "STRLEN key2",                        /* 5 */
      "STRLEN key3",                        /* 6 */
      "STRLEN key4"                         /* -1 */
  };
  
  protected String[] validResponses = new String[] {
      "+OK\r\n",
      "+OK\r\n",
      "+OK\r\n",
      ":4\r\n",
      ":5\r\n",
      ":6\r\n",
      ":0\r\n"
  };
  
  protected String[] invalidRequests = new String[] {
      "strlen x",                     /* unsupported command */
      "STRLEN",                       /* wrong number of arguments*/
      "STRLEN x y"                    /* wrong number of arguments*/
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: strlen\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",  
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
