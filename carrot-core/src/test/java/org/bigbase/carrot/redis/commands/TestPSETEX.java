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

public class TestPSETEX extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "PSETEX key 100000 value",              /* OK */
      "PSETEX key 1000000 value"              /* OK */
  };
  
  protected String[] validResponses = new String[] {
      "+OK\r\n",
      "+OK\r\n"
  };
  
  
  protected String[] invalidRequests = new String[] {
      "psetex x y",                      /* unsupported command */
      "PSETEX",                          /* wrong number of arguments*/
      "PSETEX key",                      /* wrong number of arguments*/
      "PSETEX key value",                /* wrong number of arguments*/
      "PSETEX key XXX value",            /* wrong number format*/
      "PSETEX key value 10000"
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: psetex\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number format: XXX\r\n",
    "-ERR: Wrong number format: value\r\n",
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
