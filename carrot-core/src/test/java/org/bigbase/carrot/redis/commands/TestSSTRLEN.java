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

public class TestSSTRLEN extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "SSETBIT key 100 1",               /* 0 */
      "SSTRLEN key",                     /* 13 */
      "SSETBIT key 10000000000 1",       /* 0 */
      "SSTRLEN key",                     /* 1250000001 */
      "SSTRLEN key1"                     
  };
  
  protected String[] validResponses = new String[] {
      ":0\r\n",
      ":13\r\n",
      ":0\r\n",
      ":1250000001\r\n",
      ":0\r\n"
  };
  
  
  protected String[] invalidRequests = new String[] {
      "sstrlen x y",                      /* unsupported command */
      "SSTRLEN",                          /* wrong number of arguments*/
      "SSTRLEN key value"                 /* wrong number of arguments*/
      
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: sstrlen\r\n",
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
