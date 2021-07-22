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

public class TestSCARD extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "SADD key v1 v2 v3 v4 v5 v6 v7 v8 v9 v10",                /* 10 */
      "SCARD key",                                              /* 10 */
      "SADD key v11 v21 v31 v41 v51 v61 v71 v81 v91 v101",      /* 10 */
      "SCARD key",                                              /* 20 */ 
      "SCARD key1"                                              /* 0 */
  };
  
  protected String[] validResponses = new String[] {
      ":10\r\n",
      ":10\r\n",
      ":10\r\n",
      ":20\r\n", 
      ":0\r\n"
  };
  
  
  protected String[] invalidRequests = new String[] {
      "scard x y",                     /* unsupported command */
      "SCARD",                         /* wrong number of arguments*/
      "SCARD x v",                     /* wrong number of arguments*/
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: scard\r\n",
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
