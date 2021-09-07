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

public class TestRPUSH extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "RPUSH key v1 v2 v3 v4 v5 v6 v7 v8 v9 v10",               /* 10 */
      "RPUSH key v1 v2 v3 v4 v5 v6 v7 v8 v9 v10",               /* 20 */
      "rpush key v1 v2 v3 v4 v5 v6 v7 v8 v9 v10",               /* 30 */

  };
  
  protected String[] validResponses = new String[] {
      ":10\r\n",
      ":20\r\n",
      ":30\r\n"
  };
  
  
  protected String[] invalidRequests = new String[] {
      "rpus x y",                      /* unsupported command */
      "RPUSH",                          /* wrong number of arguments*/
      "RPUSH key",                      /* wrong number of arguments*/
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: RPUS\r\n",
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
