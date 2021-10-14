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


public class TestINFO extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "INFO MEMORY"               /* OK  */,
      "info memory"
  };
  
  protected String[] validResponses = new String[] {
      SKIP_VERIFY,
      SKIP_VERIFY
  };
  
  
  protected String[] invalidRequests = new String[] {
      "infos x y",                      /* unsupported command */
      "info something else",            /* wrong number of arguments*/
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: INFOS\r\n",
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
