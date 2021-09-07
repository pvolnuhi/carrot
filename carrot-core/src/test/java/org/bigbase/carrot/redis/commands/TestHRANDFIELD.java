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

public class TestHRANDFIELD extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "HSET key1 field1 value1 field2 value2 field3 value3 field4 value4",        /* 4 */
      
      "HRANDFIELD key1",
      
      "hrandfield key2",
      "HRANDFIELD key2 2",
      "HRANDFIELD key2 -2",
      "HRANDFIELD key2 2 WITHVALUES",
      "HRANDFIELD key2 -2 WITHVALUES",
      
      "HRANDFIELD key1 2",
      "HRANDFIELD key1 2 WITHVALUES",
      "HRANDFIELD key1 3 WITHVALUES",
      
      "HRANDFIELD key1 5 WITHVALUES",
      "HRANDFIELD key1 5",
      "HRANDFIELD key1 10",
      
      "HRANDFIELD key1 -5",
      "HRANDFIELD key1 -10",
      "HRANDFIELD key1 -20",
      "HRANDFIELD key1 -10 WITHVALUES"
  };
  
  protected String[] validResponses = new String[] {
      ":4\r\n",
      SKIP_VERIFY,
      "$-1\r\n",
      "*0\r\n",
      "*0\r\n",
      "*0\r\n",      
      "*0\r\n",      

      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      "*8\r\n$6\r\nfield1\r\n$6\r\nvalue1\r\n$6\r\nfield2\r\n$6\r\nvalue2\r\n"+
      "$6\r\nfield3\r\n$6\r\nvalue3\r\n$6\r\nfield4\r\n$6\r\nvalue4\r\n",
      
      "*4\r\n$6\r\nfield1\r\n$6\r\nfield2\r\n$6\r\nfield3\r\n$6\r\nfield4\r\n",
      "*4\r\n$6\r\nfield1\r\n$6\r\nfield2\r\n$6\r\nfield3\r\n$6\r\nfield4\r\n",
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY,
      SKIP_VERIFY
  };
  
  protected String[] invalidRequests = new String[] {
      "HRANDFIELD",                       /* Wrong number of arguments*/
      "HRANDFIELD x x",                   /* Wrong number format */
      "HRANDFIELD x x WITHVALUES",        /* Wrong number format */
      "HRANDFIELD x 10 ABC"               /* Wrong number format */
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number format: x\r\n",
    "-ERR: Wrong number format: x\r\n",
    "-ERR: Wrong command format, unexpected argument: ABC\r\n"
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
