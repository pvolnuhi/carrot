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

public class TestRPOPLPUSH extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "RPUSH key1 v11 v12 v13 v14 v15",   /* 5 */
      
      "RPOPLPUSH key1 key2",              /* v15 */
      "RPOPLPUSH key1 key2",              /* v14, v15 */
      "RPOPLPUSH key1 key2",              /* v13, v14, v15 */
      "RPOPLPUSH key1 key2",              /* v12, v13, v14, v15 */
      "RPOPLPUSH key1 key2",              /* v11, v12, v13, v14, v15 */
      "RPOPLPUSH key1 key2",              /* NULL */
      "LLEN key1",                        /* 0 */ 
      "LLEN key2",                        /* 5 */ 
      
      "RPOPLPUSH key2 key2",              /* v15, v11, v12, v13, v14 */
      "RPOPLPUSH key2 key2",              /* v14, v15, v11, v12, v13 */
      "RPOPLPUSH key2 key2",              /* v13, v14, v15, v11, v12 */
      "RPOPLPUSH key2 key2",              /* v12, v13, v14, v15, v11 */
      "RPOPLPUSH key2 key2"               /* v11, v12, v13, v14, v15 */

  };
  
  protected String[] validResponses = new String[] {
      ":5\r\n",

      "$3\r\nv15\r\n",
      "$3\r\nv14\r\n",
      "$3\r\nv13\r\n",
      "$3\r\nv12\r\n",
      "$3\r\nv11\r\n",
      "$-1\r\n",

      ":0\r\n",
      ":5\r\n",
      
      "$3\r\nv15\r\n",
      "$3\r\nv14\r\n",
      "$3\r\nv13\r\n",
      "$3\r\nv12\r\n",
      "$3\r\nv11\r\n"
  };
  
  
  protected String[] invalidRequests = new String[] {
      "rpoplpush x y",                 /* unsupported command */
      "RPOPLPUSH",                     /* wrong number of arguments*/
      "RPOPLPUSH x",                   /* wrong number of arguments*/
      "RPOPLPUSH x y z",               /* wrong number of arguments*/      
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: rpoplpush\r\n",
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
