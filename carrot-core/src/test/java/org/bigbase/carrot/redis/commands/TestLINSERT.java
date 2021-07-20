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

public class TestLINSERT extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "RPUSH key v1 v2 v3 v4 v5 v6 v7 v8 v9 v10",                 /* 10 */
      "LINSERT key BEFORE v1 v0",                                 /* 11 */      
      "LINDEX key 0",                                             /* v0 */
      "LINSERT key BEFORE v10 v99",                               /* 12 */      
      "LINDEX key 10",                                            /* v99 */
      "LINSERT key AFTER v10 v100",                               /* 13 */      
      "LINDEX key 12",                                            /* v100 */
      "LINSERT key BEFORE v5 v45",                                /* 14 */      
      "LINDEX key 5",                                             /* v45 */
      "LINSERT key AFTER v5 v55",                                 /* 15 */      
      "LINDEX key 7",                                             /* v55 */

      "LINSERT key BEFORE v555 v155",                              /* -1 */      
      "LINSERT key AFTER v5555 v155",                              /* -1 */      
      "LINSERT key1 BEFORE v555 v155",                             /* -1 */      
      "LINSERT key1 AFTER v5555 v155",                             /* -1 */      

      
      "LINDEX key 0",                                             /* v0 */
      "LINDEX key 1",                                             /* v1 */
      "LINDEX key 2",                                             /* v2 */
      "LINDEX key 3",                                             /* v3 */
      "LINDEX key 4",                                             /* v4 */
      "LINDEX key 5",                                             /* v45 */
      "LINDEX key 6",                                             /* v5 */
      "LINDEX key 7",                                             /* v55 */
      "LINDEX key 8",                                             /* v6 */
      "LINDEX key 9",                                             /* v7 */
      "LINDEX key 10",                                            /* v8 */
      "LINDEX key 11",                                            /* v9 */
      "LINDEX key 12",                                            /* v99 */
      "LINDEX key 13",                                            /* v10 */
      "LINDEX key 14"                                             /* v100 */
  };
  
  protected String[] validResponses = new String[] {
      ":10\r\n",
      ":11\r\n",

      "$2\r\nv0\r\n",
      ":12\r\n",
      "$3\r\nv99\r\n",

      ":13\r\n",
      "$4\r\nv100\r\n",
      
      ":14\r\n",
      "$3\r\nv45\r\n",
      
      ":15\r\n",
      "$3\r\nv55\r\n",

      ":-1\r\n",
      ":-1\r\n",
      ":-1\r\n",
      ":-1\r\n",
      
      "$2\r\nv0\r\n",
      "$2\r\nv1\r\n",
      "$2\r\nv2\r\n",
      "$2\r\nv3\r\n",
      "$2\r\nv4\r\n",
      "$3\r\nv45\r\n",
      "$2\r\nv5\r\n",
      "$3\r\nv55\r\n",
      "$2\r\nv6\r\n",
      "$2\r\nv7\r\n",
      "$2\r\nv8\r\n",
      "$2\r\nv9\r\n",
      "$3\r\nv99\r\n",
      "$3\r\nv10\r\n",
      "$4\r\nv100\r\n",
 
  };
  
  
  protected String[] invalidRequests = new String[] {
      "linsert x y",                     /* unsupported command */
      "LINSERT x",                       /* wrong number of arguments*/
      "LINSERT x y",                     /* wrong number of arguments*/
      "LINSERT x y z",                   /* wrong number format*/
      "LINSERT x BEFO z a",              /* wrong number format*/
      
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: linsert\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",    
    "-ERR: Wrong command format, unexpected argument: BEFO\r\n"    
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
