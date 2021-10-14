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

public class TestLMOVE extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "RPUSH key1 v11 v12 v13 v14 v15",       /* 5 */
      "RPUSH key2 v21 v22 v23 v24 v25",       /* 5 */
      
      "LMOVE key1 key2 LEFT LEFT",        /* v11 , v21 ... v25*/
      "lmove key1 key2 left right",       /* v11, v21 .. v25 , v12*/
      "LMOVE key1 key2 RIGHT LEFT",       /* v15, v11, v21 .. v25, v12 */
      "lmove key1 key2 right right",      /* v15, v11, v21 .. v25, v12, v14*/
      "LLEN key1",                        /* 1 */
      "LLEN key2",                        /* 9 */
      
      "LMOVE key1 key2 LEFT LEFT",        /* v13, v15, v11, v21 ... v25, v12, v14*/

      "LLEN key1",                        /* 0 */
      "LLEN key2",                        /* 10 */
      
      "LMOVE key1 key2 LEFT RIGHT",        /* NULL */
      
      "lmove key2 key1 right left",       /* v14 */
      "lmove key2 key1 left left",        /* v13, v14*/
      "LMOVE key2 key1 RIGHT LEFT",       /* v12, v13, v14 */
      "LMOVE key2 key1 LEFT RIGHT",       /* v12, v13, v14, v15 */
      "LMOVE key2 key1 LEFT LEFT",        /* v11, v12, v13, v14, v15 */
      
      "LLEN key1",                        /* 5 */
      "LLEN key2",                        /* 5 */
      
   
      "LMOVE key1 key1 LEFT RIGHT",       /* v11 */
      "LMOVE key1 key1 LEFT RIGHT",       /* v12 */
      "LMOVE key1 key1 LEFT RIGHT",       /* v13 */
      "LMOVE key1 key1 LEFT RIGHT",       /* v14 */
      "lmove key1 key1 LEFT RIGHT",       /* v15 */
      
      "LINDEX key1 0",                     /* v11 */
      "LINDEX key1 1",                     /* v12 */
      "LINDEX key1 2",                     /* v13 */
      "LINDEX key1 3",                     /* v14 */
      "LINDEX key1 4",                     /* v15 */
      
  };
  
  protected String[] validResponses = new String[] {
      ":5\r\n",
      ":5\r\n",

      "$3\r\nv11\r\n",
      "$3\r\nv12\r\n",
      "$3\r\nv15\r\n",
      "$3\r\nv14\r\n",

      ":1\r\n",
      ":9\r\n",
      
      "$3\r\nv13\r\n",
      ":0\r\n",
      ":10\r\n",

      "$-1\r\n",

      "$3\r\nv14\r\n",
      "$3\r\nv13\r\n",
      "$3\r\nv12\r\n",
      "$3\r\nv15\r\n",
      "$3\r\nv11\r\n",

      
      ":5\r\n",
      ":5\r\n",
      
      "$3\r\nv11\r\n",
      "$3\r\nv12\r\n",
      "$3\r\nv13\r\n",
      "$3\r\nv14\r\n",
      "$3\r\nv15\r\n",
 
      "$3\r\nv11\r\n",
      "$3\r\nv12\r\n",
      "$3\r\nv13\r\n",
      "$3\r\nv14\r\n",
      "$3\r\nv15\r\n",
  };
  
  
  protected String[] invalidRequests = new String[] {
      "lmov x y",                     /* unsupported command */
      "LMOVE",                         /* wrong number of arguments*/
      "LMOVE x",                       /* wrong number of arguments*/
      "LMOVE x y",                     /* wrong number of arguments*/
      "LMOVE x y z",                   /* wrong number of arguments*/
      "LMOVE x y z LEFT",              /* wrong command format */
      "LMOVE x y LEFT RIGH",           /* wrong command format */
      
      
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Unsupported command: LMOV\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n", 
    "-ERR: Wrong number of arguments\r\n",    

    "-ERR: Wrong command format, unexpected argument: z\r\n",   
    "-ERR: Wrong command format, unexpected argument: RIGH\r\n"   
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
