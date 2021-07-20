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

public class TestGETEX extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "SET key value",              /* OK */
      "GETEX key",                  /* VALUE */
      "GETEX key EX 1000",          /* VALUE */
      "GETEX key PX 10000",         /* VALUE */
      "GETEX key EXAT 100000",      /* VALUE */
      "GETEX key PXAT 1000000",     /* VALUE */
      "GETEX key PERSIST",          /* VALUE */
      "GETEX key1",                  /* NULL */
      "GETEX key1 EX 1000",          /* NULL */
      "GETEX key1 PX 10000",         /* NULL */
      "GETEX key1 EXAT 100000",      /* NULL */
      "GETEX key1 PXAT 1000000",     /* NULL */
      "GETEX key1 PERSIST"          /* NULL */
  
  };
  
  protected String[] validResponses = new String[] {
      "+OK\r\n",
      "$5\r\nvalue\r\n",
      "$5\r\nvalue\r\n",
      "$5\r\nvalue\r\n",
      "$5\r\nvalue\r\n",
      "$5\r\nvalue\r\n",
      "$5\r\nvalue\r\n",

      "$-1\r\n",
      "$-1\r\n",      
      "$-1\r\n",
      "$-1\r\n",      
      "$-1\r\n",
      "$-1\r\n"
  };
  
  
  protected String[] invalidRequests = new String[] {
      "getex x y",                      /* unsupported command */
      "GETEX",                          /* wrong number of arguments*/
      "GETEX key EX 1000 1000",         /* wrong number of arguments*/
      
      "GETEX key value",                /* wrong format*/
      "GETEX key EX",                   /* wrong format*/
      "GETEX key EXAT",                 /* wrong format*/
      "GETEX key PX",                   /* wrong format*/
      "GETEX key PXAT",                 /* wrong format*/
      "GETEX key EX xxx",               /* wrong number format*/
      "GETEX key EXAT xxx",             /* wrong number format*/
      "GETEX key PX xxx",               /* wrong number format*/
      "GETEX key PXAT xxx"             /* wrong number format*/
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: getex\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",

    "-ERR: Wrong command format, unexpected argument: value\r\n",
    "-ERR: Wrong command format, unexpected argument: EX\r\n",
    "-ERR: Wrong command format, unexpected argument: EXAT\r\n",
    "-ERR: Wrong command format, unexpected argument: PX\r\n",
    "-ERR: Wrong command format, unexpected argument: PXAT\r\n",  
    "-ERR: Wrong number format: xxx\r\n",
    "-ERR: Wrong number format: xxx\r\n",
    "-ERR: Wrong number format: xxx\r\n",
    "-ERR: Wrong number format: xxx\r\n"
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
