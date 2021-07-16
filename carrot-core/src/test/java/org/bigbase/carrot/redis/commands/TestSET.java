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

public class TestSET extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "SET key value",                  /* OK */
      "SET key value EX 1000",          /* OK */
      "SET key value PX 10000",         /* OK */
      "SET key value EXAT 100000",      /* OK */
      "SET key value PXAT 1000000",     /* OK */
      "SET key value KEEPTTL",          /* OK */

      "SET key value NX",               /* NULL */
      "SET key value XX",               /* OK */
      "SET key1 value XX",              /* NULL*/
      "SET key1 value NX",              /* OK */ 
      
      
      "SET key value EX 1000 NX",       /* NULL */
      "SET key value EX 1000 XX",       /* OK */
      "SET key2 value EX 1000 XX",      /* NULL */
      "SET key2 value EX 1000 NX",      /* OK */
     

      "SET key value PX 1000 NX",       /* NULL */
      "SET key value PX 1000 XX",       /* OK */
      "SET key3 value PX 1000 XX",      /* NULL */
      "SET key3 value PX 1000 NX",      /* OK */

      "SET key value EXAT 1000 NX",     /* NULL */
      "SET key value EXAT 1000 XX",     /* OK */
      "SET key4 value EXAT 1000 XX",    /* NULL*/
      "SET key4 value EXAT 1000 NX",    /* OK */
      
      "SET key value PXAT 1000 NX",     /* NULL */
      "SET key value PXAT 1000 XX",     /* OK */
      "SET key5 value PXAT 1000 XX",    /* NULL */
      "SET key5 value PXAT 1000 NX",    /* OK */
      
      "SET key value KEEPTTL NX",       /* NULL */
      "SET key value KEEPTTL XX",       /* OK */
      "SET key6 value KEEPTTL XX",      /* NULL */
      "SET key6 value KEEPTTL NX",      /* OK */
      
      "SET key value GET",              /* VALUE */

      
      "SET key value NX GET",           /* VALUE */
      "SET key value XX GET",           /* VALUE */
      "SET key7 value NX GET",          /* NULL */
      "SET key7 value XX GET",          /* VALUE */

      
      "SET key value EX 1000 NX GET",   /* VALUE */
      "SET key value EX 1000 XX GET",   /* VALUE */
      "SET key8 value EX 1000 NX GET",  /* NULL */
      "SET key8 value EX 1000 XX GET",  /* VALUE*/
      
      "SET key value PX 1000 NX GET",
      "SET key value PX 1000 XX GET",
      "SET key9 value PX 1000 NX GET",
      "SET key9 value PX 1000 XX GET",

      "SET key value EXAT 1000 NX GET",
      "SET key value EXAT 1000 XX GET",
      "SET key10 value EXAT 1000 NX GET",
      "SET key10 value EXAT 1000 XX GET",
      
      "SET key value PXAT 1000 NX GET",
      "SET key value PXAT 1000 XX GET",
      "SET key11 value PXAT 1000 NX GET",
      "SET key11 value PXAT 1000 XX GET",
      
      "SET key value KEEPTTL NX GET",
      "SET key value KEEPTTL XX GET",
      "SET key12 value KEEPTTL NX GET",
      "SET key12 value KEEPTTL XX GET"
  };
  
  protected String[] validResponses = new String[] {
      "+OK\r\n",
      "+OK\r\n",
      "+OK\r\n",
      "+OK\r\n",
      "+OK\r\n",
      "+OK\r\n",

      "$-1\r\n",
      "+OK\r\n",
      "$-1\r\n",
      "+OK\r\n",
      
      "$-1\r\n",
      "+OK\r\n",
      "$-1\r\n",
      "+OK\r\n",
      
      "$-1\r\n",
      "+OK\r\n",
      "$-1\r\n",
      "+OK\r\n",
      
      "$-1\r\n",
      "+OK\r\n",
      "$-1\r\n",
      "+OK\r\n",
      
      "$-1\r\n",
      "+OK\r\n",
      "$-1\r\n",
      "+OK\r\n",
      
      "$-1\r\n",
      "+OK\r\n",
      "$-1\r\n",
      "+OK\r\n",
      
      "$5\r\nvalue\r\n",

      "$5\r\nvalue\r\n",
      "$5\r\nvalue\r\n",
      "$-1\r\n",
      "$5\r\nvalue\r\n",
      
      "$5\r\nvalue\r\n",
      "$5\r\nvalue\r\n",
      "$-1\r\n",
      "$5\r\nvalue\r\n",
      
      "$5\r\nvalue\r\n",
      "$5\r\nvalue\r\n",
      "$-1\r\n",
      "$5\r\nvalue\r\n",

      "$5\r\nvalue\r\n",
      "$5\r\nvalue\r\n",
      "$-1\r\n",
      "$5\r\nvalue\r\n",
      
      "$5\r\nvalue\r\n",
      "$5\r\nvalue\r\n",
      "$-1\r\n",
      "$5\r\nvalue\r\n",
      
      "$5\r\nvalue\r\n",
      "$5\r\nvalue\r\n",
      "$-1\r\n",
      "$5\r\nvalue\r\n"
  };
  
  
  protected String[] invalidRequests = new String[] {
      "set x y",                      /* unsupported command */
      "SET",                          /* wrong number of arguments*/
      "SET key",                      /* wrong number of arguments*/
      "SET key value1 value2",        /* wrong number of arguments*/
      "SET key value EX",             /* wrong number of arguments*/
      "SET key value EX 10000 va",    /* wrong number of arguments*/
      "SET key value PX",             /* wrong number of arguments*/
      "SET key value EXAT",           /* wrong number of arguments*/
      "SET key value PXAT",           /* wrong number of arguments*/
      "SET key value PX 10000 bb",    /* wrong number of arguments*/
      "SET key value EXAT 100000 bb", /* wrong number of arguments*/
      "SET key value PXAT 1000000 bb",/* wrong number of arguments*/
      "SET key value PX a10",         /* wrong number format      */
      "SET key value EXAT bdf",       /* wrong number format      */
      "SET key value PXAT sss",       /* wrong number format      */
      "SET key value NX GET PX 1000", 
      "SET key value GET PX 1000 XX",
      "SET key9 value PX 1000 GET NX",
      "SET key9 value XX PX 1000 GET"
  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR Unsupported command: set\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong command format, unexpected argument: value2\r\n",
    "-ERR: Wrong command format, unexpected argument: EX\r\n",
    "-ERR: Wrong command format, unexpected argument: va\r\n",
    "-ERR: Wrong command format, unexpected argument: PX\r\n",
    "-ERR: Wrong command format, unexpected argument: EXAT\r\n",
    "-ERR: Wrong command format, unexpected argument: PXAT\r\n",
    "-ERR: Wrong command format, unexpected argument: bb\r\n",
    "-ERR: Wrong command format, unexpected argument: bb\r\n",
    "-ERR: Wrong command format, unexpected argument: bb\r\n",
    "-ERR: Wrong number format: a10\r\n",
    "-ERR: Wrong number format: bdf\r\n",
    "-ERR: Wrong number format: sss\r\n",
    "-ERR: Wrong command format, unexpected argument: PX\r\n",
    "-ERR: Wrong command format, unexpected argument: PX\r\n",
    "-ERR: Wrong command format, unexpected argument: NX\r\n",
    "-ERR: Wrong command format, unexpected argument: PX\r\n"
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
