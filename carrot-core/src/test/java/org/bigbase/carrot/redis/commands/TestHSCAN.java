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

public class TestHSCAN extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "HSET key v10 a v11 a v12 a v13 a v14 a v15 a v20 a v21 a v22 a v23 a v24 a v25 a v30 a v31 a v32 a v33 a v34 a v35 a" +
          " a10 a a11 a a12 a a13 a a14 a a15 a a20 a a21 a a22 a a23 a a24 a a25 a a30 a a31 a a32 a a33 a a34 a a35 a",       /* 36 */     
      "HLEN key",                                                                                                               /* 36 */
      "hscan key 0",
      "HSCAN key 1",
      "HSCAN key 2",
      "HSCAN key 3",
      "HSCAN key 4",
      "HSCAN key 5",
      "HSCAN key 6",
      "HSCAN key 7",
      "HSCAN key 8",
      
      "HSCAN key 0 COUNT 20",
      "HSCAN key 10 COUNT 20",
      "HSCAN key 11 COUNT 20",
      "HSCAN key 12 COUNT 20",
      "hscan key 13 count 20",
      
      // Scan all which start with 'a'
      "HSCAN key 0 MATCH ^a.* COUNT 20",
      "HSCAN key 15 MATCH ^a.* COUNT 20",
      "hscan key 16 match ^a.* count 20",
      
  };
  
  protected String[] validResponses = new String[] {
      ":36\r\n",
      ":36\r\n",
      
      "*2\r\n$1\r\n1\r\n" +
      "*10\r\n$3\r\na10\r\n$1\r\na\r\n$3\r\na11\r\n$1\r\na\r\n$3\r\na12\r\n$1\r\na\r\n$3\r\na13\r\n$1\r\na\r\n$3\r\na14\r\n$1\r\na\r\n",
      
      "*2\r\n$1\r\n2\r\n" +
      "*10\r\n$3\r\na15\r\n$1\r\na\r\n$3\r\na20\r\n$1\r\na\r\n$3\r\na21\r\n$1\r\na\r\n$3\r\na22\r\n$1\r\na\r\n$3\r\na23\r\n$1\r\na\r\n",
      
      "*2\r\n$1\r\n3\r\n" +
      "*10\r\n$3\r\na24\r\n$1\r\na\r\n$3\r\na25\r\n$1\r\na\r\n$3\r\na30\r\n$1\r\na\r\n$3\r\na31\r\n$1\r\na\r\n$3\r\na32\r\n$1\r\na\r\n",
            
      "*2\r\n$1\r\n4\r\n" +
      "*10\r\n$3\r\na33\r\n$1\r\na\r\n$3\r\na34\r\n$1\r\na\r\n$3\r\na35\r\n$1\r\na\r\n$3\r\nv10\r\n$1\r\na\r\n$3\r\nv11\r\n$1\r\na\r\n",
      
      "*2\r\n$1\r\n5\r\n" +
      "*10\r\n$3\r\nv12\r\n$1\r\na\r\n$3\r\nv13\r\n$1\r\na\r\n$3\r\nv14\r\n$1\r\na\r\n$3\r\nv15\r\n$1\r\na\r\n$3\r\nv20\r\n$1\r\na\r\n",
      
      "*2\r\n$1\r\n6\r\n" +
      "*10\r\n$3\r\nv21\r\n$1\r\na\r\n$3\r\nv22\r\n$1\r\na\r\n$3\r\nv23\r\n$1\r\na\r\n$3\r\nv24\r\n$1\r\na\r\n$3\r\nv25\r\n$1\r\na\r\n",
 
      "*2\r\n$1\r\n7\r\n" +
      "*10\r\n$3\r\nv30\r\n$1\r\na\r\n$3\r\nv31\r\n$1\r\na\r\n$3\r\nv32\r\n$1\r\na\r\n$3\r\nv33\r\n$1\r\na\r\n$3\r\nv34\r\n$1\r\na\r\n",
      
      "*2\r\n$1\r\n8\r\n" +
      "*2\r\n$3\r\nv35\r\n$1\r\na\r\n",
      
      "*2\r\n$1\r\n0\r\n*0\r\n",


      "*2\r\n$2\r\n10\r\n" +
      "*20\r\n$3\r\na10\r\n$1\r\na\r\n$3\r\na11\r\n$1\r\na\r\n$3\r\na12\r\n$1\r\na\r\n$3\r\na13\r\n$1\r\na\r\n$3\r\na14\r\n$1\r\na\r\n" +
      "$3\r\na15\r\n$1\r\na\r\n$3\r\na20\r\n$1\r\na\r\n$3\r\na21\r\n$1\r\na\r\n$3\r\na22\r\n$1\r\na\r\n$3\r\na23\r\n$1\r\na\r\n",
      
      "*2\r\n$2\r\n11\r\n" +
      "*20\r\n$3\r\na24\r\n$1\r\na\r\n$3\r\na25\r\n$1\r\na\r\n$3\r\na30\r\n$1\r\na\r\n$3\r\na31\r\n$1\r\na\r\n$3\r\na32\r\n$1\r\na\r\n" +            
      "$3\r\na33\r\n$1\r\na\r\n$3\r\na34\r\n$1\r\na\r\n$3\r\na35\r\n$1\r\na\r\n$3\r\nv10\r\n$1\r\na\r\n$3\r\nv11\r\n$1\r\na\r\n",

      "*2\r\n$2\r\n12\r\n" +
      "*20\r\n$3\r\nv12\r\n$1\r\na\r\n$3\r\nv13\r\n$1\r\na\r\n$3\r\nv14\r\n$1\r\na\r\n$3\r\nv15\r\n$1\r\na\r\n$3\r\nv20\r\n$1\r\na\r\n" +
      "$3\r\nv21\r\n$1\r\na\r\n$3\r\nv22\r\n$1\r\na\r\n$3\r\nv23\r\n$1\r\na\r\n$3\r\nv24\r\n$1\r\na\r\n$3\r\nv25\r\n$1\r\na\r\n",
     
      "*2\r\n$2\r\n13\r\n" +
      "*12\r\n$3\r\nv30\r\n$1\r\na\r\n$3\r\nv31\r\n$1\r\na\r\n$3\r\nv32\r\n$1\r\na\r\n$3\r\nv33\r\n$1\r\na\r\n$3\r\nv34\r\n$1\r\na\r\n" +
      "$3\r\nv35\r\n$1\r\na\r\n",
      
      "*2\r\n$1\r\n0\r\n*0\r\n",
      
      "*2\r\n$2\r\n15\r\n" +
      "*20\r\n$3\r\na10\r\n$1\r\na\r\n$3\r\na11\r\n$1\r\na\r\n$3\r\na12\r\n$1\r\na\r\n$3\r\na13\r\n$1\r\na\r\n$3\r\na14\r\n$1\r\na\r\n" +
      "$3\r\na15\r\n$1\r\na\r\n$3\r\na20\r\n$1\r\na\r\n$3\r\na21\r\n$1\r\na\r\n$3\r\na22\r\n$1\r\na\r\n$3\r\na23\r\n$1\r\na\r\n",
      
      "*2\r\n$2\r\n16\r\n" +
      "*16\r\n$3\r\na24\r\n$1\r\na\r\n$3\r\na25\r\n$1\r\na\r\n$3\r\na30\r\n$1\r\na\r\n$3\r\na31\r\n$1\r\na\r\n$3\r\na32\r\n$1\r\na\r\n" +            
      "$3\r\na33\r\n$1\r\na\r\n$3\r\na34\r\n$1\r\na\r\n$3\r\na35\r\n$1\r\na\r\n",
      
      "*2\r\n$1\r\n0\r\n*0\r\n",

  };
  
  protected String[] invalidRequests = new String[] {
      "HSCAN",                          /* wrong number of arguments */
      "HSCAN x",                        /* wrong number of arguments */
      "HSCAN x y",                      /* invalid cursor */
      "HSCAN x 0 METCH ^a.*",          /* Wrong command format */
      "HSCAN x 0 MATCH ^a.* CENT 10",  /* Wrong command format */
      "HSCAN x 0 MATCH ^a.* COUNT 10a",/* Wrong number format */

  };
  
  protected String[] invalidResponses = new String[] {
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Wrong number of arguments\r\n",
    "-ERR: Invalid cursor: y\r\n",
    "-ERR: Wrong command format, unexpected argument: METCH\r\n",
    "-ERR: Wrong command format, unexpected argument: CENT\r\n",
    "-ERR: Wrong number format: 10a\r\n"
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
