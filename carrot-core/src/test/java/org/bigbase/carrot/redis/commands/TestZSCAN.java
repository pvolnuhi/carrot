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

public class TestZSCAN extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "ZADD key 0 v10 0 v11 0 v12 0 v13 0 v14 0 v15 0 v20 0 v21 0 v22 0 v23 0 v24 0 v25 0 v30 0 v31 0 v32 0 v33 0 v34 0 v35 0" +
          " a10 0 a11 0 a12 0 a13 0 a14 0 a15 0 a20 0 a21 0 a22 0 a23 0 a24 0 a25 0 a30 0 a31 0 a32 0 a33 0 a34 0 a35",       /* 36 */     
      "ZCARD key",                                                                                                            /* 36 */
      "zscan key 0 COUNT 5", // lower case name
      "ZSCAN key 1 COUNT 5",
      "ZSCAN key 2 COUNT 5",
      "ZSCAN key 3 COUNT 5",
      "ZSCAN key 4 COUNT 5",
      "ZSCAN key 5 COUNT 5",
      "ZSCAN key 6 COUNT 5",
      "ZSCAN key 7 COUNT 5",
      "ZSCAN key 8 COUNT 5",
      
      "ZSCAN key 0 COUNT 10",
      "ZSCAN key 10 COUNT 10",
      "ZSCAN key 11 COUNT 10",
      "ZSCAN key 12 COUNT 10",
      "ZSCAN key 13 COUNT 10",
      
      // Scan all which start with 'a'
      "ZSCAN key 0 MATCH ^a.* COUNT 10",
      "ZSCAN key 15 MATCH ^a.* COUNT 10",
      "zscan key 16 match ^a.* count 10"
      
  };
  
  protected String[] validResponses = new String[] {
      ":36\r\n",
      ":36\r\n",
      
      "*2\r\n$1\r\n1\r\n" +
      "*10\r\n$3\r\na10\r\n$3\r\n0.0\r\n$3\r\na11\r\n$3\r\n0.0\r\n$3\r\na12\r\n$3\r\n0.0\r\n$3\r\na13\r\n$3\r\n0.0\r\n$3\r\na14\r\n$3\r\n0.0\r\n",
      
      "*2\r\n$1\r\n2\r\n" +
      "*10\r\n$3\r\na15\r\n$3\r\n0.0\r\n$3\r\na20\r\n$3\r\n0.0\r\n$3\r\na21\r\n$3\r\n0.0\r\n$3\r\na22\r\n$3\r\n0.0\r\n$3\r\na23\r\n$3\r\n0.0\r\n",
      
      "*2\r\n$1\r\n3\r\n" +
      "*10\r\n$3\r\na24\r\n$3\r\n0.0\r\n$3\r\na25\r\n$3\r\n0.0\r\n$3\r\na30\r\n$3\r\n0.0\r\n$3\r\na31\r\n$3\r\n0.0\r\n$3\r\na32\r\n$3\r\n0.0\r\n",
            
      "*2\r\n$1\r\n4\r\n" +
      "*10\r\n$3\r\na33\r\n$3\r\n0.0\r\n$3\r\na34\r\n$3\r\n0.0\r\n$3\r\na35\r\n$3\r\n0.0\r\n$3\r\nv10\r\n$3\r\n0.0\r\n$3\r\nv11\r\n$3\r\n0.0\r\n",
      
      "*2\r\n$1\r\n5\r\n" +
      "*10\r\n$3\r\nv12\r\n$3\r\n0.0\r\n$3\r\nv13\r\n$3\r\n0.0\r\n$3\r\nv14\r\n$3\r\n0.0\r\n$3\r\nv15\r\n$3\r\n0.0\r\n$3\r\nv20\r\n$3\r\n0.0\r\n",
      
      "*2\r\n$1\r\n6\r\n" +
      "*10\r\n$3\r\nv21\r\n$3\r\n0.0\r\n$3\r\nv22\r\n$3\r\n0.0\r\n$3\r\nv23\r\n$3\r\n0.0\r\n$3\r\nv24\r\n$3\r\n0.0\r\n$3\r\nv25\r\n$3\r\n0.0\r\n",
 
      "*2\r\n$1\r\n7\r\n" +
      "*10\r\n$3\r\nv30\r\n$3\r\n0.0\r\n$3\r\nv31\r\n$3\r\n0.0\r\n$3\r\nv32\r\n$3\r\n0.0\r\n$3\r\nv33\r\n$3\r\n0.0\r\n$3\r\nv34\r\n$3\r\n0.0\r\n",
      
      "*2\r\n$1\r\n8\r\n" +
      "*2\r\n$3\r\nv35\r\n$3\r\n0.0\r\n",
      
      "*2\r\n$1\r\n0\r\n*0\r\n",


      "*2\r\n$2\r\n10\r\n" +
      "*20\r\n$3\r\na10\r\n$3\r\n0.0\r\n$3\r\na11\r\n$3\r\n0.0\r\n$3\r\na12\r\n$3\r\n0.0\r\n$3\r\na13\r\n$3\r\n0.0\r\n$3\r\na14\r\n$3\r\n0.0\r\n" +
      "$3\r\na15\r\n$3\r\n0.0\r\n$3\r\na20\r\n$3\r\n0.0\r\n$3\r\na21\r\n$3\r\n0.0\r\n$3\r\na22\r\n$3\r\n0.0\r\n$3\r\na23\r\n$3\r\n0.0\r\n",
      
      "*2\r\n$2\r\n11\r\n" +
      "*20\r\n$3\r\na24\r\n$3\r\n0.0\r\n$3\r\na25\r\n$3\r\n0.0\r\n$3\r\na30\r\n$3\r\n0.0\r\n$3\r\na31\r\n$3\r\n0.0\r\n$3\r\na32\r\n$3\r\n0.0\r\n" +            
      "$3\r\na33\r\n$3\r\n0.0\r\n$3\r\na34\r\n$3\r\n0.0\r\n$3\r\na35\r\n$3\r\n0.0\r\n$3\r\nv10\r\n$3\r\n0.0\r\n$3\r\nv11\r\n$3\r\n0.0\r\n",

      "*2\r\n$2\r\n12\r\n" +
      "*20\r\n$3\r\nv12\r\n$3\r\n0.0\r\n$3\r\nv13\r\n$3\r\n0.0\r\n$3\r\nv14\r\n$3\r\n0.0\r\n$3\r\nv15\r\n$3\r\n0.0\r\n$3\r\nv20\r\n$3\r\n0.0\r\n" +
      "$3\r\nv21\r\n$3\r\n0.0\r\n$3\r\nv22\r\n$3\r\n0.0\r\n$3\r\nv23\r\n$3\r\n0.0\r\n$3\r\nv24\r\n$3\r\n0.0\r\n$3\r\nv25\r\n$3\r\n0.0\r\n",
     
      "*2\r\n$2\r\n13\r\n" +
      "*12\r\n$3\r\nv30\r\n$3\r\n0.0\r\n$3\r\nv31\r\n$3\r\n0.0\r\n$3\r\nv32\r\n$3\r\n0.0\r\n$3\r\nv33\r\n$3\r\n0.0\r\n$3\r\nv34\r\n$3\r\n0.0\r\n" +
      "$3\r\nv35\r\n$3\r\n0.0\r\n",
      
      "*2\r\n$1\r\n0\r\n*0\r\n",
      
      "*2\r\n$2\r\n15\r\n" +
      "*20\r\n$3\r\na10\r\n$3\r\n0.0\r\n$3\r\na11\r\n$3\r\n0.0\r\n$3\r\na12\r\n$3\r\n0.0\r\n$3\r\na13\r\n$3\r\n0.0\r\n$3\r\na14\r\n$3\r\n0.0\r\n" +
      "$3\r\na15\r\n$3\r\n0.0\r\n$3\r\na20\r\n$3\r\n0.0\r\n$3\r\na21\r\n$3\r\n0.0\r\n$3\r\na22\r\n$3\r\n0.0\r\n$3\r\na23\r\n$3\r\n0.0\r\n",
      
      "*2\r\n$2\r\n16\r\n" +
      "*16\r\n$3\r\na24\r\n$3\r\n0.0\r\n$3\r\na25\r\n$3\r\n0.0\r\n$3\r\na30\r\n$3\r\n0.0\r\n$3\r\na31\r\n$3\r\n0.0\r\n$3\r\na32\r\n$3\r\n0.0\r\n" +            
      "$3\r\na33\r\n$3\r\n0.0\r\n$3\r\na34\r\n$3\r\n0.0\r\n$3\r\na35\r\n$3\r\n0.0\r\n",
      
      "*2\r\n$1\r\n0\r\n*0\r\n",

  };
  
  protected String[] invalidRequests = new String[] {
      "ZSCAN",                          /* wrong number of arguments */
      "ZSCAN x",                        /* wrong number of arguments */
      "ZSCAN x y",                      /* invalid cursor */
      "ZSCAN x 0 METCH ^a.*",          /* Wrong command format */
      "ZSCAN x 0 MATCH ^a.* CENT 10",  /* Wrong command format */
      "ZSCAN x 0 MATCH ^a.* COUNT 10a",/* Wrong number format */

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
