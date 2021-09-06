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

public class TestSSCAN extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "SADD key v10 v11 v12 v13 v14 v15 v20 v21 v22 v23 v24 v25 v30 v31 v32 v33 v34 v35" +
          " a10 a11 a12 a13 a14 a15 a20 a21 a22 a23 a24 a25 a30 a31 a32 a33 a34 a35",                 /* 36 */     
      "SCARD key",                                                                                    /* 36 */
      "sscan key 0", // lower case
      "SSCAN key 1",
      "SSCAN key 2",
      "SSCAN key 3",
      "SSCAN key 4",
      "SSCAN key 0 COUNT 11",
      "SSCAN key 6 COUNT 11",
      "SSCAN key 7 COUNT 11",
      "SSCAN key 8 COUNT 11",
      "SSCAN key 9 COUNT 11",
      // Scan all which start with 'a'
      "SSCAN key 0 MATCH ^a.* COUNT 10",
      "SSCAN key 11 MATCH ^a.* COUNT 10",
      "SSCAN key 12 MATCH ^a.* COUNT 10",
      
  };
  
  protected String[] validResponses = new String[] {
      ":36\r\n",
      ":36\r\n",
      
      "*2\r\n$1\r\n1\r\n" +
      "*10\r\n$3\r\na10\r\n$3\r\na11\r\n$3\r\na12\r\n$3\r\na13\r\n$3\r\na14\r\n$3\r\na15\r\n" +
      "$3\r\na20\r\n$3\r\na21\r\n$3\r\na22\r\n$3\r\na23\r\n",    
      "*2\r\n$1\r\n2\r\n" +
      "*10\r\n$3\r\na24\r\n$3\r\na25\r\n$3\r\na30\r\n$3\r\na31\r\n$3\r\na32\r\n$3\r\na33\r\n" +
      "$3\r\na34\r\n$3\r\na35\r\n$3\r\nv10\r\n$3\r\nv11\r\n",
      "*2\r\n$1\r\n3\r\n" +
      "*10\r\n$3\r\nv12\r\n$3\r\nv13\r\n$3\r\nv14\r\n$3\r\nv15\r\n$3\r\nv20\r\n$3\r\nv21\r\n" +
      "$3\r\nv22\r\n$3\r\nv23\r\n$3\r\nv24\r\n$3\r\nv25\r\n",
      "*2\r\n$1\r\n4\r\n" +
      "*6\r\n$3\r\nv30\r\n$3\r\nv31\r\n$3\r\nv32\r\n$3\r\nv33\r\n$3\r\nv34\r\n$3\r\nv35\r\n",
      "*2\r\n$1\r\n0\r\n*0\r\n",
      
      "*2\r\n$1\r\n6\r\n" +
      "*11\r\n$3\r\na10\r\n$3\r\na11\r\n$3\r\na12\r\n$3\r\na13\r\n$3\r\na14\r\n$3\r\na15\r\n" +
      "$3\r\na20\r\n$3\r\na21\r\n$3\r\na22\r\n$3\r\na23\r\n$3\r\na24\r\n",    
          
      "*2\r\n$1\r\n7\r\n" +
      "*11\r\n$3\r\na25\r\n$3\r\na30\r\n$3\r\na31\r\n$3\r\na32\r\n$3\r\na33\r\n" +
      "$3\r\na34\r\n$3\r\na35\r\n$3\r\nv10\r\n$3\r\nv11\r\n$3\r\nv12\r\n$3\r\nv13\r\n",
      
      "*2\r\n$1\r\n8\r\n" +
      "*11\r\n$3\r\nv14\r\n$3\r\nv15\r\n$3\r\nv20\r\n$3\r\nv21\r\n" +
      "$3\r\nv22\r\n$3\r\nv23\r\n$3\r\nv24\r\n$3\r\nv25\r\n$3\r\nv30\r\n$3\r\nv31\r\n$3\r\nv32\r\n",
      
      "*2\r\n$1\r\n9\r\n" +
      "*3\r\n$3\r\nv33\r\n$3\r\nv34\r\n$3\r\nv35\r\n",
      
      "*2\r\n$1\r\n0\r\n*0\r\n",
      
      "*2\r\n$2\r\n11\r\n" +
      "*10\r\n$3\r\na10\r\n$3\r\na11\r\n$3\r\na12\r\n$3\r\na13\r\n$3\r\na14\r\n$3\r\na15\r\n" +
      "$3\r\na20\r\n$3\r\na21\r\n$3\r\na22\r\n$3\r\na23\r\n",    
      
      "*2\r\n$2\r\n12\r\n" +
      "*8\r\n$3\r\na24\r\n$3\r\na25\r\n$3\r\na30\r\n$3\r\na31\r\n$3\r\na32\r\n$3\r\na33\r\n" +
      "$3\r\na34\r\n$3\r\na35\r\n",
      
      "*2\r\n$1\r\n0\r\n*0\r\n",

  };
  
  protected String[] invalidRequests = new String[] {
      "SSCAN",                          /* wrong number of arguments */
      "SSCAN x",                        /* wrong number of arguments */
      "SSCAN x y",                      /* invalid cursor */
      "SSCAN x 0 METCH ^a.*",          /* Wrong command format */
      "SSCAN x 0 MATCH ^a.* CENT 10",  /* Wrong command format */
      "SSCAN x 0 MATCH ^a.* COUNT 10a",/* Wrong number format */

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
