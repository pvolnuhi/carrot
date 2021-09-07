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

public class TestDEL extends CommandBase {
  
  protected String[] validRequests = new String[] {
      "SET key1 100",
      "SET key2 100",
      "SET key3 100",
      "SET key4 100",
      "SET key5 100",
      "SET key6 100",
      "SET key7 100",
      "SET key8 100",
      "SET key9 100",
      "SET key10 100",
      "del key1", 
      "DEL key1",
      "DEL key2 key3 key4 key5",
      "DEL key2 key3 key4 key5",
      "DEL key2 key3 key4 key5 key6 key7 key8 key9 key10",
      "DEL key2 key3 key4 key5 key6 key7 key8 key9 key10"

  };
  
  protected String[] validResponses = new String[] {
      "+OK\r\n",
      "+OK\r\n",
      "+OK\r\n",
      "+OK\r\n",
      "+OK\r\n",
      "+OK\r\n",
      "+OK\r\n",
      "+OK\r\n",
      "+OK\r\n",
      "+OK\r\n",
      ":1\r\n",
      ":0\r\n",
      ":4\r\n",
      ":0\r\n",
      ":5\r\n",
      ":0\r\n"
  };
  
  
  protected String[] invalidRequests = new String[] {
      "DEL",                     /* wrong arg number */
      "del"
  };
  
  protected String[] invalidResponses = new String[] {
      "-ERR: Wrong number of arguments\r\n",
      "-ERR: Wrong number of arguments\r\n",

  };
  
  /**
   * Subclasses must override
   */
  protected String[] getValidRequests() {
    return validRequests;
  }
  
  @Override
  protected String[] getValidResponses() {
    return validResponses;
  }
  @Override  
  protected String[] getInvalidRequests() {
    return invalidRequests;
  }
  @Override
  protected String[] getInvalidResponses() {
    return invalidResponses;
  }
}
