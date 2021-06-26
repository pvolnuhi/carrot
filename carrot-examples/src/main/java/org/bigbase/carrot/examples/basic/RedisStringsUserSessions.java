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
 */

package org.bigbase.carrot.examples.basic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bigbase.carrot.examples.util.UserSession;
import org.bigbase.carrot.ops.OperationFailedException;

import redis.clients.jedis.Jedis;

/**
 * This example shows how to use Redis Strings to store user sessions objects 
 * 
 * User Session structure:
 * "SessionID" - A unique, universal identifier for the session data structure (16 bytes).
 * "Host" - host name or IP Address The location from which the client (browser) is making the request.
 * "UserID" - Set to the user's distinguished name (DN) or the application's principal name.
 * "Type" - USER or APPLICATION
 * "State" - session state: VALID, INVALID Defines whether the session is valid or invalid.
 * "MaxIdleTime" - Maximum Idle Time Maximum number of minutes without activity before the session will 
 *   expire and the user must reauthenticate.
 * "MaxSessionTime" - Maximum Session Time. Maximum number of minutes (activity or no activity) before 
 *   the session expires and the user must reauthenticate.
 * "MaxCachingTime" - Maximum number of minutes before the client contacts OpenSSO Enterprise to refresh 
 * cached session information.
 * 
 * Test description: <br>
 * 
 * UserSession object has 8 fields, one field (UserId) is used as a String key
 * 
 * Average key + session object size is 222 bytes. We load 100K user session objects
 * 
 * Results:
 * 0. Average user session data size = 222 bytes (includes key size)
 * 1. No compression. Used RAM per session object is 275 bytes (COMPRESSION= 0.8)
 * 2. LZ4 compression. Used RAM per session object is 94 bytes (COMPRESSION = 2.37)
 * 3. LZ4HC compression. Used RAM per session object is 88 bytes (COMPRESSION = 2.5)
 * 
 * Redis usage per session object, using String encoding is ~290 bytes
 * 
 * RAM usage (Redis-to-Carrot)
 * 
 * 1) No compression    290/275 ~ 1.16x
 * 2) LZ4   compression 290/94 ~ 3.4x
 * 3) LZ4HC compression 290/88 ~ 3.64x 
 * 
 * Effect of a compression:
 * 
 * LZ4  - 2.37/0.8 = 2.96    (to no compression)
 * LZ4HC - 2.5/0.8 = 3.13  (to no compression)
 * @author vrodionov
 *
 */
public class RedisStringsUserSessions {
  

  static long N = 1000000;
  static long totalDataSize = 0;
  static List<UserSession> userSessions = new ArrayList<UserSession>();
  
  static {
    for (int i = 0; i < N; i++) {
      userSessions.add(UserSession.newSession(i));
    }
    Collections.shuffle(userSessions);
  }
  
  public static void main(String[] args) throws IOException, OperationFailedException {

    System.out.println("RUN Redis");
    runTest();

  }
  
  private static void runTest() throws IOException, OperationFailedException {
    
    Jedis client = new Jedis("localhost");    
    totalDataSize = 0;
    
    long startTime = System.currentTimeMillis();
    int count =0;
    for (UserSession us: userSessions) {
      count++;
      String skey = us.getUserId();
      String svalue = us.toString();
      totalDataSize += skey.length() + svalue.length();    
      client.set(skey, svalue);
      if (count % 10000 == 0) {
        System.out.println("set "+ count);
      }
    }
    long endTime = System.currentTimeMillis();
        
    System.out.println("Loaded " + userSessions.size() +" user sessions, total size="+totalDataSize
      + " in "+ (endTime - startTime) );
   
    
    System.out.println("Press any button ...");
    System.in.read();
    startTime = System.currentTimeMillis();
    for (UserSession us: userSessions) {
      count++;
      String skey = us.getUserId();
      client.del(skey);
      if (count % 10000 == 0) {
        System.out.println("del "+ count);
      }
    }
    endTime = System.currentTimeMillis();
        
    System.out.println("Deleted " + userSessions.size() +" user sessions, total size="+totalDataSize
      + " in "+ (endTime - startTime) );
    client.close();
  }

}
