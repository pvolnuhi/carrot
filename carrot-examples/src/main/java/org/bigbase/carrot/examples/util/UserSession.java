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

package org.bigbase.carrot.examples.util;

import java.util.Properties;
import java.util.Random;

import org.bigbase.carrot.util.Bytes;

/**
 * Simple user session class
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
 * "StartTime" - start session time (timestamp - 8 bytes)
 * "LastActiveTime" - last interaction time (timestamp - 8 bytes)
 * @author vrodionov
 *
 */
public class UserSession extends KeyValues{ 
  static Random rnd = new Random();
  
  UserSession(Properties p){
    super(p);
  }
  
  public String getSessionId() {
    return (String)props.getProperty("SessionId");
  }
  
  public String getUserId() {
    return (String)props.getProperty("UserId");
  }
  
  public String getHost() {
    return (String)props.getProperty("Host");
  }
  
  public String getType() {
    return (String)props.getProperty("Type");
  }
  
  public String getState() {
    return (String)props.getProperty("State");
  }
  
  public String getMaxIdleTime() {
    return (String)props.getProperty("MaxIdleTime");
  }
  
  public String getMaxSessionTime() {
    return (String)props.getProperty("MaxSessionTime");
  }
  
  public String getMaxCachingTime() {
    return (String)props.getProperty("MaxCachingTime");
  }

  public String getStartTime() {
    return (String)props.getProperty("StartTime");
  }

  public String getLastActiveTime() {
    return (String)props.getProperty("LastActiveTime");
  }

  
  public static UserSession newSession(int i) {
    Properties p = new Properties();
    
    p.put("SessionId", sessionId());
    p.put("Host", host());
    p.put("UserId", userId(i));
    p.put("Type", type());
    p.put("State", state());
    p.put("MaxIdleTime", maxIdleTime());
    p.put("MaxSessionTime", maxSessionTime());
    p.put("MaxCachingTime", maxCachingTime());
    p.put("StartTime", startTime());
    p.put("LastActiveTime", lastActiveTime());
    
    return new UserSession(p);
  }
  
  private static String sessionId() {
    byte[] buf = new byte[8];
    rnd.nextBytes(buf);
    return Bytes.toHex(buf);
  }
  
  private static String host() {
    
    int v1 = rnd.nextInt(256);
    int v2 = rnd.nextInt(256);
    int v3 = rnd.nextInt(256);
    int v4 = rnd.nextInt(256);
    return v1 + "." + v2 + "." + v3 +"." + v4;
  }
  
  private static String userId(int userId) {
    return "session:user:"+userId;
  }
  
  private static String type() {
    double d = rnd.nextDouble();
    if (d < 0.1) return "APPLICATION";
    return "USER";
  }
  
  private static String state() {
    double d = rnd.nextDouble();
    if (d < 0.01) return "INVALID";
    return "VALID";
  }
  
  private static String maxIdleTime() {
    return "30";
  }
  
  private static String maxSessionTime() {
    return "600";
  }
  
  private static String maxCachingTime() {
    return "10";
  }
  
  private static String startTime() {
    long time = System.currentTimeMillis()/1000; // discard milliseconds
    int period = 24 * 3600; 
    return Long.toString(time - rnd.nextInt(period));
  }
  
  private static String lastActiveTime() {
    long time = System.currentTimeMillis()/1000; // discard milliseconds
    int period = 3600; 
    return Long.toString(time - rnd.nextInt(period));
  }
  
  public String toString() {
    return props.toString();
  }
  
  public int size() {
    return props.size();
  }

  @Override
  public String getKey() {
    // TODO Auto-generated method stub
    return null;
  }
  
}