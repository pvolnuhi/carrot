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

package org.bigbase.carrot.examples.twitter;

import java.io.IOException;
import java.util.List;


import redis.clients.jedis.Jedis;

public class TestRedisTwitter {
  
  static Jedis client = new Jedis("localhost");
  
  
  public static void main(String[] args) throws IOException {
    runUsers();
    runUserStatuses();
    runUserTimelines();
    runUserFollowers();
    runUserFollowing();
  }
  
  private static void runUsers() throws IOException {
    System.out.println("Run Users");
    int numUsers = 100000;
    List<User> users = User.newUsers(numUsers);
    int count = 0;
    for(User u: users) {
      count++;
      u.saveToRedis(client);
      if (count  % 10000 == 0) {
        System.out.println("Loaded " + count+ " users");
      }
    }
   System.out.println("Print any button ...");
   System.in.read();
   
  }
  
  
  private static void runUserStatuses() throws IOException {
    int numUsers = 1000;
    System.out.println("Run User Statuses");

    List<User> users = User.newUsers(numUsers);
    List<UserStatus> statuses = null;
    int count = 0;
    for(User user: users) {
      count++;
      statuses = UserStatus.newUserStatuses(user);
      for(UserStatus us: statuses) {
        us.saveToRedis(client);
      }
      if (count  % 100 == 0) {
        System.out.println("Loaded " + count+ " user statuses");
      }
    }
    
    System.out.println("Print any button ...");
    System.in.read();
  }
  
  
  private static void runUserTimelines() throws IOException {
    int numUsers = 1000;
    System.out.println("Run User Timeline");

    List<User> users = User.newUsers(numUsers);
    int count = 0;
    for(User user: users) {
      count++;
      Timeline timeline = new Timeline(user);
      timeline.saveToRedis(client);
      if (count  % 100 == 0) {
        System.out.println("Loaded " + count+ " user timelines");
      }
    }
    System.out.println("Print any button ...");
    System.in.read();
  }

  
  private static void runUserFollowers() throws IOException {
    int numUsers = 10000;
    System.out.println("Run User Followers");

    List<User> users = User.newUsers(numUsers);
    int count = 0;
    for(User user: users) {
      count++;
      Followers followers = new Followers(user);
      followers.saveToRedis(client);
      if (count  % 100 == 0) {
        System.out.println("Loaded " + count+ " user followers");
      }
    }
    
    System.out.println("Print any button ...");
    System.in.read();
  }
 
  private static void runUserFollowing() throws IOException {
    int numUsers = 10000;
    System.out.println("Run User Following");

    List<User> users = User.newUsers(numUsers);
    int count = 0;
    for(User user: users) {
      count++;
      Following following = new Following(user);
      following.saveToRedis(client);
      if (count  % 1000 == 0) {
        System.out.println("Loaded " + count+ " user following");
      }
    }
    
    System.out.println("Print any button ...");
    System.in.read();
  }
  
}
