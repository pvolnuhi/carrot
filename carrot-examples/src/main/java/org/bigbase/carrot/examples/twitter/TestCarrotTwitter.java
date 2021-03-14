package org.bigbase.carrot.examples.twitter;

import java.io.IOException;
import java.util.List;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.BigSortedMapDirectMemoryScanner;
import org.bigbase.carrot.IndexBlock;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;

public class TestCarrotTwitter {
  
  static {
   // UnsafeAccess.debug = true;
  }
  
  static double avg_user_size;
  static double avg_user_status_size;
  static double avg_user_timeline_size;
  static double avg_user_followers_size;
  static double avg_user_following_size;
  
  private static void printSummary() {
    System.out.println("Carrot memory usage per user (user, statuses, profile timeline, followers, following)=" +
         (avg_user_size + avg_user_status_size + avg_user_timeline_size + avg_user_followers_size +
             avg_user_following_size) );
  }
  
  public static void main(String[] args) {
    runUsersNoCompression();
    runUsersLZ4Compression();
    runUsersLZ4HCCompression();
    runUserStatusNoCompression();
    runUserStatusLZ4Compression();
    runUserStatusLZ4HCCompression();
    
    runUserTimelinesNoCompression();
    runUserTimelineLZ4Compression();
    runUserTimelineLZ4HCCompression();
    
    runUserFollowersNoCompression();
    runUserFollowersLZ4Compression();
    runUserFollowersLZ4HCCompression();
    runUserFollowingNoCompression();
    runUserFollowingLZ4Compression();
    runUserFollowingLZ4HCCompression();    
    printSummary();
  }
  
  private static void runUsers() {
    int numUsers = 100000;
    BigSortedMap map = new BigSortedMap(1000000000);
    List<User> users = User.newUsers(numUsers);
    int count = 0;
    for(User u: users) {
      count++;
      u.saveToCarrot(map);
      if (count  % 10000 == 0) {
        System.out.println("Loaded " + count+ " users");
      }
    }
    
    count = (int)countRecords(map);
    
    if (count != numUsers) {
      System.err.println("count=" + count + " expected=" + numUsers);
      System.exit(-1);
    }
    count = 0;
    IndexBlock.DEBUG = true;
    
    for(User u: users) {
      //System.out.println("Verify " + u.getKey());
      if (!u.verify(map)) {
        System.exit(-1);
      }
      if (++count  % 10000 == 0) {
        System.out.println("Verified " + (++count)+ " users");
      }
    }
    long memory = BigSortedMap.getTotalAllocatedMemory();
    avg_user_size = (double) memory / numUsers;
    map.dispose();
    System.out.println("avg_user_size="+ avg_user_size + " bytes");
  }
  
  private static long countRecords(BigSortedMap map)  {
    BigSortedMapDirectMemoryScanner scanner = map.getScanner(0, 0, 0, 0);
    long count = 0;
    try {
      while(scanner.hasNext()) {
        count++;
        scanner.next();
      }
    } catch(IOException e) {
      return -1;
    }
    finally {
      try {
        scanner.close();
      } catch(IOException e) {
        
      }
    }
    
    return count;
  }
  
  private static void runUsersNoCompression() {
    System.out.println("\nTest Users, compression=None");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    runUsers();
  }
  
  private static void runUsersLZ4Compression() {
    System.out.println("\nTest Users, compression=LZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    runUsers();
  }
  
  private static void runUsersLZ4HCCompression() {
    System.out.println("\nTest Users, compression=LZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    runUsers();
  }
  
  private static void runUserStatuses() {
    int numUsers = 1000;
    BigSortedMap map = new BigSortedMap(1000000000);
    List<User> users = User.newUsers(numUsers);
    List<UserStatus> statuses = null;
    int count = 0;
    for(User user: users) {
      count++;
      statuses = UserStatus.newUserStatuses(user);
      for(UserStatus us: statuses) {
        us.saveToCarrot(map);
      }
      if (count  % 100 == 0) {
        System.out.println("Loaded " + count+ " user statuses");
      }
    }
    count = 0;
    for(UserStatus u: statuses) {
      if (!u.verify(map)) {
        System.exit(-1);
      }
      if (count++  % 10000 == 0) {
        System.out.println("Verified " + count+ " users");
      }
    }
    long memory = BigSortedMap.getTotalAllocatedMemory();
    avg_user_status_size = (double) memory / numUsers;
    map.dispose();
    System.out.println("avg_user_status_size="+ avg_user_status_size + " bytes");
  }
  
  
  
  private static void runUserStatusNoCompression() {
    System.out.println("\nTest User Status, compression=None");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    runUserStatuses();
  }
  
  private static void runUserStatusLZ4Compression() {
    System.out.println("\nTest User Status, compression=LZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    runUserStatuses();
  }
  
  private static void runUserStatusLZ4HCCompression() {
    System.out.println("\nTest User Status, compression=LZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    runUserStatuses();
  }
  
  private static void runUserTimelines() {
    int numUsers = 1000;
    BigSortedMap map = new BigSortedMap(1000000000);
    List<User> users = User.newUsers(numUsers);
    int count = 0;
    for(User user: users) {
      count++;
      Timeline timeline = new Timeline(user);
      timeline.saveToCarrot(map);
      if (count  % 100 == 0) {
        System.out.println("Loaded " + count+ " user timelines");
      }
    }
    
    long memory = BigSortedMap.getTotalAllocatedMemory();
    avg_user_timeline_size = (double) memory / numUsers;
    map.dispose();
    System.out.println("avg_user_timeline_size="+ avg_user_timeline_size + " bytes");
  }
  
  private static void runUserTimelinesNoCompression() {
    System.out.println("\nTest User Timeline, compression=None");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    runUserTimelines();
  }
  
  private static void runUserTimelineLZ4Compression() {
    System.out.println("\nTest User Timeline, compression=LZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    runUserTimelines();
  }
  
  private static void runUserTimelineLZ4HCCompression() {
    System.out.println("\nTest User Timeline, compression=LZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    runUserTimelines();
  }
  
  private static void runUserFollowers() {
    int numUsers = 10000;
    BigSortedMap map = new BigSortedMap(1000000000);
    List<User> users = User.newUsers(numUsers);
    int count = 0;
    long total = 0;
    for(User user: users) {
      count++;
      Followers followers = new Followers(user);
      total += followers.size();
      followers.saveToCarrot(map);
      if (count  % 100 == 0) {
        System.out.println("Loaded " + count+ " user followers");
      }
    }
    
    long memory = BigSortedMap.getTotalAllocatedMemory();
    avg_user_followers_size = (double) memory / numUsers;
    map.dispose();
    System.out.println("avg_user_followers_size="+ avg_user_followers_size + " bytes. Avg #folowers=" + (total/numUsers));
  }
  
  private static void runUserFollowersNoCompression() {
    System.out.println("\nTest User Followers, compression=None");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    runUserFollowers();
  }
  
  private static void runUserFollowersLZ4Compression() {
    System.out.println("\nTest User Followers, compression=LZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    runUserFollowers();
  }
  
  private static void runUserFollowersLZ4HCCompression() {
    System.out.println("\nTest User Followers, compression=LZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    runUserFollowers();
  }
  
  private static void runUserFollowing() {
    int numUsers = 10000;
    BigSortedMap map = new BigSortedMap(1000000000);
    List<User> users = User.newUsers(numUsers);
    int count = 0;
    long total = 0;
    for(User user: users) {
      count++;
      Following following = new Following(user);
      total += following.size();
      following.saveToCarrot(map);
      if (count  % 1000 == 0) {
        System.out.println("Loaded " + count+ " user following");
      }
    }
    
    long memory = BigSortedMap.getTotalAllocatedMemory();
    avg_user_following_size = (double) memory / numUsers;
    map.dispose();
    System.out.println("avg_user_following_size="+ avg_user_following_size + " bytes. Avg #folowing=" + (total/numUsers));
  }
  
  private static void runUserFollowingNoCompression() {
    System.out.println("\nTest User Following, compression=None");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    runUserFollowing();
  }
  
  private static void runUserFollowingLZ4Compression() {
    System.out.println("\nTest User Following, compression=LZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    runUserFollowing();
  }
  
  private static void runUserFollowingLZ4HCCompression() {
    System.out.println("\nTest User Following, compression=LZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    runUserFollowing();
  }
}
