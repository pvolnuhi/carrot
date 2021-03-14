package org.bigbase.carrot.examples.twitter;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

/**
 * Class Following - represents list of users, given user is following to
 * @author vrodionov
 *
 */
public class Following extends Users {

  Following(User user) {
    super(user);
  }

  @Override
  public String getKey() {
    return "following:" + user.getId();
  }

  @Override
  /**
   * Generate followers for a given user
   * @param user user to follow
   * @return user's followers
   */
  public void generateUsers() {
    users = new ArrayList<GenuineUser>();

    long registered = Long.valueOf(user.getSignup());
    int numFollowing = user.getFollowing();
    if (numFollowing == 0) return;
    
    Calendar cal = Calendar.getInstance();
    Date today = cal.getTime();
    cal.setTimeInMillis(registered);
    Date regtime = cal.getTime();
    long interval = (today.getTime() - registered) / numFollowing;
    int count = 0;
    while(count++ < numFollowing) {
      long time = regtime.getTime();
      String id = Long.toString(Id.nextId(time));
      users.add(new GenuineUser(id, time / 1000)); // We keep seconds only
      regtime.setTime (time + interval);
    }
  }

}
