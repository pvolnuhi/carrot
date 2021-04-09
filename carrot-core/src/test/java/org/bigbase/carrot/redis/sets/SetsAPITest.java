package org.bigbase.carrot.redis.sets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.List;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.util.UnsafeAccess;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SetsAPITest {
  BigSortedMap map;
  int valSize = 8;
  long n = 1000;

  
  static {
    //UnsafeAccess.debug = true;
  }
    
  @Test
  public void testSimpleCalls() throws IOException {
    System.out.println("Test Sets ADD/ISMEMBER/MEMBERS API calls");
    // Adding to set which does not exists
    int result = Sets.SADD(map, "key", "member1");
    assertEquals(1, result);
    // One more time - exists already
    result = Sets.SADD(map, "key", "member1");    
    assertEquals(0, result);
    // Positive result
    result = Sets.SISMEMBER(map, "key", "member1");
    assertEquals(1, result);
    // Negative result
    result = Sets.SISMEMBER(map, "key", "member2");
    assertEquals(0, result);
    // Wrong key
    result = Sets.SISMEMBER(map, "key1", "member1");
    assertEquals(0, result);
    // Add multiple members
    result = Sets.SADD(map, "key" , new String[] {"member1", "member2", "member3"});
    // We expect only 2 new elements
    assertEquals(2, result);
    
    List<byte[]> members = Sets.SMEMBERS(map, "key".getBytes(), 1000);
    assertEquals(3, members.size());
    assertEquals("member1", new String(members.get(0)));
    assertEquals("member2", new String(members.get(1)));
    assertEquals("member3", new String(members.get(2)));
    // Wrong key
    result = Sets.SISMEMBER(map, "key1", "member1");
    assertEquals(0, result);
    // Wrong key
    result = Sets.SISMEMBER(map, "key1", "member2");
    assertEquals(0, result);
    // Wrong key
    result = Sets.SISMEMBER(map, "key1", "member3");
    assertEquals(0, result);
        
    // Wrong key greater than "key"
    members = Sets.SMEMBERS(map, "key1".getBytes(), 1000);
    assertNull(members);
    
    // Wrong key less than "key"
    members = Sets.SMEMBERS(map, "kex".getBytes(), 1000);
    assertNull(members);
    
    // Small buffer - empty list
    members = Sets.SMEMBERS(map, "key".getBytes(), 10);
    assertEquals(0, members.size());
    // small buffer - list == 1
    members = Sets.SMEMBERS(map, "key".getBytes(), 20);
    assertEquals(1, members.size());
    
    // small buffer - list == 2
    members = Sets.SMEMBERS(map, "key".getBytes(), 30);
    assertEquals(2, members.size());
    
    // small buffer - list == 3
    members = Sets.SMEMBERS(map, "key".getBytes(), 40);
    assertEquals(3, members.size());
    
    
    // Add multiple members
    result = Sets.SADD(map, "key" , new String[] {"member1", "member2", "member3", "member4", "member5"});
    // We expect only 2 new elements
    assertEquals(2, result);
    
    members = Sets.SMEMBERS(map, "key".getBytes(), 1000);
    assertEquals(5, members.size());
    assertEquals("member1", new String(members.get(0)));
    assertEquals("member2", new String(members.get(1)));
    assertEquals("member3", new String(members.get(2)));
    assertEquals("member4", new String(members.get(3)));
    assertEquals("member5", new String(members.get(4)));
    
  }
  
  @Test
  public void testMoveOperation() {
    System.out.println("Test Sets SMOVE API call");
    // Add multiple members
    int result = Sets.SADD(map, "key" , new String[] {"member1", "member2", "member3", "member4", "member5"});
    // We expect only 2 new elements
    assertEquals(5, result);
    result = Sets.SMOVE(map, "key", "key1", "member1");
    assertEquals(1, result);
    assertEquals(4, (int)Sets.SCARD(map, "key"));
    assertEquals(1, (int)Sets.SCARD(map, "key1"));
    
    // Try one more time
    result = Sets.SMOVE(map, "key", "key1", "member1");
    // Now result = 0
    assertEquals(0, result);
    assertEquals(4, (int)Sets.SCARD(map, "key"));
    assertEquals(1, (int)Sets.SCARD(map, "key1"));
    // Try one more time
    result = Sets.SMOVE(map, "key", "key1", "member2");
    // Now result = 1
    assertEquals(1, result);
    assertEquals(3, (int)Sets.SCARD(map, "key"));
    assertEquals(2, (int)Sets.SCARD(map, "key1"));
    // Try one more time
    result = Sets.SMOVE(map, "key", "key1", "member3");
    // Now result = 1
    assertEquals(1, result);
    assertEquals(2, (int)Sets.SCARD(map, "key"));
    assertEquals(3, (int)Sets.SCARD(map, "key1"));
    // Try one more time
    result = Sets.SMOVE(map, "key", "key1", "member4");
    // Now result = 1
    assertEquals(1, result);
    assertEquals(1, (int)Sets.SCARD(map, "key"));
    assertEquals(4, (int)Sets.SCARD(map, "key1"));
    // Try one more time
    result = Sets.SMOVE(map, "key", "key1", "member5");
    // Now result = 1
    assertEquals(1, result);
    assertEquals(0, (int)Sets.SCARD(map, "key"));
    assertEquals(5, (int)Sets.SCARD(map, "key1"));

  }
  
  @Before
  public void setUp() {
    map = new BigSortedMap(100000);
  }
  
  @After
  public void tearDown() {
    // Dispose
    map.dispose();
    UnsafeAccess.mallocStats.printStats();
  }
}
