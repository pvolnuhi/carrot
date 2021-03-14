package org.bigbase.carrot.examples.basic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.KeyValue;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.examples.util.UserSession;
import org.bigbase.carrot.redis.OperationFailedException;
import org.bigbase.carrot.redis.hashes.Hashes;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * This example shows how to use Carrot Hashes to store user session objects 
 * 
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
 * "StartTime" - session start time (seconds since 01.01.1970)
 * "LastActiveTime" - last interaction time (seconds since 01.01.1970)
 * userId is used as a KEY for Carrot and Redis Hash
 * 
 * Test description: <br>
 * 
 * UserSession object has 10 fields, one field (UserId) is used as a Hash key
 * 
 * Average key + session object size is 192 bytes. We load 100K user session objects
 * 
 * Results:
 * 0. Average user session data size = 192 bytes
 * 1. No compression. Used RAM per session object is 249 bytes (COMPRESSION= 0.77)
 * 2. LZ4 compression. Used RAM per session object is 90 bytes (COMPRESSION = 2.14)
 * 3. LZ4HC compression. Used RAM per session object is 87 bytes (COMPRESSION = 2.20)
 * 
 * Redis estimate per session object, using Hashes with ziplist encodings is 290 
 * (actually it can be more, this is a low estimate based on evaluating Redis code) 
 * 
 * RAM usage (Redis-to-Carrot)
 * 
 * 1) No compression    290/249 = 1.17x
 * 2) LZ4   compression 290/90 = 3.22x
 * 3) LZ4HC compression 290/87 = 3.33x
 * 
 * Effect of a compression:
 * 
 * LZ4  - 3.22/1.17 = 2.75x (to no compression)
 * LZ4HC - 3.33/1.17 = 2.85x (to no compression)
 * 
 * @author vrodionov
 *
 */
public class HashesUserSessions {
  
  static {
    UnsafeAccess.debug = true;
  }
  
  static long keyBuf = UnsafeAccess.malloc(64);
  static long fieldBuf = UnsafeAccess.malloc(64);
  static long valBuf = UnsafeAccess.malloc(64);
  
  static long N = 100000;
  static long totalDataSize = 0;
  static List<UserSession> userSessions = new ArrayList<UserSession>();
  
  static {
    for (int i = 0; i < N; i++) {
      userSessions.add(UserSession.newSession(i));
    }
    Collections.shuffle(userSessions);
  }
  
  public static void main(String[] args) throws IOException, OperationFailedException {

    System.out.println("RUN compression = NONE");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    runTest();
    System.out.println("RUN compression = LZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    runTest();
    System.out.println("RUN compression = LZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    runTest();

  }
  
  private static void runTest() throws IOException, OperationFailedException {
    
    BigSortedMap map =  new BigSortedMap(1000000000);
    
    totalDataSize = 0;
    
    long startTime = System.currentTimeMillis();
    int count =0;
    
    for (UserSession us: userSessions) {
      count++;
      String skey = us.getUserId();
      byte[] bkey = skey.getBytes();
      int keySize = bkey.length;
      UnsafeAccess.copy(bkey,  0,  keyBuf, keySize);
      
      totalDataSize += keySize; 
      
      List<KeyValue> list = us.asList();
      totalDataSize += Utils.size(list);
      
      int num  = Hashes.HSET(map, keyBuf, keySize, list);
      if (num != list.size()) {
        System.err.println("ERROR in HSET");
        System.exit(-1);
      }
      
      if (count % 10000 == 0) {
        System.out.println("set "+ count);
      }
      
      list.forEach(x -> x.free());
      
    }
    long endTime = System.currentTimeMillis();
    
    System.out.println("Loaded " + userSessions.size() +" user sessions, total size="+totalDataSize
      + " in "+ (endTime - startTime) + "ms. RAM usage="+ (UnsafeAccess.getAllocatedMemory())  
      + " COMPRESSION=" + (((double)totalDataSize))/ UnsafeAccess.getAllocatedMemory());
    
    BigSortedMap.printMemoryAllocationStats();
    map.dispose();
    
  }

}
