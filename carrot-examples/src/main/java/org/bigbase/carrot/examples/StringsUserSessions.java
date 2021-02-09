package org.bigbase.carrot.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.examples.util.UserSession;
import org.bigbase.carrot.redis.MutationOptions;
import org.bigbase.carrot.redis.OperationFailedException;
import org.bigbase.carrot.redis.strings.Strings;
import org.bigbase.carrot.util.UnsafeAccess;

/**
 * This example shows how to use Carrot Strings to store user sessions objects 
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
 * Redis estimate per session object, using String encoding is ~320 bytes
 * 
 * RAM usage (Redis-to-Carrot)
 * 
 * 1) No compression    320/275 ~ 1.16x
 * 2) LZ4   compression 320/94 ~ 3.4x
 * 3) LZ4HC compression 320/88 ~ 3.64x 
 * 
 * Effect of a compression:
 * 
 * LZ4  - 2.37/0.8 = 2.96    (to no compression)
 * LZ4HC - 2.5/0.8 = 3.13  (to no compression)
 * @author vrodionov
 *
 */
public class StringsUserSessions {
  
  static {
    UnsafeAccess.debug = true;
  }
  
  static long keyBuf = UnsafeAccess.malloc(64);
  static long valBuf = UnsafeAccess.malloc(512);
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
      String svalue = us.toString();
      byte[] bvalue = svalue.getBytes();
      
      totalDataSize += bkey.length + bvalue.length;
      
      UnsafeAccess.copy(bkey,  0,  keyBuf, bkey.length);
      UnsafeAccess.copy(bvalue,  0,  valBuf, bvalue.length);
      
      boolean result = 
          Strings.SET(map, keyBuf, bkey.length, valBuf, bvalue.length, 0, MutationOptions.NONE, true);
      if (!result) {
        System.err.println("ERROR in SET");
        System.exit(-1);
      }
      
      if (count % 10000 == 0) {
        System.out.println("set "+ count);
      }
    }
    long endTime = System.currentTimeMillis();
    
    System.out.println("Loaded " + userSessions.size() +" user sessions, total size="+totalDataSize
      + " in "+ (endTime - startTime) + "ms. RAM usage="+ (UnsafeAccess.getAllocatedMemory())  
      + " COMPRESSION=" + (((double)totalDataSize))/ UnsafeAccess.getAllocatedMemory());
    
    BigSortedMap.printMemoryAllocationStats();
    map.dispose();
    
  }

}
