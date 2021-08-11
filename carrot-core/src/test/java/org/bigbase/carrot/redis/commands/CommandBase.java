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

import static org.bigbase.carrot.util.Utils.byteBufferToString;
import static org.bigbase.carrot.util.Utils.strToByteBuffer;
import static org.junit.Assert.assertEquals;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.CommandProcessor;
import org.bigbase.carrot.redis.RedisConf;
import org.bigbase.carrot.redis.Server;
import org.bigbase.carrot.redis.db.DBSystem;
import org.bigbase.carrot.redis.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public abstract class CommandBase {

  protected final static String SKIP_VERIFY = "@"; // Skip verify if expected reply equals
  protected BigSortedMap map;
  protected ByteBuffer in, inDirect;
  protected ByteBuffer out, outDirect;
  
  /**
   * Subclasses must override
   */
  protected abstract String[] getValidRequests();
  protected abstract String[] getValidResponses();
  protected abstract String[] getInvalidRequests();
  protected abstract String[] getInvalidResponses();

  @Before
  public void setUp() {
    map = new BigSortedMap(1000000);
    in = ByteBuffer.allocate(4096);
    out = ByteBuffer.allocate(4096);
    inDirect = ByteBuffer.allocateDirect(4096);
    outDirect = ByteBuffer.allocateDirect(4096);
  }
  
  //@Ignore
  @Test
  public void testValidRequests() {
    String[] validRequests = getValidRequests();
    String[] validResponses = getValidResponses();
    
    for (int i = 0; i < validRequests.length; i++) {
      in.clear();
      out.clear();
      String inline = validRequests[i];
      String request = Utils.inlineToRedisRequest(inline);
      System.out.println("REQUEST:");
      System.out.println(inline);
      //System.out.println(request);
      strToByteBuffer(request, in);
      CommandProcessor.process(map, in, out);
      String result = byteBufferToString(out);
      System.out.println("\nRESULT:");
      System.out.println(result);

      if (validResponses[i].equals(SKIP_VERIFY)) {
        //TODO: we need some verification
      } else {
        assertEquals(validResponses[i], result);
      }
    }
  }
  
  @Test
  public void testValidRequestsNetworkMode() throws UnknownHostException, IOException, InterruptedException {
    // Start server
    Server.start();    
    
    // Connect client
    Socket client = new Socket("localhost", RedisConf.getInstance().getServerPort());
    DataOutputStream os =new DataOutputStream(client.getOutputStream());
    DataInputStream is = new DataInputStream(client.getInputStream());
    
    String[] validRequests = getValidRequests();
    String[] validResponses = getValidResponses();
    
    for (int i = 0; i < validRequests.length; i++) {
      
      String inline = validRequests[i];
      String request = Utils.inlineToRedisRequest(inline);
      String expResponse = validResponses[i];
      byte[] expBytes = new byte[expResponse.length()];
      System.out.println("REQUEST:");
      System.out.println(inline);
      
      os.write(request.getBytes());
      is.readFully(expBytes);
      
      String result = new String(expBytes);
      
      System.out.println("\nRESULT:");
      System.out.println(result);

      if (validResponses[i].equals(SKIP_VERIFY)) {
        //TODO: we need some verification
      } else {
        assertEquals(validResponses[i], result);
      }
    }
    
    client.close();
    Server.shutdown();
  }
  
  //@Ignore
  @Test
  public void testValidRequestsInline() {
    String[] validRequests = getValidRequests();
    String[] validResponses = getValidResponses();
    for (int i = 0; i < validRequests.length; i++) {
      in.clear();
      out.clear();
      String inline = validRequests[i];
      String request = inline;
      strToByteBuffer(request, in);
      CommandProcessor.process(map, in, out);
      String result = byteBufferToString(out);
      if (validResponses[i].equals(SKIP_VERIFY)) {
        System.out.println(result);
      } else {
        assertEquals(validResponses[i], result);
      }    
    }
  }
  
  //@Ignore
  @Test
  public void testValidRequestsDirectBuffer() {
    String[] validRequests = getValidRequests();
    String[] validResponses = getValidResponses();
    for (int i = 0; i < validRequests.length; i++) {
      inDirect.clear();
      outDirect.clear();
      String inline = validRequests[i];
      String request = Utils.inlineToRedisRequest(inline);
      strToByteBuffer(request, inDirect);
      CommandProcessor.process(map, inDirect, outDirect);
      String result = byteBufferToString(outDirect);
      if (validResponses[i].equals(SKIP_VERIFY)) {
        System.out.println(result);
      } else {
        assertEquals(validResponses[i], result);
      }
    }
  }
  
  //@Ignore
  @Test
  public void testValidRequestsInlineDirectBuffer() {
    String[] validRequests = getValidRequests();
    String[] validResponses = getValidResponses();
    for (int i = 0; i < validRequests.length; i++) {
      inDirect.clear();
      outDirect.clear();
      String inline = validRequests[i];
      String request = inline;
      strToByteBuffer(request, inDirect);
      CommandProcessor.process(map, inDirect, outDirect);
      String result = byteBufferToString(outDirect);
      if (validResponses[i].equals(SKIP_VERIFY)) {
        System.out.println(result);
      } else {
        assertEquals(validResponses[i], result);
      }
    }
  }
  // INVALID REQUESTS
  
 // @Ignore
  @Test
  public void testInValidRequests() {
    String[] invalidRequests = getInvalidRequests();
    String[] invalidResponses = getInvalidResponses();
    for (int i = 0; i < invalidRequests.length; i++) {
      in.clear();
      out.clear();
      String inline = invalidRequests[i];
      String request = Utils.inlineToRedisRequest(inline);
      System.out.println(inline);
      strToByteBuffer(request, in);
      CommandProcessor.process(map, in, out);
      String result = byteBufferToString(out);
      if (invalidResponses[i].equals(SKIP_VERIFY)) {
        System.out.println(result);
      } else {
        assertEquals(invalidResponses[i], result);
      }    
    }
  }
  
  //@Ignore
  @Test
  public void testInValidRequestsInline() {
    String[] invalidRequests = getInvalidRequests();
    String[] invalidResponses = getInvalidResponses();
    for (int i = 0; i < invalidRequests.length; i++) {
      in.clear();
      out.clear();
      String inline = invalidRequests[i];
      String request = inline;
      strToByteBuffer(request, in);
      CommandProcessor.process(map, in, out);
      String result = byteBufferToString(out);
      if (invalidResponses[i].equals(SKIP_VERIFY)) {
        System.out.println(result);
      } else {
        assertEquals(invalidResponses[i], result);
      }
    }
  }
  
  //@Ignore
  @Test
  public void testInValidRequestsDirectBuffer() {
    String[] invalidRequests = getInvalidRequests();
    String[] invalidResponses = getInvalidResponses();
    for (int i = 0; i < invalidRequests.length; i++) {
      inDirect.clear();
      outDirect.clear();
      String inline = invalidRequests[i];
      String request = Utils.inlineToRedisRequest(inline);
      strToByteBuffer(request, inDirect);
      CommandProcessor.process(map, inDirect, outDirect);
      String result = byteBufferToString(outDirect);
      if (invalidResponses[i].equals(SKIP_VERIFY)) {
        System.out.println(result);
      } else {
        assertEquals(invalidResponses[i], result);
      }    
    }
  }
  
  //@Ignore
  @Test
  public void testInValidRequestsInlineDirectBuffer() {
    String[] invalidRequests = getInvalidRequests();
    String[] invalidResponses = getInvalidResponses();
    for (int i = 0; i < invalidRequests.length; i++) {
      inDirect.clear();
      outDirect.clear();
      String inline = invalidRequests[i];
      String request = inline;
      strToByteBuffer(request, inDirect);
      CommandProcessor.process(map, inDirect, outDirect);
      String result = byteBufferToString(outDirect);
      if (invalidResponses[i].equals(SKIP_VERIFY)) {
        System.out.println(result);
      } else {
        assertEquals(invalidResponses[i], result);
      }    
    }
  }
  
  @After
  public void tearDown() {
    map.dispose();
    DBSystem.reset();
  }
}
