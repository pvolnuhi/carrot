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

package org.bigbase.carrot.examples.geoip;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.redis.strings.Strings;
import org.bigbase.carrot.redis.util.MutationOptions;
import org.bigbase.carrot.util.UnsafeAccess;

import redis.clients.jedis.Jedis;

public class CityLocation {
  private String data;
  private int id;
  
  private CityLocation(int id, String data) {
    this.id = id;
    this.data = data;
  }
  
  @SuppressWarnings("deprecation")
  public static List<CityLocation> load (String file) throws IOException{
    File f = new File(file);
    FileInputStream fis = new FileInputStream(f);
    DataInputStream dis = new DataInputStream(fis);
    // skip CSV header
    dis.readLine();
    List<CityLocation> list = new ArrayList<CityLocation>();
    String line = null;
    while((line = dis.readLine()) != null) {
      String[] parts = line.split(",");
      String sid = parts[0];
      int id = Integer.parseInt(sid);  
      CityLocation block = new CityLocation(id, line);
      list.add(block);
    }
    dis.close();
    return list;
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("ID:" + id()).append(" Data="+ data);
    return sb.toString();
  }
  
  public void saveToCarrot(BigSortedMap map) {
    long ptr = UnsafeAccess.allocAndCopy(data, 0, data.length());
    int size = data.length();
    String key = "city:" + id();
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    Strings.SET(map, keyPtr, keySize, ptr, size, 0, MutationOptions.NONE, true);
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(ptr);
  }
  
  public byte[] getKey() {
    String key = "city:" + id();
    return key.getBytes();
  }
  
  public void saveToRedis (Jedis client) {
    String key = "city:" + id();
    client.set(key, data);
  }
  
  private String id() {
    String s = Integer.toString(id);
    int len = s.length();
    for (int i=0; i < 9 - len; i++) {
      s = '0' + s;
    }
    return s;
  }
  
  public static void main(String[] args) throws IOException {
    List<CityLocation> list = load(args[0]);
    
    System.out.println("Loaded "+ list.size());
    
    for (int i =0; i < 20; i++) {
      System.out.println(list.get(i));
    }
  }
}
