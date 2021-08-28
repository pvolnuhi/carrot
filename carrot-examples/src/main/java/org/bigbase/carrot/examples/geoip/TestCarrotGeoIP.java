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

import java.io.IOException;
import java.util.List;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.UnsafeAccess;

/**
 * Test Carrot GeoIp
 * GeoIP application as described in Redis Book chapter 5.3 "IP-to-city and country lookup":
 * https://redislabs.com/ebook/part-2-core-concepts/chapter-5-using-redis-for-application-support/5-3-ip-to-city-and-country-lookup/
 * 
 * We implemented application in both: Carrot and Redis. 
 * 
 * Carrot implementation details:
 * 
 * Carrot uses SET data type to store combined NetworkAddress, City ID pair. Carrot's SETs are ordered, 
 * so they can be used to  answer the following questions:
 * 
 * Give me the greatest member which is less or equals to a given search key, therefore it can be used 
 * to locate network which a given IP address belongs to. 
 * 
 * Carrot uses STRING data type (plain key-value) to keep association between CityID and City location, 
 * name and other data.
 * 
 * key = CityId
 * value = {comma separated string of a city data} 
 * 
 * We used Ip-Geo database free version from www.maxmind.com
 * 
 * Redis implementation details:
 * 
 * Redis uses ZSET (ordered set) to keep NetworkAddress -> city ID  association. 
 * 
 * Memory usage:
 * 
 * 1. No compression    -  69MB
 * 2. LZ4 compression   -  35.43MB
 * 3. LZ4HC compression -  34.86MB
 * 
 * Redis usage - 388.5 MB
 * 
 * Redis/Carrot:
 * 
 * No compression  388.5/69 = 5.63
 * LZ4             388.5/35.43 = 10.97
 * LZ4HC           388.5/34.86 = 11.14
 * @author vrodionov
 *
 */
public class TestCarrotGeoIP {
  static List<CityBlock> blockList;
  static List<CityLocation> locList;
  
  public static void main(String[] args) throws IOException {
    runNoCompression(args[0], args[1]);
    runCompressionLZ4(args[0], args[1]);
    runCompressionLZ4HC(args[0], args[1]);
  }
  
  
  private static void runNoCompression(String f1, String f2) throws IOException {
    System.out.println("Compression=NONE");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    runTest(f1, f2);
  }
  
  private static void runCompressionLZ4(String f1, String f2) throws IOException {
    System.out.println("Compression=LZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    runTest(f1, f2);
  }
  
  private static void runCompressionLZ4HC(String f1, String f2) throws IOException {
    System.out.println("Compression=LZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    runTest(f1, f2);
  }
  
  private static void runTest(String f1, String f2) throws IOException {
    BigSortedMap map = new BigSortedMap(1000000000);
    if (blockList == null) {
      blockList = CityBlock.load(f1);
    }
    long ptr = UnsafeAccess.allocAndCopy("key1", 0, "key1".length());
    int size = "key1".length();
    long start = System.currentTimeMillis();
    int total = 0;
    for (CityBlock cb: blockList) {
      cb.saveToCarrot(map, ptr, size);
      total++;
      if (total % 100000 == 0) {
        System.out.println("Total blocks="+ total);
      }
    }
    long end = System.currentTimeMillis();
    
    System.out.println("Loaded "+ blockList.size() +" blocks in "+ (end-start)+"ms");
    total = 0;
    if (locList == null) {
      locList = CityLocation.load(f2);
    }
    start = System.currentTimeMillis();
    for (CityLocation cl: locList) {
      cl.saveToCarrot(map);
      total++;
      if (total % 100000 == 0) {
        System.out.println("Total locs="+ total);
      }
    }
    end = System.currentTimeMillis();
    
    System.out.println("Loaded "+ locList.size() +" locations in "+ (end-start)+"ms");
    System.out.println("Total memory used="+ BigSortedMap.getGlobalAllocatedMemory());
    
    map.dispose();
  }
}
