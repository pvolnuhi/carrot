package org.bigbase.carrot.examples.basic;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.bigbase.carrot.examples.util.Address;
import org.bigbase.carrot.ops.OperationFailedException;

import redis.clients.jedis.Jedis;


/**
 * This example shows how to use Redis Hashes to store user Address objects 
 * Data set is the random sample of a openaddress.org data set for US-WEST region
 * 
 * File: all-clean-sub-shuffle.csv
 * 
 * User Address structure:
 
 * "LON"      - Address longitude (skip)
 * "LAT"      - Address latitude  (skip)
 * "NUMBER"   - House number
 * "STREET"   - Street
 * "UNIT"     - Unit number
 * "CITY"     - City
 * "DISTRICT" - Region
 * "REGION"   - Region (State)
 * 
 * Test description: <br>
 * 
 * Address object has up to 6 fields (some of them can be missing)
 * Key is synthetic: "address:user:number" <- format of a key
 * 
 * Average key + address object size is 86 bytes. We load 413689 user address objects
 * 
 * Results:
 * 0. Average user address data size = 86 bytes
 * 1. No compression. Used RAM per address object is 124 bytes (COMPRESSION= 0.7)
 * 2. LZ4 compression. Used RAM per address object is 66 bytes (COMPRESSION = 1.3)
 * 3. LZ4HC compression. Used RAM per address object is 63.6 bytes (COMPRESSION = 1.35)
 * 
 * Redis, per address object, using Hashes with ziplist encodings (most efficient) is 193
 * 
 * RAM usage (Redis-to-Carrot)
 * 
 * 1) No compression    193/124 = 1.6x
 * 2) LZ4   compression 193/66 = 2.9x
 * 3) LZ4HC compression 193/63.6 = 3.0x 
 * 
 * Effect of a compression:
 * 
 * LZ4  - 1.3/0.7 = 1.86x (to no compression)
 * LZ4HC - 1.35/0.7 = 1.93x (to no compression)
 * 
 * @author vrodionov
 *
 */
public class RedisHashesAddresses {
  
  static long totalDataSize = 0;
  static List<Address> addressList ;
  
  
  
  public static void main(String[] args) throws IOException, OperationFailedException {
    
    addressList = Address.loadFromFile(args[0]);
    System.out.println("RUN Redis ");
    runTest();
  }
  
  private static void runTest() throws IOException, OperationFailedException {
    
    Jedis client = new Jedis("localhost");
        
    long startTime = System.currentTimeMillis();
    int count =0;
    
    for (Address us: addressList) {
      count++;
      String skey = Address.getUserId(count);      
      Map<String, String> map = us.getPropsMap();
      if (count % 10000 == 0) {
        System.out.println("set "+ count);
      }
      client.hset(skey, map);
    }
    long endTime = System.currentTimeMillis();
    
    System.out.println("Loaded " + addressList.size() +" user address objects"
      + " in "+ (endTime - startTime));
    client.close();
    
    System.out.println("Press any button ...");
    System.in.read();
    
    count = 0;
    int listSize = addressList.size();
    for (int i = 0; i< listSize; i++) {
      count++;
      String skey = Address.getUserId(count);      
      if (count % 10000 == 0) {
        System.out.println("del "+ count);
      }
      client.del(skey);
    }
  }

}
