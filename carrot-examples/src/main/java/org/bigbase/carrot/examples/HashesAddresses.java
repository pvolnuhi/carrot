package org.bigbase.carrot.examples;

import java.io.IOException;
import java.util.List;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.KeyValue;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.examples.util.Address;
import org.bigbase.carrot.redis.OperationFailedException;
import org.bigbase.carrot.redis.hashes.Hashes;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * This example shows how to use Carrot Hashes to store user Address objects 
 * This example demonstrate how compression works with a real data set
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
 * Redis estimate per address object, using Hashes with ziplist encodings (most efficient) is 161
 * (actually it can be more, this is a low estimate based on evaluation of a Redis code) 
 * 
 * RAM usage (Redis-to-Carrot)
 * 
 * 1) No compression    161/124 = 1.3x
 * 2) LZ4   compression 161/66 = 2.44x
 * 3) LZ4HC compression 161/63.6 = 2.53x 
 * 
 * Effect of a compression:
 * 
 * LZ4  - 1.3/0.7 = 1.86x (to no compression)
 * LZ4HC - 1.35/0.7 = 1.93x (to no compression)
 * 
 * @author vrodionov
 *
 */
public class HashesAddresses {
  
  static {
    UnsafeAccess.debug = true;
  }
  
  static long keyBuf = UnsafeAccess.malloc(64);
  static long fieldBuf = UnsafeAccess.malloc(64);
  static long valBuf = UnsafeAccess.malloc(64);
  
  static long N = 100000;
  static long totalDataSize = 0;
  static List<Address> addressList ;
  
  public static void main(String[] args) throws IOException, OperationFailedException {
    
    addressList = Address.loadFromFile(args[0]);
    
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
    
    for (Address us: addressList) {
      count++;
      String skey = Address.getUserId(count);
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
    
    System.out.println("Loaded " + addressList.size() +" user address objects, total size="+totalDataSize
      + " in "+ (endTime - startTime) + "ms. RAM usage="+ (UnsafeAccess.getAllocatedMemory())  
      + " COMPRESSION=" + (((double)totalDataSize))/ UnsafeAccess.getAllocatedMemory());
    
    BigSortedMap.printMemoryAllocationStats();
    
    map.dispose();
    
  }

}
