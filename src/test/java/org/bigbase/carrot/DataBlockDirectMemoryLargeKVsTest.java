package org.bigbase.carrot;

import java.util.ArrayList;
import java.util.Random;

import org.bigbase.carrot.util.UnsafeAccess;

public class DataBlockDirectMemoryLargeKVsTest extends DataBlockDirectMemoryTest{

  
  protected ArrayList<Key> fillDataBlock (DataBlock b) throws RetryOperationException {
    ArrayList<Key> keys = new ArrayList<Key>();
    Random r = new Random();
//    long seed = r.nextLong();
//    r.setSeed(seed);
//    System.out.println("seed="+seed);
    int maxSize = 4096;

    boolean result = true;
    while(result == true) {
      int len = r.nextInt(maxSize) + 1;
      byte[] key = new byte[len];
      r.nextBytes(key);
      long ptr = UnsafeAccess.malloc(len);
      UnsafeAccess.copy(key, 0, ptr, len);
      result = b.put(key, 0, key.length, key, 0, key.length, Long.MAX_VALUE, 0);
      if(result) {
        keys.add(new Key(ptr, len));
      }
    }
    System.out.println("M: "+ DataBlock.getTotalAllocatedMemory() +" D:"+DataBlock.getTotalDataSize());
    return keys;
  }
  
}
