package org.bigbase.carrot;

import java.util.ArrayList;
import java.util.Random;

import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.UnsafeAccess;

public class DataBlockLargeKVsTest extends DataBlockTest{
  
  static {
    UnsafeAccess.debug = true;
  }
  protected ArrayList<byte[]> fillDataBlock (DataBlock b) throws RetryOperationException {
    ArrayList<byte[]> keys = new ArrayList<byte[]>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED="+seed);
    
    int maxSize = 2048;
    boolean result = true;
    while(result == true) {
      int len = r.nextInt(maxSize) +1;
      byte[] key = new byte[len];
      r.nextBytes(key);
      key = Bytes.toHex(key).getBytes();
      result = b.put(key, 0, key.length, key, 0, key.length, 0, 0);
      if(result) {
        keys.add(key);
      }
    }
    System.out.println("records="+b.getNumberOfRecords()  + "  data size=" + b.getDataSize());
    return keys;
  }
}
