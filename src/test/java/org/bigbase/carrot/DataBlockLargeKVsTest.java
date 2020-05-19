package org.bigbase.carrot;

import java.util.ArrayList;
import java.util.Random;

public class DataBlockLargeKVsTest extends DataBlockTest{
  
  protected ArrayList<byte[]> fillDataBlock (DataBlock b) throws RetryOperationException {
    ArrayList<byte[]> keys = new ArrayList<byte[]>();
    Random r = new Random();
    int maxSize = 4096;
    boolean result = true;
    while(result == true) {
      int len = r.nextInt(maxSize) +1;
      byte[] key = new byte[len];
      r.nextBytes(key);
      result = b.put(key, 0, key.length, key, 0, key.length, 0, 0);
      if(result) {
        keys.add(key);
      }
    }
    System.out.println("records="+b.getNumberOfRecords()  + "  data size=" + b.getDataSize());
    return keys;
  }
}
