package org.bigbase.carrot;

import java.util.ArrayList;
import java.util.Random;

public class IndexBlockScannerLargeKVsTest extends IndexBlockScannerTest{
  protected ArrayList<byte[]> fillIndexBlock (IndexBlock b) throws RetryOperationException {
    ArrayList<byte[]> keys = new ArrayList<byte[]>();
    Random r = new Random();

    int maxSize = 4096;
    boolean result = true;
    while(true) {
      int len = r.nextInt(maxSize) + 1;
      byte[] key = new byte[len];
      r.nextBytes(key);
      result = b.put(key, 0, key.length, key, 0, key.length, 0, 0);
      if(result) {
        keys.add(key);
      } else {
        break;
      }
    }
    System.out.println("Number of data blocks="+b.getNumberOfDataBlock() + " "  + " index block data size =" + 
        b.getDataSize()+" num records=" + keys.size());
    return keys;
  }
}
