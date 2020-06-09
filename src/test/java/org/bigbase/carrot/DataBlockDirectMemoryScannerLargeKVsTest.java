package org.bigbase.carrot;

import java.util.ArrayList;
import java.util.Random;

import org.bigbase.carrot.util.UnsafeAccess;

public class DataBlockDirectMemoryScannerLargeKVsTest extends DataBlockDirectMemoryScannerTest{

  protected ArrayList<Key> fillDataBlock (DataBlock b) throws RetryOperationException {
    ArrayList<Key> keys = new ArrayList<Key>();
    Random r = new Random();
    int maxSize = 2048;
    boolean result = true;
    while(result == true) {
      int len = r.nextInt(maxSize) +1;
      byte[] key = new byte[len];
      r.nextBytes(key);
      long ptr = UnsafeAccess.malloc(len);
      UnsafeAccess.copy(key, 0, ptr, len);
      result = b.put(ptr, len, ptr, len, 0, 0);
      if(result) {
        keys.add( new Key(ptr, len));
      }
    }
    System.out.println("records="+b.getNumberOfRecords()  + "  data size=" + b.getDataSize());
    return keys;
  }
}
