package org.bigbase.carrot;

import java.util.ArrayList;
import java.util.Random;

import org.bigbase.carrot.util.UnsafeAccess;

public class IndexBlockDirectMemoryLargeKVsTest extends IndexBlockDirectMemoryTest{

  protected ArrayList<Key> fillIndexBlock (IndexBlock b) throws RetryOperationException {
    ArrayList<Key> keys = new ArrayList<Key>();
    Random r = new Random();
    int maxSize = 4096;
    boolean result = true;
    while(result == true) {
      byte[] key = new byte[r.nextInt(maxSize) +1];
      r.nextBytes(key);
      long ptr = UnsafeAccess.malloc(key.length);
      UnsafeAccess.copy(key, 0, ptr, key.length);
      result = b.put(key, 0, key.length, key, 0, key.length, Long.MAX_VALUE, 0);
      if(result) {
        keys.add(new Key(ptr, key.length));
      }
    }
    System.out.println("Number of data blocks="+b.getNumberOfDataBlock() + " "  + " index block data size =" + 
        b.getDataSize()+" num records=" + keys.size());
    return keys;
  }
}
