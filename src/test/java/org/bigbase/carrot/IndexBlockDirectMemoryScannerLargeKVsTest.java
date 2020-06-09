package org.bigbase.carrot;

import java.util.ArrayList;
import java.util.Random;

import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.UnsafeAccess;

public class IndexBlockDirectMemoryScannerLargeKVsTest extends IndexBlockDirectMemoryTest{

  protected ArrayList<Key> fillIndexBlock (IndexBlock b) throws RetryOperationException {
    ArrayList<Key> keys = new ArrayList<Key>();
    Random r = new Random();

    int maxSize = 2048;
    boolean result = true;
    while(true) {
      int len = r.nextInt(maxSize) + 1;
      byte[] key = new byte[len];
      r.nextBytes(key);
      key = Bytes.toHex(key).getBytes();
      len = key.length;
      long ptr = UnsafeAccess.malloc(len);
      UnsafeAccess.copy(key, 0,  ptr, len);
      result = b.put(ptr, len, ptr, len, 0, 0);
      if(result) {
        keys.add( new Key(ptr, len));
      } else {
        break;
      }
    }
    System.out.println("Number of data blocks="+b.getNumberOfDataBlock()  + " index block data size =" + 
        b.getDataSize()+" num records=" + keys.size());
    return keys;
  }
}
