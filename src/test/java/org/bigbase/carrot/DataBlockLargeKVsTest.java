package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Test;

public class DataBlockLargeKVsTest extends DataBlockTest{
  
  static {
    UnsafeAccess.debug = true;
  }
  
  
  /**
   * 
   * 1. K & V are both in data block - we do not test this
   * 2. K & V both external, reducing value size should keep them both external
   * 3. K  is in data block , V  is external, reducing V size to 12 bytes and less will guarantee
   *    that there will be no overflow in a data block and new V will be kept in a data block 
   */
  @Test
  public void testOverwriteSmallerValueSize() throws RetryOperationException, IOException {
    System.out.println("testOverwriteSmallerValueSize - LargeKVs");
    for (int i = 0; i < 1000; i++) {
      Random r = new Random();
      DataBlock b = getDataBlock();
      List<byte[]> keys = fillDataBlock(b);
      for (byte[] key : keys) {
        int keySize = key.length;
        int valueSize = 0;
        DataBlock.AllocType type = DataBlock.getAllocType(keySize, keySize);
        if (type == DataBlock.AllocType.EMBEDDED) {
          continue;
        } else if (type == DataBlock.AllocType.EXT_KEY_VALUE) {
          valueSize = keySize/2 ;
        } else { // DataBlock.AllocType.EXT_VALUE
          valueSize = 12;
        }
        byte[] value = new byte[valueSize];
        r.nextBytes(value);
        byte[] buf = new byte[valueSize];
        boolean res = b.put(key, 0, keySize, value, 0, value.length, Long.MAX_VALUE, 0);
        assertTrue(res);
        long size = b.get(key, 0, keySize, buf, 0, Long.MAX_VALUE);
        assertEquals(valueSize, (int) size);
        assertTrue(Utils.compareTo(buf, 0, buf.length, value, 0, value.length) == 0);
      }
      assertEquals(keys.size() + 1, (int) b.getNumberOfRecords());
      assertEquals(0, (int) b.getNumberOfDeletedAndUpdatedRecords());
      scanAndVerify(b, keys);
      b.free();
    }

  }
  /**
   * 
   * 1. K & V are both in data block and > 12 - push V out of data block
   * 2. K & V both external, increasing value size should keep them both external
   * 3. K  is in data block , V  is external, increasing V size is safe 
   */
  @Test
  public void testOverwriteLargerValueSize() throws RetryOperationException, IOException {
    System.out.println("testOverwriteLargerValueSize- LargeKVs");
    for (int i = 0; i < 1000; i++) {
      Random r = new Random();
      DataBlock b = getDataBlock();
      List<byte[]> keys = fillDataBlock(b);
      for (byte[] key : keys) {
        int keySize = key.length;
        int valueSize = 0;
        DataBlock.AllocType type = DataBlock.getAllocType(keySize, keySize);
        if (type == DataBlock.AllocType.EMBEDDED) {
          if (keySize < 12) {
            continue;
          } else {
            valueSize = 2067;
          }
          
        } else  { // VALUE is outside data block, increasing it will keep it outside
          valueSize = keySize * 2 ;
        } 
        byte[] value = new byte[valueSize];
        r.nextBytes(value);
        byte[] buf = new byte[valueSize];
        boolean res = b.put(key, 0, keySize, value, 0, value.length, Long.MAX_VALUE, 0);
        assertTrue(res);
        long size = b.get(key, 0, keySize, buf, 0, Long.MAX_VALUE);
        assertEquals(valueSize, (int) size);
        assertTrue(Utils.compareTo(buf, 0, buf.length, value, 0, value.length) == 0);
      }
      assertEquals(keys.size() + 1, (int) b.getNumberOfRecords());
      assertEquals(0, (int) b.getNumberOfDeletedAndUpdatedRecords());
      scanAndVerify(b, keys);
      b.free();
    }
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
