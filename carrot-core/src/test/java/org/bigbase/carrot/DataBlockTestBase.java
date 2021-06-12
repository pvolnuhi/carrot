package org.bigbase.carrot;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class DataBlockTestBase {
  
  
  protected DataBlock getDataBlock() {
    IndexBlock ib = new IndexBlock(4096);
    ib.setFirstIndexBlock();
    ib.firstBlock();
    return ib.firstBlock();
  } 
  
  protected boolean contains(long key, int keySize, List<Key> keys) {
    for (Key k : keys) {
      if (Utils.compareTo(k.address, k.length, key, keySize) == 0) {
        return true;
      }
    }
    return false;
  }
  
 
  protected boolean isValidFailure(DataBlock b,  Key key, int valLen, int oldValLen) {
    int dataSize = b.getDataInBlockSize();
    int blockSize = b.getBlockSize();
    int newRecSize = key.length + valLen + DataBlock.RECORD_TOTAL_OVERHEAD;
    if (DataBlock.mustStoreExternally(key.length, valLen)) {
      newRecSize = 12 + DataBlock.RECORD_TOTAL_OVERHEAD;
    }
    int oldRecSize = key.length + oldValLen + DataBlock.RECORD_TOTAL_OVERHEAD;
    if (DataBlock.mustStoreExternally(key.length, oldValLen)) {
      oldRecSize = 12 + DataBlock.RECORD_TOTAL_OVERHEAD;
    }
    
    return dataSize + newRecSize -oldRecSize > blockSize;
  }
  
  protected void scanAndVerify(DataBlock b) throws RetryOperationException, IOException {
    long buffer ;
    long tmp =0;

    DataBlockScanner bs = 
        DataBlockScanner.getScanner(b, 0, 0, 0, 0, Long.MAX_VALUE);
    int prevKeySize=0;
    int count = 0;
    while (bs.hasNext()) {
      int keySize = bs.keySize();
      buffer = UnsafeAccess.malloc(keySize);
      bs.key(buffer, keySize);
      bs.next();
      count++;
      if (count > 1) {
        // compare
        int res = Utils.compareTo(tmp, prevKeySize, buffer, keySize);
        assertTrue (res < 0);
        UnsafeAccess.free(tmp);
      }
      tmp = buffer;
      prevKeySize = keySize;
    }
    UnsafeAccess.free(tmp);
    bs.close();
    System.out.println("Scanned ="+ count);
  }


  
  
}
