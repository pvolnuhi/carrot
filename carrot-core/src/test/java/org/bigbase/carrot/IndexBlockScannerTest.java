package org.bigbase.carrot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexBlockScannerTest {
  Logger LOG = LoggerFactory.getLogger(IndexBlockScannerTest.class);
  
  static {
    UnsafeAccess.debug = true;
  }
  
  @Test
  public void testAll() throws IOException {
    for (int i = 0; i < 100; i++) {
      System.out.println("\nRUN "+ i+"\n");
      testFullScan();
      testFullScanWithCompressionLZ4();
      testFullScanWithCompressionLZ4HC();
      testOpenEndScan();
      testOpenEndScanWithCompressionLZ4();
      testOpenEndScanWithCompressionLZ4HC();
      testOpenStartScan();
      testOpenStartScanWithCompressionLZ4();
      testOpenStartScanWithCompressionLZ4HC();
      testSubScan();
      testSubScanWithCompressionLZ4();
      testSubScanWithCompressionLZ4HC();
    }
    BigSortedMap.printMemoryAllocationStats();
    UnsafeAccess.mallocStats();
  }
  
  @Ignore
  @Test
  public void testFullScan() throws IOException {
    System.out.println("testFullScan");  
    IndexBlock ib = getIndexBlock(4096);
    List<byte[]> keys = fillIndexBlock(ib);
    Utils.sort(keys);
    System.out.println("Loaded "+ keys.size()+" kvs");
    IndexBlockScanner scanner = IndexBlockScanner.getScanner(ib, null, null, Long.MAX_VALUE);
    verifyScanner(scanner, keys);
    scanner.close();
    ib.free();
  }

  @Ignore
  @Test
  public void testFullScanWithCompressionLZ4() throws IOException {
    System.out.println("testFullScanWithCompressionLZ4");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    testFullScan();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testFullScanWithCompressionLZ4HC() throws IOException {
    System.out.println("testFullScanWithCompressionLZ4HC");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    testFullScan();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testOpenStartScan() throws IOException {
    System.out.println("testOpenStartScan");  
    IndexBlock ib = getIndexBlock(4096);
    List<byte[]> keys = fillIndexBlock(ib);

    Utils.sort(keys);
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("testOpenStartScan seed="+ seed);
    int stopRowIndex = r.nextInt(keys.size());
    byte[] stopRow = keys.get(stopRowIndex);
    System.out.println("Loaded "+ keys.size()+" kvs");
    keys = keys.subList(0, stopRowIndex);
    System.out.println("Selected "+ keys.size()+" kvs");
    IndexBlockScanner scanner = IndexBlockScanner.getScanner(ib, null, stopRow, Long.MAX_VALUE);
    verifyScanner(scanner, keys);
    scanner.close();
    ib.free();
  }
  
  @Ignore
  @Test
  public void testOpenStartScanWithCompressionLZ4() throws IOException {
    System.out.println("testOpenStartScanWithCompressionLZ4");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    testOpenStartScan();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testOpenStartScanWithCompressionLZ4HC() throws IOException {
    System.out.println("testOpenStartScanWithCompressionLZ4HC");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    testOpenStartScan();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testOpenEndScan() throws IOException {
    System.out.println("testOpenEndScan");  
    IndexBlock ib = getIndexBlock(4096);
    List<byte[]> keys = fillIndexBlock(ib);
    Utils.sort(keys);
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("testOpenEndScan seed="+ seed);
    int startRowIndex = r.nextInt(keys.size());
    byte[] startRow = keys.get(startRowIndex);
    System.out.println("Loaded "+ keys.size()+" kvs");
    keys = keys.subList(startRowIndex, keys.size());
    System.out.println("Selected "+ keys.size()+" kvs");
    IndexBlockScanner scanner = IndexBlockScanner.getScanner(ib, startRow, null, Long.MAX_VALUE);
    verifyScanner(scanner, keys);
    scanner.close();
    ib.free();

  }
  
  @Ignore
  @Test
  public void testOpenEndScanWithCompressionLZ4() throws IOException {
    System.out.println("testOpenStartScanWithCompressionLZ4");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    testOpenEndScan();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  
  @Ignore
  @Test
  public void testOpenEndScanWithCompressionLZ4HC() throws IOException {
    System.out.println("testOpenStartScanWithCompressionLZ4HC");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    testOpenEndScan();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testSubScan() throws IOException {
    System.out.println("testSubScan");  
    IndexBlock ib = getIndexBlock(4096);
    List<byte[]> keys = fillIndexBlock(ib);
    Utils.sort(keys);
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("testSubScan seed="+ seed);
    int startRowIndex = r.nextInt(keys.size());
    int stopRowIndex = r.nextInt(keys.size());
    int tmp = startRowIndex;
    if (startRowIndex > stopRowIndex) {
      startRowIndex = stopRowIndex;
      stopRowIndex = tmp;
    }

    byte[] startRow = keys.get(startRowIndex);
    byte[] stopRow = keys.get(stopRowIndex);
    System.out.println("Loaded "+ keys.size()+" kvs");
    keys = keys.subList(startRowIndex, stopRowIndex);
    System.out.println("Selected "+ keys.size()+" kvs");
    IndexBlockScanner scanner = IndexBlockScanner.getScanner(ib, startRow, stopRow, Long.MAX_VALUE);
    verifyScanner(scanner, keys);
    scanner.close();
    ib.free();

  }
  
  
  @Ignore
  @Test
  public void testSubScanWithCompressionLZ4() throws IOException {
    System.out.println("testSubScanWithCompressionLZ4");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    testSubScan();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  @Ignore
  @Test
  public void testSubScanWithCompressionLZ4HC() throws IOException {
    System.out.println("testSubScanWithCompressionLZ4HC");  
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    testSubScan();
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
  }
  
  private void verifyScanner(IndexBlockScanner scanner, List<byte[]> keys) throws IOException {
    int count = 0;
    DataBlockScanner dbscn=null;
    
    while ((dbscn = scanner.nextBlockScanner()) != null){
      while(dbscn.hasNext()) {
        count++;
        byte[] key = keys.get(count-1);
        int keySize = dbscn.keySize();
        int valSize = dbscn.valueSize();
        //System.out.println("key expected size="+ key.length + " actual="+ keySize);
        assertEquals(key.length, keySize);
        assertEquals(key.length, valSize);
        byte[] buf = new byte[keySize];
        dbscn.key(buf, 0);
        assertTrue(Utils.compareTo(key, 0, key.length, buf, 0, buf.length) == 0);
        dbscn.value(buf, 0);
        assertTrue(Utils.compareTo(key, 0, key.length, buf, 0, buf.length) == 0);
        dbscn.next();
      } 
      dbscn.close();
    } 
    assertEquals(keys.size(), count);
  }
  
  private IndexBlock getIndexBlock(int size) {
    IndexBlock ib = new IndexBlock(size);
    ib.setFirstIndexBlock();
    return ib;
  }
  
  protected List<byte[]> fillIndexBlock (IndexBlock b) throws RetryOperationException {
    ArrayList<byte[]> keys = new ArrayList<byte[]>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("FILL seed="+ seed);
    int kvSize = 32;
    boolean result = true;
    while(true) {
      byte[] key = new byte[kvSize];
      r.nextBytes(key);
      result = b.put(key, 0, key.length, key, 0, key.length, 0, 0);
      if(result) {
        keys.add(key);
      } else {
        break;
      }
    }
    System.out.println("Number of data blocks="+b.getNumberOfDataBlock() + " "  + " index block data size =" + 
        b.getDataInBlockSize()+" num records=" + keys.size());
    return keys;
  }
  
}
