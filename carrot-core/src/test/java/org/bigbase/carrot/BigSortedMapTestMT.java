package org.bigbase.carrot;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

import org.bigbase.carrot.util.Utils;
import org.junit.Test;

import com.google.common.util.concurrent.AtomicDouble;

public class BigSortedMapTestMT {
  static int totalThreads = 8;
	static AtomicDouble putsPs = new AtomicDouble();
	static AtomicDouble comboPs = new AtomicDouble();
	static AtomicDouble scanPs = new AtomicDouble();
	static AtomicLong totalLoaded = new AtomicLong(0);
	static AtomicLong totalDeleted = new AtomicLong(0);
	static CyclicBarrier barrier = new CyclicBarrier(totalThreads);
	
  static class Worker extends Thread {
    BigSortedMap map;
    long totalOps = 0;
    int keySize  = 16;
    int maxValueSize = 10000;
    byte[] key = new byte[16];
    byte[] value = new byte[maxValueSize];
    int[] valueSizeDistribution = new int[] {10 /*p0*/,  100 /*p50*/, 220 /*p90*/, 450 /*p95*/, 
        3000 /*p99*/, 9500 /*p100*/};
    double[] pp = new double[] {0, 0.5, 0.9, 0.95, 0.99, 1.0};
    Random r = new Random();
    
    public Worker(BigSortedMap map, String name) {
      super(name);
      this.map = map;
    }

    public void run() {
      try {
        // init value
        r.nextBytes(value);
        long start = System.currentTimeMillis();
        runPuts();
        long end = System.currentTimeMillis();
        putsPs.addAndGet((double) (totalOps * 1000) / (end - start));
        totalOps = 0;
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
        start = System.currentTimeMillis();
        runPutDeleteGetsScans();
        end = System.currentTimeMillis();
        comboPs.addAndGet((double) (totalOps * 1000) / (end - start));
        totalOps = 0;
      } catch (RuntimeException e) {
        e.printStackTrace();
        System.exit(-1);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        System.exit(-1);
      }
    }
    
    private void runPutDeleteGetsScans() throws IOException {
        System.out.println(Thread.currentThread().getName()+ "-test runPutDeleteGetsScans ");	

      int num = (int) totalLoaded.get();
      long start = System.currentTimeMillis();
      Random r = new Random(); 
      for (int i = 0; i < num; i++) {
        double d = r.nextDouble();
        boolean result = false;
        if (BigSortedMap.getTotalAllocatedMemory() < 0.99 * map.getMaxMemory()) {
          if (d < 0.30) {
            result = put();
            if (!result) {
              Thread.dumpStack();
              System.exit(-1);
            }
            assertTrue(result);
            
          } else if (d < 0.5) {
            result = overwrite();
            assertTrue(result);
          }  
          else if (d < 0.8) {
            result = delete();
            if (!result) {
              Thread.dumpStack();
              System.exit(-1);
            }
            assertTrue(result);
            
          } else  if (d < 0.9){
            // 0.9 is for safety, not all up to totalLoaded
            // can be really loaded (last ones may be in flight)
            int n = r.nextInt((int)(totalLoaded.get()* 0.9));
            long size = get(n);
            if (size < 0 && n > totalDeleted.get()) {
              fail("Not found "+n+" deleted=" + totalDeleted.get());
            }
            if (size > 0) {
              assertTrue(Thread.currentThread().getName() + 
                " ERROR  num="+i+" totalLoaded=" + totalLoaded.get() + " size=" + size, 
                size >= valueSizeDistribution[0] && 
                size <= valueSizeDistribution[valueSizeDistribution.length-1]);
            }
          } else {
            int n = r.nextInt((int)(totalLoaded.get()* 0.9));
            scan(n);
          }
        } else {
          // Do not do puts
          if (d < 0.5) {
            result = delete();
            if (!result) {
              Thread.dumpStack();
              System.exit(-1);
            }
            assertTrue(result);
          } else  if (d < 0.9){
            // 0.9 is for safety, not all up to totalLoaded
            // can be really loaded (last ones may be in flight)
            int n = r.nextInt((int)(totalLoaded.get()* 0.9));
            long size = get(n);
            if (size < 0 && n > totalDeleted.get()) {
              fail("Not found "+n+" deleted=" + totalDeleted.get());
            }
            if (size > 0) {
              assertTrue(Thread.currentThread().getName() + 
                " ERROR  num="+i+" totalLoaded=" + totalLoaded.get() + " size=" + size, 
                size >= valueSizeDistribution[0] && 
                size <= valueSizeDistribution[valueSizeDistribution.length-1]);
            }
          } else {
            int n = r.nextInt((int)(totalLoaded.get()* 0.9));
            scan(n);
          }
        }
        if(i % 1000000 == 0) {
            System.out.println(Thread.currentThread().getName()+ "- " + i + " allocated=" +
            BigSortedMap.getTotalAllocatedMemory() + " data=" + BigSortedMap.getTotalDataSize() + 
            " index=" + BigSortedMap.getTotalIndexSize() + " max=" + map.getMaxMemory());	
        }
      }

      long end = System.currentTimeMillis();
      totalOps = num;
      System.out.println("Time to get " + num + " =" + (end - start) + "ms");
      try {
        scanAll();
      } catch (InterruptedException | BrokenBarrierException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

    }
    
    private final void getKey(byte[] key, long v) {    	
      r.setSeed(v);
      r.nextBytes(key);
    }
    
    private byte[] getValue() {
      //int size = getValueSize();
      //byte[] value = new byte[size];
      r.nextBytes(value);
      return value;
    }
    
    private final int getValueSize() {
      double d = r.nextDouble();
      int size;
      int start, end;
      double range, diff;
      if (d < 0.5) {
        start = valueSizeDistribution[0];
        end = valueSizeDistribution[1];
        range = 0.5;
        diff = d - 0;
      } else if (d < 0.9) {
        start = valueSizeDistribution[1];
        end = valueSizeDistribution[2];
        range = 0.4;
        diff = d - 0.5;
      } else if (d < 0.95) {
        start = valueSizeDistribution[2];
        end = valueSizeDistribution[3];
        range = 0.05;
        diff = d - 0.9;
      } else if (d < 0.99) {
        start = valueSizeDistribution[3];
        end = valueSizeDistribution[4];
        range = 0.04;
        diff = d - 0.95;
      } else {
        start = valueSizeDistribution[4];
        end = valueSizeDistribution[5];
        range = 0.01;
        diff = d - 0.99;
      }
      size = (int)((start * (range - diff) + end * diff)/ range);
      return size;
    }
    private void runPuts() {
      System.out.println(Thread.currentThread().getName()+ "-test PUTs");	
      long start = System.currentTimeMillis();
      long totalSize=0;
      while (true) {    
        if (BigSortedMap.getTotalAllocatedMemory() > 0.90 * map.getMaxMemory()) {
          break;
        }
        long n = totalLoaded.incrementAndGet();
        getKey(key, n);
        int valueSize = getValueSize();
        totalSize += key.length + value.length;
        boolean res = map.put(key, 0, key.length, value, 0, valueSize, 0);
        if (res == false) {
          totalLoaded.decrementAndGet();
          break;
        } 

        totalOps ++;
        if(n % 1000000 == 0) {
            System.out.println(Thread.currentThread().getName()+ "- " + n);	
        }
      }
      
      long end = System.currentTimeMillis();
      System.out.println("Time to put "+ totalOps+" = "+ (end-start)+"ms. Total size=" + totalSize);
    }
    
    private boolean put() {
      long n = totalLoaded.incrementAndGet();
      getKey(key, n);
      int valueSize = getValueSize();
      return map.put(key, 0, key.length, value, 0, valueSize, 0);
    }
    
    private boolean overwrite() {
      long n = totalLoaded.get();
      long k = totalDeleted.get();
      int m = r.nextInt((int)(n - k));
      m += k;
      getKey(key, m);
      int valueSize = getValueSize();
      return map.put(key, 0, key.length, value, 0, valueSize, 0);
    }
    
    private boolean delete() {
      long n = totalDeleted.incrementAndGet();
      getKey(key, n);
      return map.delete(key, 0, key.length);
    }
    
    private long get(long n) {
      getKey(key, n);
      return map.get(key, 0, key.length, value, 0, Long.MAX_VALUE);
    }
    
    /*
     * Short scan operation
     */
    private void scan (long n) throws IOException {
      getKey(key, n);
      int toScan = 221;
      
      BigSortedMapScanner scanner = map.getScanner(key, null);
      int count = 0;
      byte[] prev = new byte[key.length] ;
      byte[] current = new byte[key.length];
      while(scanner.hasNext() && count++ < toScan) {
        scanner.key(current, 0);
        if (count > 1) {
          int result = Utils.compareTo(current, 0, current.length, prev, 0, prev.length);
          if (result <= 0) {
            System.out.println(result+" **********************************************************************************");
          }
          assertTrue(result > 0);
        }
        System.arraycopy(current, 0, prev, 0, prev.length);
        scanner.next();
      }
      scanner.close();
    }
    
    private void scanAll () throws IOException, InterruptedException, BrokenBarrierException {
      barrier.await();   
      if (Thread.currentThread().getName().equals("0")) {
        barrier.reset();
      }
      
      BigSortedMapScanner scanner = map.getScanner(null, null);
      int count = 0;
      byte[] prev = new byte[key.length] ;
      long prevVersion = 0;
      Op prevType = null;
      byte[] current = new byte[key.length];
      long start = System.currentTimeMillis();
      while(scanner.hasNext()) {
        count++;
        scanner.key(current, 0);
        if (count > 1) {
          int result = Utils.compareTo(current, 0, current.length, prev, 0, prev.length);
          if (result <= 0) {
            System.out.println(result +" prevVersion="+prevVersion +" prevType="+prevType + 
              " curVersion=" + scanner.keyVersion() + " curType=" + scanner.keyOpType());
          }
          assertTrue(result > 0);
        }
        prevVersion = scanner.keyVersion();
        prevType = scanner.keyOpType();
        System.arraycopy(current, 0, prev, 0, prev.length);
        scanner.next();
      }
      scanner.close();
      long end = System.currentTimeMillis();
      System.out.println(Thread.currentThread().getName() + " scanned " + count +" in " + (end -start)+"ms");
    }
    
  }
  

  
  @Test
	public void testPerf() throws IOException {

		BigSortedMap.setMaxBlockSize(4096);
		//int numThreads = 8;

		int totalCycles = 1000;
		int cycle = 0;
		while (cycle++ < totalCycles) {
		  totalLoaded.set(0);
		  totalDeleted.set(0);
			System.out.println("LOOP="+ cycle);
			BigSortedMap map = new BigSortedMap((long)  1 * 1024 * 1024 * 1024);

			Worker[] workers = new Worker[totalThreads];
			for (int i = 0; i < totalThreads; i++) {
				workers[i] = new Worker(map, getName(i));
				workers[i].start();
			}

			for (int i = 0; i < totalThreads; i++) {
				try {
					workers[i].join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			System.out.println("MEM=" + BigSortedMap.getTotalAllocatedMemory() + "\nDATA=" + BigSortedMap.getTotalDataSize()
					+ "\nUTILIZATION=" + (((double) BigSortedMap.getTotalDataSize()) / BigSortedMap.getTotalAllocatedMemory()));
			System.out.println("num threads=" + totalThreads + " PUT=" + putsPs.get() + " GET=" + comboPs.get() + " SCAN="
					+ scanPs.get());
			map.dispose();
			putsPs.set(0);
			comboPs.set(0);
			scanPs.set(0);
		}
	}

  private String getName(int i) {
	  return Integer.toString(i);
  }
        
}
