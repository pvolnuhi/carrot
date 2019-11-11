package org.bigbase.zcache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.bigbase.util.Bytes;
import org.bigbase.util.Utils;
import org.junit.Test;

import com.google.common.util.concurrent.AtomicDouble;

public class BigSortedMapTestMT {

	static AtomicDouble putsPs = new AtomicDouble();
	static AtomicDouble getsPs = new AtomicDouble();
	static AtomicDouble scanPs = new AtomicDouble();
	
  static class Worker extends Thread {
    long totalLoaded = 1;
    BigSortedMapOld map;
    long totalOps = 0;
    public Worker(BigSortedMapOld map, String name) {
      super(name);
      this.map = map;
    }
    public void run() {      
      initKeyValue();
      long start = System.currentTimeMillis();
      runPuts();
      long end = System.currentTimeMillis();
      putsPs.addAndGet((double) (totalOps * 1000)/(end-start));
      totalOps = 0;
      start = System.currentTimeMillis();
      runGets();
      end = System.currentTimeMillis();
      getsPs.addAndGet((double) (totalOps * 1000)/(end-start));
      totalOps = 0;
      start = System.currentTimeMillis();
      runScan();
      end = System.currentTimeMillis();
      scanPs.addAndGet((double) (totalOps * 1000)/(end-start));
      totalOps = 0;
    }  
    
    
    private void runScan() {
        System.out.println(Thread.currentThread().getName()+ "-test SCANs");	

      byte[] startKey = getThreadName();
      byte[] endKey = getThreadName();
      endKey[endKey.length -1]++;
      BigSortedMapScannerOld scanner = map.getScanner(startKey, endKey);
      long counter = 0;
      //TODO: change this if keys are diff sizes
      byte[] current = new byte[key.length];
      byte[] value = new byte[this.value.length];
      byte[] prev = new byte[key.length];
      long start = System.currentTimeMillis();
      
      while(scanner.hasNext()) {
        int keySize = scanner.keySize();
        assertTrue(keySize > 0);
        scanner.key(current, 0);
        scanner.value(value, 0);
//        if (counter > 0) {
//          int result = Utils.compareTo(prev, 0, prev.length, current, 0, current.length);
//          if(result >=0) {
//            System.out.println(Thread.currentThread().getName()+ 
//            		" ERROR prev="+ new String (prev) + " cur="+new String(current));
//            System.exit(-1);
//          }
//          
//        }
        counter++;        
//        System.arraycopy(current,  0,  prev,  0, prev.length);
        scanner.next();
      }
      totalOps = counter;
      long end = System.currentTimeMillis();
      System.out.println(getName()+": Time to scan "+ counter+" records is "+ (end -start)+"ms");
      if(counter != totalLoaded) {
          System.out.println(Thread.currentThread().getName()+" ERROR totalLoaded="+ totalLoaded +" scanned="+ counter);
         //System.exit(-1);
          
      }
      assertEquals(totalLoaded, counter);
      
    }
    
    private void runGets() {
        System.out.println(Thread.currentThread().getName()+ "-test GETs ");	

      long start = System.currentTimeMillis();

      byte[] tmp = new byte[value.length];
      for (int i = 1; i <= totalLoaded; i++) {
        getKey(key, i);
//        getValue(value, i);
        long size = map.get(key, 0, key.length, tmp, 0);
        if (size < 0) {
        	System.out.println(Thread.currentThread().getName() + " ERROR i="+i+" size="+size+" key=" + new String(key));
        	System.exit(-1);
        }
//        assertTrue("i="+i+" size="+size, size > 0);
//        if(value.length != size) {
//        	System.out.println(Thread.currentThread().getName() + " ERROR i="+i+" size="+size + " value size="+value.length);
//        	System.exit(-1);
//        }
//        assertEquals(value.length, size);
//
//        int res = Utils.compareTo(value, 0, value.length, tmp, 0, (int) size);
//        if(res != 0) {
//        	System.out.println(Thread.currentThread().getName() + " ERROR i="+i+
//        			" real=" + new String(tmp, 0, (int)size)+" exp=" + new String(value));
//        	System.exit(-1);
//        }
        
        if(i % 10000000 == 0) {
            System.out.println(Thread.currentThread().getName()+ "- " + i);	
        }
        totalOps++;
      }

      long end = System.currentTimeMillis();
      System.out.println("Time to get " + totalLoaded + " =" + (end - start) + "ms");
    }
    
    private final void toBytes(long val, byte[] b) {
        
        long v = val;
        int max = 10;
        int bl = b.length;
    	for (int i = 0; i < max; i++) {
          b[bl - i - 1] =   (byte)('0' + (byte) (v % 10));
          v = v/10 ;
        }
      }
    
    private final void getKey(byte[] key, long v) {    	
    	toBytes(v, key);
    }
    
    
    private final void getValue(byte[] value, long v) {
    	toBytes(v, value);

    }
    
    byte[] key;
    byte[] value;
    
    
    private byte[] getThreadName() {
    	return getName().getBytes();
    }
    
    void initKeyValue() {
    	key = new byte[17];
        byte[] tnBytes = getThreadName();
        
        System.arraycopy(tnBytes, 0, key, 0, tnBytes.length);
        key[4] = (byte) 'K';
        key[5] = (byte) 'E';
        key[6] = (byte) 'Y';
        
        value = new byte[15];
        value[0] = (byte) 'V';
        value[1] = (byte) 'A';
        value[2] = (byte) 'L';
        value[3] = (byte) 'U';
        value[4] = (byte) 'E';
    }
    
    private void runPuts() {
      System.out.println(Thread.currentThread().getName()+ "-test PUTs");	
      long start = System.currentTimeMillis();

      while (true) {        
        getKey(key, totalLoaded);
        getValue(value, totalLoaded);
        boolean res = map.put(key, 0, key.length, value, 0, value.length);
        if (res == false) {
          totalLoaded--;
          break;
        } else {
          totalLoaded++;
//          // Check first key
//          getKey(key, 1);
//          long size = map.get(key, 0, key.length, value, 0);
//          if (size < 0) {
//        	  System.out.println(getName()+" ERROR get first NULL when Put="+ (totalLoaded -1) + "key looked=" 
//        			  + new String(key));
//        	  
//        	  getKey(key, totalLoaded -1);
//        	  System.out.println("Failed key=" + new String(key));
//        	  System.exit(-1);
//          }
        }
        if(totalLoaded % 10000000 == 0) {
            System.out.println(Thread.currentThread().getName()+ "- " + totalLoaded);	
        }
      }
      totalOps = totalLoaded;
      long end = System.currentTimeMillis();
      System.out.println("Time to put "+ totalLoaded+" = "+ (end-start)+"ms");
    }
    
    public long getTotalLoaded() {
      return totalLoaded;
    }
  }
  
  
  
  @Test
	public void testPerf() throws IOException {

		BigSortedMapOld.setMaxBlockSize(4096);
		int numThreads = 32;

		int totalCycles = 2;
		int cycle = 0;
		while (cycle++ < totalCycles) {
			System.out.println("LOOP="+ cycle);
			BigSortedMapOld map = new BigSortedMapOld((long)  20 * 1024 * 1024 * 1024);

			Worker[] workers = new Worker[numThreads];
			for (int i = 0; i < numThreads; i++) {
				workers[i] = new Worker(map, getName(i));
				workers[i].start();
			}

			for (int i = 0; i < numThreads; i++) {
				try {
					workers[i].join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			System.out.println("MEM=" + Block.getTotalAllocatedMemory() + "\nDATA=" + Block.getTotalDataSize()
					+ "\nUTILIZATION=" + (((double) Block.getTotalDataSize()) / Block.getTotalAllocatedMemory()));
			System.out.println("num threads=" + numThreads + " PUT=" + putsPs.get() + " GET=" + getsPs.get() + " SCAN="
					+ scanPs.get());
			map.dispose();
			putsPs.set(0);
			getsPs.set(0);
			scanPs.set(0);
		}
	}



  private String getName(int i) {
	  return "T" + format(i);
  }



private String format(int i) {
	if ( i < 10) {
		return "00" + i;
	} else if (i < 100) {
		return "0" + i;
	};
	return Integer.toString(i);
}
        
}
