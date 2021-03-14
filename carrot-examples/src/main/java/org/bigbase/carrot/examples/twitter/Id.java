package org.bigbase.carrot.examples.twitter;

import java.util.Random;

public class Id {
  
  static Random r = new Random(1);
  
  static long epoch = 1288834974657L;
  
  public static long nextId(long time) {
    
    return Math.abs(r.nextLong());
//    int worker = r.nextInt(32);
//    int datacenterId = r.nextInt(32);
//    int sequenceId = r.nextInt(4096);
//    double d = r.nextDouble();  
//    // Random time between epoch and now
//    time = (long)(epoch + d * (time - epoch));
//    return (time - epoch) << 22 |
//            datacenterId << 17 | worker << 12 | sequenceId;
  }
}
