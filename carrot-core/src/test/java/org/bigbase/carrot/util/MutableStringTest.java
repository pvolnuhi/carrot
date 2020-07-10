package org.bigbase.carrot.util;

import org.junit.Test;

public class MutableStringTest {


  @Test
  public void testMutableString() {
    MutableString ms = new MutableString();
    String tmp = "0123456789";
    ms.setBuffer(tmp.toCharArray());
    System.out.println( ms.toString() + " len="+ ms.toString().length());
  }

}
