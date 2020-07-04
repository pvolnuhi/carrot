package org.bigbase.carrot.extensions.hashes;

import java.io.Closeable;
import java.io.IOException;

import org.bigbase.carrot.BigSortedMap;

public class HashScanner implements Closeable{
  
  public HashScanner(BigSortedMap map, long keyPtr, int keySize) {
    
  }
  public boolean hasNext() {
    return false;
  }
  
  public void next() {
    
  }
  
  public long fieldAddress() {
    return 0;
  }
  
  public int fieldSize() {
    return 0;
  }
  
  public long valueAddress() {
    return 0;
  }
  
  
  public int valueSize() {
    return 0;
  }
  
  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
    
  }
  
}