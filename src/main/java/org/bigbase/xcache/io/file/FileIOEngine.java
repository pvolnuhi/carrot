package org.bigbase.xcache.io.file;

import java.io.IOException;

import org.bigbase.xcache.io.IOEngine;

public class FileIOEngine implements IOEngine {

  @Override
  public boolean isPersistent() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public byte[] read(long offset, int length) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean read(long offset, int length, byte[] value, int off) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void write(byte[] key, int keyOff, int keyLen, byte[] value, int valOff, int valLen,
      long offset) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void sync() throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void shutdown() {
    // TODO Auto-generated method stub
    
  }

}
