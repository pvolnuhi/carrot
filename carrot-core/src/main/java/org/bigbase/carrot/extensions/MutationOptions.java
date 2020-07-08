package org.bigbase.carrot.extensions;

/*
 *   NONE - no restriction on mutation
 *   NX -- Only set the key if it does not already exist.
 *   XX -- Only set the key if it already exist.
 *
 */
public enum MutationOptions {

  NONE, 
  NX, 
  XX;
}
