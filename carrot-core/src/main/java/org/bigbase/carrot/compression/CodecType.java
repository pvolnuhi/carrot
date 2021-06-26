/**
 *    Copyright (C) 2021-present Carrot, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 */
package org.bigbase.carrot.compression;

// TODO: Auto-generated Javadoc
/**
 * The Enum CodecType.
 */
public enum CodecType {

  /** No compression. */
  NONE(0),
  /** LZ4 */
  LZ4(1),
  /** LZ4-HC */
  LZ4HC(2),
  /** Bitmap codec*/
  BITMAP(3),
  /** ZSTD */
  ZSTD(4);
  /** The id. */
  private int id;

  /**
   * Instantiates a new codec type.
   * 
   * @param id
   *          the id
   */
  private CodecType(int id) {
    this.id = id;
  }

  /**
   * Id.
   * 
   * @return the int
   */
  public int id() {
    return id;
  }

  /**
   * Gets the codec.
   * 
   * @return the codec
   */
  public Codec getCodec() {
    switch (id) {
      case 0:
        return null;
      case 1:
        return CodecFactory.getInstance().getCodec(CodecType.LZ4);
      case 2: 
        return CodecFactory.getInstance().getCodec(CodecType.LZ4HC);  
    }
    return null;
  }
  

  
}
