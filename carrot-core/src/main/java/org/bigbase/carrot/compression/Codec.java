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

import java.io.IOException;
import java.nio.ByteBuffer;

// TODO: Auto-generated Javadoc
/**
 * The Interface Codec.
 */
public interface Codec {

	/** The Constant COMPRESSION_THRESHOLD. */
	public final static String COMPRESSION_THRESHOLD = "compression.threshold";
	
    /**
     * Compress the content in the given input buffer. After the compression,
     * you can retrieve the compressed data from the output buffer [pos() ... limit())
     * (compressed data size = limit() - pos() = remaining())
     * uncompressed - buffer[pos() ... limit()) containing the input data
     * compressed - output of the compressed data. Uses range [pos()..limit()].
     *
     * @param src the src
     * @param dst the dst
     * @return byte size of the compressed data.
     * @throws IOException Signals that an I/O exception has occurred.
     */
	public int compress(ByteBuffer src, ByteBuffer dst) throws IOException;
	
	/**
	 * Uncompress the content in the input buffer. The result is dumped
	 * to the specified output buffer. Note that if you pass the wrong data
	 * or the range [pos(), limit()) that cannot be uncompressed, your JVM might
	 * crash due to the access violation exception issued in the native code
	 * written in C++. To avoid this type of crash,
	 * use isValidCompressedBuffer(ByteBuffer) first.
	 *
	 * @param src - buffer[pos() ... limit()) containing the input data
	 * @param dst - output of the the uncompressed data. It uses buffer[pot()..]
	 * @return  uncompressed data size
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public int decompress(ByteBuffer src, ByteBuffer dst) throws IOException;
	
	
	/**
	 * Compress memory directly
	 * @param src source address
	 * @param srcSize source size
	 * @param dst destination address
	 * @param dstCapacity destination capacity
	 * @return compressed size or 0 if capacity was not enough
	 */
	public int compress(long src, int srcSize, long dst, int dstCapacity);
	  
	 /**
   * Decompress memory directly
   * @param src source address
   * @param srcSize source size
   * @param dst destination address
   * @param dstCapacity destination capacity
   * @return compressed size or 0 if capacity was not enough
   */
	public int decompress(long src, int srcSize, long dst, int dstCapacity);
	
	/**
	 * Gets the compression threshold.
	 *
	 * @return the compression threshold
	 */
	public int getCompressionThreshold();
	
	/**
	 * Sets the compression threshold.
	 *
	 * @param val the new compression threshold
	 */
	public void setCompressionThreshold(int val);
	
	/**
	 * Gets the type.
	 *
	 * @return the type
	 */
	public CodecType getType();
	
	/**
	 * Gets the avg compression ratio.
	 *
	 * @return the avg compression ratio
	 */
	public double getAvgCompressionRatio();
	
	public long getTotalProcessed();
	
	/**
	 * Sets the level.
	 *
	 * @param level the new level
	 */
	public void setLevel(int level);
	
	/**
	 * Gets the level.
	 *
	 * @return the level
	 */
	public int getLevel();
	
}
