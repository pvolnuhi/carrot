package org.bigbase.carrot;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.bigbase.util.UnsafeAccess;
import org.bigbase.util.Utils;

/**
 * TODO: 1. minimize block overhead
 *       2. Check logic
 *       3. Do not keep address, but index in a array of addresses (?)
 *          
 * Records are sorted Format: [DATA_BLOCK_STATIC_PREFIX][BLOCK_START_KEY] 
 * 
 [DATA_BLOCK_STATIC_PREFIX] - 19 bytes
 
     dataPtr                  (8 bytes)
     blockSize                (2 bytes)
     dataSize                 (2 bytes)
     numRecords               (2 bytes)
     seqNumberSplitOrMerge    (1 byte)
     compressed               (1 byte)
     threadSafe               (1 byte)
     numDeletedAndUpdatedRecords (2 bytes)

 [BLOCK_START_KEY] = [len][key]
     [len] - 2 bytes
     [key] - key data
     [version] - 8 bytes
     [type]    - 1 byte (DELETE=0, PUT =1) 

 */
/**
 * TODO: how to handle deletion of a first key in a first data block?
 * @author jenium65
 *
 */

public class IndexBlock implements Comparable<IndexBlock> {

	public final static int KEY_SIZE_LENGTH = 2;
	public final static long NOT_FOUND = -1L;
	public final static double MIN_COMPACT_RATIO = 0.25d;
	public final static double MAX_MERGE_RATIO = 0.25d;

	public final static String MAX_BLOCK_SIZE_KEY = "max.index.block.size";
	public static int MAX_BLOCK_SIZE = 4096;

	public final static int DATA_BLOCK_STATIC_PREFIX = 19;
	public final static int VERSION_SIZE = 8;
	public final static int TYPE_SIZE = 1;
	public final static int DATA_BLOCK_STATIC_OVERHEAD = DATA_BLOCK_STATIC_PREFIX + VERSION_SIZE + TYPE_SIZE; // 28
																												// bytes

	static {
		String val = System.getProperty(MAX_BLOCK_SIZE_KEY);
		if (val != null) {
			MAX_BLOCK_SIZE = Integer.parseInt(val);
		}
	}

	static ThreadLocal<Long> bufPtr = new ThreadLocal<Long>() {

		@Override
		protected Long initialValue() {
			int maxSize = BigSortedMap.getMaxBlockSize() + 80;
			return UnsafeAccess.malloc(maxSize);
		}

	};

	static ThreadLocal<DataBlock> block = new ThreadLocal<DataBlock>() {

		@Override
		protected DataBlock initialValue() {
			String val = System.getProperty(DataBlock.MAX_BLOCK_SIZE_KEY);
			int size = 0;
			if (val != null) {
				size = Integer.parseInt(val);
			} else {
				size = DataBlock.MAX_BLOCK_SIZE;
			}
			return new DataBlock((short) size);
		}

	};

	/*
	 * TODO: make this configurable
	 */

	static float[] BLOCK_RATIOS = new float[] { 0.25f, 0.5f, 0.75f, 1.0f };
	/*
	 * Read-Write Lock TODO: StampedLock (Java 8)
	 */
	static ReentrantReadWriteLock[] locks = new ReentrantReadWriteLock[11113];
	static {
		for (int i = 0; i < locks.length; i++) {
			locks[i] = new ReentrantReadWriteLock();
		}
	}

	/**
	 * Get min size greater than current
	 * 
	 * @param max     - max size
	 * @param current current size
	 * @return min size or -1;
	 */
	static int getMinSizeGreaterThan(int max, int current) {
		for (int i = 0; i < BLOCK_RATIOS.length; i++) {
			int size = Math.round(max * BLOCK_RATIOS[i]);
			if (size > current)
				return size;
		}
		return -1;
	}

	/**
	 * Get min size greater than current
	 * 
	 * @param max     - max size
	 * @param current current size
	 * @return min size or -1;
	 */
	static int getMinSizeGreaterOrEqualsThan(int max, int current) {
		for (int i = 0; i < BLOCK_RATIOS.length; i++) {
			int size = Math.round(max * BLOCK_RATIOS[i]);
			if (size >= current)
				return size;
		}
		return -1;
	}

	/*
	 * Index block address (current)
	 */
	volatile long dataPtr;

	/*
	 * Index block size
	 */
	short blockSize;

	/*
	 * Total data size (all index records)
	 */
	short dataSize = 0;
	/*
	 * Number of index records in a block - index size (volatile?)
	 */
	short numDataBlocks = 0;

	/*
	 * Split/Merge sequence number.
	 */
	volatile byte seqNumberSplitOrMerge;

	/*
	 * Is index block compressed
	 */
	boolean compressed = false;

	/*
	 * Is thread safe block
	 */

	boolean threadSafe;

	/*
	 * Is block still valid
	 */
	volatile boolean valid = true;

	/*
	 * First key of this index block (used for comparisons) including version and
	 * type (DELETE = 0, PUT=1)
	 */
	byte[] firstKey;
	long version;
	byte type;

	/**
	 * Constructor
	 * 
	 * @param initial size
	 */
	IndexBlock(int size) {
		this.dataPtr = UnsafeAccess.malloc(size);
		if (dataPtr == 0) {
			// TODO: OOM handling
			throw new RuntimeException("Failed to allocate " + size + " bytes");
		}
		// This is not accurate
		BigSortedMap.totalAllocatedMemory.addAndGet(size);
		BigSortedMap.totalIndexSize.addAndGet(size);
		this.blockSize = (short) size;
	}

	/**
	 * The method is used only during navigating through CSLM
	 * 
	 * @param key     key array
	 * @param off     offset in array
	 * @param len     length of a key
	 * @param version version
	 */
	public void putForSearch(byte[] key, int off, int len, long version) {
		this.threadSafe = true;
		long address = this.dataPtr + DATA_BLOCK_STATIC_PREFIX;
		UnsafeAccess.putShort(address, (short) len);
		address += KEY_SIZE_LENGTH;
		UnsafeAccess.copy(key, off, address, len);
		address += len;
		UnsafeAccess.putLong(address, version);
		address += VERSION_SIZE;
		UnsafeAccess.putByte(address, (byte) Op.DELETE.ordinal());
	}

	/**
	 * The method is used only during navigating through CSLM
	 * 
	 * @param key     key array
	 * @param off     offset in array
	 * @param len     length of a key
	 * @param version version
	 */
	public void putForSearch(long keyPtr, int len, long version) {
		this.threadSafe = true;
		long address = this.dataPtr + DATA_BLOCK_STATIC_PREFIX;
		UnsafeAccess.putShort(address, (short) len);
		address += KEY_SIZE_LENGTH;
		UnsafeAccess.copy(keyPtr, address, len);
		address += len;
		UnsafeAccess.putLong(address, version);
		address += VERSION_SIZE;
		UnsafeAccess.putByte(address, (byte) Op.DELETE.ordinal());
	}

	/**
	 * Get atomically list of data blocks (write lock)
	 * 
	 * @param input list of data blocks to initialize (all are MAX_SIZE)
	 * @return number of data blocks in this index block
	 */
	public int getDataBlocks(List<DataBlock> list) {

		try {
			writeLock();
			int size = list.size();
			if (size < this.numDataBlocks) {
				for (int i = 0; i < this.numDataBlocks - size; i++) {
					list.add(new DataBlock());
				}
			}
			DataBlock block = firstBlock();
			if (block == null) {
				return 0;
			} else {
				Iterator<DataBlock> it = list.iterator();
				do {
					DataBlock db = it.next();
					db.copyOf(block);
				} while ((block = nextBlock(block)) != null);
			}
			return this.numDataBlocks;
		} finally {
			writeUnlock();
		}
	}

	private final int keyLength(long blockAddress) {
		return UnsafeAccess.toShort(blockAddress + DATA_BLOCK_STATIC_PREFIX);
	}

	private final long version(long blockAddress) {
		return UnsafeAccess.toLong(blockAddress + DATA_BLOCK_STATIC_PREFIX + KEY_SIZE_LENGTH + keyLength(blockAddress));
	}

	private final byte type(long blockAddress) {
		return UnsafeAccess.toByte(
				blockAddress + DATA_BLOCK_STATIC_PREFIX + KEY_SIZE_LENGTH + keyLength(blockAddress) + VERSION_SIZE);
	}

	private final long keyAddress(long blockAddress) {
		return blockAddress + DATA_BLOCK_STATIC_PREFIX + KEY_SIZE_LENGTH;
	}

	/**
	 * Set thread safe
	 * 
	 * @param b thread safe (true/false)
	 */
	public void setThreadSafe(boolean b) {
		this.threadSafe = b;
	}

	/**
	 * Is thread safe
	 * 
	 * @return
	 */
	public boolean isThreadSafe() {
		return threadSafe;
	}

	public boolean isCompressed() {
		return compressed;
	}

	public boolean isValid() {
		return valid;
	}

	/**
	 * Reset block for reuse
	 */
	public void reset() {
		this.seqNumberSplitOrMerge = 0;
		this.numDataBlocks = 0;
		this.dataSize = 0;
		this.compressed = false;
		this.firstKey = null;
		this.version = 0;
		this.type = 0;
	}

	/**
	 * Read lock
	 * 
	 * @throws RetryOperationException
	 * @throws InterruptedException
	 */
	public void readLock() throws RetryOperationException {
		// TODO: optimize, loop until success
		if (isThreadSafe())
			return;
		long before = this.seqNumberSplitOrMerge;
		int index = (hashCode() % locks.length);
		ReentrantReadWriteLock lock = locks[index];
		lock.readLock().lock();
		long after = this.seqNumberSplitOrMerge;
		if (before != after) {
			throw new RetryOperationException();
		}
	}

	/**
	 * Read unlock
	 */
	public void readUnlock() {
		if (isThreadSafe())
			return;
		int index = (hashCode() % locks.length);
		ReentrantReadWriteLock lock = locks[index];
		lock.readLock().unlock();
	}

	/**
	 * Write lock
	 * 
	 * @throws RetryOperationException
	 * @throws InterruptedException
	 */
	public void writeLock() throws RetryOperationException {
		// Optimize: loop until success
		if (isThreadSafe())
			return;
		long before = this.seqNumberSplitOrMerge;
		int index = (hashCode() % locks.length);
		ReentrantReadWriteLock lock = locks[index];
		lock.writeLock().lock();
		long after = this.seqNumberSplitOrMerge;
		if (before != after) {
			throw new RetryOperationException();
		}
	}

	/**
	 * Write unlock
	 */
	public void writeUnlock() {
		if (isThreadSafe())
			return;
		int index = (hashCode() % locks.length);
		ReentrantReadWriteLock lock = locks[index];
		lock.writeLock().unlock();
	}

	/**
	 * Put operation
	 * 
	 * @param key
	 * @param keyOffset
	 * @param keyLength
	 * @param value
	 * @param valueOffset
	 * @param valueLength
	 * @param version
	 * @return true, if success, false otherwise (no room, split block)
	 */
	public boolean put(byte[] key, int keyOffset, int keyLength, byte[] value, int valueOffset, int valueLength,
			long version, long expire) throws RetryOperationException {
		try {
			// TODO: key-value size check
			// TODO: optimize locking: we do double locking: index block and data block
			writeLock();

			if (isEmpty()) {
				// Should be OK if k-v size is below block size
				return putEmpty(key, keyOffset, keyLength, value, valueOffset, valueLength, version, expire);
			}
			long ptr = search(key, keyOffset, keyLength, version, Op.PUT);
			// Try to insert, split if necessary (if split is possible)
			// return false if splitting of index block is required
			DataBlock b = block.get();
			b.set(this, ptr - dataPtr);
			boolean res = false;
			while (true) {
				res = b.put(key, keyOffset, keyLength, value, valueOffset, valueLength, version, expire);
				if (res == false) {
					if (b.canSplit() && !b.isLargerThanMax(key, keyOffset, keyLength, version)) {
						// get split position
						long addr = b.splitPos(false);
						// check if we will be able to insert new block
						int startKeyLength = UnsafeAccess.toShort(addr);
						int required = DATA_BLOCK_STATIC_OVERHEAD + KEY_SIZE_LENGTH + startKeyLength;
						if (dataSize + required > blockSize) {
							// index block split is required
							return false;
						}
						// Do not enforce compaction, it was done already during put
						DataBlock right = b.split(false);
						// we do not check result - it must be true
						insertBlock(right);
						// select which block we should put k-v now
						int r1 = right.compareTo(key, keyOffset, keyLength, version, Op.PUT);
						if (r1 >= 0) {
							b = right;
						}
						continue;
					} else {
						int required = DATA_BLOCK_STATIC_OVERHEAD + KEY_SIZE_LENGTH + keyLength;
						if (dataSize + required > blockSize) {
							// index block split is required
							return false;
						}
						// new block insert after
						DataBlock bb = new DataBlock();
						insertNewBlock(bb, key, keyOffset, keyLength, version, Op.PUT);
						// we do not check result - it should be OK (empty block)
						bb.put(key, keyOffset, keyLength, value, valueOffset, valueLength, version, expire);
						return true; // if false, then index block split is required
					}
				} else {
					break;
				}
			}
			// if false, then index block split is required
			return res;
		} finally {
			writeUnlock();
		}
	}

	private boolean insertBlock(DataBlock bb) {
		// required space for new entry
		int len = bb.getFirstKeyLength();
		int required = len + KEY_SIZE_LENGTH + DATA_BLOCK_STATIC_OVERHEAD;
		if (dataSize + required > blockSize) {
			return false;
		}
		long addr = bb.getFirstKeyAddress();
		long version = bb.getFirstKeyVersion();
		Op type = bb.getFirstKeyType();
		long pos = search(addr, len, version, type);
		// Get to the next index record
		int skip = DATA_BLOCK_STATIC_OVERHEAD + KEY_SIZE_LENGTH + keyLength(pos);
		pos += skip;

		UnsafeAccess.copy(pos, pos + required, dataSize - (pos - dataPtr));
		bb.register(this, pos - dataPtr);
		/* DEBUG */ System.out.println("insert block " + bb.getDataPtr());
		this.dataSize += required;
		this.numDataBlocks += 1;
		updateFirstKey(bb);
		return true;
	}

	private boolean insertNewBlock(DataBlock bb, byte[] key, int off, int len, long version, Op type) {

		// required space for new entry
		int required = len + KEY_SIZE_LENGTH + DATA_BLOCK_STATIC_OVERHEAD;
		if (dataSize + required > blockSize) {
			return false;
		}
		long pos = search(key, off, len, version, type);
		// Get to the next index record
		int skip = DATA_BLOCK_STATIC_OVERHEAD + KEY_SIZE_LENGTH + keyLength(pos);
		pos += skip;
		UnsafeAccess.copy(pos, pos + required, dataSize - (pos - dataPtr));
		bb.register(this, pos - dataPtr);
		/* DEBUG */ System.out.println("insert NEW block " + bb.getDataPtr());
		this.dataSize += required;
		this.numDataBlocks += 1;
		// set first key
		updateFirstKey(pos, key, off, len, version, type);
		return true;
	}

	private boolean insertNewBlock(DataBlock bb, long key, int len, long version, Op type) {
		// required space for new entry
		int required = len + KEY_SIZE_LENGTH + DATA_BLOCK_STATIC_OVERHEAD;
		if (dataSize + required > blockSize) {
			return false;
		}
		long pos = search(key, len, version, type);
		// Get to the next index record
		int skip = DATA_BLOCK_STATIC_OVERHEAD + KEY_SIZE_LENGTH + keyLength(pos);
		pos += skip;
		UnsafeAccess.copy(pos, pos + required, dataSize - (pos - dataPtr));
		bb.register(this, pos - dataPtr);
		this.dataSize += required;
		this.numDataBlocks += 1;
		updateFirstKey(pos, key, len, version, type);
		return true;
	}

	public boolean canSplit() {
		return numDataBlocks > 1;
	}

	private boolean putEmpty(byte[] key, int keyOffset, int keyLength, byte[] value, int valueOffset, int valueLength,
			long version, long expire) {
		DataBlock b = new DataBlock();
		b.register(this, 0);
		UnsafeAccess.putShort(dataPtr + DATA_BLOCK_STATIC_PREFIX, (short) keyLength);
		UnsafeAccess.copy(key, keyOffset, dataPtr + DATA_BLOCK_STATIC_PREFIX + KEY_SIZE_LENGTH, keyLength);
		UnsafeAccess.putLong(dataPtr + DATA_BLOCK_STATIC_PREFIX + KEY_SIZE_LENGTH + keyLength, version);
		UnsafeAccess.putByte(dataPtr + DATA_BLOCK_STATIC_PREFIX + KEY_SIZE_LENGTH + keyLength + VERSION_SIZE,
				(byte) Op.PUT.ordinal());
		this.version = version;
		this.type = (byte) Op.PUT.ordinal();
		this.numDataBlocks++;
		this.dataSize += DATA_BLOCK_STATIC_OVERHEAD + KEY_SIZE_LENGTH + keyLength;
		return b.put(key, keyOffset, keyLength, value, valueOffset, valueLength, version, expire);
	}

	private boolean isEmpty() {
		return numDataBlocks == 0;
	}

	/**
	 * Must be called only after Block2.register
	 * 
	 * @param block
	 */
	void updateFirstKey(DataBlock block) {
		// TODO: handle key size change
		// If we deleted first key in a block
		long indexPtr = block.getIndexPtr();
		long key = block.getFirstKeyAddress();
		int keyLength = block.getFirstKeyLength();
		long version = block.getFirstKeyVersion();
		Op type = block.getFirstKeyType();
		UnsafeAccess.putShort(indexPtr + DATA_BLOCK_STATIC_PREFIX, (short) keyLength);
		UnsafeAccess.copy(key, indexPtr + DATA_BLOCK_STATIC_PREFIX + KEY_SIZE_LENGTH, keyLength);
		UnsafeAccess.putLong(indexPtr + DATA_BLOCK_STATIC_PREFIX + KEY_SIZE_LENGTH + keyLength, version);
		UnsafeAccess.putByte(indexPtr + DATA_BLOCK_STATIC_PREFIX + KEY_SIZE_LENGTH + keyLength + VERSION_SIZE,
				(byte) type.ordinal());
	}

	void updateFirstKey(long indexPtr, byte[] key, int keyOffset, int keyLength, long version, Op type) {
		// TODO: handle key size change
		// If we deleted first key in a block
		UnsafeAccess.putShort(indexPtr + DATA_BLOCK_STATIC_PREFIX, (short) keyLength);
		UnsafeAccess.copy(key, keyOffset, indexPtr + DATA_BLOCK_STATIC_PREFIX + KEY_SIZE_LENGTH, keyLength);
		UnsafeAccess.putLong(indexPtr + DATA_BLOCK_STATIC_PREFIX + KEY_SIZE_LENGTH + keyLength, version);
		UnsafeAccess.putByte(indexPtr + DATA_BLOCK_STATIC_PREFIX + KEY_SIZE_LENGTH + keyLength + VERSION_SIZE,
				(byte) type.ordinal());
	}

	void updateFirstKey(long indexPtr, long keyPtr, int keyLength, long version, Op type) {
		// TODO: handle key size change
		// If we deleted first key in a block
		UnsafeAccess.putShort(indexPtr + DATA_BLOCK_STATIC_PREFIX, (short) keyLength);
		UnsafeAccess.copy(keyPtr, indexPtr + DATA_BLOCK_STATIC_PREFIX + KEY_SIZE_LENGTH, keyLength);
		UnsafeAccess.putLong(indexPtr + DATA_BLOCK_STATIC_PREFIX + KEY_SIZE_LENGTH + keyLength, version);
		UnsafeAccess.putByte(indexPtr + DATA_BLOCK_STATIC_PREFIX + KEY_SIZE_LENGTH + keyLength + VERSION_SIZE,
				(byte) type.ordinal());
	}

	/**
	 * Checks if a given key is larger than maximum key in this data block
	 * 
	 * @param key
	 * @param keyOffset
	 * @param keyLength
	 * @param version
	 * @return
	 */
	public boolean isLargerThanMax(byte[] key, int keyOffset, int keyLength, long version) {
		// TODO: Locking is required, make sure we hold read lock
		// try {
		// readLock();
		long address = search(key, keyOffset, keyLength, version, Op.PUT);
		return address == dataPtr + getDataSize();
		// } finally {
		// readUnlock();
		// }
	}

	/**
	 * Checks if a given key is larger than maximum key in this data block
	 * 
	 * @param key
	 * @param keyOffset
	 * @param keyLength
	 * @param version
	 * @return
	 */
	public boolean isLargerThanMax(long keyPtr, int keyLength, long version) {
		// TODO: Locking is required, make sure we hold read lock
		// try {
		// readLock();
		long address = search(keyPtr, keyLength, version, Op.PUT);
		return address == dataPtr + getDataSize();
		// } finally {
		// readUnlock();
		// }
	}

	/**
	 * Put k-v operation
	 * 
	 * @param keyPtr
	 * @param keyLength
	 * @param valuePtr
	 * @param valueLength
	 * @param version
	 * @return true, if success, false otherwise
	 * @throws RetryOperationException
	 */
	public boolean put(long keyPtr, int keyLength, long valuePtr, int valueLength, long version, long expire)
			throws RetryOperationException {
		try {
			// TODO: key-value size check
			// TODO: optimize locking: we do double locking: index block and data block
			writeLock();

			if (isEmpty()) {
				// Should be OK if k-v size is below block size
				return putEmpty(keyPtr, keyLength, valuePtr, valueLength, version, expire);
			}
			long ptr = search(keyPtr, keyLength, version, Op.PUT);
			// Try to insert, split if necessary (if split is possible)
			// return false if splitting of index block is required
			DataBlock b = block.get();
			b.set(this, ptr - dataPtr);
			boolean res = false;
			while (true) {
				res = b.put(keyPtr, keyLength, valuePtr, valueLength, version, expire);
				if (res == false) {
					if (b.canSplit() && !b.isLargerThanMax(keyPtr, keyLength, version)) {
						// get split position
						long addr = b.splitPos(false);
						// check if we will be able to insert new block
						int startKeyLength = UnsafeAccess.toShort(addr);
						int required = DATA_BLOCK_STATIC_OVERHEAD + KEY_SIZE_LENGTH + startKeyLength;
						if (dataSize + required > blockSize) {
							// index block split is required
							return false;
						}
						DataBlock right = b.split(false);
						// we do not check result - it must be true
						insertBlock(right);
						// select which block we should put k-v now
						int r1 = right.compareTo(keyPtr, keyLength, version, Op.PUT);
						if (r1 >= 0) {
							b = right;
						}
						continue;
					} else {
						int required = DATA_BLOCK_STATIC_OVERHEAD + KEY_SIZE_LENGTH + keyLength;
						if (dataSize + required > blockSize) {
							// index block split is required
							return false;
						}
						// new block insert after
						DataBlock bb = new DataBlock();
						insertNewBlock(bb, keyPtr, keyLength, version, Op.PUT);
						// we do not check result - it should be OK (empty block)
						bb.put(keyPtr, keyLength, valuePtr, valueLength, version, expire);
						return true; // if false, then index block split is required
					}
				} else {
					break;
				}
			}
			// if false, then index block split is required
			return res;
		} finally {
			writeUnlock();
		}
	}

	private boolean putEmpty(long keyPtr, int keyLength, long valuePtr, int valueLength, long version, long expire) {
		DataBlock b = new DataBlock();
		b.register(this, 0);
		UnsafeAccess.putShort(dataPtr + DATA_BLOCK_STATIC_PREFIX, (short) keyLength);
		UnsafeAccess.putLong(dataPtr + DATA_BLOCK_STATIC_PREFIX + KEY_SIZE_LENGTH + keyLength, version);
		UnsafeAccess.putByte(dataPtr + DATA_BLOCK_STATIC_PREFIX + KEY_SIZE_LENGTH + keyLength + VERSION_SIZE,
				(byte) Op.PUT.ordinal());
		this.version = version;
		this.type = (byte) Op.PUT.ordinal();
		UnsafeAccess.copy(keyPtr, dataPtr + DATA_BLOCK_STATIC_PREFIX + KEY_SIZE_LENGTH, keyLength);
		numDataBlocks++;
		dataSize += DATA_BLOCK_STATIC_OVERHEAD + KEY_SIZE_LENGTH + keyLength;
		return b.put(keyPtr, keyLength, valuePtr, valueLength, version, expire);
	}

	/**
	 * TODO: handle not found (new block) Search position of a block, which can
	 * contain this key
	 * 
	 * @param key
	 * @param keyOffset
	 * @param keyLength
	 * @return address to insert (or update)
	 */
	long search(byte[] key, int keyOffset, int keyLength, long version, Op type) {
		long ptr = dataPtr;
		long prevPtr = NOT_FOUND;
		int count = 0;

		try {
			while (count++ < numDataBlocks) {
				short keylen = (short) keyLength(ptr);
				int res = Utils.compareTo(key, keyOffset, keyLength, keyAddress(ptr), keylen);
				if (res < 0) {
					if (prevPtr == NOT_FOUND) {
						// FATAL error
						return ptr = NOT_FOUND;
					} else {
						return prevPtr;
					}
				} else if (res == 0) {
					// compare versions
					long ver = version(ptr);
					if (ver < version) {
						if (prevPtr == NOT_FOUND) {
							// FATAL error
							return ptr = NOT_FOUND;
						}
						return prevPtr;
					} else if (ver == version) {
						byte _type = type(ptr);
						if (_type > type.ordinal()) {
							if (prevPtr == NOT_FOUND) {
								// FATAL error
								return ptr = NOT_FOUND;
							}
							return prevPtr;
						}
					}
				}
				prevPtr = ptr;
				ptr += keylen + KEY_SIZE_LENGTH + DATA_BLOCK_STATIC_OVERHEAD;
			}
			// last data block
			return prevPtr;
		} finally {
			checkPointer(ptr);
		}
	}

	/**
	 * TODO: handle not found (new block) Search position of a block, which can
	 * contain this key
	 * 
	 * @param key
	 * @param keyOffset
	 * @param keyLength
	 * @return address to insert (or update)
	 */
  long searchForGet(byte[] key, int keyOffset, int keyLength, long start) {
    long ptr = start;
    short keylen = (short) keyLength(ptr);
    ptr += keylen + KEY_SIZE_LENGTH + DATA_BLOCK_STATIC_OVERHEAD;
    if (ptr >= this.dataPtr + this.dataSize) {
      return NOT_FOUND;
    }
    int res = Utils.compareTo(key, keyOffset, keyLength, keyAddress(ptr), keyLength(ptr));
    if (res != 0) {
      return ptr = NOT_FOUND;
    } else {
      return ptr;
    }

  }

	/**
	 * Public API, therefore we lock/unlock
	 * 
	 * @param key
	 * @param keyOffset
	 * @param keyLength
	 * @return block found or null
	 */
	DataBlock searchBlock(byte[] key, int keyOffset, int keyLength, long version, Op type) {

		try {
			readLock();
			long ptr = search(key, keyOffset, keyLength, version, type);
			if (ptr == NOT_FOUND) {
				return null;
			} else {
				DataBlock b = block.get();
				b.set(this, ptr - dataPtr);
				return b;
			}
		} finally {
			readUnlock();
		}
	}

	/**
	 * Public API,
	 * 
	 * @param key
	 * @param keyOffset
	 * @param keyLength
	 * @return block found or null
	 */
	DataBlock searchBlock(List<DataBlock> list, byte[] key, int keyOffset, int keyLength, long version, Op type) {
		DataBlock prev = null;
		for (int i = 0; i < numDataBlocks; i++) {
			DataBlock b = list.get(i);
			if (b.compareTo(key, keyOffset, keyLength, version, type) < 0) {
				return prev;
			}
		}
		return prev;
	}

	/**
	 * Return first block if present
	 * 
	 * @return block
	 */
	DataBlock firstBlock() {
		if (isEmpty())
			return null;
		try {
			readLock();
			DataBlock b = block.get();
			b.set(this, 0);
			return b;
		} finally {
			readUnlock();
		}
	}

	/**
	 * TODO: Implement first block Public API, therefore we lock/unlock
	 * 
	 * @param blck current blck, can be null
	 * @return block found or null
	 */
	DataBlock nextBlock(DataBlock blck) {

		try {
			readLock();
			long ptr = blck != null ? blck.getIndexPtr() : getAddress();
			int keyLength = blck.getFirstKeyLength();
			if (ptr + DATA_BLOCK_STATIC_OVERHEAD + KEY_SIZE_LENGTH + keyLength >= dataPtr + dataSize) {
				// last block
				return null;
			}
			DataBlock b = block.get();
			// b.reset();
			b.set(this, ptr + DATA_BLOCK_STATIC_OVERHEAD + KEY_SIZE_LENGTH + keyLength - dataPtr);
			return b;
		} finally {
			readUnlock();
		}
	}

	/**
	 * Search position of a first key which is greater or equals to a given key
	 * 
	 * @param keyPtr
	 * @param keyLength
	 * @return address to insert (or update)
	 */
	long search(long keyPtr, int keyLength, long version, Op type) {
		long ptr = dataPtr;
		long prevPtr = NOT_FOUND;
		int count = 0;

		try {
			while (count++ < numDataBlocks) {
				short keylen = (short) keyLength(ptr);
				int res = Utils.compareTo(keyPtr, keyLength, keyAddress(ptr), keylen);
				if (res < 0) {
					if (prevPtr == NOT_FOUND) {
						// FATAL error
						return ptr = NOT_FOUND;
					}
					return prevPtr;
				} else if (res == 0) {
					// compare versions
					long ver = version(ptr);
					if (ver < version) {
						if (prevPtr == NOT_FOUND) {
							// FATAL error
							return ptr = NOT_FOUND;
						}
						return prevPtr;
					} else if (ver == version) {
						byte _type = type(ptr);
						if (_type > type.ordinal()) {
							if (prevPtr == NOT_FOUND) {
								// FATAL error
								return ptr = NOT_FOUND;
							}
							return prevPtr;
						}
					}
				}
				prevPtr = ptr;
				ptr += keylen + KEY_SIZE_LENGTH + DATA_BLOCK_STATIC_OVERHEAD;
			}
			// last data block
			return prevPtr;
		} finally {
			checkPointer(ptr);
		}

	}

	/**
	 * TODO: handle not found (new block) Search position of a block, which can
	 * contain this key
	 * 
	 * @param key
	 * @param keyOffset
	 * @param keyLength
	 * @return address to insert (or update)
	 */
	long searchForGet(long keyPtr, int keyLength, long start) {
		long ptr = start;

		try {
			short keylen = (short) keyLength(ptr);
			ptr += keylen + KEY_SIZE_LENGTH + DATA_BLOCK_STATIC_OVERHEAD;
			if (ptr >= this.dataPtr + this.dataSize) {
				return NOT_FOUND;
			}
			int res = Utils.compareTo(keyPtr, keyLength, keyAddress(ptr), keyLength(ptr));
			if (res != 0) {
				return ptr = NOT_FOUND;
			} else {
				return ptr;
			}
		} finally {
			// checkPointer(ptr);
		}
	}

	/**
	 * Search block by a given key equals to a given key
	 * 
	 * @param keyPtr
	 * @param keyLength
	 * @return address to insert (or update)
	 */
	DataBlock searchBlock(long keyPtr, int keyLength, long version, Op type) {
		try {
			long ptr = search(keyPtr, keyLength, version, type);
			if (ptr == NOT_FOUND) {
				return null;
			} else {
				DataBlock b = block.get();
				b.set(this, ptr - dataPtr);
				return b;
			}
		} finally {
			readUnlock();
		}
	}

	/**
	 * Delete operation. FOR NOW deleting first key in a data block does not affect
	 * its key in an index block TODO: Is it safe?
	 * 
	 * @param key
	 * @param keyOffset
	 * @param keyLength
	 * @param version
	 * @return true, if success, false otherwise
	 * @throws RetryOperationException
	 */

	public OpResult delete(byte[] key, int keyOffset, int keyLength, long version) 
	    throws RetryOperationException {

		try {
			writeLock();
			long ptr = search(key, keyOffset, keyLength, version, Op.DELETE);
			DataBlock b = block.get();
			b.set(this, ptr - dataPtr);
			return deleteInBlock(b, ptr, key, keyOffset, keyLength, version);
		} finally {
			writeUnlock();
		}
	}

//  public OpResult delete(byte[] key, int keyOffset, int keyLength, long version) 
//      throws RetryOperationException {
//
//    try {
//      writeLock();
//      long ptr = search(key, keyOffset, keyLength, version, Op.DELETE);
//      DataBlock b = block.get();
//      b.set(this, ptr - dataPtr);
//      while (true) {
//        OpResult res = b.delete(key, keyOffset, keyLength, version);
//        if (res == OpResult.SPLIT_REQUIRED) {
//          if (b.canSplit()) {
//            // get split position
//            long addr = b.splitPos(false);
//            // check if we will be able to insert new block
//            int startKeyLength = UnsafeAccess.toShort(addr);
//            int required = DATA_BLOCK_STATIC_OVERHEAD + KEY_SIZE_LENGTH + startKeyLength;
//            if (dataSize + required > blockSize) {
//              // index block split is required
//              return OpResult.SPLIT_REQUIRED;
//            }
//            DataBlock right = b.split(false);
//            // we do not check result - it must be true
//            insertBlock(right);
//            // select which block we should delete k-v now
//            int r1 = right.compareTo(key, keyOffset, keyLength, version, Op.DELETE);
//            if (r1 >= 0) {
//              b = right;
//            }
//            continue;
//          } else {
//            // We can not split block and can delete directly
//            // we need to *add* delete tombstone to a new block
//            int required = DATA_BLOCK_STATIC_OVERHEAD + KEY_SIZE_LENGTH + keyLength;
//            if (dataSize + required > blockSize) {
//              // index block split is required
//              return OpResult.SPLIT_REQUIRED;
//            }
//            // new block insert after
//            DataBlock bb = new DataBlock();
//            insertNewBlock(bb, key, keyOffset, keyLength, version, Op.DELETE);
//            // we do not check result - it should be OK (empty block)
//            bb.addDelete(key, keyOffset, keyLength, version);
//            return OpResult.OK; // if false, then index block split is required
//          }
//        } else if (res == OpResult.NOT_FOUND) {
//          long address = ptr;
//          while ((address = searchForGet(key, keyOffset, keyLength, address)) != NOT_FOUND) {
//            b.set(this, address - this.dataPtr);
//            res = b.delete(key, keyOffset, keyLength, version);
//            // TODO: SPLIT_REQUIRED
//            if (res != OpResult.NOT_FOUND) {
//              return res;
//            }
//          }
//          return OpResult.NOT_FOUND;
//        } else {
//          return res;
//        }
//      }
//    } finally {
//      writeUnlock();
//    }
//  }	
	
	private OpResult deleteInBlock(DataBlock b, long address, byte[] key, int keyOffset, 
	    int keyLength, long version) {
    while (true) {
      OpResult res = b.delete(key, keyOffset, keyLength, version);
      if (res == OpResult.SPLIT_REQUIRED) {
        if (b.canSplit()) {
          // get split position
          long addr = b.splitPos(false);
          // check if we will be able to insert new block
          int startKeyLength = UnsafeAccess.toShort(addr);
          int required = DATA_BLOCK_STATIC_OVERHEAD + KEY_SIZE_LENGTH + startKeyLength;
          if (dataSize + required > blockSize) {
            // index block split is required
            return OpResult.SPLIT_REQUIRED;
          }
          DataBlock right = b.split(false);
          // we do not check result - it must be true
          insertBlock(right);
          // select which block we should delete k-v now
          int r1 = right.compareTo(key, keyOffset, keyLength, version, Op.DELETE);
          if (r1 >= 0) {
            b = right;
          }
          continue;
        } else {
          // We can not split block and can delete directly
          // we need to *add* delete tombstone to a new block
          int required = DATA_BLOCK_STATIC_OVERHEAD + KEY_SIZE_LENGTH + keyLength;
          if (dataSize + required > blockSize) {
            // index block split is required
            return OpResult.SPLIT_REQUIRED;
          }
          // new block insert after
          DataBlock bb = new DataBlock();
          insertNewBlock(bb, key, keyOffset, keyLength, version, Op.DELETE);
          // we do not check result - it should be OK (empty block)
          bb.addDelete(key, keyOffset, keyLength, version);
          return OpResult.OK; // if false, then index block split is required
        }
      } else if (res == OpResult.NOT_FOUND) {
        // This method checks next data block and
        // if it exists and has the same start key as a given key
        // it returns its address
        address = searchForGet(key, keyOffset, keyLength, address);
        if (address == NOT_FOUND) {
          return OpResult.NOT_FOUND; 
        } else {
          b.set(this, address - this.dataPtr);
          res = b.delete(key, keyOffset, keyLength, version);
          if (res == OpResult.OK) {
            return res;
          } else {
            // Call recursively if SPLIT_REQUIRED or NOT_FOUND (will check next block)
            return deleteInBlock(b, address, key, keyOffset, keyLength, version);
          } 
        }
      } else {
        return res;
      }
    }
	}
	
	private final void checkPointer(long ptr) {
		if (ptr == NOT_FOUND) {
			throw new RuntimeException("FATAL: ptr = " + NOT_FOUND);
		}
		if (ptr == 0) {
			throw new RuntimeException("Memory allocation failed");
		}
	}

	/**
	 * Delete operation. FOR NOW deleting first key in a data block does not affect
	 * its key in an index block TODO: Is it safe?
	 * 
	 * @param keyPtr
	 * @param keyLength
	 * @param version
	 * @return true if success, false otherwise
	 * @throws RetryOperationException
	 */
	public OpResult delete(long keyPtr, int keyLength, long version) throws RetryOperationException {
		try {
			writeLock();
			long address = search(keyPtr, keyLength, version, Op.DELETE);
			DataBlock b = block.get();
			b.set(this, address - dataPtr);
			return deleteInBlock(b, address, keyPtr, keyLength, version);
		} finally {
			writeUnlock();
		}
	}

//  public OpResult delete(long keyPtr, int keyLength, long version) throws RetryOperationException {
//    try {
//      writeLock();
//      long ptr = search(keyPtr, keyLength, version, Op.DELETE);
//      DataBlock b = block.get();
//      b.set(this, ptr - dataPtr);
//      while (true) {
//        OpResult res = b.delete(keyPtr, keyLength, version);
//        if (res == OpResult.SPLIT_REQUIRED) {
//          if (b.canSplit()) {
//            // get split position
//            long addr = b.splitPos(false);
//            // check if we will be able to insert new block
//            int startKeyLength = UnsafeAccess.toShort(addr);
//            int required = DATA_BLOCK_STATIC_OVERHEAD + KEY_SIZE_LENGTH + startKeyLength;
//            if (dataSize + required > blockSize) {
//              // index block split is required
//              return OpResult.SPLIT_REQUIRED;
//            }
//            DataBlock right = b.split(false);
//            // we do not check result - it must be true
//            insertBlock(right);
//            // select which block we should put k-v now
//            int r1 = right.compareTo(keyPtr, keyLength, version, Op.DELETE);
//            if (r1 >= 0) {
//              b = right;
//            }
//            continue;
//          } else {
//            // We can not split block and can not delete directly
//            // we need to *add* delete tomb stone to a new block
//            int required = DATA_BLOCK_STATIC_OVERHEAD + KEY_SIZE_LENGTH + keyLength;
//            if (dataSize + required > blockSize) {
//              // index block split is required
//              return OpResult.SPLIT_REQUIRED;
//            }
//            // new block insert after
//            DataBlock bb = new DataBlock();
//            insertNewBlock(bb, keyPtr, keyLength, version, Op.DELETE);
//            // we do not check result - it should be OK (empty block)
//            bb.addDelete(keyPtr, keyLength, version);
//            return OpResult.OK; // if false, then index block split is required
//          }
//        } else if (res == OpResult.NOT_FOUND) {
//          long address = ptr;
//          while ((address = searchForGet(keyPtr, keyLength, address)) != NOT_FOUND) {
//            b.set(this, address - this.dataPtr);
//            res = b.delete(keyPtr, keyLength, version);
//            // TODO: SPLIT_REQUIRED
//            if (res != OpResult.NOT_FOUND) {
//              return res;
//            }
//          }
//          return OpResult.NOT_FOUND;
//        } else {
//          return res;
//        }
//      }
//    } finally {
//      writeUnlock();
//    }
//  }
	
	private OpResult deleteInBlock(DataBlock b, long address, long keyPtr, int keyLength, long version) {
    while (true) {
      OpResult res = b.delete(keyPtr, keyLength, version);
      if (res == OpResult.SPLIT_REQUIRED) {
        if (b.canSplit()) {
          // get split position
          long addr = b.splitPos(false);
          // check if we will be able to insert new block
          int startKeyLength = UnsafeAccess.toShort(addr);
          int required = DATA_BLOCK_STATIC_OVERHEAD + KEY_SIZE_LENGTH + startKeyLength;
          if (dataSize + required > blockSize) {
            // index block split is required
            return OpResult.SPLIT_REQUIRED;
          }
          DataBlock right = b.split(false);
          // we do not check result - it must be true
          insertBlock(right);
          // select which block we should put k-v now
          int r1 = right.compareTo(keyPtr, keyLength, version, Op.DELETE);
          if (r1 >= 0) {
            b = right;
          }
          continue;
        } else {
          // We can not split block and can not delete directly
          // we need to *add* delete tomb stone to a new block
          int required = DATA_BLOCK_STATIC_OVERHEAD + KEY_SIZE_LENGTH + keyLength;
          if (dataSize + required > blockSize) {
            // index block split is required
            return OpResult.SPLIT_REQUIRED;
          }
          // new block insert after
          DataBlock bb = new DataBlock();
          insertNewBlock(bb, keyPtr, keyLength, version, Op.DELETE);
          // we do not check result - it should be OK (empty block)
          bb.addDelete(keyPtr, keyLength, version);
          return OpResult.OK; // if false, then index block split is required
        }
      } else if (res == OpResult.NOT_FOUND) {
        // This method checks next data block and
        // if it exists and has the same start key as a given key
        // it returns its address
        address = searchForGet(keyPtr, keyLength, address);
        if (address ==NOT_FOUND) {
          return OpResult.NOT_FOUND;
        } else {
          b.set(this, address - this.dataPtr);
          res = b.delete(keyPtr, keyLength, version);
          if (res == OpResult.OK) {
            return res;
          } else {
            // Call recursively if NOT_FOUND or SPLIT_REQUIRED
            return deleteInBlock(b, address, keyPtr, keyLength, version);
          }
        }
      } else {
        return res;
      }
    }
	}
	/**
	 * Get key-value offset in a block
	 * 
	 * @param key
	 * @param keyOffset
	 * @param keyLength
	 * @return record offset or -1 if not found
	 * @throws RetryOperationException
	 */
	public long get(byte[] key, int keyOffset, int keyLength, long version) throws RetryOperationException {
		try {
			readLock();
			long ptr = search(key, keyOffset, keyLength, version, Op.DELETE);// TODO: is it right?
			DataBlock b = block.get();
			b.set(this, ptr - dataPtr);
			long res = b.get(key, keyOffset, keyLength, version);
			if (res != DataBlock.NOT_FOUND) {
				return res;
			} else {
				long address = ptr;
				while ((address = searchForGet(key, keyOffset, keyLength, address)) != NOT_FOUND) {
					b.set(this, address - this.dataPtr);
					res = b.get(key, keyOffset, keyLength, version);
					if (res != DataBlock.NOT_FOUND) {
						return res;
					}
				}
				return NOT_FOUND;
			}
		} finally {
			readUnlock();
		}
	}

	/**
	 * Get value by key in a block
	 * 
	 * @param key
	 * @param keyOffset
	 * @param keyLength
	 * @return value length or NOT_FOUND if not found
	 * @throws RetryOperationException
	 */
	public long get(byte[] key, int keyOffset, int keyLength, byte[] valueBuf, int valOffset, long version)
			throws RetryOperationException {
		// TODO: verify that block is still valid
		try {
			readLock();
			long ptr = search(key, keyOffset, keyLength, version, Op.DELETE);
			DataBlock b = block.get();
			b.set(this, ptr - dataPtr);
			long res = b.get(key, keyOffset, keyLength, valueBuf, valOffset, version);
			if (res != DataBlock.NOT_FOUND) {
				return res;
			} else {
				long address = ptr;
				while ((address = searchForGet(key, keyOffset, keyLength, address)) != NOT_FOUND) {
					b.set(this, address - this.dataPtr);
					res = b.get(key, keyOffset, keyLength, valueBuf, valOffset, version);
					if (res != DataBlock.NOT_FOUND) {
						return res;
					}
				}
				return NOT_FOUND;
			}
		} finally {
			readUnlock();
		}

	}

	/**
	 * Get key-value offset in a block
	 * 
	 * @param key
	 * @param keyOffset
	 * @param keyLength
	 * @return record offset or NOT_FOUND if not found
	 * @throws RetryOperationException
	 */
	public long get(long keyPtr, int keyLength, long version) throws RetryOperationException {
		try {
			readLock();
			long ptr = search(keyPtr, keyLength, version, Op.DELETE);
			DataBlock b = block.get();
			b.set(this, ptr - dataPtr);
			long res = b.get(keyPtr, keyLength, version);
			if (res != DataBlock.NOT_FOUND) {
				return res;
			} else {
				long address = ptr;
				while ((address = searchForGet(keyPtr, keyLength, address)) != NOT_FOUND) {
					b.set(this, address - this.dataPtr);
					res = b.get(keyPtr, keyLength, version);
					if (res != DataBlock.NOT_FOUND) {
						return res;
					}
				}
				return NOT_FOUND;
			}
		} finally {
			readUnlock();
		}
	}

	/**
	 * Get key-value offset in a block
	 * 
	 * @param key
	 * @param keyOffset
	 * @param keyLength
	 * @param valueBufLength value buffer length
	 * @return value length if found, or NOT_FOUND. if value length > valueBufLength
	 *         no copy will be made - one must repeat call with new value buffer
	 * @throws RetryOperationException
	 */
	public long get(long keyPtr, int keyLength, long valueBuf, int valueBufLength, long version)
			throws RetryOperationException {
		try {
			readLock();
			long ptr = search(keyPtr, keyLength, version, Op.DELETE);
			DataBlock b = block.get();
			b.set(this, ptr - dataPtr);
			long res = b.get(keyPtr, keyLength, valueBuf, valueBufLength, version);
			if (res != DataBlock.NOT_FOUND) {
				return res;
			} else {
				long address = ptr;
				while ((address = searchForGet(keyPtr, keyLength, address)) != NOT_FOUND) {
					b.set(this, address - this.dataPtr);
					res = b.get(keyPtr, keyLength, valueBuf, valueBufLength, version);
					if (res != DataBlock.NOT_FOUND) {
						return res;
					}
				}
				return NOT_FOUND;
			}
		} finally {
			readUnlock();
		}

	}

	/**
	 * Get block size
	 * 
	 * @return block size
	 */
	public int getBlockSize() {
		return blockSize;
	}

	/**
	 * Get data size
	 * 
	 * @return data size
	 */
	public int getDataSize() {
		return dataSize;
	}

	/**
	 * Get number of records in a block
	 * 
	 * @return number of records
	 */
	public int getNumberOfDataBlock() {
		return numDataBlocks;
	}

	public long getSeqNumber() {
		return seqNumberSplitOrMerge;
	}

	/**
	 * Increment
	 * 
	 * @param delta
	 */
	public final void incrSeqNumberSplitOrMerge() {
		byte v = seqNumberSplitOrMerge;
		if (v == Byte.MAX_VALUE) {
			v = 0;
		} else {
			v += 1;
		}
		seqNumberSplitOrMerge = v;
	}

	/**
	 * Must always compact before splitting block
	 * 
	 * @return new (left) block
	 * @throws RetryOperationException
	 */
	public IndexBlock split() throws RetryOperationException {

		try {
			writeLock();
			incrSeqNumberSplitOrMerge();
			long ptr = dataPtr;
			int recCount = 0;

			// Now we should have zero deleted records
			while (recCount < this.numDataBlocks) {
				short keylen = (short) keyLength(ptr);
				ptr += keylen + KEY_SIZE_LENGTH + DATA_BLOCK_STATIC_OVERHEAD;
				recCount++;
			}
			int oldDataSize = this.dataSize;
			int oldNumRecords = this.numDataBlocks;
			this.dataSize = (short) (ptr - dataPtr);
			this.numDataBlocks = (short) recCount;
			int leftDataSize = oldDataSize - this.dataSize;
			int leftBlockSize = getMinSizeGreaterThan(getBlockSize(), leftDataSize);
			IndexBlock right = new IndexBlock(leftBlockSize);

			right.numDataBlocks = (short) (oldNumRecords - this.numDataBlocks);
			right.dataSize = (short) (oldDataSize - this.dataSize);
			UnsafeAccess.copy(ptr, right.dataPtr, right.dataSize);
			return right;
		} finally {
			writeUnlock();
		}
	}

	/**
	 * Should merge this block
	 * 
	 * @return true, false
	 */
	public boolean shouldMerge() {
		return (double) dataSize / blockSize < MAX_MERGE_RATIO;
	}

	/**
	 * Merge two adjacent blocks
	 * 
	 * @param left
	 * @param forceCompact
	 * @param forceMerge
	 * @return true, if merge successful, false - otherwise
	 * @throws RetryOperationException
	 */
	public boolean merge(IndexBlock left, boolean forceCompact) throws RetryOperationException {

		try {
			writeLock();
			left.writeLock();
			incrSeqNumberSplitOrMerge();
			left.incrSeqNumberSplitOrMerge();
			if ((!shouldMerge() || !left.shouldMerge())) {
				return false;
			}

			UnsafeAccess.copy(left.dataPtr, this.dataPtr + dataSize, left.dataSize);
			this.numDataBlocks += left.numDataBlocks;
			this.dataSize += left.dataSize;

			// After merge left block becomes invalid
			// TODO
			return true;
		} finally {
			left.writeUnlock();
			writeUnlock();
		}
	}

	/**
	 * Get block address
	 * 
	 * @return address
	 */
	public long getAddress() {
		return dataPtr;
	}

	/**
	 * Free memory
	 */
	public void free() {
		// TODO: deallocate data blocks
		UnsafeAccess.free(dataPtr);
		valid = false;
	}

	@Override
	public int compareTo(IndexBlock o) {
		if (this == o)
			return 0;

		byte[] firstKey = getFirstKey();
		byte[] firstKey1 = o.getFirstKey();
		int res = Utils.compareTo(firstKey, 0, firstKey.length, firstKey1, 0, firstKey1.length);
		if (res == 0) {
			long ver = o.version;
			if (version == ver) {
				return type - o.type;
			} else if (ver < version) {
				return -1;
			} else {
				return 1;
			}
		} else {
			return res;
		}
	}

	public byte[] getFirstKey() {
		if (firstKey != null) {
			return firstKey;
		}
		try {
			readLock();
			short keylen = (short) keyLength(dataPtr);
			byte[] buf = new byte[keylen];
			UnsafeAccess.copy(keyAddress(dataPtr), buf, 0, buf.length);
			firstKey = buf;
			return firstKey;
		} finally {
			readUnlock();
		}
	}
}
