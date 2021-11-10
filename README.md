# Carrot - Redis on steroids

## Introduction

Carrot is the new in-memory data store, which provides full Redis compatibility (up to the current 6.2.5 version) and can be used with any existing Redis clients. It will follow open source Redis as close as possible to facilitate easy migration for existing Redis users, but surely, Carrtot has its own flavors, which differ it from the original. 

### In memory data compression. 

First of all, Carrot supports on-the-fly compression and decompression data (currently, LZ4 is supported with ZSTD following shortly). The compression works on blocks of Key-Value pairs - not on single Key-Value. This results in a much supperior performance (try to compress 100 byte value and 4K block of key values, ordered by key and compare the result).

### Custom B-Tree engine

The Carrot's data store engine is the custom B-tree (not to be confused with binary trees) with a very high fan-out. The engine is mostly thread-safe (as of today) and several threads can access data in paralel. Mostly - means that there are still issues which we have being working out, therefore for first releases, we have decided to use 1 data store per processing thread (not a real multi-threading per se, but does not require running separate processes, everything is inside a single Carrot server process).

### Very low memory overhead for data types

Besides supporting compression of in memory data, Carrot utilizes many techniques to reduce memory usage when compression is off. Even with compression disabled, Carrot at least 2-3 times more memory efficient in our large benchmark suite. Again, read WiKi **(TODO: reference)**. 

### Fast fork-less data snapshots

Carrot implements totally fork-less in memory data snapshots. Therefore, there is no need for 50% memory overprovision as for Redis. Snapshots are done by a separate thread which reads data in parallel with a major processing thread (remember we support multi-threaded access to a data store). The data can be restored by loading snapshot from a disk and replaying idempotent mutation operations one-by-one from a Write-Ahead-Log file (not in a first release yet). Why Redis requires the same amount of memory, which data occupies, during a data store snapshot? There is a lot of expalnations on the Internet, but the short answer is: Redis does process fork, and two processes share the same memory during the snapshot operation. If data changes in a parent process, copy-on-write is performed to save old version of a memory page. If during a snapshot, ALL memory pages are being modified (for example, server experiences heavy write load), all parent memory occupied by data must be page-copied to a free space - this means that data set can not occupy more than 50% of available physical RAM.   
Carrot is **much** faster when saving and loading data to/from disk during snapshots and on start-up. The smaller key-values are, the larger performance difference is. Example: Data store with 10M of 250 bytes key-values. It takes 23 sec in Redis 6.2.5 to save data set to a very fast SSD, approximately the same time - to load data on start-up. Carrot numbers: 1.2, 0.5, 2.5, 2.2. Snapshot (compression disabled) - 1.2 sec. Snapshot (compression LZ4) - 0.5 sec. Data loading 2.5 and 2.2 respectively. So, this is up to 45x times faster. This performance opens a direct path to TB-size Carrot in memory stores.

### Support for a very large data sets (1TB+)

Carrot supports multiple data nodes inside a single process, each data node can have separate location for data on disk, snapshot taking and loading can be done in parallel by all data nodes at a full disk(s) speed. For example, with 8 data nodes per Carrot process, each having its dedicated fast SSD for data, the overall throughput can exceed 20GB/S. So, for 1TB data set in memory it will take only 50 sec to save and load data.  What about Redis? No, Redis people recommend 25GB per server instance (**LINK**), so you will need 40 nodes cluster to keep 1TB of data. Ooops, We forgot that 1TB RAM in Carrot can translate to 4-5TB in Redis, so multiply that by 4-5. 160-200 nodes. It is a nightmare for sysops. 

### Support for new data types

#### Sparsed bitsets
First release introduces new data type: **sparsed bitset**. The sparse bitset has the same API as a regular Redis bitset. The difference is in memory usage:
* Sparse bitset does not allocate memory continuosly, For example:  ```SETBIT key 1000000000 1``` in Redis will allocate at least 125MB of RAM first to keep all the bitset up to the last bit set. The command for Carrot: ```SSETBIT 1000000000 1``` will allocate only 4Kb of data and will compress it after that. 
* Sparse bitset are very memory efficient when population counts (%% of set bits) is < 10% or > 90%. In this case it provides very decent compression. Sparse bitsets length is limited only by available physical RAM (bit index is a signed 64-bit integer). Current compression codec is LZ4, this is the general compression codec, not specifically designed to compress bitsets. Future releases will introduce bitset-optimized compression using novell compression scheme - **bitcom4**. *Bitcom4* borrows some ideas from a very popular bitset compression algorithm - *Roaring bitmap*, namely - 16-bit compression scheme for 64K bit blocks, but introduces two new ones: 8-bit and 4-bit block compression for 256 and 16 bit block sizes respectively. There are 4 codecs in *bitcom4* compression algorithm: 16-bit codec for large sparse blocks (with bit density below 0.001), 8 - bit codec - for blocks with bit density between 0.001 and 0.02 (approximately), 4 - bit codec for blocks with bit density between 0.02 and 0.2 and RAW (no compression codec) for bit densities above 0.2. Combination of four codecs allows better compression, especially when bit density is above 0.02. For example, *Roaring bitmap* can not compress bitsets with bit densities above 0.06. *bitcom4* compresses random bitmaps with 0.06 density up 2.5x times.       

#### B-tree data type

Release 0.3 will introduce B-Tree data type, which will allow range queries with filters and aggregations. Stay tuned   

### New features in existing data types

* Sets elements are ordered lexicographically. In upcoming releases Sets API will be extended by allowing some types of a range queries on set elements.
* Hashes are ordered lexicographically by field names as well, therefore, **ZLEXCOUNT key min max** will always work correctly and does not require for all set's members to have the same score (as in Redis), The same is true for the following commands as well: **ZRANGEBYLEX**, **ZREVRANGEBYLEX** and **ZREMRANGEBYLEX**. 

## What is Carrot not for

If you use Redis as a cache mostly and average size of a cache item is larger than 2KB, you will not probably need it (except for faster snapshots, which Carrot provides). Stay with Redis. It is safe, fast and robust. Carrot is not a cache, although it can be used as a cache. Carrot was designed and implemented to support sophisticated applications, which need much more than simple SET/GET commands. 

## Why Java

Short answer is - this is the primary programming language of the Carrot's developer. Java is versatile, widely used and adopted programming language. It has its own pluses and minuses, of course. Big pluses: dynamic nature (one can easily deploy new code piece or add new functionality to the Carrot server just by adding java jar file to a system's class path), another plus - it is much easier to find decent Java programmer than decent C/C++, if someone decides to extend functionality of the Carrot server. Big minuses: automatic GC, poor query latency distributions (due to background GC activity) and not so sharp performance (in comparison to C). So, how have we addressed these concerns? 

* All the data is kept outside of a Java heap and is not controlled by automatic GC. So, we have implemented our own memory management (yeah, manual malloc and free :). The system requires approximatley 1MB of Java heap for every 1GB of data. So, to keep in memory 1TB of data one need only 1GB of a Java heap. This is very manageable and does not look scary at all ;).
* The server is very conservative in producing temporary Java objects, most of the code paths are almost free of any object allocations. So, this helps a lot when someone want predictable performance and query latencies. 
* Performance and memory efficiency have been top two priorities since the beginning of the project.
* As a result, memory efficiency of a Carrot is much better than Redis. Especially, for a small key-values, because of a very low Carrot engine overhead. For example, to store 1M  of 8 bytes set members, Carrot needs less than 10MB of RAM (less than 2 bytes overhead) and Redis needs close to 70MB.
* Surprisngly, performance is good as well and pretty close to Redis in most of the tests. See WiKi fo test results **(TODO)**.  

## What is covered in the first release

Most of data types commands (total number is 105 for the release 0.1) of Redis 6.2.x have been implemented. The list of supported commands is **HERE**. The following data types are suported (with some commands still missing): **sets, ordered sets, hashes, lists, strings, bitmaps**. No administration, cluster, ACL yet. These will follow in the next releases. Persistence is supported, as well as a **SAVE** and **BGSAVE** commands.

## How to build

### Build prerequisits

Java 11+, Gnu cc compiler (any recent version will work), maven 3.x.

### Carrot source build instructions:

* Copy `settings.xml.template` to `settings.xml` and edit parameters to match your folders, make sure you specify correctly all C-include locations necessary to build native Java-C binding.

* Use `mvn --settings settings.xml`, it will point to settings.xml to set important properties (native code in lz4 depends on it)
- run the following command to build Carrot: 
```
$ mvn --settings settings.xml clean install -DskipTests
```
To create Eclipse environment files:

```
$ mvn --settings settings.xml eclipse:eclipse -DskipTests
```

## Usage and Redis client compatibility

Carrot was tested with Java Jedis, should work with other clients as well. The client **cluster support is required** to use Carrot at a full potential (multiple data nodes per server). **TODO**.

## Benchmark summary

Accros all benchmark tests, Redis requires between 2 and 12 times more memory to keep the same data set. Carrot performance is within 5-10% of Redis, when Carrot compression is disabled and 40% less (only for updates and deletes) when compression is on. See performance benchmark tests in the WiKi **TODO**. We have not started code optimization yet, no profiling not hot spot detection. So, this is preproduction data and, definetely, performance will be improved by the time of the first official release.  

## Releases timeline

First release, which is 0.1 was done on Sept 3rd 2021. We plan to release new version every 1-2 months. Stay tuned.

## License

Similar to MongoDB, permissive, but not fully open source.

## Copyright

(C) 2021 - current. Carrot Incorporated, Vladimir Rodionov.




















