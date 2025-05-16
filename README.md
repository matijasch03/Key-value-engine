# Key-Value Engine

A persistent, memory-efficient key-value store supporting fast reads, writes, compaction, and advanced probabilistic data structures. Inspired by LSM-tree architecture, the system is designed for high-performance workloads with tunable configuration and optional data compression.  
This group project was done in the scope of course advanced algorithms and data structures.

---

## 🚀 Features

- PUT, GET, DELETE operations
- Persistent Write-Ahead Log (WAL)
- In-memory Memtable (HashMap / Skip List / B-Tree based)
- Disk-based SSTable with Index, Bloom Filter, Summary, Metadata
- LSM Tree with size-tiered and leveled compaction
- Optional data compression and encoding
- Probabilistic structures: Bloom Filter, Count-Min Sketch, HyperLogLog, SimHash
- Scan and iterator operations (prefix/range)
- LRU-based cache
- Token Bucket rate limiting
- Full external configuration support

---

## 🔧 Core Operations

### `PUT(key, value)`
Adds or updates a key-value pair. Value can be provided as a byte array or string (automatically encoded).

### `GET(key)`
Retrieves the value associated with the given key.

### `DELETE(key)`
Marks the record as deleted (tombstone flag).

---

## 📝 Write Path

1. **Write-Ahead Log (WAL)**: Each update is logged to a segment-based WAL.
2. **Memtable**: Confirmed updates are stored in-memory.
3. **Flush to SSTable**: When Memtable reaches max size, it's flushed to disk.
4. **Compaction**: Periodic merging and reorganization of SSTables across LSM tree levels.

---

## 🔍 Read Path

1. Check **Memtable** for the key.
2. If not found, check **Cache** (LRU-based).
3. Search SSTables level by level:
   - Use **Bloom Filter** to skip unlikely SSTables.
   - If Bloom Filter might match, consult **Summary**, then **Index**, then **Data**.
   - Validate data using **Merkle Tree** hashes.
4. Return value or null if not found.

---

## 🗃️ Data Components

### Write-Ahead Log (WAL)
- Segment-based logs with CRC integrity check
- Sequential on-disk storage
- Read one record at a time

### Memtable
- In-memory structure (HashMap, Skip List, or B-Tree)
- Supports N Memtables (1 write, N-1 read-only)
- Populated from WAL on startup

### SSTable Structure
- **Data**: Serialized key-value entries
- **Bloom Filter**: Fast key existence check
- **Index**: Maps keys to Data offsets
- **Summary**: Sparse index with range info
- **Metadata**: Merkle Tree for integrity verification

---

## 🌲 LSM Tree

- SSTables are organized into levels
- Supports **size-tiered** and **leveled** compaction
- Compact files within a level and promote as needed
- Fully tunable via configuration

---

## 🧠 Cache

- Least Recently Used (LRU) strategy
- Configurable cache size
- Automatically invalidated on writes

---

## ⚙️ Configuration

All tunable parameters are defined in an external configuration JSON file, including:

- Memtable type and size
- WAL segment size
- Cache size
- Compression settings
- Compaction algorithm and thresholds
- Bloom filter false-positive rate
- Rate limiting parameters

Defaults are provided if not specified.

---

## 🔐 Rate Limiting

Implemented using **Token Bucket** algorithm:

- User-defined refill interval and token count
- Token state is persisted
- Internal-only system record, hidden from external operations

---

## 🔢 Probabilistic Structures

### Bloom Filter
- Create, delete, add elements, check membership

### Count-Min Sketch
- Track event frequency with space-efficient hashing

### HyperLogLog
- Estimate cardinality of a large dataset

### SimHash
- Generate and compare fingerprint similarity via Hamming distance

All probabilistic structures are internally persisted and not exposed through standard key-value APIs.

---

## 🔎 Scan Operations

### `PREFIX_SCAN(prefix, pageNumber, pageSize)`
Returns all key-value pairs where keys start with the specified prefix, sorted ascendingly. Supports pagination.

### `RANGE_SCAN(range, pageNumber, pageSize)`
Returns all key-value pairs within a key range (inclusive), sorted ascendingly. Supports pagination.

---

## 🔁 Iterator Operations

### `PREFIX_ITERATE(prefix)`
Interactive iterator over keys with a given prefix.

### `RANGE_ITERATE(range)`
Interactive iterator over keys within a specified range.

Each iterator supports:
- `next` – fetch next record
- `stop` – terminate iteration

---

## 📂 Storage Format

- SSTables can be stored in separate or combined files
- Optional global dictionary-based key compression
- Efficient variable-length encoding for numeric fields
- Compression and encoding are configurable

---

## 📌 Example Use Case

- Write 100,000 records using 100 or 50,000 unique keys
- Benchmark with and without compression
- Analyze performance impact on read/write/compaction

---

## 📁 Scripts

Scripts are available to:
- Populate the store with large-scale test data
- Toggle compression for benchmarking

---

## 🧪 Integrity & Fault Tolerance

- CRC for WAL segments
- Merkle Tree verification on read
- Safe WAL recovery on system restart
- SSTable compatibility across versions/configs

---

## 🛠️ Technology

- Programming language: Golang

## 👤 Parts done mostly by [matijasch03] (https://github.com/matijasch03)
- Count Min Sketch (used to estimate frequencies of elements in a data stream using limited memory)
- Cache (the lowest and the fastest memory in which the key is first sought)
- Iterator operations
